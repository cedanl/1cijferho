import math
import os
import glob as _glob
import json
import datetime
import streamlit as st
import eencijferho.core.pipeline as pipeline
from eencijferho.config import OutputConfig
from eencijferho.core.decoder_info import (
    get_available_decode_columns,
    get_available_enrich_variables,
    get_decode_column_info,
    get_enrich_variable_info,
)
from typing import Any
from config import get_input_dir, get_output_dir, get_metadata_dir

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
#st.set_page_config(
#    page_title="⚡ Turbo Convert",
#    layout="centered",
#    initial_sidebar_state="expanded"
#)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def get_matched_files() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Get all matched files from the validation logs"""
    logs_dir = os.path.join(get_metadata_dir(), "logs")
    if not os.path.exists(logs_dir):
        return [], []
    
    matching_log_files = _glob.glob(os.path.join(logs_dir, "*file_matching_log_latest.json"))
    
    successful_pairs = []
    skipped_pairs = []
    
    if matching_log_files:
        try:
            with open(matching_log_files[0], 'r') as f:
                matching_data = json.load(f)
            
            for file_info in matching_data.get('processed_files', []):
                if file_info.get('status') == 'matched':
                    for match in file_info.get('matches', []):
                        pair_info = {
                            'input_file': file_info['input_file'],
                            'rows': file_info.get('row_count', 0),
                            'metadata_file': match['validation_file'].replace('Bestandsbeschrijving_', '').split('_')[0]
                        }
                        
                        if match.get('validation_status') == 'success':
                            successful_pairs.append(pair_info)
                        else:
                            skipped_pairs.append(pair_info)
        except (json.JSONDecodeError, FileNotFoundError):
            pass
    
    return successful_pairs, skipped_pairs


def get_output_files() -> list[dict[str, Any]]:
    """Get all files from the output directory"""
    output_dir = get_output_dir()
    if not os.path.exists(output_dir):
        return []
    
    files = []
    for file in os.listdir(output_dir):
        if os.path.isfile(os.path.join(output_dir, file)):
            file_path = os.path.join(output_dir, file)
            file_size = os.path.getsize(file_path)
            files.append({
                'name': file,
                'size': file_size,
                'size_formatted': format_file_size(file_size)
            })
    
    # Sort files by name
    files.sort(key=lambda x: x['name'])
    return files

def format_file_size(size_bytes: int) -> str:
    """Format file size in human readable format"""
    if size_bytes == 0:
        return "0 B"
    size_names = ["B", "KB", "MB", "GB"]
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_names[i]}"

def start_conversion() -> None:
    """Callback function to start the conversion process"""
    st.session_state.start_turbo_convert = True


# Schema version for run_config.json — bump when structure changes
_RUN_CONFIG_SCHEMA_VERSION = 1


def write_run_config(output_dir: str, output_cfg: OutputConfig, opt_decode_columns: list | None, opt_enrich_variables: list | None) -> str:
    """Write run_config.json to output_dir and return the file path."""
    os.makedirs(output_dir, exist_ok=True)
    config_path = os.path.join(output_dir, "run_config.json")
    run_config = {
        "meta": {
            "_schema": _RUN_CONFIG_SCHEMA_VERSION,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        },
        "settings": {
            "opt_convert_ev": output_cfg.convert_ev,
            "opt_convert_vakhavw": output_cfg.convert_vakhavw,
            "opt_decoded": "decoded" in output_cfg.variants,
            "opt_enriched": "enriched" in output_cfg.variants,
            "opt_parquet": "parquet" in output_cfg.formats,
            "opt_encrypt": output_cfg.encrypt,
            "opt_snake_case": output_cfg.column_casing == "snake_case",
            "opt_decode_columns": opt_decode_columns,
            "opt_enrich_variables": opt_enrich_variables,
        },
    }
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(run_config, f, indent=2, ensure_ascii=False)
    return config_path

# Initialize conversion trigger
if 'start_turbo_convert' not in st.session_state:
    st.session_state.start_turbo_convert = False

# -----------------------------------------------------------------------------
# Main Content
# -----------------------------------------------------------------------------
st.markdown('<span class="step-badge">Stap 3 van 3</span>', unsafe_allow_html=True)
st.title("Turbo Conversie")
st.markdown("""
<div class="page-intro">
    Zet uw vaste-breedte bestanden om naar nette CSV- en Parquet-bestanden met leesbare kolomnamen.
</div>
""", unsafe_allow_html=True)

with st.expander("Wat doet deze stap precies?"):
    st.markdown("""
- Vaste-breedte data splitsen naar kolommen op basis van de gevalideerde metadata
- Resultaten controleren op fouten
- Parquet-bestanden aanmaken (60–80% kleiner)
- Gevoelige kolommen versleutelen in een aparte kopie
- Kolomnamen omzetten naar snake_case
""")


# Get files and display status
successful_pairs, skipped_pairs = get_matched_files()
total_pairs = len(successful_pairs) + len(skipped_pairs)

if total_pairs == 0:
    st.error("**Geen bestanden klaar voor conversie.** Voer eerst stap 2 uit en zorg dat alle bestanden succesvol zijn gekoppeld.")
    if st.button("← Terug naar stap 2", type="secondary"):
        st.switch_page("frontend/Modules/Validate_Metadata.py")
else:
    st.success(f"✅ **{len(successful_pairs)} bestanden klaar voor conversie** ({len(skipped_pairs)} niet verwerkt wegens validatiefouten)")
    
    # Show file pairs in compact expander - closed by default
    if successful_pairs or skipped_pairs:
        with st.expander(f"Bestanddetails ({len(successful_pairs)} klaar, {len(skipped_pairs)} niet verwerkt)", expanded=False):
            tab1, tab2 = st.tabs([f"✅ Klaar ({len(successful_pairs)})", f"❌ Niet verwerkt ({len(skipped_pairs)})"])
            
            with tab1:
                if successful_pairs:
                    st.write("**Bestanden klaar voor conversie:**")
                    for pair in successful_pairs:
                        st.write(f"• `{pair['input_file']}` ({pair['rows']:,} rijen)")
                else:
                    st.info("Geen bestanden klaar voor conversie.")
            
            with tab2:
                if skipped_pairs:
                    st.write("**Bestanden met validatiefouten — ga terug naar 🛡️ Metadata valideren om dit op te lossen:**")
                    for pair in skipped_pairs:
                        st.write(f"• `{pair['input_file']}` ({pair['rows']:,} rijen)")
                else:
                    st.info("Geen validatiefouten.")
    
    # -------------------------------------------------------------------------
    # Output options — two-layer structure
    # Layer 1: 7 base checkboxes in 3 groups (compact, always scannable)
    # Layer 2: column selectors in sub-expanders (only when relevant)
    # -------------------------------------------------------------------------

    # Pre-load column metadata (needed for counts in expander labels)
    metadata_dir = get_metadata_dir()
    json_dir = os.path.join(metadata_dir, "json")
    dec_json_matches = _glob.glob(os.path.join(json_dir, "Bestandsbeschrijving_Dec-bestanden*.json"))
    variable_metadata_path = os.path.join(json_dir, "variable_metadata.json")
    dec_json = dec_json_matches[0] if dec_json_matches else ""
    available_decode = get_available_decode_columns(dec_json)
    available_enrich = get_available_enrich_variables(variable_metadata_path)
    decode_info = get_decode_column_info(dec_json)
    enrich_info = get_enrich_variable_info(variable_metadata_path)

    opt_decode_columns = None
    opt_enrich_variables = None

    if successful_pairs:
        st.divider()
        st.markdown("#### Uitvoeropties")
        st.caption("Standaardinstellingen zijn geschikt voor de meeste gebruikers.")

        # --- Groep 1: Welke bestanden ---
        st.markdown("**Welke bestanden omzetten?**")
        col_ev, col_vak = st.columns(2)
        with col_ev:
            st.checkbox(
                "EV-bestanden", value=True, key="opt_convert_ev",
                help="Zet de EV-hoofdbestanden om van vaste-breedte naar CSV.")
        with col_vak:
            st.checkbox(
                "VAKHAVW-bestanden", value=True, key="opt_convert_vakhavw",
                help="Zet de VAKHAVW-hoofdbestanden om van vaste-breedte naar CSV.")
        no_main_files = (
            not st.session_state.get("opt_convert_ev", True)
            and not st.session_state.get("opt_convert_vakhavw", True)
        )

        st.divider()

        # --- Groep 2: Uitvoervarianten ---
        st.markdown("**Uitvoervarianten**")
        col_dec, col_enr = st.columns(2)
        with col_dec:
            st.checkbox(
                "Gedecodeerde variant",
                value=True, key="opt_decoded",
                disabled=no_main_files,
                help="Voegt omschrijvingen toe vanuit Dec_*-bestanden (bijv. `landcode` → `landcode_oms`).")
            st.caption("`_decoded`")
        with col_enr:
            _decoded_on = st.session_state.get("opt_decoded", True)
            st.checkbox(
                "Verrijkte variant",
                value=True, key="opt_enriched",
                disabled=no_main_files or not _decoded_on,
                help="Vervangt codes door leesbare labels (bijv. `M` → `man`). Vereist gedecodeerde variant.")
            if not _decoded_on or no_main_files:
                st.caption("`_enriched` — vereist decoded")
            else:
                st.caption("`_enriched`")

        # --- Kolomselectie: direct onder uitvoervarianten ---
        show_decode = not no_main_files and st.session_state.get("opt_decoded", True)
        show_enrich = show_decode and st.session_state.get("opt_enriched", True)

        if show_decode:
            if available_decode:
                n_decode_selected = sum(
                    1 for col in available_decode if st.session_state.get(f"decode_col_{col}", True)
                )
                non_default_parts = []
                if n_decode_selected < len(available_decode):
                    non_default_parts.append(f"{n_decode_selected}/{len(available_decode)} decode")
                if show_enrich and available_enrich:
                    n_enrich_selected = sum(
                        1 for var in available_enrich if st.session_state.get(f"enrich_var_{var}", True)
                    )
                    if n_enrich_selected < len(available_enrich):
                        non_default_parts.append(f"{n_enrich_selected}/{len(available_enrich)} verrijken")
                btn_label = "Kolomselectie instellen →"
                if non_default_parts:
                    btn_label += f" ({' · '.join(non_default_parts)})"
                if st.button(
                    btn_label,
                    key="configure_columns_btn",
                    help="Kies welke kolommen worden gedecodeerd en verrijkt.",
                ):
                    st.switch_page("frontend/Modules/Configure_Columns.py")
            else:
                st.caption("Kolomselectie beschikbaar nadat metadata is geëxtraheerd.")

        st.divider()

        # --- Groep 3: Bestandsopties ---
        st.markdown("**Bestandsopties**")
        col_p, col_e, col_s = st.columns(3)
        with col_p:
            st.checkbox(
                "Parquet", value=True, key="opt_parquet",
                help="Slaat elk CSV-bestand ook op als Parquet (60–80% kleiner, sneller te laden).")
        with col_e:
            st.checkbox(
                "Versleutelen", value=True, key="opt_encrypt",
                help="Maakt een aparte versleutelde kopie voor bestanden met gevoelige kolommen (BSN).")
        with col_s:
            st.checkbox(
                "snake_case", value=True, key="opt_snake_case",
                help="Converteert kolomnamen naar snake_case (bijv. `Naam Student` → `naam_student`).")

        # Compute opt_decode_columns / opt_enrich_variables from session state
        if show_decode and available_decode:
            selected_decode = [col for col in available_decode if st.session_state.get(f"decode_col_{col}", True)]
            if len(selected_decode) < len(available_decode):
                opt_decode_columns = selected_decode
        if show_enrich and available_enrich:
            selected_enrich = [var for var in available_enrich if st.session_state.get(f"enrich_var_{var}", True)]
            if len(selected_enrich) < len(available_enrich):
                opt_enrich_variables = selected_enrich

    # Warning about existing converted files — shown before the action button
    if os.path.exists(get_output_dir()) and os.listdir(get_output_dir()):
        st.warning("⚠️ Er zijn al eerder geconverteerde bestanden aanwezig. Een nieuwe conversie overschrijft deze.")

    # Side-by-side buttons with equal width
    if successful_pairs:
        col1, col2 = st.columns(2)

        with col1:
            # Use callback-based button (recommended Streamlit pattern)
            st.button("⚡ Start Turbo Convert ⚡",
                     type="primary",
                     use_container_width=True,
                     key="turbo_convert_btn",
                     on_click=start_conversion)

        with col2:
            output_files = get_output_files()
            next_page_clicked = st.button(
                "➡️ Output valideren (optioneel)",
                type="secondary",
                disabled=len(output_files) == 0,
                use_container_width=True,
                key="next_step_btn",
            )

        if next_page_clicked:
            st.switch_page("frontend/Modules/Validate_Output.py")

        # Handle conversion logic using session state flag
        if st.session_state.start_turbo_convert:
            # Reset the flag immediately
            st.session_state.start_turbo_convert = False
            
            # Reset console log at the start of each conversion
            st.session_state.convert_console_log = ""
            
            # Create progress bar and status containers
            progress_bar = st.progress(0)
            status_text = st.empty()
            console_container = st.empty()
            
            def update_console() -> None:
                """Update the console display"""
                with console_container.container():
                    if st.session_state.convert_console_log:
                        st.code(st.session_state.convert_console_log, language=None)
                    else:
                        st.info("Conversie gestart...")
            
            try:
                st.session_state.convert_console_log += "Conversie gestart...\n"
                update_console()
                progress_bar.progress(10)

                do_convert_ev = st.session_state.get("opt_convert_ev", True)
                do_convert_vakhavw = st.session_state.get("opt_convert_vakhavw", True)
                any_main = do_convert_ev or do_convert_vakhavw
                variants = []
                if any_main and st.session_state.get("opt_decoded", True):
                    variants.append("decoded")
                    if st.session_state.get("opt_enriched", True):
                        variants.append("enriched")
                output_cfg = OutputConfig(
                    variants=variants,
                    formats=["parquet"] if st.session_state.get("opt_parquet", True) else [],
                    encrypt=st.session_state.get("opt_encrypt", True),
                    column_casing="snake_case" if st.session_state.get("opt_snake_case", True) else "none",
                    convert_ev=do_convert_ev,
                    convert_vakhavw=do_convert_vakhavw,
                    decode_columns=opt_decode_columns if opt_decode_columns and available_decode and len(opt_decode_columns) < len(available_decode) else None,
                    enrich_variables=opt_enrich_variables if opt_enrich_variables and available_enrich and len(opt_enrich_variables) < len(available_enrich) else None,
                )

                log, output_files = pipeline.run_turbo_convert_pipeline(
                    input_dir=get_input_dir(),
                    output_dir=get_output_dir(),
                    metadata_dir=get_metadata_dir(),
                    dec_metadata_json=st.session_state.get("dec_metadata_json"),
                    progress_callback=progress_bar.progress,
                    status_callback=status_text.text,
                    output_config=output_cfg,
                )
                st.session_state.convert_console_log += log
                update_console()
                progress_bar.progress(100)


                status_text.text("✅ Verwerking succesvol voltooid!")

                output_dir = get_output_dir()
                st.success(f"✅ **Verwerking voltooid!** Bestanden geconverteerd, gevalideerd, gecomprimeerd en versleuteld. Resultaten staan in `{get_output_dir()}/`.")

                # Show converted files
                output_files = get_output_files()
                if output_files:
                    with st.expander(f"Geconverteerde bestanden ({len(output_files)} bestanden)", expanded=True):
                        st.write("**Aangemaakte bestanden:**")

                        # Group files by type for better organization
                        csv_files = [f for f in output_files if f['name'].endswith('.csv') and not f['name'].endswith('_encrypted.csv') and not f['name'].endswith('_decoded.csv') and not f['name'].endswith('_enriched.csv')]
                        decoded_files = [f for f in output_files if f['name'].endswith('_decoded.csv') or f['name'].endswith('_enriched.csv')]
                        parquet_files = [f for f in output_files if f['name'].endswith('.parquet')]
                        encrypted_files = [f for f in output_files if f['name'].endswith('_encrypted.csv')]

                        if csv_files:
                            st.write("**CSV-bestanden:**")
                            for file in csv_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")

                        if decoded_files:
                            st.write("**Gedecodeerde bestanden:**")
                            for file in decoded_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")

                        if parquet_files:
                            st.write("**Parquet-bestanden (gecomprimeerd):**")
                            for file in parquet_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")

                        if encrypted_files:
                            st.write("**Versleutelde bestanden:**")
                            for file in encrypted_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")
                
                # Write run_config.json for reproducibility and future presets
                try:
                    config_path = write_run_config(
                        output_dir=output_dir,
                        output_cfg=output_cfg,
                        opt_decode_columns=opt_decode_columns,
                        opt_enrich_variables=opt_enrich_variables,
                    )
                    st.session_state.convert_console_log += f"run_config.json opgeslagen: {config_path}\n"
                    update_console()
                except Exception as cfg_err:
                    st.session_state.convert_console_log += f"⚠️ run_config.json kon niet worden opgeslagen: {cfg_err}\n"
                    update_console()

                # Celebrate with balloons!
                st.balloons()
                
                progress_bar.empty()
                status_text.empty()
                console_container.empty()
                
                # Rerun to update any button states
                st.rerun()
                
            except Exception as e:
                st.session_state.convert_console_log += f"❌ Fout: {str(e)}\n"
                update_console()
                progress_bar.progress(0)
                status_text.text("❌ Verwerking mislukt")
                st.error(
                    "❌ **Verwerking mislukt.** Controleer of alle benodigde bestanden aanwezig zijn en probeer het opnieuw. Bekijk het console log hieronder voor meer details."
                )
                with st.expander("Technische foutdetails"):
                    st.code(str(e))
    else:
        st.warning("⚠️ Alle gekoppelde bestanden hebben validatiefouten en kunnen niet worden geconverteerd. Ga terug naar de validatiestap om de fouten te bekijken en te corrigeren.")

# Console Log expander — open only when there is log content
_convert_log_has_content = 'convert_console_log' in st.session_state and bool(st.session_state.convert_console_log)
with st.expander("Console Log", expanded=_convert_log_has_content):
    st.caption("Sommige technische meldingen zijn onschuldig en horen bij de normale werking van de conversiebibliotheek.")
    if _convert_log_has_content:
        st.code(st.session_state.convert_console_log, language=None)
    else:
        st.info("Nog geen conversieproces gestart. Klik op 'Start Turbo Convert' om te beginnen.")



# Show existing converted files (if any)
output_files = get_output_files()
if output_files:
    with st.expander(f"Geconverteerde bestanden ({len(output_files)} bestanden)", expanded=False):
        st.write("**Bestanden in de uitvoermap:**")

        # Group files by type for better organization
        csv_files = [f for f in output_files if f['name'].endswith('.csv') and not f['name'].endswith('_encrypted.csv') and not f['name'].endswith('_decoded.csv') and not f['name'].endswith('_enriched.csv')]
        decoded_files = [f for f in output_files if f['name'].endswith('_decoded.csv') or f['name'].endswith('_enriched.csv')]
        parquet_files = [f for f in output_files if f['name'].endswith('.parquet')]
        encrypted_files = [f for f in output_files if f['name'].endswith('_encrypted.csv')]

        if csv_files:
            st.write("**CSV-bestanden:**")
            for file in csv_files:
                st.write(f"• `{file['name']}` ({file['size_formatted']})")

        if decoded_files:
            st.write("**Gedecodeerde bestanden:**")
            for file in decoded_files:
                st.write(f"• `{file['name']}` ({file['size_formatted']})")

        if parquet_files:
            st.write("**Parquet-bestanden (gecomprimeerd):**")
            for file in parquet_files:
                st.write(f"• `{file['name']}` ({file['size_formatted']})")

        if encrypted_files:
            st.write("**Versleutelde bestanden:**")
            for file in encrypted_files:
                st.write(f"• `{file['name']}` ({file['size_formatted']})")
        

