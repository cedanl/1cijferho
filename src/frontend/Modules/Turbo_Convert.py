import os
import glob
import streamlit as st
import eencijferho.core.pipeline as pipeline
from eencijferho.config import OutputConfig
from typing import Any, Dict, List, Tuple
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
def get_matched_files() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Get all matched files from the validation logs"""
    logs_dir = os.path.join(get_metadata_dir(), "logs")
    if not os.path.exists(logs_dir):
        return [], []
    
    matching_log_files = glob.glob(os.path.join(logs_dir, "*file_matching_log_latest.json"))
    
    successful_pairs = []
    skipped_pairs = []
    
    if matching_log_files:
        try:
            import json
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

def clear_console_log() -> None:
    """Clear the console log in session state"""
    if 'convert_console_log' in st.session_state:
        del st.session_state['convert_console_log']

def get_output_files() -> List[Dict[str, Any]]:
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
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_names[i]}"

def start_conversion() -> None:
    """Callback function to start the conversion process"""
    st.session_state.start_turbo_convert = True

# -----------------------------------------------------------------------------
# Initialize/Clear Console Log on Page Load
# -----------------------------------------------------------------------------
# Clear console log when page is loaded
clear_console_log()

# Initialize conversion trigger
if 'start_turbo_convert' not in st.session_state:
    st.session_state.start_turbo_convert = False

# Set page initialization flag
st.session_state.page_initialized_convert = True

# -----------------------------------------------------------------------------
# Main Content
# -----------------------------------------------------------------------------
st.title("⚡ Turbo Convert")

# Introductie
st.write("""
**Stap 3: Data omzetten en verwerken**

We gebruiken de gevalideerde metadata om uw hoofd- en dec-bestanden om te zetten. Uw vaste-breedte data wordt zo omgezet naar veilige, gecomprimeerde en direct bruikbare bestanden.

Wat gebeurt er:
- Bestanden omzetten naar leesbare kolommen
- Resultaten controleren op fouten
- Bestanden comprimeren voor efficiënte opslag
- Gevoelige gegevens versleutelen in een aparte kopie
- Kolomnamen standaardiseren
- Alles opslaan in de uitvoermap + ballonnen 🎈 als het klaar is!

Gaat er iets mis? Kijk dan hieronder in het log voor details.
""")

# Get files and display status
successful_pairs, skipped_pairs = get_matched_files()
total_pairs = len(successful_pairs) + len(skipped_pairs)

if total_pairs == 0:
    st.error("🚨 **Geen bestanden klaar voor conversie.** Voer eerst de validatiestap uit en zorg dat alle bestanden succesvol zijn gekoppeld.")
else:
    st.success(f"✅ **{len(successful_pairs)} bestanden klaar voor conversie** ({len(skipped_pairs)} niet verwerkt wegens validatiefouten)")
    
    # Show file pairs in compact expander - closed by default
    if successful_pairs or skipped_pairs:
        with st.expander(f"📁 Bestanddetails ({len(successful_pairs)} klaar, {len(skipped_pairs)} niet verwerkt)", expanded=False):
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
    
    # Output options
    if successful_pairs:
        with st.expander("⚙️ Uitvoeropties", expanded=False):
            st.caption("Pas aan welke uitvoerbestanden worden aangemaakt. De standaardinstellingen zijn geschikt voor de meeste gebruikers.")
            col_a, col_b = st.columns(2)
            with col_a:
                opt_decoded = st.checkbox("Gedecodeerde bestanden (_decoded)", value=True, key="opt_decoded",
                    help="Koppelt codes aan omschrijvingen uit de Dec_*-opzoekbestanden (bijv. '01' → 'Nederland' dmv Dec_landcode). Vereist voor verrijkte bestanden.")
                opt_enriched = st.checkbox("Verrijkte bestanden (_enriched)", value=True, key="opt_enriched",
                    disabled=not st.session_state.get("opt_decoded", True),
                    help="Vervangt codes met waarden uit de bestandsbeschrijvingen. Alleen beschikbaar als 'Gedecodeerde bestanden' is aangevinkt.")
            with col_b:
                opt_parquet = st.checkbox("Parquet-bestanden (gecomprimeerd)", value=True, key="opt_parquet",
                    help="Slaat elk CSV-bestand ook op als compact Parquet-bestand voor snellere analyse.")
                opt_encrypt = st.checkbox("Gevoelige gegevens versleutelen", value=True, key="opt_encrypt",
                    help="Maakt een aparte versleutelde kopie van bestanden met gevoelige kolommen (bijv. BSN).")
                opt_snake_case = st.checkbox("Kolomnamen standaardiseren (snake_case)", value=True, key="opt_snake_case",
                    help="Converteert kolomnamen naar snake_case (bijv. 'Naam Student' → 'naam_student').")

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
                st.session_state.convert_console_log += "🔄 Conversie gestart...\n"
                update_console()
                progress_bar.progress(10)

                variants = []
                do_decoded = st.session_state.get("opt_decoded", True)
                if do_decoded:
                    variants.append("decoded")
                    if st.session_state.get("opt_enriched", True):
                        variants.append("enriched")
                output_cfg = OutputConfig(
                    variants=variants,
                    formats=["parquet"] if st.session_state.get("opt_parquet", True) else [],
                    encrypt=st.session_state.get("opt_encrypt", True),
                    column_casing="snake_case" if st.session_state.get("opt_snake_case", True) else "none",
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
                    with st.expander(f"📁 Geconverteerde bestanden ({len(output_files)} bestanden)", expanded=True):
                        st.write("**Aangemaakte bestanden:**")
                        
                        # Group files by type for better organization
                        csv_files = [f for f in output_files if f['name'].endswith('.csv') and not f['name'].endswith('_encrypted.csv') and not f['name'].endswith('_decoded.csv')]
                        decoded_files = [f for f in output_files if f['name'].endswith('_decoded.csv')]
                        parquet_files = [f for f in output_files if f['name'].endswith('.parquet')]
                        encrypted_files = [f for f in output_files if f['name'].endswith('_encrypted.csv')]

                        if csv_files:
                            st.write("**📄 CSV-bestanden (geconverteerd):**")
                            for file in csv_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")

                        if decoded_files:
                            st.write("**🔤 Gedecodeerde bestanden (hoofdbestanden met gedecodeerde kolommen):**")
                            for file in decoded_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")

                        if parquet_files:
                            st.write("**🗜️ Parquet-bestanden (gecomprimeerd):**")
                            for file in parquet_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")

                        if encrypted_files:
                            st.write("**🔒 Versleutelde bestanden (definitief):**")
                            for file in encrypted_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")
                
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
                with st.expander("🔍 Technische foutdetails"):
                    st.code(str(e))
    else:
        st.warning("⚠️ Alle gekoppelde bestanden hebben validatiefouten en kunnen niet worden geconverteerd. Ga terug naar de validatiestap om de fouten te bekijken en te corrigeren.")

# Console Log expander
with st.expander("📋 Console Log", expanded=True):
    st.caption("💡 Let op: Sommige technische meldingen in het log zijn onschuldig en kunnen worden genegeerd — ze horen bij de normale werking van de conversiebibliotheek.")
    if 'convert_console_log' in st.session_state and st.session_state.convert_console_log:
        st.code(st.session_state.convert_console_log, language=None)
    else:
        st.info("Nog geen conversieproces gestart. Klik op 'Start Turbo Convert' om te beginnen.")



# Show existing converted files (if any)
output_files = get_output_files()
output_dir = get_output_dir()
if output_files:
    with st.expander(f"📁 Geconverteerde bestanden ({len(output_files)} bestanden)", expanded=False):
        st.write("**Bestanden in de uitvoermap:**")
        
        # Group files by type for better organization
        csv_files = [f for f in output_files if f['name'].endswith('.csv') and not f['name'].endswith('_encrypted.csv')]
        parquet_files = [f for f in output_files if f['name'].endswith('.parquet')]
        encrypted_files = [f for f in output_files if f['name'].endswith('_encrypted.csv')]
        
        if csv_files:
            st.write("**📄 CSV-bestanden (geconverteerd):**")
            for file in csv_files:
                st.write(f"• `{file['name']}` ({file['size_formatted']})")
        
        if parquet_files:
            st.write("**🗜️ Parquet-bestanden (gecomprimeerd):**")
            for file in parquet_files:
                st.write(f"• `{file['name']}` ({file['size_formatted']})")
        
        if encrypted_files:
            st.write("**🔒 Versleutelde bestanden (definitief):**")
            for file in encrypted_files:
                st.write(f"• `{file['name']}` ({file['size_formatted']})")
        
else:
    st.info("📁 Nog geen geconverteerde bestanden gevonden. Voer eerst de conversie uit.")

# Warning about existing files
if os.path.exists(output_dir) and os.listdir(output_dir):
    st.warning("⚠️ Er zijn al eerder geconverteerde bestanden aanwezig. Een nieuwe conversie overschrijft deze.")
