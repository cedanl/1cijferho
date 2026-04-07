import streamlit as st
import os
import glob
import json
import eencijferho.utils.extractor_validation as ex_val
import eencijferho.utils.converter_match as cm
import io
import contextlib
from typing import Any
from config import get_input_dir, get_metadata_dir


# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def get_metadata_files() -> list[str]:
    metadata_dir = get_metadata_dir()
    if not os.path.exists(metadata_dir):
        return []
    return sorted([os.path.basename(f) for f in glob.glob(os.path.join(metadata_dir, "*.xlsx"))])


def load_validation_logs() -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
    """Load the latest validation logs and return failure information"""
    logs_dir = os.path.join(get_metadata_dir(), "logs")
    if not os.path.exists(logs_dir):
        return None, None

    xlsx_log_files = glob.glob(os.path.join(logs_dir, "*xlsx_validation_log_latest.json"))
    matching_log_files = glob.glob(os.path.join(logs_dir, "*file_matching_log_latest.json"))

    xlsx_failures = []
    matching_failures = []

    if xlsx_log_files:
        try:
            with open(xlsx_log_files[0], 'r') as f:
                xlsx_data = json.load(f)
            for file_info in xlsx_data.get('processed_files', []):
                if file_info.get('status') == 'failed':
                    issues = file_info.get('issues', {})
                    details = []
                    if issues.get('position_errors'):
                        details.append(f"{len(issues['position_errors'])} positiefout(en)")
                    if issues.get('length_mismatch'):
                        details.append("lengte komt niet overeen")
                    if issues.get('duplicates'):
                        details.append(f"{len(issues['duplicates'])} duplicaat/duplicaten")
                    xlsx_failures.append({
                        'file': file_info['file'],
                        'details': ', '.join(details) if details else 'validatie mislukt'
                    })
        except (json.JSONDecodeError, FileNotFoundError) as e:
            st.error(f"Fout bij laden van validatielog: {e}")

    if matching_log_files:
        try:
            with open(matching_log_files[0], 'r') as f:
                matching_data = json.load(f)
            for file_info in matching_data.get('processed_files', []):
                if file_info.get('status') == 'unmatched':
                    matching_failures.append({
                        'type': 'Niet gekoppeld invoerbestand',
                        'file': file_info['input_file'],
                        'details': f"Geen bijpassende metadata gevonden ({file_info.get('row_count', 0)} rijen)"
                    })
                elif file_info.get('status') == 'matched':
                    for match in file_info.get('matches', []):
                        if match.get('validation_status') == 'failed':
                            matching_failures.append({
                                'type': 'Validatie mislukt',
                                'file': f"{file_info['input_file']} → {match['validation_file']}",
                                'details': 'Metadata-validatie mislukt voor dit bestandspaar'
                            })
            for unmatched in matching_data.get('unmatched_validation', []):
                matching_failures.append({
                    'type': 'Niet gekoppeld metadata-bestand',
                    'file': unmatched['validation_file'],
                    'details': 'Geen bijpassend invoerbestand gevonden'
                })
        except (json.JSONDecodeError, FileNotFoundError) as e:
            st.error(f"Fout bij laden van koppelingslog: {e}")

    return xlsx_failures, matching_failures


def clear_console_log() -> None:
    if 'validate_console_log' in st.session_state:
        del st.session_state['validate_console_log']


if 'page_initialized' not in st.session_state:
    clear_console_log()
    st.session_state.page_initialized = True


# -----------------------------------------------------------------------------
# Page
# -----------------------------------------------------------------------------
st.markdown('<span class="step-badge">Stap 2 van 3</span>', unsafe_allow_html=True)
st.title("Metadata valideren")
st.markdown("""
<div class="page-intro">
    Controleer de geëxtraheerde Excel-bestanden op fouten en koppel ze aan de bijbehorende hoofd- en dec-bestanden.
</div>
""", unsafe_allow_html=True)

with st.expander("Wat doet deze stap precies?"):
    st.markdown("""
- Excel-metadata valideren op positiefouten, lengteproblemen en duplicaten
- Excel-bestanden koppelen aan hoofd- en dec-bestanden
- Validatierapporten opslaan voor de conversiestap

Als validatie of koppeling mislukt, kunt u de Excel-bestanden aanpassen en de validatie opnieuw uitvoeren.
""")

# -----------------------------------------------------------------------------
# Status + Actions
# -----------------------------------------------------------------------------
metadata_files = get_metadata_files()

if not metadata_files:
    st.error("**Geen metadata-bestanden gevonden.** Voer eerst stap 1 uit.")
    if st.button("← Terug naar stap 1", type="secondary"):
        st.switch_page("frontend/Modules/Extract_Metadata.py")
else:
    st.success(f"✅ **{len(metadata_files)} metadata-bestand(en) gevonden**")

    # Warn before action button
    _logs_dir_pre = os.path.join(get_metadata_dir(), "logs")
    if os.path.exists(_logs_dir_pre) and os.listdir(_logs_dir_pre):
        st.warning("⚠️ Er zijn al eerder validatieresultaten aanwezig. Een nieuwe validatie overschrijft deze.")

    logs_dir = os.path.join(get_metadata_dir(), "logs")
    validation_complete = False
    if os.path.exists(logs_dir):
        xlsx_log_files = glob.glob(os.path.join(logs_dir, "*xlsx_validation_log_latest.json"))
        matching_log_files = glob.glob(os.path.join(logs_dir, "*file_matching_log_latest.json"))
        validation_complete = len(xlsx_log_files) > 0 and len(matching_log_files) > 0

    col1, col2 = st.columns(2)
    with col1:
        validate_clicked = st.button("Validatie starten", type="primary", use_container_width=True, key="validate_btn")
    with col2:
        next_page_clicked = st.button(
            "Ga door naar stap 3 →",
            type="secondary",
            disabled=not validation_complete,
            use_container_width=True,
            key="next_step_btn",
        )

    if next_page_clicked:
        st.switch_page("frontend/Modules/Turbo_Convert.py")

    # Show existing validation issues (from previous run)
    xlsx_failures, matching_failures = load_validation_logs()

    if xlsx_failures:
        with st.expander(f"{len(xlsx_failures)} validatiefout(en) gevonden — klik om te bekijken"):
            for failure in xlsx_failures:
                st.markdown(f"**`{failure['file']}`** — {failure['details']}")

    if matching_failures:
        unmatched_count = len([f for f in matching_failures if 'gekoppeld' in f['type']])
        with st.expander(f"{unmatched_count} bestand(en) konden niet worden gekoppeld — klik om te bekijken"):
            for failure in matching_failures:
                st.markdown(f"**{failure['type']}:** `{failure['file']}` — {failure['details']}")

    # Validation Logic
    if validate_clicked:
        clear_console_log()
        st.session_state.validate_console_log = ""

        with st.spinner("Bezig met valideren..."):
            try:
                metadata_dir = get_metadata_dir()
                logs_dir_val = os.path.join(metadata_dir, "logs")
                validation_log_path = os.path.join(logs_dir_val, "(3)_xlsx_validation_log_latest.json")

                st.session_state.validate_console_log += "Validatie gestart...\n"
                st.session_state.validate_console_log += "Metadata-bestanden valideren...\n"

                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    ex_val.validate_metadata_folder(metadata_folder=metadata_dir)
                st.session_state.validate_console_log += captured_output.getvalue()
                st.session_state.validate_console_log += "✅ Metadata-validatie voltooid\n"

                st.session_state.validate_console_log += "Bestanden koppelen...\n"
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    cm.match_files(get_input_dir(), log_path=validation_log_path)
                st.session_state.validate_console_log += captured_output.getvalue()
                st.session_state.validate_console_log += "✅ Bestanden gekoppeld\n"
                st.session_state.validate_console_log += "✅ Validatie succesvol afgerond\n"

                st.success("✅ **Validatie voltooid.** Ga door naar stap 3 om de conversie te starten.")
                st.rerun()

            except Exception as e:
                st.session_state.validate_console_log += f"❌ Fout: {str(e)}\n"
                st.error("❌ **Validatie mislukt.** Bekijk het console log hieronder voor details.")
                with st.expander("Technische foutdetails"):
                    st.code(str(e))

    # Console Log
    _log_has_content = 'validate_console_log' in st.session_state and bool(st.session_state.validate_console_log)
    with st.expander("Console Log", expanded=_log_has_content):
        if _log_has_content:
            st.code(st.session_state.validate_console_log, language=None)
        else:
            st.caption("Nog geen validatieproces gestart.")
