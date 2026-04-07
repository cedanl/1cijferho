import os
import glob
import streamlit as st
from eencijferho.core.decoder_info import (
    get_available_decode_columns,
    get_available_enrich_variables,
    get_decode_column_info,
    get_enrich_variable_info,
)
from config import get_metadata_dir

# -----------------------------------------------------------------------------
# Load column metadata
# -----------------------------------------------------------------------------
metadata_dir = get_metadata_dir()
json_dir = os.path.join(metadata_dir, "json")
dec_json_matches = glob.glob(os.path.join(json_dir, "Bestandsbeschrijving_Dec-bestanden*.json"))
variable_metadata_path = os.path.join(json_dir, "variable_metadata.json")

dec_json = dec_json_matches[0] if dec_json_matches else ""
available_decode = get_available_decode_columns(dec_json)
available_enrich = get_available_enrich_variables(variable_metadata_path)
decode_info = get_decode_column_info(dec_json)
enrich_info = get_enrich_variable_info(variable_metadata_path)

show_decode = st.session_state.get("opt_decoded", True) and not (
    not st.session_state.get("opt_convert_ev", True)
    and not st.session_state.get("opt_convert_vakhavw", True)
)
show_enrich = show_decode and st.session_state.get("opt_enriched", True)

# -----------------------------------------------------------------------------
# Page
# -----------------------------------------------------------------------------
if st.button("← Klaar — terug naar conversie", type="secondary"):
    st.switch_page("frontend/Modules/Turbo_Convert.py")

st.title("Kolomselectie")
st.markdown("""
<div class="page-intro">
    Kies welke kolommen worden gedecodeerd en verrijkt. Wijzigingen worden automatisch opgeslagen.
</div>
""", unsafe_allow_html=True)

if not show_decode and not show_enrich:
    st.info("Kolomselectie is niet actief. Schakel 'Gedecodeerde variant' in op de conversiepagina om kolommen te selecteren.")
    st.stop()

# -----------------------------------------------------------------------------
# Sectie 1: Decoderen
# -----------------------------------------------------------------------------
if show_decode:
    n_selected = sum(1 for col in available_decode if st.session_state.get(f"decode_col_{col}", True))

    st.subheader(f"Decoderen — {n_selected} van {len(available_decode)} geselecteerd")
    st.caption(
        "Per geselecteerde kolom wordt een extra omschrijvingskolom toegevoegd vanuit de Dec_\\*-bestanden. "
        "Bijv. `landcode` → `landcode_oms`."
    )

    if not available_decode:
        st.warning("Dec-metadata niet beschikbaar. Voer eerst de extractiestap uit.")
    else:
        has_corrupt = any("\ufffd" in col or "ï¿½" in col for col in available_decode)
        if has_corrupt:
            st.warning("⚠️ Kolomnamen bevatten onleesbare tekens door een extractiefout. Voer de extractiestap opnieuw uit.")

        btn1, btn2, _ = st.columns([1, 1, 5])
        with btn1:
            if st.button("Alles aan", key="decode_select_all"):
                for col in available_decode:
                    st.session_state[f"decode_col_{col}"] = True
                st.rerun()
        with btn2:
            if st.button("Alles uit", key="decode_deselect_all"):
                for col in available_decode:
                    st.session_state[f"decode_col_{col}"] = False
                st.rerun()

        st.write("")
        # Two-column checkbox layout for readability
        mid = (len(available_decode) + 1) // 2
        col_a, col_b = st.columns(2)
        for i, col in enumerate(available_decode):
            labels = decode_info.get(col, [])
            col_help = "Toegevoegde kolommen:\n" + "\n".join(labels) if labels else None
            target_col = col_a if i < mid else col_b
            with target_col:
                st.checkbox(col, value=True, key=f"decode_col_{col}", help=col_help)

# -----------------------------------------------------------------------------
# Sectie 2: Verrijken
# -----------------------------------------------------------------------------
if show_enrich:
    st.divider()
    n_selected_e = sum(1 for var in available_enrich if st.session_state.get(f"enrich_var_{var}", True))

    st.subheader(f"Verrijken — {n_selected_e} van {len(available_enrich)} geselecteerd")
    st.caption(
        "Per geselecteerde variabele worden codes vervangen door leesbare labels uit de bestandsbeschrijving. "
        "Bijv. `M` → `man`."
    )

    if not available_enrich:
        st.warning("Variabele-metadata niet beschikbaar. Voer eerst de extractiestap uit.")
    else:
        btn3, btn4, _ = st.columns([1, 1, 5])
        with btn3:
            if st.button("Alles aan", key="enrich_select_all"):
                for var in available_enrich:
                    st.session_state[f"enrich_var_{var}"] = True
                st.rerun()
        with btn4:
            if st.button("Alles uit", key="enrich_deselect_all"):
                for var in available_enrich:
                    st.session_state[f"enrich_var_{var}"] = False
                st.rerun()

        st.write("")
        mid_e = (len(available_enrich) + 1) // 2
        col_c, col_d = st.columns(2)
        for i, var in enumerate(available_enrich):
            sample = enrich_info.get(var, {})
            var_help = "\n".join(f"{k} → {v}" for k, v in sample.items()) if sample else None
            target_col = col_c if i < mid_e else col_d
            with target_col:
                st.checkbox(var, value=True, key=f"enrich_var_{var}", help=var_help)

# -----------------------------------------------------------------------------
# Bottom back button
# -----------------------------------------------------------------------------
st.divider()
st.caption("Selecties worden automatisch opgeslagen.")
if st.button("← Klaar — terug naar conversie", type="primary", use_container_width=True, key="back_bottom"):
    st.switch_page("frontend/Modules/Turbo_Convert.py")
