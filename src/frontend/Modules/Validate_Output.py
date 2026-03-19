import os
import json
import datetime
import streamlit as st
import eencijferho.utils.value_validation as vv
import eencijferho.utils.dec_validation as dv
from config import get_input_dir, get_output_dir, get_metadata_dir

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _load_val_log():
    log_path = os.path.join(get_metadata_dir(), "logs", "(5b)_value_validation_log_latest.json")
    if not os.path.exists(log_path):
        return None
    with open(log_path, encoding="utf-8") as f:
        return json.load(f)


def _load_dec_log():
    log_path = os.path.join(get_metadata_dir(), "logs", "(5c)_dec_validation_log_latest.json")
    if not os.path.exists(log_path):
        return None
    with open(log_path, encoding="utf-8") as f:
        return json.load(f)


def _run_value_validation():
    output_dir = get_output_dir()
    logs_dir = os.path.join(get_metadata_dir(), "logs")
    variable_metadata_path = os.path.join(get_metadata_dir(), "json", "variable_metadata.json")

    if not os.path.isfile(variable_metadata_path):
        return None, "variable_metadata.json niet gevonden. Voer eerst de extractiestap uit."

    val_summary = vv.validate_column_values_folder(output_dir, variable_metadata_path)
    failed_cols = [
        (fname, col["column"], col["invalid_values"])
        for fname, res in val_summary.items()
        for col in res["results"].get("column_results", [])
        if col["status"] == "failed"
    ]
    val_log = {
        "timestamp": datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
        "status": "completed",
        "total_files_checked": len(val_summary),
        "total_failed_columns": len(failed_cols),
        "details": {
            fname: res["results"]
            for fname, res in val_summary.items()
            if res["results"].get("columns_checked", 0) > 0
        },
    }
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, "(5b)_value_validation_log_latest.json")
    with open(log_path, "w", encoding="utf-8") as fh:
        json.dump(val_log, fh, ensure_ascii=False, indent=2)
    return val_log, None


def _run_dec_validation():
    input_dir = get_input_dir()
    output_dir = get_output_dir()
    logs_dir = os.path.join(get_metadata_dir(), "logs")

    dec_txt_candidates = [
        f for f in os.listdir(input_dir)
        if f.startswith("Bestandsbeschrijving_Dec") and f.endswith(".txt")
    ] if os.path.isdir(input_dir) else []

    if not dec_txt_candidates:
        return None, "Geen Bestandsbeschrijving_Dec*.txt gevonden in de invoermap."

    dec_txt_path = os.path.join(input_dir, dec_txt_candidates[0])
    dec_summary = dv.validate_with_dec_files_folder(output_dir, dec_txt_path)
    dec_failed = [
        (fname, col["column"], col["dec_file"], col["invalid_values"])
        for fname, res in dec_summary.items()
        for col in res["results"].get("column_results", [])
        if col["status"] == "failed"
    ]
    dec_log = {
        "timestamp": datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
        "status": "completed",
        "total_files_checked": len(dec_summary),
        "total_failed_columns": len(dec_failed),
        "details": {
            fname: res["results"]
            for fname, res in dec_summary.items()
            if res["results"].get("columns_checked", 0) > 0
        },
    }
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, "(5c)_dec_validation_log_latest.json")
    with open(log_path, "w", encoding="utf-8") as fh:
        json.dump(dec_log, fh, ensure_ascii=False, indent=2)
    return dec_log, None


# -----------------------------------------------------------------------------
# Page
# -----------------------------------------------------------------------------

st.title("Output valideren")

st.write("""
**Optionele stap: geconverteerde bestanden valideren**

Draai deze stap nadat de pipeline klaar is. Er zijn twee validaties beschikbaar:

- **Kolomwaarden** — controleert of waarden overeenkomen met de toegestane waarden
  uit de bestandsbeschrijving (`variable_metadata.json`).
- **DEC codes** — controleert of codes en codeparen voorkomen in de DEC-decodeerbestanden
  (`Bestandsbeschrijving_Dec-bestanden_*.txt`), inclusief samengestelde sleutels.
""")

output_dir = get_output_dir()
if not os.path.isdir(output_dir) or not any(
    f.endswith(".csv") for f in os.listdir(output_dir)
):
    st.error("Geen geconverteerde CSV-bestanden gevonden. Draai eerst de Turbo Conversie.")
else:
    # --- Section 1: Value validation ---
    st.subheader("Kolomwaarden validatie")
    st.caption("Controleert toegestane waarden uit `variable_metadata.json`")

    col1, col2 = st.columns(2)
    with col1:
        val_clicked = st.button("Start kolomwaarden validatie", type="primary", use_container_width=True)
    with col2:
        val_log_exists = os.path.exists(
            os.path.join(get_metadata_dir(), "logs", "(5b)_value_validation_log_latest.json")
        )
        st.button("Validatie al uitgevoerd", disabled=not val_log_exists,
                  use_container_width=True, type="secondary")

    if val_clicked:
        with st.spinner("Kolomwaarden valideren..."):
            val_log, error = _run_value_validation()
        if error:
            st.warning(error)
        else:
            total = sum(d.get("columns_checked", 0) for d in val_log["details"].values())
            failed = val_log["total_failed_columns"]
            if failed == 0:
                st.success(f"Alle {total} kolom(men) OK — geen ongeldige waarden gevonden.")
            else:
                st.warning(f"{failed} kolom(men) bevatten ongeldige waarden (van {total} gecontroleerd).")
            st.rerun()

    val_log = _load_val_log()
    if val_log:
        failures = vv.read_value_validation_log(
            os.path.join(get_metadata_dir(), "logs", "(5b)_value_validation_log_latest.json")
        )
        st.caption(f"Laatste run: {val_log.get('timestamp', '-')}")
        if not failures:
            st.success("Alle kolommen OK.")
        else:
            for f in failures:
                vals = ", ".join(str(v) for v in f["invalid_values"][:10])
                if len(f["invalid_values"]) > 10:
                    vals += f" ... (+{len(f['invalid_values']) - 10} meer)"
                st.warning(f"**{f['file']}** — kolom `{f['column']}`: {vals}")
        with st.expander("Volledige details"):
            for fname, details in val_log.get("details", {}).items():
                st.markdown(f"**{fname}** — {details.get('columns_checked',0)} gecontroleerd, {details.get('columns_failed',0)} gefaald")
                for col_result in details.get("column_results", []):
                    icon = "✅" if col_result["status"] == "ok" else "❌"
                    st.markdown(f"  {icon} `{col_result['column']}`")

    st.divider()

    # --- Section 2: DEC validation ---
    st.subheader("DEC codes validatie")
    st.caption("Controleert codes en codeparen uit `Bestandsbeschrijving_Dec-bestanden_*.txt`")

    col3, col4 = st.columns(2)
    with col3:
        dec_clicked = st.button("Start DEC validatie", type="primary", use_container_width=True)
    with col4:
        dec_log_exists = os.path.exists(
            os.path.join(get_metadata_dir(), "logs", "(5c)_dec_validation_log_latest.json")
        )
        st.button("Validatie al uitgevoerd ", disabled=not dec_log_exists,
                  use_container_width=True, type="secondary")

    if dec_clicked:
        with st.spinner("DEC validatie uitvoeren..."):
            dec_log, error = _run_dec_validation()
        if error:
            st.warning(error)
        else:
            total = sum(d.get("columns_checked", 0) for d in dec_log["details"].values())
            failed = dec_log["total_failed_columns"]
            if failed == 0:
                st.success(f"Alle {total} kolom(men) OK — geen ongeldige waarden gevonden.")
            else:
                st.warning(f"{failed} kolom(men) bevatten ongeldige waarden (van {total} gecontroleerd).")
            st.rerun()

    dec_log = _load_dec_log()
    if dec_log:
        failures = dv.read_dec_validation_log(
            os.path.join(get_metadata_dir(), "logs", "(5c)_dec_validation_log_latest.json")
        )
        st.caption(f"Laatste run: {dec_log.get('timestamp', '-')}")
        if not failures:
            st.success("Alle kolommen OK.")
        else:
            for f in failures:
                vals = ", ".join(str(v) for v in f["invalid_values"][:10])
                if len(f["invalid_values"]) > 10:
                    vals += f" ... (+{len(f['invalid_values']) - 10} meer)"
                st.warning(f"**{f['file']}** — kolom `{f['column']}` (via `{f['dec_file']}`): {vals}")
        with st.expander("Volledige details"):
            for fname, details in dec_log.get("details", {}).items():
                st.markdown(f"**{fname}** — {details.get('columns_checked',0)} gecontroleerd, {details.get('columns_failed',0)} gefaald")
                for col_result in details.get("column_results", []):
                    icon = "✅" if col_result["status"] == "ok" else "❌"
                    st.markdown(f"  {icon} `{col_result['column']}` via `{col_result.get('dec_file','')}`")
