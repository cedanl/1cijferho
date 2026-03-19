import os
import json
import datetime
import streamlit as st
import eencijferho.utils.value_validation as vv
from config import get_output_dir, get_metadata_dir

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _load_val_log():
    log_path = os.path.join(get_metadata_dir(), "logs", "(5b)_value_validation_log_latest.json")
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


# -----------------------------------------------------------------------------
# Page
# -----------------------------------------------------------------------------

st.title("Output valideren")

st.write("""
**Optionele stap: geconverteerde bestanden valideren**

Controleer of de kolomwaarden in de geconverteerde CSV-bestanden overeenkomen met de
toegestane waarden uit de bestandsbeschrijving (`variable_metadata.json`). Draai deze
stap nadat de pipeline klaar is.

Wat er gecontroleerd wordt:
- Elke kolom met een vaste waardelijst in de bestandsbeschrijving wordt gevalideerd.
- Kolommen met bereiken, externe verwijzingen of open lijsten worden overgeslagen.
- Ongeldige waarden worden per kolom gerapporteerd.

Log wordt opgeslagen in `data/02-output/<map>/metadata/logs/(5b)_value_validation_log_latest.json`.
""")

output_dir = get_output_dir()
if not os.path.isdir(output_dir) or not any(
    f.endswith(".csv") for f in os.listdir(output_dir)
):
    st.error("Geen geconverteerde CSV-bestanden gevonden. Draai eerst de Turbo Conversie.")
else:
    col1, col2 = st.columns(2)
    with col1:
        run_clicked = st.button(
            "Start kolomwaarden validatie", type="primary", use_container_width=True
        )
    with col2:
        log_exists = os.path.exists(
            os.path.join(get_metadata_dir(), "logs", "(5b)_value_validation_log_latest.json")
        )
        st.button(
            "Validatie al uitgevoerd",
            disabled=not log_exists,
            use_container_width=True,
            type="secondary",
        )

    if run_clicked:
        with st.spinner("Kolomwaarden valideren..."):
            val_log, error = _run_value_validation()

        if error:
            st.warning(error)
        else:
            total_cols = sum(
                d.get("columns_checked", 0) for d in val_log["details"].values()
            )
            total_failed = val_log["total_failed_columns"]
            if total_failed == 0:
                st.success(
                    f"Alle {total_cols} kolom(men) OK — geen ongeldige waarden gevonden."
                )
            else:
                st.warning(
                    f"{total_failed} kolom(men) bevatten ongeldige waarden "
                    f"(van {total_cols} gecontroleerde kolommen)."
                )
            st.rerun()

    # Show results from last run
    val_log = _load_val_log()
    if val_log:
        failures = vv.read_value_validation_log(
            os.path.join(get_metadata_dir(), "logs", "(5b)_value_validation_log_latest.json")
        )

        st.divider()
        st.subheader("Laatste validatieresultaten")
        st.caption(f"Uitgevoerd op: {val_log.get('timestamp', '-')}")

        if not failures:
            st.success("Alle kolommen OK.")
        else:
            for f in failures:
                vals = ", ".join(str(v) for v in f["invalid_values"][:10])
                if len(f["invalid_values"]) > 10:
                    vals += f" ... (+{len(f['invalid_values']) - 10} meer)"
                st.warning(
                    f"**{f['file']}** — kolom `{f['column']}`: {vals}"
                )

        with st.expander("Volledige details per bestand"):
            for fname, details in val_log.get("details", {}).items():
                checked = details.get("columns_checked", 0)
                failed = details.get("columns_failed", 0)
                st.markdown(f"**{fname}** — {checked} gecontroleerd, {failed} gefaald")
                for col_result in details.get("column_results", []):
                    status_icon = "✅" if col_result["status"] == "ok" else "❌"
                    st.markdown(
                        f"  {status_icon} `{col_result['column']}`"
                    )
