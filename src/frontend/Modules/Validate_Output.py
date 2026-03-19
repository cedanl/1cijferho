import os
import json
import glob
import datetime
import streamlit as st
import eencijferho.utils.dec_validation as dv
from config import get_input_dir, get_output_dir, get_metadata_dir

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _load_dec_log():
    log_path = os.path.join(get_metadata_dir(), "logs", "(5c)_dec_validation_log_latest.json")
    if not os.path.exists(log_path):
        return None
    with open(log_path, encoding="utf-8") as f:
        return json.load(f)


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

Controleer of de kolomwaarden in de geconverteerde CSV-bestanden overeenkomen met de
geldige codes uit de DEC-decodeerbestanden. Draai deze stap nadat de pipeline klaar is.

Wat er gecontroleerd wordt:
- Elke kolom die in `Bestandsbeschrijving_Dec-bestanden_*.txt` vermeld staat wordt
  vergeleken met de eerste kolom van het bijbehorende DEC-bestand.
- Ongeldige waarden worden per kolom gerapporteerd.

Log wordt opgeslagen in `data/02-output/<map>/metadata/logs/(5c)_dec_validation_log_latest.json`.
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
            "Start DEC validatie", type="primary", use_container_width=True
        )
    with col2:
        log_exists = os.path.exists(
            os.path.join(get_metadata_dir(), "logs", "(5c)_dec_validation_log_latest.json")
        )
        st.button(
            "Validatie al uitgevoerd",
            disabled=not log_exists,
            use_container_width=True,
            type="secondary",
        )

    if run_clicked:
        with st.spinner("DEC validatie uitvoeren..."):
            dec_log, error = _run_dec_validation()

        if error:
            st.warning(error)
        else:
            total_cols = sum(
                d.get("columns_checked", 0) for d in dec_log["details"].values()
            )
            total_failed = dec_log["total_failed_columns"]
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
    dec_log = _load_dec_log()
    if dec_log:
        failures = dv.read_dec_validation_log(
            os.path.join(get_metadata_dir(), "logs", "(5c)_dec_validation_log_latest.json")
        )

        st.divider()
        st.subheader("Laatste validatieresultaten")
        st.caption(f"Uitgevoerd op: {dec_log.get('timestamp', '-')}")

        if not failures:
            st.success("Alle kolommen OK.")
        else:
            for f in failures:
                vals = ", ".join(str(v) for v in f["invalid_values"][:10])
                if len(f["invalid_values"]) > 10:
                    vals += f" ... (+{len(f['invalid_values']) - 10} meer)"
                st.warning(
                    f"**{f['file']}** — kolom `{f['column']}` "
                    f"(via `{f['dec_file']}`): {vals}"
                )

        with st.expander("Volledige details per bestand"):
            for fname, details in dec_log.get("details", {}).items():
                checked = details.get("columns_checked", 0)
                failed = details.get("columns_failed", 0)
                st.markdown(f"**{fname}** — {checked} gecontroleerd, {failed} gefaald")
                for col_result in details.get("column_results", []):
                    status_icon = "✅" if col_result["status"] == "ok" else "❌"
                    st.markdown(
                        f"  {status_icon} `{col_result['column']}` via `{col_result['dec_file']}`"
                    )
