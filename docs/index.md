# 1CijferHO

**Maak 1cijferHO-data direct bruikbaar voor analyse en onderzoek.**

1CijferHO Tool automatiseert het verwerken van DUO-onderwijsdata (fixed-width ASCII bestanden) naar schone CSV- en Parquet-bestanden — zonder programmeerkennis.

[![Windows](https://custom-icon-badges.demolab.com/badge/Windows-0078D6?logo=windows11&logoColor=white)](#)
[![macOS](https://img.shields.io/badge/macOS-000000?logo=apple&logoColor=F0F0F0)](#)
[![Linux](https://img.shields.io/badge/Linux-FCC624?logo=linux&logoColor=black)](#)
[![License](https://img.shields.io/github/license/cedanl/1cijferho)](https://github.com/cedanl/1cijferho/blob/main/LICENSE)

---

## Wat doet het?

| Stap | Wat er gebeurt |
|------|----------------|
| **Upload** | Zet DUO-bestanden in `data/01-input/` |
| **Extract** | Metadata uit `.txt`-beschrijvingen wordt uitgelezen |
| **Validate** | Veldposities en bestandskoppelingen worden gecontroleerd |
| **Convert** | Fixed-width ASCII → CSV / Parquet via multiprocessing |
| **Output** | Schone, gestructureerde bestanden klaar voor analyse |

Gevoelige gegevens (BSN) worden automatisch geanonimiseerd.

---

## Demo

![Demo](assets/demo.gif)

---

## Installatie in één minuut

```bash
git clone https://github.com/cedanl/1cijferho.git
cd 1cijferho
uv sync --extra frontend
uv run streamlit run src/main.py
```

Of installeer als Python-pakket:

```bash
pip install eencijferho
```

Zie [Aan de slag](getting-started.md) voor de volledige installatiestappen.

---

## Pakket of UI?

=== "Streamlit UI"
    Geen programmeerkennis nodig. Start de tool en upload via de browser.

    ```bash
    uv run streamlit run src/main.py
    ```

=== "CLI"
    Voor automatisering in scripts en pipelines.

    ```bash
    eencijferho pipeline --input data/01-input --output data/02-output
    ```

    Zie de [CLI-referentie](cli.md) voor alle commando's.

=== "Python API"
    Integreer de verwerking in je eigen code.

    ```python
    from eencijferho import run_turbo_convert_pipeline

    run_turbo_convert_pipeline(
        input_dir="data/01-input",
        output_dir="data/02-output",
    )
    ```

    Zie de [API-referentie](api/index.md) voor de volledige API.
