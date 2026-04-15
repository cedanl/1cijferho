# Aan de slag

## Vereisten

- **Besturingssysteem**: Windows, macOS of Linux
- **Software**: [uv](https://docs.astral.sh/uv/getting-started/installation/) — Python package manager

### uv installeren

=== "Linux / macOS"
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

=== "Windows"
    ```powershell
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```

Controleer daarna:
```bash
uv self update
```

---

## Optie 1: Streamlit UI (aanbevolen)

Geen programmeerkennis nodig.

```bash
# 1. Download de tool
git clone https://github.com/cedanl/1cijferho.git
cd 1cijferho

# 2. Installeer dependencies
uv sync --extra frontend

# 3. Start de applicatie
uv run streamlit run src/main.py
```

De browser opent automatisch met de interface.

---

## Optie 2: Python-pakket

Voor gebruik als backend-bibliotheek of via de CLI.

=== "pip"
    ```bash
    pip install eencijferho
    ```

=== "uv"
    ```bash
    uv add eencijferho
    ```

=== "poetry"
    ```bash
    poetry add eencijferho
    ```

### Optionele backends

```bash
# MinIO (S3-compatibele opslag)
pip install eencijferho[minio]

# PostgreSQL
pip install eencijferho[postgres]

# Alle backends
pip install eencijferho[all-backends]
```

---

## Mapstructuur verwacht

```
data/
├── 01-input/
│   ├── EV_2023.asc          # fixed-width databestand
│   ├── EV_2023.txt          # bijbehorende bestandsbeschrijving
│   └── Bestandsbeschrijving_Dec-bestanden.txt   # optioneel: DEC-opzoektabellen
└── 02-output/               # wordt aangemaakt door de tool
```

!!! tip "Demo-modus"
    Zet bestanden met `*_DEMO*` in de naam in `data/01-input/` om de tool te testen zonder echte data.

---

## Volledig in één stap

```bash
eencijferho pipeline --input data/01-input --output data/02-output
```

Of stap voor stap — zie de [CLI-referentie](cli.md).
