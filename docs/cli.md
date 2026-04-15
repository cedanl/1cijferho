# CLI-referentie

De `eencijferho` CLI verwerkt DUO-bestanden via losse stappen of in één pipeline-aanroep.

!!! info "Altijd actueel"
    Gebruik `eencijferho --help` of `eencijferho <commando> --help` voor de meest actuele beschrijving rechtstreeks uit de tool.

---

## Gedeelde opties

Alle commando's vereisen:

| Optie | Beschrijving |
|-------|-------------|
| `--input PATH` | Map met invoerbestanden (`data/01-input`) |
| `--output PATH` | Map voor uitvoerbestanden (`data/02-output`) |

---

## Commando's

### `pipeline` — alles in één stap

```bash
eencijferho pipeline --input data/01-input --output data/02-output
```

Voert extract → validate → convert achter elkaar uit.

::: eencijferho.cli.cmd_pipeline
    options:
      show_signature: false

---

### `extract`

```bash
eencijferho extract --input data/01-input --output data/02-output
```

::: eencijferho.cli.cmd_extract
    options:
      show_signature: false

---

### `validate`

```bash
eencijferho validate --input data/01-input --output data/02-output
```

::: eencijferho.cli.cmd_validate
    options:
      show_signature: false

---

### `convert`

```bash
eencijferho convert --input data/01-input --output data/02-output [opties]
```

::: eencijferho.cli.cmd_convert
    options:
      show_signature: false

#### Uitvoer-opties

| Optie | Effect |
|-------|--------|
| `--skip-decode` | Geen `_decoded` CSV-varianten |
| `--skip-enrich` | Geen `_enriched` CSV-varianten |
| `--skip-parquet` | Geen Parquet-compressie |
| `--skip-encrypt` | BSN-kolommen niet versleutelen |
| `--skip-snake-case` | Originele kolomnamen behouden |
| `--skip-ev` | EV-bestanden overslaan |
| `--skip-vakhavw` | VAKHAVW-bestanden overslaan |
| `--decode-columns KOLOM ...` | Alleen opgegeven kolommen decoderen |
| `--enrich-variables VAR ...` | Alleen opgegeven variabelen verrijken |

---

### `decode`

```bash
eencijferho decode --input data/01-input --output data/02-output
```

::: eencijferho.cli.cmd_decode
    options:
      show_signature: false

---

### `enrich`

```bash
eencijferho enrich --input data/01-input --output data/02-output
```

::: eencijferho.cli.cmd_enrich
    options:
      show_signature: false

---

### `validate-output`

```bash
eencijferho validate-output --input data/01-input --output data/02-output
```

::: eencijferho.cli.cmd_validate_output
    options:
      show_signature: false

---

## Uitvoerstructuur

Na een volledige pipeline-run:

```
data/02-output/
├── metadata/
│   ├── json/
│   │   ├── EV_2023.json
│   │   └── variable_metadata.json
│   ├── logs/
│   │   ├── (3)_xlsx_validation_log_latest.json
│   │   ├── (4)_file_matching_log_latest.json
│   │   └── (5)_conversion_log_latest.json
│   └── EV_2023.xlsx
├── EV_2023.csv
├── EV_2023_decoded.csv
├── EV_2023_enriched.csv
└── EV_2023.parquet
```
