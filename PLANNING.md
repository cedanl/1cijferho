# PLANNING: Issue #80 - Kolomwaarden valideren o.b.v. bestandsbeschrijving

## Context

De `.txt` bestandsbeschrijving bestanden (bijv. `Bestandsbeschrijving_1cyferho_2023_v1.1_DEMO.txt`)
bevatten per veld een "Mogelijke waarden:" sectie. `parse_metadata.py` parseert dit al naar een
`values` dict in `variable_metadata.json`.

We moeten de geconverteerde CSV-bestanden valideren tegen deze toegestane waarden.

## Wat er al is

- `src/eencijferho/core/parse_metadata.py` — parseert `.txt` naar `[{name, description, values}]`
- `data/00-metadata/json/variable_metadata.json` — output van parse_metadata, bevat values per kolom
- `src/eencijferho/utils/extractor_validation.py` — valideert xlsx metadatabestanden
- `src/frontend/Modules/Validate_Metadata.py` — Stap 2 frontend

Kolommen met bruikbare `values` zijn key-value dicts zoals:
```json
{"1": "voltijd", "2": "deeltijd", "3": "co-op"}
```
Kolommen met `{"reference": "..."}` of `{"list": [...]}` worden overgeslagen (geen vaste waarden).

## Aanpak

### Stap 1 — Backend: `value_validation.py`

Nieuw bestand: `src/eencijferho/utils/value_validation.py`

Functie: `validate_column_values(data_file_path, variable_metadata_path)`

- Laadt `variable_metadata.json`
- Filtert variabelen met key-value `values` (geen `reference`/`list`/leeg)
- Laadt de CSV (Polars, separator=`;`, encoding=`utf-8`)
- Matcht kolomnamen op variabelenamen (case-insensitive, snake_case-tolerant)
- Per kolom: check of alle unieke waarden in de toegestane keys zitten
- Geeft terug: `(success: bool, results: dict)` met per kolom de uitkomst

### Stap 2 — Integratie in de pipeline

In `src/eencijferho/core/pipeline.py`:
- Na conversie (stap 1) en voor compressie (stap 4): voeg `validate_column_values` toe
- Log resultaat, rapporteer fouten via `status_callback`

### Stap 3 — Frontend: Validate_Metadata.py

In `src/frontend/Modules/Validate_Metadata.py`:
- Extra sectie: "Kolomwaarden valideren" (aparte knop of onderdeel van de bestaande flow)
- Toont per kolom: OK / fouten met de afwijkende waarden
- Werkt alleen als er al geconverteerde CSV's zijn in `data/02-output/`

### Stap 4 — Unit tests

Nieuw bestand: `tests/test_value_validation.py`

- `test_valid_data()` — alle waarden zijn toegestaan, verwacht `success=True`
- `test_invalid_data()` — kolom bevat verboden waarde, verwacht `success=False` + details

## Bestandswijzigingen

| Bestand | Actie |
|---|---|
| `src/eencijferho/utils/value_validation.py` | Nieuw |
| `src/eencijferho/core/pipeline.py` | Uitbreiden |
| `src/frontend/Modules/Validate_Metadata.py` | Uitbreiden |
| `tests/test_value_validation.py` | Nieuw |

## Acceptatiecriteria check

- [x] Alle kolommen met waarden worden gecontroleerd
- [x] Duidelijke melding (frontend + pipeline log)
- [x] Unit test happy path + fout-case
