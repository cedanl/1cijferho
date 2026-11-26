---
icon: lucide/code
---

# API-referentie

De publieke API van `eencijferho` is beschikbaar via directe import:

```python
from eencijferho import run_turbo_convert_pipeline
from eencijferho import process_txt_folder, write_variable_metadata
```

Alle publieke functies staan in [`__all__`](https://github.com/cedanl/1cijferho/blob/main/src/eencijferho/__init__.py).

## Welke functie voor welk doel?

| Doel | Functie | Zelfde stap in de CLI |
|---|---|---|
| Metadata uit bestandsbeschrijvingen (`.txt`) halen | `process_txt_folder`, `write_variable_metadata`, `process_json_folder` | `eencijferho extract` |
| Bestandsbeschrijvingen controleren | `validate_metadata_folder` | `eencijferho validate` |
| Invoerbestanden aan een bestandsbeschrijving koppelen | `match_files` | `eencijferho validate` |
| Omzetten, decoderen, verrijken, Parquet | `run_turbo_convert_pipeline` | `eencijferho convert` |
| Studentnummer toevoegen via een koppelbestand | `translate_bsn_to_local_id`, of `OutputConfig(bsn_mapping_file=…)` | `--bsn-mapping-file` |

`eencijferho pipeline` voert extract, validate en convert achter elkaar uit.

## Volledig voorbeeld

De omzetting leest de metadata uit de uitvoermap. Draai dus eerst de extractiestappen, zoals de CLI doet. Het voorbeeld gebruikt de DEMO-map en is gecontroleerd op de `main`-versie na release 0.1.8. Draai het in de hoofdmap van de repository.

```python
from pathlib import Path

from eencijferho import (
    process_json_folder,
    process_txt_folder,
    run_turbo_convert_pipeline,
    write_variable_metadata,
)
from eencijferho.config import OutputConfig
from eencijferho.utils.converter_match import match_files
from eencijferho.utils.extractor_validation import validate_metadata_folder

invoer = "data/01-input/DEMO"
uitvoer = Path("data/02-output/DEMO")
metadata = uitvoer / "metadata"
json_map = metadata / "json"
validatielog = metadata / "logs" / "(3)_xlsx_validation_log_latest.json"

# 1. Metadata uit de bestandsbeschrijvingen halen
process_txt_folder(invoer, json_output_folder=str(json_map))
write_variable_metadata(input_dir=invoer, json_folder=str(json_map))
process_json_folder(json_input_folder=str(json_map), excel_output_folder=str(metadata))

# 2. Controleren en elk invoerbestand aan een bestandsbeschrijving koppelen
validate_metadata_folder(metadata_folder=str(metadata))
match_files(invoer, log_path=str(validatielog))

# 3. Omzetten
log, bestanden = run_turbo_convert_pipeline(
    input_dir=invoer,
    output_dir=str(uitvoer),
    metadata_dir=str(metadata),
    output_config=OutputConfig(),
)
print(len(bestanden), "bestanden")
```

Geef `OutputConfig` opties mee voor een andere uitvoer, bijvoorbeeld `OutputConfig(bsn_mapping_file="data/koppeling_bsn_studentnummer_DEMO.csv")`.

---

## Pipeline

::: eencijferho.core.pipeline.run_turbo_convert_pipeline

---

## Extractor

::: eencijferho.core.extractor.process_txt_folder

::: eencijferho.core.extractor.write_variable_metadata

::: eencijferho.core.extractor.process_json_folder

::: eencijferho.core.extractor.get_fwf_params

::: eencijferho.core.extractor.list_fwf_tables

---

## Validatie

::: eencijferho.utils.extractor_validation.validate_metadata_folder

::: eencijferho.utils.converter_validation.converter_validation

::: eencijferho.utils.converter_match.match_files

---

## Uitvoer-hulpfuncties

::: eencijferho.utils.compressor.convert_csv_to_parquet

::: eencijferho.utils.converter_headers.convert_csv_headers_to_snake_case

---

## Vertaling

::: eencijferho.utils.translator.translate_bsn_to_local_id

---

## Configuratie

::: eencijferho.config.OutputConfig
