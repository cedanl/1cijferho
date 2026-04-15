# API-referentie

De publieke API van `eencijferho` is beschikbaar via directe import:

```python
from eencijferho import run_turbo_convert_pipeline
from eencijferho import process_txt_folder, write_variable_metadata
```

Alle publieke functies staan in [`__all__`](https://github.com/cedanl/1cijferho/blob/main/src/eencijferho/__init__.py).

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

::: eencijferho.utils.encryptor.encryptor

::: eencijferho.utils.converter_headers.convert_csv_headers_to_snake_case

---

## Configuratie

::: eencijferho.config.OutputConfig
