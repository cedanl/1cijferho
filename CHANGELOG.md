# Changelog

Alle opmerkelijke wijzigingen van dit project worden in dit bestand gedocumenteerd.

Het formaat volgt [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
en dit project volgt [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `--fail-on-empty-output` flag voor pipeline en convert commands (#230)
- Decodeerlog `(7)_decoding_log_latest.json` naast de bestaande logs, en `--strict` flag voor pipeline en convert (#210)

### Changed
- `(1)_json_processing_log`: bestanden zonder tabellen worden geteld (`no_tables_found_count`, `no_tables_found_files`) in plaats van per bestand in `processed_files` opgenomen (#210)

### Fixed
- **Startpositie als autoriteit**: Layout-validatie leest nu Startpositie correct (#223)
- **Parquet in sync met CSV**: Parquet-bestanden na header-normalisatie (#219)

### Removed
- `sanitize_variable_metadata.py`: Dode module (#212)
