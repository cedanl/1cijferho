# Changelog

Alle opmerkelijke wijzigingen van dit project worden in dit bestand gedocumenteerd.

Het formaat volgt [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
en dit project volgt [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `--fail-on-empty-output` flag voor pipeline en convert commands (#230)

### Fixed
- **Startpositie als autoriteit**: Layout-validatie leest nu Startpositie correct (#223)
- **Parquet in sync met CSV**: Parquet-bestanden na header-normalisatie (#219)
- **Eén leesstrategie voor bestandsbeschrijvingen**: extractor, metadata-parser en DEC-validatie lezen bestandsbeschrijvingen via één functie (BOM → UTF-8 → latin-1), zodat "vóór" overal dezelfde naam oplevert. Een bestandsbeschrijving met vervangingstekens (U+FFFD) laat `extract` nu stoppen met een foutmelding (bestand + regelnummers) in plaats van stil verder te werken; herstel zo'n bestand of sla het op als UTF-8 of ISO-8859-1. De regex-noodoplossing in de DEC-validatie, de beschadigde spelling in de NFWA-preset en de beschadigde regels in de DEMO-`Bestandsbeschrijving_Dec-bestanden.txt` zijn verwijderd/hersteld (#203)

### Removed
- `sanitize_variable_metadata.py`: Dode module (#212)
