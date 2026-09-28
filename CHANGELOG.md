# Changelog

Alle opmerkelijke wijzigingen van dit project worden in dit bestand gedocumenteerd.

Het formaat volgt [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
en dit project volgt [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `--fail-on-empty-output` flag voor pipeline en convert commands, zodat CI-jobs met nul outputs expliciet falen (#230)

### Fixed
- **Startpositie als autoriteit**: Layout-validatie leest nu de `Startpositie`-kolom uit DUO-bestandsbeschrijvingen, waardoor gaten tussen velden correct worden gedetecteerd. Eerder werden gaten stilzwijgend ingevuld (#223)
- **Parquet in sync met CSV**: Parquet-bestanden worden nu geschreven na header-normalisatie naar snake_case, niet daarvoor. De kolomnamen zijn nu consistent tussen CSV en Parquet formaten (#219)

### Changed
- Demo fixture voor year-matching: beide 2023 en 2024 bestandsbeschrijvingen zijn nu present, waardoor `EV299XX24_DEMO.asc` correct tegen jaargang 2024 gematchd wordt (#226, #224)

### Removed
- `sanitize_variable_metadata.py`: Dode module die stiekem DUO-labels wijzigde (verwijdering van komma's/puntkomma's zonder gebruiker daarvan op de hoogte te stellen). Kolomnaam-normalisatie gebeurt nu via `clean_header_name()` en `normalize_name()` (#212)

## [0.1.8] - 2026-09-28

### Added
- Year matching voor EV-bestanden: EV-bestanden worden nu gematchd aan bestandsbeschrijvingen op basis van jaargang (#198)
- SVO (Select Voorbeelden Onderwijsgegevens) dataset ondersteuning
- Enhanced documentation voor decodering proces

### Fixed
- Decoder schrijft geen voorloopnullen meer weg uit datakolommen
- Parse metadata behandelt het DUO-continuatieteken `>` correct
- Fuzzy string-match voor kolomselectie werkt betrouwbaarder

## [0.1.7] - 2026-09-15

### Added
- Eerste versie van 1CijferHO pipeline
- Fixed-width ASCII bestand parsing
- DUO bestandsbeschrijving extraction
- CSV en Parquet export formaten
- Polars-gebaseerde data processing

[Unreleased]: https://github.com/cedanl/1cijferho/compare/v0.1.8...HEAD
[0.1.8]: https://github.com/cedanl/1cijferho/releases/tag/v0.1.8
[0.1.7]: https://github.com/cedanl/1cijferho/releases/tag/v0.1.7
