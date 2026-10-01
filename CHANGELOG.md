# Changelog

Alle opmerkelijke wijzigingen van dit project worden in dit bestand gedocumenteerd.

Het formaat volgt [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
en dit project volgt [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `--fail-on-empty-output` flag voor pipeline en convert commands (#230)

### Changed
- Parquet gebruikt dezelfde kolomnamen en waarden als CSV, met alle velden als tekst; bron- en lookupcodes behouden voorloopnullen. Zie [outputcontract-migratie](docs/outputcontract-migratie.md).
- Gedeeltelijk mislukte conversies stoppen de pipeline met exitcode 1; nabewerking en rapportage gebruiken alleen nieuw gemaakte producten.
- Snapshots vergelijken SHA256-inhoudhashes en keuren onleesbare/onverwachte output af; alle demo-hoofdproducten staan nu in de full-pipeline-baseline.

### Fixed
- Jaarmatching ondersteunt echte BRIN-namen en twee-/viercijferige jaren; onbekende conventies worden expliciet gelogd (#198/#224).
- Identieke dubbele Dec-layouts worden gecontroleerd gededupliceerd, zonder de vakcode-uitbreidingen te verliezen; conflicterende layouts blijven fouten.
- Dec-joins bewaren beide broncodes, vereisen unieke lookup-sleutels en houden rijvolgorde vast (#199/#222).
- Herhaald decode/enrich leest codes als tekst, slaat afgeleide CSV's over en behoudt de kolomstijl (#206).
- Metadata-continuatienotities staan niet meer in waardelabels; niet-uitputtende lijsten zijn expliciet gemarkeerd (#221).
- Fixed-width-conversie bewaart rijvolgorde en quote CSV-velden/headers correct.
- **Startpositie als autoriteit**: Layout-validatie leest nu Startpositie correct (#223)
- **Parquet in sync met CSV**: Parquet-bestanden na header-normalisatie (#219)

### Removed
- `sanitize_variable_metadata.py`: Dode module (#212)
- Niet-werkende stille layoutcorrectie voor Dec-land/nationaliteitscodes (#217)
