# Changelog

Alle opmerkelijke wijzigingen van dit project worden in dit bestand gedocumenteerd.

Het formaat volgt [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
en dit project volgt [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **⚠️ Breaking: Parquet is volledig String — numerieke casts nodig** (#219): alle kolommen in de Parquet-output zijn nu `String` in plaats van geïnfereerd `Int64`/`Float64`/`String`, zodat identifiers zoals `burgerservicenummer` en `persoonsgebonden_nummer` hun voorloopnullen behouden. Directe Parquet-gebruikers moeten numerieke velden zelf casten. De CSV-output verandert hierdoor niet, dus consumenten die CSV lezen merken niets. Zie [Outputcontract: migratiegids](docs/outputcontract-migratie.md).
- **Parquet-kolomnamen identiek aan CSV** (#219): Parquet krijgt dezelfde kolomnamen en kolomvolgorde als de CSV (standaard snake_case); voorheen had Parquet de kale DUO-namen.

### Added
- `--fail-on-empty-output` flag voor pipeline en convert commands (#230)
- Decodeerlog `(7)_decoding_log_latest.json` naast de bestaande logs, en `--strict` flag voor pipeline en convert (#210)
- `--layout-year JJJJ` voor `validate` en `pipeline`: forceer de bestandsbeschrijving van een jaar voor EV- en VAKHAVW-bestanden, met een waarschuwing in console en koppelingslog (#198)

### Changed
- `(1)_json_processing_log`: bestanden zonder tabellen worden geteld (`no_tables_found_count`, `no_tables_found_files`) in plaats van per bestand in `processed_files` opgenomen (#210)
- **Breaking: bestandsbeschrijving per jaargang**: EV- en VAKHAVW-bestanden worden gekoppeld aan de bestandsbeschrijving van het jaar in hun bestandsnaam (`EV299XX24` → 2024). Staan er meerdere bestandsbeschrijvingen in de map en wijst het jaar er niet precies één aan, dan stoppen `validate` en `pipeline` met exitcode 1 en een melding met alle kandidaten, in plaats van stilzwijgend de eerste te gebruiken. Een map die zo eerder "toevallig" slaagde, geeft nu een fout; gebruik dan `--layout-year`. De conversie weigert een koppelingslog met een andere status dan `completed`, en een bestand met meerdere geldige bestandsbeschrijvingen met verschillende layouts krijgt status `failed` (#198)

### Fixed
- **Startpositie als autoriteit**: Layout-validatie leest nu Startpositie correct (#223)
- **Parquet in sync met CSV**: Parquet-bestanden na header-normalisatie (#219)
- **Eén leesstrategie voor bestandsbeschrijvingen**: extractor, metadata-parser en DEC-validatie lezen bestandsbeschrijvingen via één functie (BOM → UTF-8 → latin-1), zodat "vóór" overal dezelfde naam oplevert. Een bestandsbeschrijving met vervangingstekens (U+FFFD) laat `extract` nu stoppen met een foutmelding (bestand + regelnummers) in plaats van stil verder te werken; herstel zo'n bestand of sla het op als UTF-8 of ISO-8859-1. De regex-noodoplossing in de DEC-validatie, de beschadigde spelling in de NFWA-preset en de beschadigde regels in de DEMO-`Bestandsbeschrijving_Dec-bestanden.txt` zijn verwijderd/hersteld (#203)
- **Niet-Latin-1 tekens blijven behouden**: Header-normalisatie wijzigt alleen kolomnamen; tekens als `–`, `€`, `œ` en `⁄` in celwaarden worden niet meer vervangen door `?` of `/` (#208)
- **Turbo Convert loopt niet meer vast op Linux** (#263): de converter sluit de procespool na de laatste chunk met `close()` en `join()`. Onder Streamlit erfden de workers een SIGTERM-handler die het proces niet beëindigt, waardoor het afsluiten van de pool eindeloos wachtte en de conversie in de UI nooit klaar was.
- **DEC-validatie in de UI** (#264): een bestandsbeschrijving met vervangingstekens (U+FFFD) geeft op 'Output valideren' een foutmelding met bestand en regelnummers in plaats van een traceback.
- **Versleutelen behoudt tekens buiten ASCII** (#265): 'Versleutelen & uploaden' en 'Persoonsgegevens uploaden' decoderen de CSV in de browser als UTF-8. Elke letter buiten ASCII kwam voorheen als twee tekens in het resultaat terecht (`ü` werd `Ã¼`).

### Removed
- `sanitize_variable_metadata.py`: Dode module (#212)
