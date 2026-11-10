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
- `data/koppeling_bsn_studentnummer_DEMO.csv`: koppelbestand met een fictief studentnummer voor elk BSN in de DEMO-data, zodat de studentnummerkoppeling op DEMO te proberen is

### Changed
- `(1)_json_processing_log`: bestanden zonder tabellen worden geteld (`no_tables_found_count`, `no_tables_found_files`) in plaats van per bestand in `processed_files` opgenomen (#210)
- **Breaking: bestandsbeschrijving per jaargang**: EV- en VAKHAVW-bestanden worden gekoppeld aan de bestandsbeschrijving van het jaar in hun bestandsnaam (`EV299XX24` → 2024). Staan er meerdere bestandsbeschrijvingen in de map en wijst het jaar er niet precies één aan, dan stoppen `validate` en `pipeline` met exitcode 1 en een melding met alle kandidaten, in plaats van stilzwijgend de eerste te gebruiken. Een map die zo eerder "toevallig" slaagde, geeft nu een fout; gebruik dan `--layout-year`. De conversie weigert een koppelingslog met een andere status dan `completed`, en een bestand met meerdere geldige bestandsbeschrijvingen met verschillende layouts krijgt status `failed` (#198)

### Fixed
- **Preset NFWA en Evaluatietool: onbekend diplomajaar krijgt een label**: `diplomajaar_hoogste_vooropleiding` bevat voor code `0000` nu het label uit de bestandsbeschrijving, net als de standaarduitvoer. NFWA las de code als het jaar 0 en telde de student als tussenjaar.
- **Startpositie als autoriteit**: Layout-validatie leest nu Startpositie correct (#223)
- **Parquet in sync met CSV**: Parquet-bestanden na header-normalisatie (#219)
- **Eén leesstrategie voor bestandsbeschrijvingen**: extractor, metadata-parser en DEC-validatie lezen bestandsbeschrijvingen via één functie (BOM → UTF-8 → latin-1), zodat "vóór" overal dezelfde naam oplevert. Een bestandsbeschrijving met vervangingstekens (U+FFFD) laat `extract` nu stoppen met een foutmelding (bestand + regelnummers) in plaats van stil verder te werken; herstel zo'n bestand of sla het op als UTF-8 of ISO-8859-1. De regex-noodoplossing in de DEC-validatie, de beschadigde spelling in de NFWA-preset en de beschadigde regels in de DEMO-`Bestandsbeschrijving_Dec-bestanden.txt` zijn verwijderd/hersteld (#203)
- **Niet-Latin-1 tekens blijven behouden**: Header-normalisatie wijzigt alleen kolomnamen; tekens als `–`, `€`, `œ` en `⁄` in celwaarden worden niet meer vervangen door `?` of `/` (#208)
- **Labels terug bij zes velden met een afwijkende DUO-naam** (#202): sinds de decoder alleen op exacte naam koppelt, bleven `diplomajaar_hoogste_vooropleiding` en drie `indicatie_eerstejaars…`-kolommen codes in `_enriched.csv` en ontbraken de labelkolommen bij `hoogste_vooropleiding_…_oorspronkelijke_code`. DUO noemt deze velden in de beschrijvingen anders dan in de lay-out; een vaste naamtabel koppelt ze weer, zonder te gokken op gelijkenis. NFWA las de code `0000` als het jaar 0.
- **Turbo Convert loopt niet meer vast op Linux** (#263): de converter sluit de procespool na de laatste chunk met `close()` en `join()`. Onder Streamlit erfden de workers een SIGTERM-handler die het proces niet beëindigt, waardoor het afsluiten van de pool eindeloos wachtte en de conversie in de UI nooit klaar was.
- **DEC-validatie in de UI** (#264): een bestandsbeschrijving met vervangingstekens (U+FFFD) geeft op 'Output valideren' een foutmelding met bestand en regelnummers in plaats van een traceback.
- **Niet-gemapte codes zichtbaar in het decodeerlog** (#207): elke `variable_metadata`-kolom in `(7)_decoding_log_latest.json` krijgt `mapped_rows`, `unmapped_rows` en `semantics` (`label`, `mixed` of `code`), `summary` krijgt `total_unmapped_rows` en `variable_mappings_skipped`, en de console noemt het aantal niet-gemapte rijen. Niet-gemapte waarden blijven als code in de kolom staan; er komt geen kolom bij.
- **Lege waarden krijgen een vast label** (#207): de sleutel `[leeg]` gaat voor, daarna andere `leeg`-sleutels op alfabetische volgorde, los van de volgorde in de metadata. `[leeg] of 0` geldt nu ook voor code `0`.
- **⚠️ Code beëindiging blijft een code** (#207): een `variable_metadata`-mapping met een label langer dan 300 tekens wordt niet toegepast. Op DEMO-data geldt dat alleen voor `Code beëindiging`: in `_enriched` en Parquet staan daar nu de codes (`0`, `1` … `9`) in plaats van alinea's DUO-uitleg. Het log noteert `skip_reason: "labels_too_long"`. Zie [Decodering](docs/decodering.md#lange-labels-worden-niet-ingevuld).
- **Versleutelen behoudt tekens buiten ASCII** (#265): 'Versleutelen & uploaden' en 'Persoonsgegevens uploaden' decoderen de CSV in de browser als UTF-8. Elke letter buiten ASCII kwam voorheen als twee tekens in het resultaat terecht (`ü` werd `Ã¼`).
- **`decode` en `enrich` op pipeline-output geven dezelfde bestanden als de pipeline** (#206): `decode` slaat `*_enriched.csv` over (gebruikt dezelfde bestandsselectie als de pipeline) en schrijft dus geen los `*_enriched_decoded.csv` meer; `enrich` verrijkt alleen `X_decoded.csv` van een hoofdbestand `X.csv` en schrijft nooit `*_enriched_enriched.csv`. Heeft het bronbestand al snake_case-kolomnamen (standaard pipeline-output), dan krijgen de nieuwe Dec-kolommen ook de snake_case-naam van de pipeline (`geboorteland_naam_land` in plaats van `geboorteland__naam_land`). **Let op:** wie losse `decode`/`enrich` draaide op snake_case-output en de `__`-namen gebruikt, moet die namen aanpassen. Bij output met originele kolomnamen (`--skip-snake-case`) blijven de `__`-namen, net als in de pipeline. Losse bestanden die eerdere versies al schreven worden niet opgeruimd.

### Removed
- `sanitize_variable_metadata.py`: Dode module (#212)
