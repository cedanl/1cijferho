---
icon: lucide/life-buoy
---

# Problemen oplossen

Zoek je symptoom in de tabel. Elke rij is gecontroleerd met een draaiende `eencijferho`, of volgt uit de broncode of de CHANGELOG. Alle logs staan in `metadata/logs/` in de uitvoermap.

## Welke log zegt wat?

| Log | Inhoud |
|---|---|
| `(3)_xlsx_validation_log_latest.json` | controle van de bestandsbeschrijvingen |
| `(4)_file_matching_log_latest.json` | welk invoerbestand bij welke bestandsbeschrijving hoort |
| `(5)_conversion_log_latest.json` | per bestand: gelukt, mislukt, overgeslagen of niet ondersteund |
| `(7)_decoding_log_latest.json` | wat is gedecodeerd, welke codes geen label kregen |
| `(8)_run_log_latest.log` | wat is overgeslagen of mislukt, één regel per probleem |

Met `--debug` staat de volledige foutmelding op de console en in het runlog.

## Tabel

| Symptoom | Oorzaak | Oplossing |
|---|---|---|
| `Path traversal attempt detected: …` | De invoer- of uitvoermap ligt buiten de werkmap. | Draai het commando in een map waar `--input` en `--output` binnen vallen, bijvoorbeeld de hoofdmap van de repository. |
| Exitcode 1 en `2 bestandsbeschrijvingen voor jaar 2024; verwijder de overbodige` | Er staan twee bestandsbeschrijvingen voor hetzelfde jaar in de invoermap. | Verwijder de overbodige. De melding noemt de kandidaten. De koppelingslog `(4)` krijgt dan status `failed`. Zie [CLI-referentie](cli.md#bestandsbeschrijving-per-jaargang). |
| Exitcode 1 en `Jaar 2024 in de bestandsnaam komt niet overeen met de bestandsbeschrijving(en) voor 2023` | Het jaar in de bestandsnaam komt niet voor bij de bestandsbeschrijvingen. | Controleer de jaartallen in de namen, of kies het jaar met `--layout-year JJJJ`. |
| Python-fout `FileNotFoundError … (3)_xlsx_validation_log_latest.json`, met eerder `No Bestandsbeschrijving*.txt files found` | Er staat geen bestandsbeschrijving (`Bestandsbeschrijving*.txt`) in de invoermap. | Zet de bestandsbeschrijving bij de gegevens in de invoermap. Dit hoort een duidelijke melding te geven (zie [#285](https://github.com/cedanl/1cijferho/issues/285)). |
| Een bestand ontbreekt in de uitvoer | In `(5)` staat het bij `unsupported_files` (geen bestandsbeschrijving die het bestand beschrijft) of bij `skipped_file_pairs`. | Lees de `reason` bij het bestand. Bij een bestand met jaartal noemt de reden de jaren die wel beschreven zijn. Op de DEMO-map zijn zes bestanden "niet ondersteund" (onder andere `Croho.asc`): dat is verwacht. |
| `Fout: … vervangingstekens` bij `extract` | Een bestandsbeschrijving bevat tekens die niet te lezen zijn (U+FFFD). | De melding noemt bestand en regelnummers. Herstel het bestand of sla het op als UTF-8 of ISO-8859-1. |
| Parquet-kolommen zijn tekst | Een Parquet-bestand bevat alleen tekstkolommen, zodat identifiers hun voorloopnullen houden. | Cast numerieke velden zelf. Zie de [migratiegids](outputcontract-migratie.md). |
| EV en VAKHAVW koppelen geeft geen treffers | Het persoonsgebonden nummer is tekst en houdt de schrijfwijze van het bronbestand (`2` en `000000000002` verschillen). | Koppel op een genormaliseerde sleutel. Zie de [migratiegids](outputcontract-migratie.md#ev-en-vakhavw-koppelen-op-persoonsgebonden-nummer). |
| `_decoded` of `_enriched` mist labels bij sommige rijen | De code komt niet voor in de Dec-tabel, of het label is langer dan 300 tekens. | Open `(7)`. `unmapped_rows` telt de rijen die hun code hielden. Een mapping met `skip_reason: "labels_too_long"` is niet toegepast. Zie [Decodering](decodering.md#lange-labels-worden-niet-ingevuld). |
| `--strict` keurt de run af | In `(7)` staan Dec-tabellen die niet laden of codes zonder match (`strict_violations`). | Lees `summary.strict_violations`. De uitvoerbestanden en het log zijn dan al geschreven. Op de DEMO-map keurt `--strict` de run af. |
| Koppeling studentnummer: `Kolom(men) […] niet gevonden in koppelbestand` | Het koppelbestand heeft andere kolomnamen dan `burgerservicenummer` en `studentnummer`. | Hernoem de kolommen, of geef andere namen mee met `OutputConfig(bsn_mapping_right_on=…, bsn_mapping_id_col=…)` in de Python API. |
| Koppeling studentnummer: `koppeling heeft het aantal rijen gewijzigd` | Het koppelbestand heeft dubbele burgerservicenummers. | Verwijder de dubbelen. De tool heeft dan geen bestand gewijzigd. |
| Turbo Convert in Streamlit blijft op Linux hangen | Een bekende fout (#263): de procespool sloot niet af. | Opgelost in de eerstvolgende release na 0.1.8. Zie de [CHANGELOG](https://github.com/cedanl/1cijferho/blob/main/CHANGELOG.md). |

## Komt je probleem er niet in voor?

Draai opnieuw met `--debug` en kijk in `(8)_run_log_latest.log`. Meld het dan als [issue](https://github.com/cedanl/1cijferho/issues), zonder persoonsgegevens in de tekst.
