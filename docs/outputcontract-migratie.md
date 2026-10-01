# Outputcontract: wijzigingen in de gecombineerde producerfixes

Deze wijzigingen combineren de kern van PR's #219, #221, #222, #223 en #224, met de cleanup uit #217. Ze zijn nog **niet uitgebracht**. Een versienummer in `pyproject.toml` is geen bewijs dat de wijzigingen al in die PyPI-release zitten; pin tot de release een gecontroleerde commit.

## Migratie-impact

- **Parquet is volledig tekst.** Kolomnamen komen overeen met CSV; met de standaardconfiguratie zijn ze snake_case. Numerieke analyses moeten hun numerieke velden expliciet casten. Houd identifiers als tekst.
- **Voorloopnullen blijven behouden.** Dit geldt voor broncodes in decoded-output en voor als tekst ingelezen lookupvelden, zoals gemeentecodes. Pas externe joins aan die voorheen gestripte codes gebruikten. Normaliseer alleen een afgeleide koppelsleutel, niet de oorspronkelijke codekolom.
- **Layouts en jaren worden gecontroleerd.** Herkende EV-namen met twee- of viercijferige jaren worden gekoppeld aan de passende bestandsbeschrijving. Een onbekende naamconventie krijgt een expliciete waarschuwing dat de jaargang niet is gecontroleerd. Meerdere EV/VAK-layouts worden geweigerd. Alleen inhoudelijk identieke Dec-definities mogen worden gededupliceerd; de gekozen en equivalente bestanden staan in de conversielog.
- **Fouten leiden niet meer tot een succesvolle gedeeltelijke pipeline-run.** Een mislukte conversie of overgeslagen, ingeschakeld hoofdbestand veroorzaakt `PipelineError` en CLI-exitcode 1. Header- en Parquet-fouten zijn in de pipeline eveneens fataal. Alleen producten van de huidige run worden gerapporteerd en nabewerkt; oude bestanden blijven fysiek staan. Gebruik een aparte outputmap per configuratie.
- **`>`-notities worden metadata, geen onderdeel van een label.** Niet-uitputtende waardelijsten worden gemarkeerd met `non_exhaustive_values`. `Diplomajaar` heeft de sentinel `0000` plus jaartallen, niet een gesloten domein `0/1/99`.

## NFWA en Staat1cho zijn verschillende consumenten

| Consument | EV | VAKHAVW |
|---|---|---|
| NFWA | `_enriched.csv`, bij voorkeur gegenereerd met `--preset nfwa` | `_enriched.csv` indien gemaakt, anders `_decoded.csv`; controleer de benodigde kolommen |
| Staat1cho / Staat van de Onderwijsinstelling | Standaard `_enriched.csv`, met categorische labels | `_decoded.csv`, met ruwe `afkorting_vak` en DUO-cijfers ×10 |

Een VAKHAVW `_enriched.csv` wordt niet gemaakt wanneer hij identiek is aan `_decoded.csv`. Dit is bestaand gedrag; een consument mag die bestandsnaam dus niet onvoorwaardelijk eisen.

Stap niet algemeen met Staat1cho over naar EV `_decoded.csv`: de huidige cohortfilters verwachten labels. De huidige Staat1cho-inleeshelper herkent numerieke nul-labels mede aan het oude `>`-patroon. Na de labelcorrectie kunnen bekende nulwaarden extra waarschuwingen veroorzaken; deze afstemming en echte R-ketentests blijven een acceptatiepunt. Geen geregistreerd examen is een ontbrekend diplomajaar, geen geldige jaartalwaarde die moet worden teruggewonnen.

Er wordt door deze wijzigingen **geen automatische pseudonimisering toegevoegd**. `OutputConfig.encrypt` blijft gereserveerd en niet actief. BSN, DUO-PGN, onderwijsnummer en lokaal studentnummer zijn niet vanzelf dezelfde sleutel.

## Regressiecontrole

Snapshots bevatten nu SHA256-hashes, rij-aantallen, kolomnamen en Parquet-dtypes. Onleesbare output en onverwachte bestanden laten de controle falen. Oude snapshots zonder hashes worden expliciet als schema-only controle gemeld. Een nieuwe baseline mag niet van onleesbare bestanden worden gegenereerd.

De drie gecontroleerde demo-configuraties bevatten:

| Configuratie | Producten |
|---|---:|
| `csv-raw` | 15 |
| `csv-decoded-enriched` | 18 |
| `full-pipeline` | 23, inclusief EV/VAKHAVW en vijf Parquetbestanden |

Op de meegeleverde demo zijn alle 115 EV-bronkolommen behouden in decoded-output, bevat VAKHAVW decoded 24 kolommen en zijn de vijf CSV/Parquet-paren inhoudelijk gelijk. Opnieuw `decode` en `enrich` uitvoeren verandert de volledige snapshot niet.

Dit is geen releasegoedkeuring: lokale tests draaiden op Python 3.11 met de gelockte kernbibliotheken. De gebouwde Python 3.13-omgeving mist `_ssl` en `_ctypes` en kon de tests niet uitvoeren. CI op de officiële Python-omgeving, echte MinIO/S3-uitvoering en de R-/model-/dashboardketen moeten nog worden beoordeeld.
