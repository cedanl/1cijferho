# Outputcontract: migratiegids voor de komende release

De komende release bundelt drie wijzigingen aan de output (#219, #221, #222). Dit document
beschrijft wat er voor consumenten verandert en wat al vandaag geldt. De wijzigingen zijn pas
van kracht **vanaf de release die deze PR's bevat** — een versienummer in `pyproject.toml` is
geen bewijs dat ze in een eerdere release zitten.

## Wat verandert er in de komende release

- **Parquet wordt volledig tekst en volgt de CSV** (#219). Kolomnamen komen overeen met de
  CSV (standaard snake_case) en elke kolom is `String`, zodat identifiers als BSN hun
  voorloopnullen behouden. Numerieke analyses moeten hun numerieke velden expliciet casten;
  houd identifiers als tekst. De CSV-output verandert door deze fix niet.
- **Voorloopnullen blijven behouden in codekolommen** (#222). De decoder herschrijft de
  datakolom niet meer bij het matchen op Dec-codes: `00411` blijft `00411`. Externe joins die
  op de voorheen gestripte codes waren gebouwd, moeten hun koppelsleutel aanpassen —
  normaliseer een afgeleide koppelsleutel, niet de oorspronkelijke codekolom.
- **`>`-notities worden metadata, geen onderdeel van een label** (#221). DUO's
  continuatienoot (`> 0000 voor overige inschrijvingen`) verhuist van het waardelabel naar de
  variabelebeschrijving; niet-uitputtende waardelijsten krijgen de vlag
  `non_exhaustive_values`. Alle 16 markers in de bestandsbeschrijving zitten op
  verblijfsjaar- en diplomajaar-variabelen. Consumenten die lege waarden herkennen aan het
  `>`-patroon in het label moeten dat aanpassen; voor Staat1cho is dit belegd in
  cedanl/staat-van-onderwijsinstelling#50.

## NFWA en Staat1cho zijn verschillende consumenten

| Consument | EV | VAKHAVW |
|---|---|---|
| NFWA | `_enriched.csv` | `_enriched.csv` indien gemaakt, anders `_decoded.csv` |
| Staat1cho / Staat van de Onderwijsinstelling | `_enriched.csv`, met categorische labels | `_decoded.csv` |

Een VAKHAVW `_enriched.csv` wordt niet gemaakt wanneer hij identiek is aan `_decoded.csv`.
Dit is bestaand gedrag; een consument mag die bestandsnaam dus niet onvoorwaardelijk eisen.

Er wordt door deze wijzigingen **geen automatische pseudonimisering toegevoegd**. BSN,
DUO-PGN, onderwijsnummer en lokaal studentnummer zijn niet vanzelf dezelfde sleutel.

## Regressiecontrole (geldt vanaf nu)

- De CI-pipeline draait met `--fail-on-empty-output`: een run die nul outputs oplevert faalt,
  in plaats van stil te slagen.
- De converter levert een reproduceerbare rijvolgorde (de invoervolgorde), zodat twee runs op
  dezelfde invoer dezelfde bestanden opleveren.
- De snapshotvalidator ondersteunt SHA256-inhoudhashes (snapshotversie 2), weigert een
  baseline te maken van onleesbare bestanden, en meldt een onleesbaar outputbestand als fout.
  De huidige baselines zijn nog versie 1 (alleen schema); de validator meldt dat expliciet
  als waarschuwing.

**Bij de release met #219/#221/#222 horen de baselines opnieuw gegenereerd te worden als
versie 2** (met hashes) en gaat de validatie strikt (`--strict-files`), zodat ook een
onverwacht extra bestand de controle laat falen. Volledige hash-stabiliteit van de
decoded/enriched-bestanden vraagt daarnaast een deterministische join-volgorde in de decoder
(`maintain_order="left"`), zoals voorgesteld bij #222.
