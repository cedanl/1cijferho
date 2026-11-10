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
- **VAKHAVW decodeert alle vier de velden uit de Vakken-lay-out** (#204). `_decoded.csv`
  krijgt naast de `vakcode_*`-kolommen ook `brinnummer_vo_instelling_*`,
  `vestigingsnummer_vo_vestiging_*` en `vooropleiding_oorspronkelijke_code_*`. Daarnaast
  verschijnen `Dec_brinnummer` en `Dec_brinvestigingsnummer` als CSV/Parquet (met een door
  1CijferHO afgeleide lay-out). In `(5)_conversion_log_latest.json` staan bestanden zonder
  bestandsbeschrijving voortaan als `unsupported` (lijst `unsupported_files`) in plaats van
  als `skipped`; de EV-output verandert niet.

## NFWA en Staat1cho zijn verschillende consumenten

| Consument | EV | VAKHAVW |
|---|---|---|
| NFWA | `_enriched.csv` | `_enriched.csv` indien gemaakt, anders `_decoded.csv` |
| Staat1cho / Staat van de Onderwijsinstelling | `_enriched.csv`, met categorische labels | `_decoded.csv` |

Een VAKHAVW `_enriched.csv` wordt niet gemaakt wanneer hij identiek is aan `_decoded.csv`.
Dit is bestaand gedrag; een consument mag die bestandsnaam dus niet onvoorwaardelijk eisen.

Er wordt door deze wijzigingen **geen automatische pseudonimisering toegevoegd**. BSN,
DUO-PGN, onderwijsnummer en lokaal studentnummer zijn niet vanzelf dezelfde sleutel.

## EV en VAKHAVW koppelen op persoonsgebonden nummer

EV en VAKHAVW koppel je op `persoonsgebonden_nummer`. Die kolom is tekst en blijft geschreven
zoals in het bronbestand (#219, #222). De schrijfwijze hoort dus bij de sleutel:
`000000000002` en `2` zijn als tekst verschillend.

In de DEMO-bestanden staat het nummer in EV zonder voorloopnullen (`2`) en in VAKHAVW met
voorloopnullen op twaalf posities (`000000000002`). Een join op de kolom zelf vindt daar geen
van de 17.053 personen uit VAKHAVW; een join op een genormaliseerde sleutel vindt ze alle
17.053. Wie de CSV inleest met type-inferentie (zoals `read.csv` in R) merkt dit niet, omdat
de kolom dan een getal wordt. Wie de Parquet-output of de CSV als tekst inleest, merkt het
wel.

Controleer bij een eigen levering hoe beide bestanden het nummer schrijven voordat je
koppelt. Verschilt de schrijfwijze, maak dan in beide tabellen een afgeleide sleutel en laat
de oorspronkelijke kolom staan:

```python
import polars as pl


def with_join_key(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("persoonsgebonden_nummer").str.zfill(12).alias("koppelsleutel")
    )


ev = with_join_key(pl.read_parquet("data/02-output/DEMO/EV299XX24_DEMO_enriched.parquet"))
vakhavw = with_join_key(pl.read_parquet("data/02-output/DEMO/VAKHAVW_99XX_DEMO_decoded.parquet"))

# Alle vakregels van personen die ook in EV voorkomen.
vakken_met_inschrijving = vakhavw.join(ev, on="koppelsleutel", how="semi")
```

## Regressiecontrole (geldt vanaf nu)

- De CI-pipeline draait met `--fail-on-empty-output`: een run die nul outputs oplevert faalt,
  in plaats van stil te slagen.
- De converter levert een reproduceerbare rijvolgorde (de invoervolgorde), zodat twee runs op
  dezelfde invoer dezelfde bestanden opleveren.
- De snapshotbaselines in `snapshots/` (csv-raw, csv-decoded-enriched, full-pipeline) zijn
  versie 2 en opnieuw gegenereerd na #219/#221/#222. Elke baseline bevat alle bestanden die de
  configuratie buiten `metadata/` schrijft, voor full-pipeline dus ook de EV- en
  VAKHAVW-bestanden en de Parquet-output.
- Een CSV wordt vergeleken op een SHA256 over de bestandsinhoud, na het omzetten van CRLF
  naar LF; een Parquet-bestand op een `data_hash` over schema en celwaarden, niet op de ruwe
  bytes, omdat de Parquet-footer de schrijver- en bibliotheekversie bevat. Een v2-baseline
  zonder hash voor een bestand faalt.
- De validatie is strikt (`--strict-files`): een ontbrekend, gewijzigd of onverwacht extra
  bestand laat de controle falen. De validator weigert een baseline te maken van onleesbare
  bestanden en meldt een onleesbaar outputbestand als fout, zonder celwaarden te tonen.
- `.gitattributes` houdt de DEMO-invoer op Windows byte-gelijk (`-text`), zodat de
  hashes op de ubuntu- en windows-runner gelijk zijn. Twee runs op dezelfde invoer, ook op
  één CPU, geven dezelfde snapshot.

Een wijziging in de output die bedoeld is, vraagt dus een nieuwe baseline:
`python -m eencijferho.utils.snapshot_validator --output <dir> --snapshot <baseline> --generate`
na een pipeline-run met dezelfde vlaggen als `.github/workflows/pipeline.yml`.
