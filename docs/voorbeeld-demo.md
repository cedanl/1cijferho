---
icon: lucide/flask-conical
---

# Voorbeeld met DEMO-gegevens

In vijf stappen van invoerbestand naar uitvoer, met de DEMO-map uit de repository. De getallen zijn gemeten op de `main`-versie na release 0.1.8. Op een andere versie kunnen ze afwijken.

!!! note "Werkmap"
    Draai de commando's in de hoofdmap van de gekloonde repository. De CLI weigert invoer- en uitvoermappen buiten de werkmap (`Path traversal attempt detected`).

## Stap 1: kijk wat er in de invoer staat

`data/01-input/DEMO/` bevat een EV-bestand (`EV299XX24_DEMO.asc`), een VAKHAVW-bestand, bestandsbeschrijvingen (`.txt`) en Dec-tabellen (`Dec_*.asc`). Het EV-bestand heeft 43.663 regels: één per inschrijving.

## Stap 2: draai de pipeline

```bash
uv sync --extra frontend
eencijferho pipeline --input data/01-input/DEMO --output data/02-output/DEMO
```

De pipeline sluit af met `Pipeline complete. Output files: 27`.

## Stap 3: bekijk de uitvoer

Voor het EV-bestand krijg je drie CSV-varianten en bij elk een Parquet-bestand:

| Bestand | Inhoud |
|---|---|
| `EV299XX24_DEMO.csv` | de ruwe data, omgezet naar CSV |
| `EV299XX24_DEMO_decoded.csv` | de ruwe data plus extra kolommen uit de Dec-tabellen, bijvoorbeeld `geboorteland_naam_land` |
| `EV299XX24_DEMO_enriched.csv` | zoals `_decoded`, en codes zijn vervangen door labels |

Een voorbeeld in kolom `opleidingsvorm`:

| Code in `.csv` en `_decoded.csv` | Label in `_enriched.csv` | Aantal rijen |
|---|---|---|
| `1` | `voltijd` | 42.726 |
| `2` | `deeltijd` | 937 |

Uitleg per kolom staat bij [Decodering](decodering.md).

## Stap 4: controleer het resultaat

1. **Rijen.** De CSV heeft evenveel rijen als de invoer, plus een koptekst: `wc -l data/02-output/DEMO/EV299XX24_DEMO.csv` geeft 43.664.
2. **Omzetting.** Open `data/02-output/DEMO/metadata/logs/(5)_conversion_log_latest.json`. Daar hoort `"failed_conversions": 0` te staan.
3. **Niet ondersteunde bestanden.** Dezelfde log noemt 6 bestanden bij `unsupported_files`, zoals `Croho.asc`. Voor die bestanden staat geen bestandsbeschrijving in de DEMO-map. Dat is verwacht, geen fout.
4. **Decoderen.** Open `(7)_decoding_log_latest.json`. Onder `details` staat per bestand hoeveel kolommen zijn gedecodeerd (`columns_decoded`) en welke codes geen match hadden (`unmatched_codes`). `summary` telt het totaal.

Op DEMO zou `--strict` de run afkeuren, omdat twee Dec-tabellen niet laden en er niet-gematchte codes zijn. Zie [Problemen oplossen](problemen-oplossen.md).

## Stap 5: koppel een studentnummer (optioneel)

```bash
eencijferho pipeline --input data/01-input/DEMO --output data/02-output/DEMO \
  --bsn-mapping-file data/koppeling_bsn_studentnummer_DEMO.csv
```

Het studentnummer komt als kolom `studentnummer` in de EV- en VAKHAVW-bestanden. Over wat dit voor persoonsgegevens betekent: zie [Privacy en persoonsgegevens](privacy.md).
