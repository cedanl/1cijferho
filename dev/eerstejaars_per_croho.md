# Eerstejaars per CROHO-opleiding

Hoe je uit de ruwe 1CijferHO-data een telling maakt van eerstejaars en
hoofdinschrijvingen per opleiding, collegejaar en herkomstgroep.

---

## Bronbestanden

| Bestand | Wat het is |
|---|---|
| `EV*.csv` / `EV*.parquet` | Hoofdbestand met één rij per inschrijving |
| `Dec_opleidingscode.csv` | Opzoektabel: ISAT-code → naam opleiding |

De pipeline zet de veldnamen om naar snake_case (spaties en accenten worden
underscores). De namen hieronder zijn de snake_case varianten.

---

## Stap 1 — Filters

Behoud alleen rijen die voldoen aan **alle drie** de volgende voorwaarden:

```
indicatie_actief_op_peildatum       = '1'    -- actief op 1 oktober
soort_inschrijving_hoger_onderwijs  IN ('1','6','A')  -- hoofdinschrijving
opleidingsfase_actueel              IN ('B','M')      -- bachelor of master
```

**Waarom deze filters?**

- `indicatie_actief_op_peildatum = '1'` — de officiële peildatum voor HO-tellingen
  is 1 oktober. Rijen met waarde 2, 3 of 4 zijn uitschrijvingen of
  na-inschrijvingen die niet meetellen.
- `soort_inschrijving_hoger_onderwijs IN ('1','6','A')` — elke student telt
  precies één keer als hoofdinschrijving. Waarde 1 = regulier, 6 =
  uitwisselingsstudent, A = aangewezen instelling.
- `opleidingsfase_actueel IN ('B','M')` — filter op bachelor (B) en master (M);
  associate degrees, postinitiële masters en ongedeelde opleidingen vallen
  buiten scope.

---

## Stap 2 — Herkomstgroep afleiden

Maak een nieuwe kolom `eer_nl_niet_eer` op basis van twee bestaande kolommen:

```
ALS   nationaliteit_1 = '0001'                  → 'NL'
ANDERS ALS indicatie_eer_op_peildatum_1_oktober = 'J'  → 'EER'
ANDERS                                           → 'niet-EER'
```

**Toelichting:**
- Nationaliteitscode `0001` staat voor de Nederlandse nationaliteit
  (zie `Dec_nationaliteitscode.csv`).
- `indicatie_eer_op_peildatum_1_oktober` is inclusief Nederland, vandaar dat de
  NL-check eerst komt.
- EER = Europese Economische Ruimte + Zwitserland + Suriname (DUO-definitie).

---

## Stap 3 — Opleidingsnaam opzoeken

Join `opleiding_actueel_equivalent` met `Dec_opleidingscode` om de naam erbij te
krijgen:

```
LEFT JOIN Dec_opleidingscode
    ON opleiding_actueel_equivalent = opleidingscode
```

Resultaat: nieuwe kolom `groepeernaam_croho` (naam van de opleiding).

**Waarom `opleiding_actueel_equivalent` en niet `opleidingscode`?**  
De actuele equivalent is de genormaliseerde code die DUO gebruikt om historische
ISAT-codes te herleiden naar de huidige code. Dit maakt vergelijking over jaren
consistent.

---

## Stap 4 — Renames

| Originele kolom | Canonieke naam |
|---|---|
| `inschrijvingsjaar` | `collegejaar` |
| `opleiding_actueel_equivalent` | `isatcode` |
| `opleidingsfase_actueel` | `examentype_code` |

---

## Stap 5 — Groeperen en aggregeren

Groepeer op:

```
collegejaar, isatcode, groepeernaam_croho, examentype_code, eer_nl_niet_eer
```

Bereken per groep:

| Uitvoerkolom | Berekening |
|---|---|
| `aantal_eerstejaars_croho` | Aantal rijen waar `indicatie_eerstejaars_opl_actueel_equivalent = '1'` |
| `aantal_hoofdinschrijvingen` | Totaal aantal rijen in de groep |

In SQL:

```sql
SELECT
    inschrijvingsjaar                                    AS collegejaar,
    opleiding_actueel_equivalent                         AS isatcode,
    d.naam_opleiding                                     AS groepeernaam_croho,
    opleidingsfase_actueel                               AS examentype_code,
    CASE
        WHEN nationaliteit_1 = '0001'                        THEN 'NL'
        WHEN indicatie_eer_op_peildatum_1_oktober = 'J'      THEN 'EER'
        ELSE 'niet-EER'
    END                                                  AS eer_nl_niet_eer,
    SUM(CASE WHEN indicatie_eerstejaars_opl_actueel_equivalent = '1'
             THEN 1 ELSE 0 END)                          AS aantal_eerstejaars_croho,
    COUNT(*)                                             AS aantal_hoofdinschrijvingen
FROM ev_data e
LEFT JOIN dec_opleidingscode d
    ON e.opleiding_actueel_equivalent = d.opleidingscode
WHERE
    indicatie_actief_op_peildatum              = '1'
    AND soort_inschrijving_hoger_onderwijs     IN ('1', '6', 'A')
    AND opleidingsfase_actueel                 IN ('B', 'M')
GROUP BY
    inschrijvingsjaar,
    opleiding_actueel_equivalent,
    d.naam_opleiding,
    opleidingsfase_actueel,
    eer_nl_niet_eer
ORDER BY
    collegejaar, isatcode, examentype_code, eer_nl_niet_eer;
```

---

## Uitvoer

Het resultaat heeft één rij per combinatie van collegejaar × opleiding ×
examentype × herkomstgroep:

| collegejaar | isatcode | groepeernaam_croho | examentype_code | eer_nl_niet_eer | aantal_eerstejaars_croho | aantal_hoofdinschrijvingen |
|---|---|---|---|---|---|---|
| 2023 | 56553 | B Gezondheidswetenschappen | B | NL | 42 | 210 |
| 2023 | 56553 | B Gezondheidswetenschappen | B | EER | 3 | 18 |
| 2023 | 56553 | B Gezondheidswetenschappen | B | niet-EER | 1 | 5 |

---

## Script draaien (Python)

```bash
uv run python dev/eerstejaars_per_croho.py
```

Of met eigen paden:

```bash
uv run python dev/eerstejaars_per_croho.py \
    --input  data/02-output/MIJN_DATA \
    --output data/02-output/MIJN_DATA/eerstejaars_croho.csv
```

Het script accepteert zowel CSV als Parquet als invoer en normaliseert
kolomnamen automatisch (snake_case).
