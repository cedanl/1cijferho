# Bevindingen decodeer-stap EV-bestanden

Onderzoek uitgevoerd op branch `74-bug-hoogste_vooropleiding_omschrijving-is-leeg-in-enriched-ev-bestand`
met DEMO-data (`EV299XX24_DEMO`, 43.663 rijen).

---

## Structuur output-bestanden (geverifieerd)

| Bestand | Grootte | Kolommen | Beschrijving |
|---|---|---|---|
| `EV*.csv` | 14 MB | 115 | Ruwe data, oorspronkelijke codes, gemixte hoofdletters |
| `EV*_decoded.csv` | 39 MB | 179 (+64) | DEC-lookups toegevoegd als extra kolommen, codes ongewijzigd |
| `EV*_enriched.csv` | 152 MB | 179 | Zelfde kolommen als decoded, maar codes vervangen door labels via `variable_metadata.json` |

Voorbeeld: `geslacht` in `_decoded` = `V`/`M`, in `_enriched` = `vrouw`/`man`.

---

## Opgelost

### 1. hoogste_vooropleiding_omschrijving altijd leeg
**Kolommen:** `hoogste_vooropleiding_omschrijving`, `hoogste_vooropleiding_binnen_het_ho_omschrijving`

**Oorzaak:** De join tussen EV-codes en `Dec_vooropl.asc` / `Dec_vopl.asc` matcht nooit:
- EV-bestand heeft 3-cijferige codes zonder leading zeroes: `403`
- DEC-bestand heeft 5-cijferige codes met leading zeroes: `00403`
- De normalisatie gebruikte `.str.zfill(2)` (padding tot minimaal 2 tekens), wat niets
  doet voor codes die al langer zijn dan 2 tekens.

**Fix:** `.str.zfill(2)` vervangen door `.str.strip_chars_start("0").str.replace("^$", "0")`
op 4 plekken in `decoder.py` (functies `decode_fields` en `decode_fields_dec_only`,
zowel DEC-kant als EV-kant). Strip leading zeroes van beide kanten zodat `"00403"` en
`"403"` beide naar `"403"` normaliseren.

**Status:** Opgelost. Beide kolommen 100% gevuld na fix.

---

### 2. postcodecijfers_van_de_hoogste_vooropl_voor_het_ho krijgt geen decoded kolommen

**Kolom zonder decoded uitbreiding:** `postcodecijfers_van_de_hoogste_vooropl_voor_het_ho`
(31.9% leeg — legitiem, maar de gevulde waarden krijgen gemeentenaam/gemeentecode)

**Oorzaak:** Encoding-bug bij het lezen van de DEC-metadata. De `ó` in de variabelenaam
`Postcodecijfers van de hoogste vooropl. vóór het HO` wordt opgeslagen als `\ufffd`
(UTF-8 replacement character). Na `strip_accents` valt het karakter volledig weg:
`vóór` → `vr`. Fuzzy match (cutoff=0.8) bridget het gat: ratio `vr`/`voor` ≈ 0.97.
Plus: multi-word kolomnamen worden nu correct geëxtraheerd via double-space split
(`content[1].split("  ")[0]` in plaats van `.split()[0]`).

**Fix:** Fuzzy match fallback in `decode_fields` en `decode_fields_dec_only` (cutoff=0.8),
plus correcte extractie van multi-word kolomnamen.

**Status:** Opgelost. `postcodecijfers_van_de_hoogste_vooropl_voor_het_ho_gemeentenaam_per_1_januari_2024`
aanwezig in output (31.9% leeg — legitiem).

---

### 3. instelling_van_de_hoogste_vooropl_voor_het_ho en instelling_van_de_hoogste_vooropleiding krijgen geen _naam

**Kolommen zonder _naam:**
- `instelling_van_de_hoogste_vooropl_voor_het_ho` (15.3% leeg, rest WEL gevuld)
- `instelling_van_de_hoogste_vooropleiding` (13.7% leeg, rest WEL gevuld)

**Oorzaak:** `Dec_instellingscode.csv` laadt niet. Het bestand bevat schoolnamen met
ingebedde aanhalingstekens (bijv. `"de Bontekoe" NTC-VO`) die niet conform RFC 4180
zijn geescaped. Polars weigert het bestand te parsen.

**Fix:** `quote_char=None` fallback bij het laden van DEC-bestanden die de strikte
RFC 4180 quote-escaping schenden.

**Status:** Opgelost. `instelling_van_de_hoogste_vooropl_voor_het_ho_naam` aanwezig
in output (15.3% leeg — legitiem, bronkolom zelf 15.3% leeg).

---

### 4. _enriched identiek aan _decoded (variable_metadata.json niet gebruikt)

**Symptoom:** `EV*_enriched.csv` had exact dezelfde grootte als `EV*_decoded.csv`;
codes zoals `M`/`V` werden niet omgezet naar `man`/`vrouw`.

**Oorzaak:** `load_variable_mappings` zocht het bestand op een hardcoded pad
`data/00-metadata/json/variable_metadata.json`, maar de pipeline schrijft het naar
`data/02-output/DEMO/metadata/json/variable_metadata.json`. Bestand niet gevonden →
lege mapping → geen labels.

**Fix:** `variable_metadata_path` parameter toegevoegd aan `decode_fields`; `pipeline.py`
bepaalt het correcte pad (`metadata_dir/json/variable_metadata.json`) en geeft dit door.

**Status:** Opgelost. `_enriched` is nu 152 MB vs `_decoded` 39 MB; 90 variabele-mappings
worden toegepast (bijv. `geslacht`: 43.663/43.663 rijen gemapped).

---

---

## Geen issue (legitiem leeg)

| Kolom(men) | % leeg | Verklaring |
|---|---|---|
| `nationaliteit_2_*` (5 kolommen) | 87.4% | Bronkolom `nationaliteit_2` zelf 87.4% leeg |
| `nationaliteit_3_*` (5 kolommen) | 99.8% | Bronkolom `nationaliteit_3` zelf 99.8% leeg |
| `instelling_van_de_hoogste_vooropl_binnen_het_ho_naam` | 51.6% | Bronkolom zelf 51.6% leeg; 0 gevallen waarbij bron gevuld maar naam leeg |
| `vestigingsnummer_diploma` | 74.6% | Verwacht: geen diploma → geen vestigingsnummer |
| `soort_diploma_instelling` | 74.6% | Idem |

---

## Buiten scope: ontbrekend DEC-bestand

`Dec_vest_ho.asc` wordt in de DEC-metadata verwacht voor de kolommen `Vestigingsnummer`
en `Vestigingsnummer diploma`, maar dit bestand bestaat niet in de DUO-levering.
Alleen `Dec_vestnr_ho.asc` en `Dec_vestnr_ho_compleet.asc` zijn aanwezig.
`Dec_vestnr_ho.asc` is bovendien "unmatched" (geen bestandsbeschrijving-xlsx).
Dit is een naamsverschil in de DUO-levering, buiten de scope van de decoder.
