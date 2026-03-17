# Bevindingen decodeer-stap EV-bestanden

Onderzoek uitgevoerd op branch `74-bug-hoogste_vooropleiding_omschrijving-is-leeg-in-enriched-ev-bestand`
met DEMO-data (`EV299XX24_DEMO_enriched.csv`, 43.663 rijen).

---

## Opgelost

### hoogste_vooropleiding_omschrijving altijd leeg
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

## Bugs (niet opgelost)

### Bug 1: postcodecijfers_van_de_hoogste_vooropl_voor_het_ho krijgt geen decoded kolommen

**Kolom zonder decoded uitbreiding:** `postcodecijfers_van_de_hoogste_vooropl_voor_het_ho`
(31.9% leeg — legitiem, maar de gevulde waarden krijgen geen gemeentenaam/gemeentecode)

Ter vergelijking krijgt `postcodecijfers_student_op_1_oktober` wel decoded uitbreiding:
`_postbus_j_n`, `_gemeentecode_per_1_januari_2024`, `_gemeentenaam_per_1_januari_2024`.

**Oorzaak:** Encoding-bug bij het lezen van de DEC-metadata. De `ó` in de variabelenaam
`Postcodecijfers van de hoogste vooropl. vóór het HO` wordt niet als Latin-1 ingelezen
maar als garbage (`\ufffd`). Na `strip_accents` valt het karakter volledig weg:

```
Genormaliseerde naam in DEC-meta:  postcodecijfers_van_de_hoogste_vooropl_vr_het_ho
Werkelijke kolomnaam in EV:        postcodecijfers_van_de_hoogste_vooropl_voor_het_ho
                                                                              ^^^^
```

De pipeline-log bevestigt dit:
```
Skipping 'Postcodecijfers van de hoogste vooropl. v??r het HO'
  (normalized: 'postcodecijfers_van_de_hoogste_vooropl_vr_het_ho')
  - not in main DataFrame. No close match found.
```

**Verwacht gedrag:** `vóór` → na strip_accents `voor` → genormaliseerd
`postcodecijfers_van_de_hoogste_vooropl_voor_het_ho` → match.

---

### Bug 2: instelling_van_de_hoogste_vooropl_voor_het_ho en instelling_van_de_hoogste_vooropleiding krijgen geen _naam

**Kolommen zonder _naam:**
- `instelling_van_de_hoogste_vooropl_voor_het_ho` (15.3% leeg, rest WEL gevuld)
- `instelling_van_de_hoogste_vooropleiding` (13.7% leeg, rest WEL gevuld)

Beide horen te worden opgelost via `Dec_instellingscode.asc` (staat zo in DEC-metadata).

**Oorzaak:** `Dec_instellingscode.csv` laadt niet. Het bestand bevat schoolnamen met
ingebedde aanhalingstekens (bijv. `"de Bontekoe" NTC-VO`) die niet conform RFC 4180
zijn geescaped. Polars weigert het bestand te parsen:

```
[decoder] Warning: Could not load Dec_instellingscode.csv:
  could not parse `"de Bontekoe" NTC-VO` as dtype `str`
  Field `"de Bontekoe" NTC-VO` is not properly escaped.
```

Voorbeeldrijen in het CSV:
```
04NA;"ABBS ""de Fluessen""";8582;OUDEGA DE FRYSKE MARREN;...
00AR;"BS ""De Maasparel""";6107;STEVENSWEERT;...
```

Sommige rijen gebruiken `""` als escape (correct), maar andere rijen bevatten
ongeescapte quotes midden in een veld.

**Fix-richting:** Laad het bestand met `quote_char=None` of `ignore_errors=True`,
of pre-process het bestand om de quotes te saneren voor het inladen.

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
