# Decodering in 1CijferHO

Dit document beschrijft welke DUO-velden worden gedecodeert naar labels en welke nog niet.

**Doel:** Voor elke DUO-regel ("te decoderen met...") traceren of deze geïmplementeerd is. Dit maakt auditwerk herhaalbaar en helpt gebruikers bepalen welke kolommen bruikbaar zijn.

---

## Samenvatting

| Status | Velden | Opmerkingen |
|--------|--------|------------|
| ✅ **Geïmplementeerd** | Geboorteland, Nationaliteiten, Opleidingscode, Vooropleidingen, Instellingen, Postcodecijfers, Brinnummers | Via `Dec_*.asc` lookup tables |
| ❌ **Niet geïmplementeerd** | Vakcode (Vakkenbestanden), Brinnummer + Vestigingsnummer (VAKHAVW combinatie) | Vakkenbestanden metadata niet ingelezen, samengestelde sleutels niet ondersteund |
| ⚠️ **Gedeeltelijk** | Vakkenbestanden (Vakcode werkt, Brinnummer/Vestigingsnummer niet) | Alleen Vakcode via handmatige patch |

---

## Geïmplementeerde Decodeerregels

### 1. Geboorteland (`Dec_landcode.asc`)

**DUO-documentatie:** "Ten behoeve van decodering van velden Geboorteland, Geboorteland ouder 1, Geboorteland ouder 2"

| Veld | Kolom | Type | Status |
|------|-------|------|--------|
| `Geboorteland` | character (4 digits) → label | Code → Naam land | ✅ **Geïmplementeerd** |
| `Geboorteland ouder 1` | character (4 digits) → label | Code → Naam land | ✅ **Geïmplementeerd** |
| `Geboorteland ouder 2` | character (4 digits) → label | Code → Naam land | ✅ **Geïmplementeerd** |

**Notitie:** De lookup bevat ook "Migratieachtergrond" kolommen, maar deze worden niet in de output gebruikt (AVG-gevoelig).

**Risico's:**
- Niet-gemapte codes (bijv. toekomstige landen) blijven als code staan
- Leeg veld ("") bevat mogelijk eerste item uit lookup (dict-afhankelijk)

---

### 2. Nationaliteit (`Dec_nationaliteitscode.asc`)

**DUO-documentatie:** "Ten behoeve van decodering van velden Nationaliteit 1, Nationaliteit 2, Nationaliteit 3"

| Veld | Kolom | Type | Status |
|------|-------|------|--------|
| `Nationaliteit 1` | character (4 digits) → label | Code → Omschrijving | ✅ **Geïmplementeerd** |
| `Nationaliteit 2` | character (4 digits) → label | Code → Omschrijving | ✅ **Geïmplementeerd** |
| `Nationaliteit 3` | character (4 digits) → label | Code → Omschrijving | ✅ **Geïmplementeerd** |

**Risico's:**
- Dezelfde als Geboorteland
- Meerdere nationaliteiten kunnen inconsistent gelabeld worden

---

### 3. Opleidingscode (`Dec_opleidingscode.asc`)

**DUO-documentatie:** "Ten behoeve van decodering van veld Opleidingscode"

| Veld | Kolom | Type | Status |
|------|-------|------|--------|
| `Opleidingscode` | character (5 digits) → label | Code → Naam opleiding | ✅ **Geïmplementeerd** |

**Output:**
- `_decoded.csv`: Opleidingscode (label)
- `_enriched.csv`: Naam opleiding (volledige tekst)

---

### 4. Vooropleiding (`Dec_vopl.asc`, `Dec_vooropl.asc`)

**DUO-documentatie:** "Ten behoeve van decodering van velden Hoogste vooropleiding v.d.HO / binnen het HO"

| Veld | Dec-bestand | Type | Status |
|------|-------------|------|--------|
| `Hoogste vooropleiding v.d. HO` | `Dec_vopl.asc` | Code → Omschrijving | ✅ **Geïmplementeerd** |
| `Hoogste vooropleiding binnen het HO` | `Dec_vopl.asc` | Code → Omschrijving | ✅ **Geïmplementeerd** |
| `Hoogste vooropleiding` | `Dec_vopl.asc` | Code → Omschrijving | ✅ **Geïmplementeerd** |
| `Hoogste vooropleiding orig. code` | `Dec_vooropl.asc` | Code → Omschrijving | ✅ **Geïmplementeerd** |
| `Hoogste vooropleiding binnen HO orig.` | `Dec_vooropl.asc` | Code → Omschrijving | ✅ **Geïmplementeerd** |

**Risico's:**
- `Dec_vooropl.asc` bevat ook "Onderwijssector" (HO/MBO/VO/OVG), maar alleen de omschrijving wordt gebruikt
- Vooropleiding buiten HO mag niet voorkomen in bepaalde inschrijvingsjaren

---

### 5. Instellingen (`Dec_ho-inst.asc`, `Dec_actuele_instelling.asc`, `Dec_instellingscode.asc`)

**DUO-documentatie:** "Ten behoeve van decodering van velden Instellingscode / Actuele instelling"

| Veld | Dec-bestand | Type | Status |
|------|-------------|------|--------|
| `Instellingscode` | `Dec_ho-inst.asc` | Code → Naam | ✅ **Geïmplementeerd** |
| `Actuele instelling` | `Dec_actuele_instelling.asc` | Code → Naam | ✅ **Geïmplementeerd** |

**Risico's:**
- Instellingen veranderen (fusies, sluitingen) — historische codes kunnen verdwenen zijn uit de lookup
- Dekking loopt achter (actuele instelling alleen tot inschrijvingsjaar X)

---

### 6. Postcodecijfers (`Dec_postcodecijfers_YYYY.asc`)

**DUO-documentatie:** "Ten behoeve van decodering van velden Postcodecijfers"

| Veld | Dec-bestand | Type | Status |
|------|-------------|------|--------|
| `Postcodecijfers student op 1 okt` | `Dec_postcodecijfers_YYYY.asc` | Code (4 digits) → Regio | ✅ **Geïmplementeerd** |
| `Postcodecijfers vooropl. v.d. HO` | `Dec_postcodecijfers_YYYY.asc` | Code (4 digits) → Regio | ✅ **Geïmplementeerd** |

**Risico's:**
- Postcodecijfers veranderen per jaar (nieuwe lookups per inschrijvingsjaar)
- Pipeline zit soms vast met oude lookup (zie issue #198)

---

## Niet Geïmplementeerde Decodeerregels

### 1. Vakkenbestanden — Vakcode (`Dec_vakcode.asc`)

**DUO-documentatie (Bestandsbeschrijving_Vakkenbestanden):** "Vakcode te decoderen met Dec_vakcode.asc"

| Veld | Dec-bestand | Type | Status |
|------|-------------|------|--------|
| `Vakcode` | `Dec_vakcode.asc` | Code → Naam vak | ⚠️ **Gedeeltelijk** |

**Implementatie:**
- Vakcode wordt gedecodeert, maar **alleen via handmatige patch** in `extractor.py:22` (hardcoded dekking)
- Vakkenbestanden-metadata wordt niet ingelezen (zie volgende punt)

**Probleem:** Vakkenbestanden_*.json wordt niet gegenereerd, dus er zijn geen vakmetadata in de pipeline. Dekodeer-handeling is "blind" (geen input).

**Gevolg:** Vakcode in VAKHAVW-bestanden wordt gedecodeert tegen wat er toevallig was hardcoded.

---

### 2. Vakkenbestanden — Brinnummer VO

**DUO-documentatie (Bestandsbeschrijving_Vakkenbestanden):** 
- "Brinnummer VO-instelling: te decoderen met Dec_brinnummer.asc"
- "Vestigingsnummer VO-vestiging: in combinatie met Brinnummer"

| Veld | Dec-bestand | Type | Status |
|------|-------------|------|--------|
| `Brinnummer VO-instelling` | `Dec_brinnummer.asc` | Code (6) + Vestigingsnr → Naam | ❌ **Niet geïmplementeerd** |
| `Vestigingsnummer VO-vestiging` | `Dec_brinnummer.asc` | Combinatie | ❌ **Niet geïmplementeerd** |

**Probleem:** Vakkenbestanden-metadata wordt niet geëxtraheerd (zie Vakcode hierboven).

**Gevolg:** Deze codes blijven ongedecodeert in de output.

**Vooropleiding oorspronkelijke code (Vakkenbestanden):**

| Veld | Dec-bestand | Type | Status |
|------|-------------|------|--------|
| `Vooropleiding oorspronkelijke code` | `Dec_vooropl.asc` | Code → Omschrijving | ❌ **Niet geïmplementeerd** |

**Probleem:** Idem — Vakkenbestanden-metadata niet ingelezen.

---

### 3. Samengestelde Sleutels (Dec-bestanden)

**DUO-documentatie (Bestandsbeschrijving_Dec-bestanden):** "Vestigingsnummer Diploma: in combinatie met Brinnummer (}}-marker)"

| Beschrijving | Velden | Sleutel | Status |
|--------------|--------|--------|--------|
| Diploma-vestiging | `Vestigingsnummer diploma` + `Brinnummer` | `Brinnummer}}Vestigingsnr` | ❌ **Niet geïmplementeerd** |
| HO-vestiging volledige sleutel | `Dec_vest_ho.asc` | `Instellingscode}}Vestigingsnr` | ❌ **Niet geïmplementeerd** |

**Probleem:** Pipeline ondersteunt geen samengestelde sleutels (meerderelige lookup). Alleen enkelvoudige kolom-naar-label.

**Gevolg:** 
- Vestigingsnummers met dezelfde code in meerdere instellingen worden verward
- Logbestand geeft "year_mismatch" of "unmatched" maar geen handeling

**Issue:** #205 — "samengestelde sleutel Dec_vestnr_ho_compleet wordt overgeslagen"

---

## Kolom-per-Kolom Decodeergedrag

### Ingestelde Velden (Main EV-bestand)

| Kolom | Type `_decoded` | Type `_enriched` | Gedrag | Risico's |
|-------|-----------------|------------------|--------|----------|
| Diplomajaar | **numeric** | **character** | Gelabeld in _enriched | ⚠️ **SVO:** zie opmerking hieronder |
| Geboorteland | string | string | Code → Label | Niet-gemapte codes + dict-volgorde voor lege waarden |
| Nationaliteit 1–3 | string | string | Code → Label | Idem |
| Instellingscode | string | string | Code → Label | Historische codes verdwenen in lookup |
| Opleidingscode | string | string | Code → Label | Niet-gemapte codes |
| Postcodecijfers `*` | string | string | Code → Regio | Lookup per jaar (fouten als oude lookup gebruikt) |
| Vooropleiding `*` | string | string | Code → Label | Lookup per jaar |
| Vooropleiding oorspronkelijk `*` | string | string | Code → Label | Idem |
| Brinnummer (VO) | character | character | ❌ **Niet gedecodeert** | Code blijft staan |
| Vestigingsnummer (VO) | character | character | ❌ **Niet gedecodeert** | Code blijft staan |
| Vakcode (VAKHAVW) | character | character | ⚠️ **Hardcoded dekking** | Foutgevoelig, onvolledig |

`*` = lookup via `Dec_postcodecijfers_YYYY.asc`

---

## Bekende Beperkingen

### 1. Vakkenbestanden-Metadata Niet Ingelezen

**Impact:** De volgende velden kunnen niet gedecodeert worden:
- Vakcode (VAKHAVW)
- Brinnummer VO + Vestigingsnummer VO (VAKHAVW)
- Vooropleiding oorspronkelijke code (VAKHAVW)

**Reden:** `process_txt_folder()` luist alleen naar `Bestandsbeschrijving_1cyferho_*.txt` en `Bestandsbeschrijving_Dec-bestanden.txt`, niet `Bestandsbeschrijving_Vakkenbestanden.txt`.

**Issue:** #204 — "Vakken-metadata wordt niet gebruikt — 3 van 4 decodes gebeuren nooit"

### 2. Samengestelde Sleutels Niet Ondersteund

**Impact:** Dit kan geen decodering:
- Vestigingsnummer diploma (vereist Brinnummer + Vestigingsnr combo)
- Vestigingsnummer HO-instelling (vereist Instellingscode + Vestigingsnr combo)

**Reden:** `decode_fields()` mappt kolom → tabel, niet (kolom1, kolom2) → tabel.

**Issue:** #205 — "samengestelde sleutel Dec_vestnr_ho_compleet wordt overgeslagen"

### 3. Niet-Gemapte Codes Blijven Staan

**Gedrag:** Wanneer een code in het bestand niet in de `Dec_*.asc` lookup staat:
- Code blijft als string staan (geen label)
- Kolom bevat deels codes, deels labels
- Geen waarschuwing in log

**Voorbeeld:** Geboorteland `9999` niet in `Dec_landcode.asc` → output: `"9999"` (niet als label)

**Risico:** Gebruiker weet niet dat kolom onvolledig is.

**Issue:** #207 — "ongemapte codes blijven stilzwijgend als code in labelkolom staan"

### 4. Voorloopnullen worden Gestript

**Gedrag (vóór fix):** Code `0001` → lookup → `1` (voorloopnullen weg)

**Probleem:** Code `0001` kan als `"1"` in de lookup ontbreken, dus decode faalt.

**Status:** ✅ **GEFIXED** in #199

### 5. ⚠️ SVO Opmerking (#229)

**Issue:** SVO rendement-berekening verwacht numerieke `diplomajaar`, maar `_enriched.csv` bevat string-waarden (gelabeld: "2023", "geen examen geregistreerd", etc.).

**Gevolg:** `dplyr::na_if(diplomajaar, 0)` faalt → NaN → 74,6% foutieve categorisering.

**Aanbeveling:** SVO-analyses gebruiken **`_decoded.csv`**, niet `_enriched.csv`:
- `_decoded` bevat numerieke diplomajaar (kan gebruikt worden in `na_if()`)
- `_enriched` bevat labels en is geschikt voor visuele analyse, niet numerieke berekeningen

**Oplossing:**
```R
# Gebruik _decoded, niet _enriched
d <- read.csv("/path/to/EV_DEMO_decoded.csv", sep=";")
# diplomajaar is nu numeric → na_if() werkt correct
```

---

## Hoe Dit Document Bijgewerkt Wordt

Wanneer een decode-regel verandert (bijv. Vakkenbestanden support toevoegen):

1. ✅ Implementatie voltooien + tests
2. ✅ Dit document bijwerken: status van ❌ → ✅, risico's toevoegen/verwijderen
3. ✅ Link naar PR toevoegen
4. ✅ `CHANGELOG.md` updaten

---

## Zie Ook

- **README.md** — Projectoverzicht en gebruiksrichtlijnen
- **CHANGELOG.md** — Geschiedenis van fixes (bijv. #199, #205)
- **Implementatie:**
  - `src/eencijferho/core/decoder.py` — `decode_fields()`
  - `src/eencijferho/core/extractor.py` — `process_txt_folder()`
  - `src/eencijferho/utils/dec_validation.py` — Dec-validatie

---

## Contact & Issues

Voor vragen of aanpassingen:
- [Issue #214](https://github.com/cedanl/1cijferho/issues/214) — Dit document
- [Issue #204](https://github.com/cedanl/1cijferho/issues/204) — Vakkenbestanden-metadata
- [Issue #205](https://github.com/cedanl/1cijferho/issues/205) — Samengestelde sleutels
- [Issue #207](https://github.com/cedanl/1cijferho/issues/207) — Niet-gemapte codes
- [Issue #229](https://github.com/cedanl/1cijferho/issues/229) — SVO documentatie
