# Decodering in 1CijferHO

Dit document beschrijft welke DUO-velden worden gedecodeert naar labels en welke nog niet.

## Samenvatting

| Status | Velden |
|--------|--------|
| ✅ **Geïmplementeerd** | Geboorteland, Nationaliteiten, Opleidingscodes, Vooropleidingen, Instellingen, Postcodecijfers |
| ❌ **Niet geïmplementeerd** | Vakkenbestanden (Vakcode, Brinnummer+Vestigingsnummer), Samengestelde sleutels |

## Geïmplementeerde Decodeerregels

### Geboorteland (`Dec_landcode.asc`)
- `Geboorteland` → Naam land ✅
- `Geboorteland ouder 1` → Naam land ✅
- `Geboorteland ouder 2` → Naam land ✅

### Nationaliteit (`Dec_nationaliteitscode.asc`)
- `Nationaliteit 1–3` → Omschrijving ✅

### Opleidingscode (`Dec_opleidingscode.asc`)
- `Opleidingscode` → Naam opleiding ✅

### Vooropleiding (`Dec_vopl.asc`, `Dec_vooropl.asc`)
- `Hoogste vooropleiding v.d. HO` ✅
- `Hoogste vooropleiding binnen HO` ✅
- `Hoogste vooropleiding oorspronkelijk` ✅

### Instellingen
- `Instellingscode` → Naam ✅
- `Actuele instelling` → Naam ✅

### Postcodecijfers (`Dec_postcodecijfers_YYYY.asc`)
- `Postcodecijfers student` → Regio ✅
- `Postcodecijfers vooropl` → Regio ✅

## Niet Geïmplementeerde Regels

### Vakkenbestanden
| Veld | Status |
|------|--------|
| Vakcode | ⚠️ Handmatig gepatchd, onvolledig |
| Brinnummer VO + Vestigingsnummer VO | ❌ Vakkenbestanden-metadata niet ingelezen |
| Vooropleiding oorspronkelijk (VAKHAVW) | ❌ Idem |

**Reden:** `process_txt_folder()` leest alleen `Bestandsbeschrijving_1cyferho_*.txt` en `Bestandsbeschrijving_Dec-bestanden.txt`, niet `Bestandsbeschrijving_Vakkenbestanden.txt`.

**Issue:** #204

### Samengestelde Sleutels
| Beschrijving | Status |
|--------------|--------|
| Vestigingsnummer diploma (Brinnummer + Vestigingsnr) | ❌ |
| Vestigingsnummer HO (Instellingscode + Vestigingsnr) | ❌ |

**Reden:** Pipeline ondersteunt geen meerderelige lookups.

**Issue:** #205

## Bekende Risico's

1. **Niet-gemapte codes blijven staan** (#207) — kolom bevat deels codes, deels labels
2. **Voorloopnullen gestript** (gefixed in #199) — codes als `0001` kunnen niet gematcht worden
3. **Vakkenbestanden niet ondersteund** (#204) — 3 van 4 decode-regels hebben geen input
4. **Samengestelde sleutels niet ondersteund** (#205) — multi-kolom combinaties werken niet

## Zie Ook

- [CHANGELOG.md](../CHANGELOG.md) — Versiegeschiedenis
- Issues: #204, #205, #207, #199
