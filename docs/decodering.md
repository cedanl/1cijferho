# Decodering in 1CijferHO

Dit document beschrijft welke DUO-velden worden gedecodeert naar labels en welke nog niet.

## Samenvatting

| Status | Velden |
|--------|--------|
| ✅ **Geïmplementeerd** | Geboorteland, Nationaliteiten, Opleidingscodes, Vooropleidingen, Instellingen, Postcodecijfers |
| ❌ **Niet geïmplementeerd** | Vakkenbestanden (Vakcode, Brinnummer+Vestigingsnummer), Samengestelde sleutels |

## Geïmplementeerde Decodeerregels

### Geboorteland
- `Geboorteland`, `Geboorteland ouder 1–2` → Naam land ✅

### Nationaliteit
- `Nationaliteit 1–3` → Omschrijving ✅

### Opleidingscode
- `Opleidingscode` → Naam opleiding ✅

### Vooropleiding
- Alle vooropleiding-velden → Omschrijving ✅

### Instellingen
- `Instellingscode`, `Actuele instelling` → Naam ✅

### Postcodecijfers
- `Postcodecijfers student`, `Postcodecijfers vooropl` → Regio ✅

## Niet Geïmplementeerde Regels

### Vakkenbestanden
- Vakcode (handmatig gepatchd), Brinnummer VO, Vestigingsnummer VO ❌
- **Reden:** Vakkenbestanden-metadata niet ingelezen (#204)

### Samengestelde Sleutels
- Vestigingsnummer diploma, vestigingsnummer HO ❌
- **Reden:** Meerderelige lookups niet ondersteund (#205)

## Kolom-Type Tabel

| Kolom | `_decoded` | `_enriched` | Gedrag |
|-------|-----------|-----------|--------|
| Diplomajaar | **numeric** | **character** | Gelabeld in _enriched (bijv. "2023", "geen examen geregistreerd") |
| Geboorteland | string → label | string → label | Code → Naam |
| Nationaliteit | string → label | string → label | Code → Omschrijving |
| Instellingscode | string → label | string → label | Code → Naam |

## ⚠️ SVO Opmerking (#229)

**Issue:** SVO rendement-berekening verwacht numerieke `diplomajaar`, maar `_enriched.csv` bevat string-waarden (gelabeld: "2023", "geen examen geregistreerd", etc.).

**Gevolg:** `dplyr::na_if(diplomajaar, 0)` faalt → NaN → 74,6% foutieve categorisering.

**Aanbeveling:** SVO-analyses gebruiken **`_decoded.csv`**, niet `_enriched.csv`:
- `_decoded` bevat numerieke diplomajaar (kan gebruikt worden in `na_if()`)
- `_enriched` bevat labels en is geschikt voor visuele analyse, niet numerische berekeningen

**Oplossing:**
```R
# Gebruik _decoded, niet _enriched
d <- read.csv("/path/to/EV_DEMO_decoded.csv", sep=";")
# diplomajaar is nu numeric → na_if() werkt correct
```

## Bekende Risico's

1. Niet-gemapte codes blijven staan (#207)
2. Voorloopnullen gestript (#199, gefixed)
3. Vakkenbestanden niet ondersteund (#204)
4. Samengestelde sleutels niet ondersteund (#205)
5. **SVO:** Moet `_decoded` gebruiken, niet `_enriched` (#229)

## Zie Ook

- [CHANGELOG.md](../CHANGELOG.md)
- Issues: #204, #205, #207, #199, #229
