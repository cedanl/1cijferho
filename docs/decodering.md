# Decodering in 1CijferHO

Dit document beschrijft welke DUO-velden worden gedecodeert naar labels en welke nog niet.

## Samenvatting

| Status | Velden |
|--------|--------|
| ✅ **Geïmplementeerd** | Geboorteland, Nationaliteiten, Opleidingscodes, Vooropleidingen, Instellingen, Postcodecijfers, Vakkenbestanden (Vakcode, Brinnummer, Brinnummer+Vestigingsnummer, Vooropleiding) |
| ❌ **Niet geïmplementeerd** | Samengestelde sleutels in het 1CijferHO-bestand |

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

### Vakkenbestanden (`VAKHAVW_*.asc`)

De decodeerregels staan in de kolom *Opmerking* van de tabel *Vakgegevens* in
`Bestandsbeschrijving_Vakkenbestanden.txt` ("te decoderen met Dec_X.asc",
eventueel "in combinatie met ..."). De decoder leest die lay-out zelf in.

| Veld | Dec-bestand | Status |
|------|-------------|--------|
| Vakcode | `Dec_vakcode.asc` | ✅ |
| Brinnummer VO-instelling | `Dec_brinnummer.asc` | ✅ |
| Vestigingsnummer VO-vestiging (+ Brinnummer) | `Dec_brinvestigingsnummer.asc` | ✅ samengestelde sleutel |
| Vooropleiding oorspronkelijke code | `Dec_vooropl.asc` | ✅ |

DUO levert voor `Dec_brinnummer.asc` en `Dec_brinvestigingsnummer.asc` geen
bestandsbeschrijving. 1CijferHO gebruikt daarom een **afgeleide lay-out**
(dezelfde opbouw als `Dec_instellingscode.asc` en `Dec_instellingscodevest.asc`).
Die lay-out komt in `metadata/json/Lay-out_afgeleid_1CijferHO.json` en in de
logs staat bij deze bestanden dat de lay-out niet van DUO komt. Alleen de
sleutelkolommen (Brinnummer, Vestigingsnummer) zijn nodig voor de koppeling.

**Issue:** #204

## Namen koppelen

De decoder koppelt een naam uit een bestandsbeschrijving aan een kolom als de
genormaliseerde namen exact gelijk zijn (#202). Er wordt niet gegokt op
gelijkenis.

DUO noemt zes velden in de Dec-beschrijving en de variabelenbeschrijving anders
dan in de lay-out van het 1CijferHO-bestand. Die paren staan in een vaste tabel
(`_LAYOUT_NAMES` in `core/decoder.py`). De decoder zoekt eerst een kolom met de
naam uit de beschrijving en gebruikt de tabel alleen als die kolom ontbreekt.

| Naam in de beschrijving | Naam in de lay-out |
|---|---|
| Diplomajaar van de hoogste vooropleiding | Diplomajaar hoogste vooropleiding |
| Hoogste vooropleiding vóór het HO oorspr. code | Hoogste vooropleiding vóór het HO oorspronkelijke code |
| Hoogste vooropleiding binnen het HO oorspr. code | Hoogste vooropleiding binnen het HO oorspronkelijke code |
| Indicatie eerstejaars opleiding actueel equivalent | Indicatie eerstejaars opl. actueel equivalent |
| Indicatie eerstejaars actuele opleiding-instelling | Indicatie eerstejaars actuele opl.-instelling |
| Indicatie eerstejaars act. inst - type ho binnen soort ho | Indicatie eerstejrs act. inst - type ho binnen soort ho |

Een naam die in geen van beide vormen als kolom voorkomt, wordt overgeslagen.
Dat staat in de console en in `variables_not_found` van
`(7)_decoding_log_latest.json`. Wijkt een naam in een nieuwe levering af, voeg
het paar dan toe aan de tabel.

## Niet Geïmplementeerde Regels

### Niet-ondersteunde bestanden

Een bestand in de invoermap waarvoor geen bestandsbeschrijving bestaat
(bijvoorbeeld `Croho.asc`, `Croho_vest.asc`, `Dec_land_naar_herkomstindikking.asc`
of een jaargang van `Dec_postcodecijfers_YYYY.asc` die niet beschreven is) wordt
niet omgezet. Het wordt gemeld als **niet ondersteund**, met de reden, in
`(5)_conversion_log_latest.json` (`unsupported_files` en status `unsupported`
in `details`), in de console en in het log van Turbo Convert. Bij
`Dec_postcodecijfers` noemt de melding de jaren die wel beschreven zijn.

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
3. **Afgeleide lay-out voor `Dec_brinnummer` / `Dec_brinvestigingsnummer`** (#204) — DUO publiceert er geen; de labelkolommen naast de sleutel zijn een beste schatting
4. **Samengestelde sleutels niet ondersteund** (#205) — multi-kolom combinaties in het 1CijferHO-bestand werken niet

## Zie Ook

- [CHANGELOG.md](../CHANGELOG.md) — Versiegeschiedenis
- Issues: #204, #205, #207, #199
