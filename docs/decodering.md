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

## Waarden uit `variable_metadata` (_enriched)

Naast de Dec-tabellen vervangt de `_enriched`-variant codes door de labels uit
`metadata/json/variable_metadata.json` (de waardenlijsten uit de
bestandsbeschrijving). Die lijsten zijn vaak niet volledig: soms staan alleen
`[leeg]` en `[gevuld]` erin, soms een bereik als `1000 t/m 9999`.

### Niet-gemapte waarden blijven een code

Een waarde zonder label in de lijst blijft staan zoals hij in het bestand staat
(de code). Er komt geen extra kolom bij. Een kolom kan dus labels en codes door
elkaar bevatten. Welke kolommen dat zijn, staat in het decodeerlog
`metadata/logs/(7)_decoding_log_latest.json`, onder `details[].variable_mappings`,
één regel per kolom:

| Veld | Betekenis |
|------|-----------|
| `semantics` | `label`: elke gevulde waarde is een label. `mixed`: een deel is een label, de rest is de code gebleven. `code`: geen enkele waarde is vervangen, of de mapping is niet toegepast (zie `skip_reason`). |
| `mapped_rows` | Aantal rijen dat een label kreeg (gelijk aan het oudere veld `mapped`). |
| `unmapped_rows` | Aantal gevulde rijen dat de code hield, omdat de lijst er geen label voor heeft. Een lege waarde zonder `[leeg]`-sleutel telt mee. |
| `unmapped_distinct`, `sample` | Aantal verschillende niet-gemapte waarden en hooguit 5 voorbeelden. Nooit voorbeelden uit BSN-, PGN- of onderwijsnummerkolommen (`sample_withheld`). |
| `skip_reason`, `skipped_rows` | Alleen als de mapping niet is toegepast: de reden (`labels_too_long`) en het aantal rijen dat zijn code hield. |

In `summary` telt `total_unmapped_rows` de niet-gemapte rijen van alle kolommen
op en `variable_mappings_skipped` het aantal niet-toegepaste mappings. De console
meldt per kolom bijvoorbeeld `'geslacht' (orig: 'Geslacht'): 9/10 gemapt, 1 rij(en)
niet-gemapt (code behouden): ['X']`. Niet-gemapte waarden laten `--strict` niet
falen: de lijsten zijn vaak bewust onvolledig.

### Lege waarden

Een lege waarde krijgt het label van de sleutel `[leeg]` (hoofdletters en spaties
tellen niet). Is die er niet, dan geldt de eerste andere sleutel met `leeg` erin,
op alfabetische volgorde. De volgorde in het bestand maakt dus niet uit. Hebben
meerdere van die sleutels een verschillend label, dan meldt de console welke
gekozen is en staat de keuze in het log (`empty_label_choice`). Een sleutel als
`[leeg] of 0` geldt voor de lege waarde én voor code `0`, tenzij `0` ook als
aparte sleutel in de lijst staat.

Let op: in de pipeline komt een leeg DUO-veld als lege cel (null) binnen. Een
lege cel blijft leeg en telt niet mee in `total_non_null` of `unmapped_rows`; de
`[leeg]`-regel geldt alleen voor een waarde die een lege tekst is, bijvoorbeeld
als je `decode_fields` op een eigen DataFrame aanroept.

### Lange labels worden niet ingevuld

Bij sommige variabelen is het "label" een hele alinea uitleg van DUO. Een mapping
met een label langer dan 300 tekens (`MAX_LABEL_LENGTH` in
`src/eencijferho/core/decoder.py`) wordt voor die kolom helemaal niet toegepast:
de kolom houdt al zijn codes, ook `0` en lege waarden, zodat hij niet half codes
en half alinea's bevat. Het log noteert de kolom met `skip_reason:
"labels_too_long"`, `skipped_rows` en `max_label_length`. De betekenis van de
codes staat in de bestandsbeschrijving.

In de DEMO-data geldt dit alleen voor **Code beëindiging** (labels tot 468
tekens). De langste labels van andere variabelen zijn korter (tot 288 tekens bij
Code examenresultaat); die kolommen houden hun labels.

### Postcodecijfers

Voor `Postcodecijfers student op 1 oktober` en `Postcodecijfers van de hoogste
vooropl. vóór het HO` geeft `variable_metadata` deze waarden:

| Waarde in de kolom | Betekenis |
|--------------------|-----------|
| `1000` t/m `9999` | De eerste vier cijfers van een Nederlandse postcode. De lijst geeft alleen het bereik, dus deze waarden blijven als code staan. |
| `0010` | woonachtig in België |
| `0020` | woonachtig in Duitsland |
| `0030` | woonachtig elders in het buitenland |
| `0040` | geen vaste woon- of verblijfplaats |
| leeg | onbekend (de cel blijft in de pipeline leeg, zie hierboven) |

In `_enriched` staat bij `0010` t/m `0040` dus het label, bij een Nederlandse
postcode de vier cijfers: `semantics` is `mixed`. `Dec_postcodecijfers_YYYY.asc`
geeft bij een Nederlandse postcode de gemeentecode en gemeentenaam per 1 januari
van dat jaar, en bij `0010` t/m `0040` gemeentecode `9910` t/m `9940` met
dezelfde omschrijving als gemeentenaam. Die staan in aparte kolommen naast de
postcodekolom.

**Issue:** #207

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

1. **Niet-gemapte codes blijven staan** (#207) — een `_enriched`-kolom kan deels codes, deels labels bevatten; het decodeerlog telt ze per kolom (`unmapped_rows`, `semantics`), zie [Niet-gemapte waarden](#niet-gemapte-waarden-blijven-een-code)
2. **Voorloopnullen gestript** (gefixed in #199) — codes als `0001` kunnen niet gematcht worden
3. **Afgeleide lay-out voor `Dec_brinnummer` / `Dec_brinvestigingsnummer`** (#204) — DUO publiceert er geen; de labelkolommen naast de sleutel zijn een beste schatting
4. **Samengestelde sleutels niet ondersteund** (#205) — multi-kolom combinaties in het 1CijferHO-bestand werken niet

## Zie Ook

- [CHANGELOG.md](../CHANGELOG.md) — Versiegeschiedenis
- Issues: #204, #205, #207, #199
