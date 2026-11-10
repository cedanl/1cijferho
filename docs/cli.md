# CLI-referentie

De `eencijferho` CLI verwerkt DUO-bestanden via losse stappen of in één pipeline-aanroep.

!!! info "Altijd actueel"
    Gebruik `eencijferho --help` of `eencijferho <commando> --help` voor de meest actuele beschrijving rechtstreeks uit de tool.

---

## Gedeelde opties

Alle commando's vereisen:

| Optie | Beschrijving |
|-------|-------------|
| `--input PATH` | Map met invoerbestanden (`data/01-input`) |
| `--output PATH` | Map voor uitvoerbestanden (`data/02-output`) |

---

## Commando's

### `pipeline` — alles in één stap

```bash
eencijferho pipeline --input data/01-input --output data/02-output
```

Voert extract → validate → convert achter elkaar uit.

::: eencijferho.cli.cmd_pipeline
    options:
      show_signature: false

---

### `extract`

```bash
eencijferho extract --input data/01-input --output data/02-output
```

::: eencijferho.cli.cmd_extract
    options:
      show_signature: false

---

### `validate`

```bash
eencijferho validate --input data/01-input --output data/02-output
```

::: eencijferho.cli.cmd_validate
    options:
      show_signature: false

#### Bestandsbeschrijving per jaargang

`validate` en `pipeline` koppelen elk EV-bestand aan precies één bestandsbeschrijving met `1cyferho` in de naam, en elk VAKHAVW-bestand aan precies één met `Vakgegevens`. Het jaar in de bestandsnaam bepaalt welke:

- invoerbestand: `XXjj` geeft 20jj (`EV299XX24.asc` → 2024), anders een los jaartal van vier cijfers (`EV2023.asc` → 2023);
- bestandsbeschrijving: het jaartal na `1cyferho_` (`Bestandsbeschrijving_1cyferho_2024_v1.1.txt` → 2024), anders een los jaartal van vier cijfers in de naam.

Is er maar één bestandsbeschrijving zonder jaartal, dan wordt die gebruikt. In alle andere twijfelgevallen stopt de CLI met exitcode 1 en een melding op stderr, nog vóór er iets wordt omgezet: geen bestandsbeschrijving, meerdere voor hetzelfde jaar, geen jaar in de bestandsnaam bij meerdere bestandsbeschrijvingen, of een jaar waarvoor geen bestandsbeschrijving is. De melding noemt per bestand de reden en alle kandidaten; de koppelingslog `(4)_file_matching_log_latest.json` krijgt status `failed` en wordt daarna door `convert` geweigerd.

| Optie | Effect |
|-------|--------|
| `--layout-year JJJJ` | Gebruik voor EV- en VAKHAVW-bestanden de bestandsbeschrijving van dit jaar, ongeacht het jaar in de bestandsnaam. De CLI toont een waarschuwing en de koppelingslog vermeldt de geforceerde keuze. Alleen bij `validate` en `pipeline`. |

```bash
eencijferho pipeline --input data/01-input --output data/02-output --layout-year 2023
```

---

### `convert`

```bash
eencijferho convert --input data/01-input --output data/02-output [opties]
```

::: eencijferho.cli.cmd_convert
    options:
      show_signature: false

#### Uitvoer-opties

| Optie | Effect |
|-------|--------|
| `--skip-decode` | Geen `_decoded` CSV-varianten |
| `--skip-enrich` | Geen `_enriched` CSV-varianten |
| `--skip-parquet` | Geen Parquet-compressie |
| `--skip-snake-case` | Originele kolomnamen behouden |
| `--skip-ev` | EV-bestanden overslaan |
| `--skip-vakhavw` | VAKHAVW-bestanden overslaan |
| `--decode-columns KOLOM ...` | Alleen opgegeven kolommen decoderen |
| `--enrich-variables VAR ...` | Alleen opgegeven variabelen verrijken |
| `--bsn-mapping-file PAD` | Koppelbestand (CSV/Parquet) om een lokaal studentnummer toe te voegen aan EV/VAKHAVW-bestanden. Verwacht kolommen `burgerservicenummer` en `studentnummer`. Voor afwijkende kolomnamen: gebruik de [Python API](api/index.md) (`OutputConfig`). |
| `--strict` | Exit-code 1 als het decodeerlog niet-geladen Dec-tabellen of niet-gematchte Dec-codes bevat. Log en outputbestanden worden eerst geschreven. |

---

### `decode`

```bash
eencijferho decode --input data/01-input --output data/02-output
```

::: eencijferho.cli.cmd_decode
    options:
      show_signature: false

---

### `enrich`

```bash
eencijferho enrich --input data/01-input --output data/02-output
```

::: eencijferho.cli.cmd_enrich
    options:
      show_signature: false

---

### `validate-output`

```bash
eencijferho validate-output --input data/01-input --output data/02-output
```

::: eencijferho.cli.cmd_validate_output
    options:
      show_signature: false

---

## Uitvoerstructuur

Na een volledige pipeline-run:

```
data/02-output/
├── metadata/
│   ├── json/
│   │   ├── EV_2023.json
│   │   └── variable_metadata.json
│   ├── logs/
│   │   ├── (3)_xlsx_validation_log_latest.json
│   │   ├── (4)_file_matching_log_latest.json
│   │   ├── (5)_conversion_log_latest.json
│   │   └── (7)_decoding_log_latest.json
│   └── EV_2023.xlsx
├── EV_2023.csv
├── EV_2023_decoded.csv
├── EV_2023_enriched.csv
└── EV_2023.parquet
```

Het decodeerlog `(7)_decoding_log_latest.json` (plus een kopie `decoding_log_<tijdstempel>.json`) legt per gedecodeerd bestand vast welke kolommen gedecodeerd zijn en welke niet (met reden), welke Dec-tabellen niet geladen konden worden, welke codes geen match hadden (geteld, met hooguit 5 voorbeeldcodes; nooit uit BSN-, PGN- of onderwijsnummerkolommen), welke `variable_metadata`-waarden zijn toegepast en welke variabelen uit de metadata niet in het bestand voorkomen. Per `variable_metadata`-kolom staan `mapped_rows` (rijen met een label), `unmapped_rows` (rijen die hun code hielden) en `semantics` (`label`, `mixed` of `code`); een mapping met te lange labels wordt niet toegepast en staat er met `skip_reason: "labels_too_long"` en `skipped_rows`. In `summary` staan `total_unmapped_rows` en `variable_mappings_skipped`. Zie [Decodering](decodering.md#waarden-uit-variable_metadata-_enriched).

Het Parquet-bestand bevat dezelfde data als de CSV waar het uit voortkomt: dezelfde kolomnamen in dezelfde volgorde (standaard snake_case, met `--skip-snake-case` de originele DUO-namen) en alle kolommen als tekst (`String`), zodat nummers zoals het BSN hun voorloopnullen houden.
