---
icon: lucide/circle-help
---

# Veelgestelde vragen

Elk antwoord verwijst naar de pagina waar het staat. Loopt iets mis? Zie [Problemen oplossen](problemen-oplossen.md).

## Heb ik programmeerkennis nodig?

Nee. De Streamlit-interface is bedoeld voor gebruik zonder programmeerkennis. Je installeert met `uv` en start de interface met één commando. Zie [Aan de slag](getting-started.md).

## Welke bestanden heb ik nodig?

Per bestandstype het databestand (`.asc`) en de bijbehorende bestandsbeschrijving (`.txt`) in `data/01-input/`. Voor decodering komen daar de Dec-tabellen en hun bestandsbeschrijving bij. Zie [Aan de slag](getting-started.md#mapstructuur-verwacht).

## Is de omzetting zonder verlies?

De omzetting van vaste breedte naar CSV is geverifieerd verliesvrij. Decoderen en verrijken zijn aanvullingen die codes kunnen wijzigen. Controleer de logs en vergelijk een `_decoded.csv` met de ruwe `.csv` voordat je die gebruikt. Zie [Decodering](decodering.md).

## Wat is het verschil tussen de ruwe, `_decoded`- en `_enriched`-bestanden?

De ruwe CSV bevat de data zoals DUO die levert. `_decoded` voegt kolommen uit de Dec-tabellen toe. `_enriched` vervangt daarnaast codes door labels. Zie het [voorbeeld met DEMO-gegevens](voorbeeld-demo.md) en [Decodering](decodering.md).

## CSV of Parquet?

Een Parquet-bestand bevat dezelfde kolommen als de CSV, maar alleen als tekst. Wil je rekenen met numerieke velden, lees dan de CSV in of cast de kolommen zelf. Zie de [migratiegids](outputcontract-migratie.md).

## Pseudonimiseert de tool het BSN?

Nee. BSN, PGN en onderwijsnummer blijven ongewijzigd in de uitvoer staan. Zie [Privacy en persoonsgegevens](privacy.md).

## Waar blijven mijn gegevens?

Standaard op je eigen schijf. Alleen als je zelf een andere opslag instelt, gaan ze daarheen. Zie [Privacy en persoonsgegevens](privacy.md#wat-verlaat-mijn-computer).

## Hoe voeg ik een lokaal studentnummer toe?

Met een koppelbestand en de optie `--bsn-mapping-file`. Zie [Aan de slag](getting-started.md#studentnummer-koppeling-optioneel).

## Welke versie documenteert deze site?

De documentatie volgt de `main`-versie van de repository. Een deel van de beschreven wijzigingen zit nog niet in release 0.1.8. Zie de statusbanner op de [migratiegids](outputcontract-migratie.md) en de [CHANGELOG](https://github.com/cedanl/1cijferho/blob/main/CHANGELOG.md).
