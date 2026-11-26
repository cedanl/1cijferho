---
icon: lucide/shield-check
---

# Privacy en persoonsgegevens

Deze pagina is voor de privacyfunctionaris of beheerder van je instelling. Ze zegt wat 1CijferHO met persoonsgegevens doet. Alles hieronder volgt uit de code en de bestaande documentatie. Het is geen juridisch advies: de afspraken over opslag, delen en bewaren maak je binnen je eigen instelling.

## Welke persoonsgegevens staan in de bestanden?

De EV-bestanden van DUO bevatten drie nummers die naar een persoon wijzen:

| Kolom in de uitvoer | Wat het is |
|---|---|
| `burgerservicenummer` | BSN |
| `persoonsgebonden_nummer` | PGN van DUO |
| `onderwijsnummer` | onderwijsnummer |

Je ziet ze terug in `EV*.csv` en in de `_decoded`- en `_enriched`-varianten. Controle: in de DEMO-uitvoer van `EV299XX24_DEMO` staan alle drie de kolommen. Het VAKHAVW-bestand bevat het persoonsgebonden nummer, waarmee je het aan EV koppelt (zie [migratiegids](outputcontract-migratie.md)).

## Wat doet de tool ermee?

- **De tool pseudonimiseert niet.** De pipeline laat BSN, PGN en onderwijsnummer ongewijzigd in de uitvoer staan. Dat staat ook in de [migratiegids](outputcontract-migratie.md): er is geen automatische pseudonimisering toegevoegd.
- **Het decodeerlog toont geen nummers.** Bij niet-gematchte codes noemt het log hooguit 5 voorbeeldcodes en nooit uit een BSN-, PGN- of onderwijsnummerkolom. Zie [CLI-referentie](cli.md#uitvoerstructuur).
- **Het runlog schrijft geen celwaarden.** Bij fouten in een gegevensstap staat alleen het type fout in het log, geen bericht dat een BSN kan bevatten. Alleen `--debug` voegt de volledige foutmelding toe.

## Wat verlaat mijn computer?

De verwerking zelf (de CLI en de Python-functies) doet geen netwerkaanroepen. Dat is gecontroleerd in de broncode van `eencijferho`, buiten de optionele opslag-backends. De standaardopslag is de lokale schijf (`STORAGE_BACKEND=disk`).

Gegevens gaan alleen weg als je zelf een andere opslag instelt:

| Situatie | Wat gebeurt er |
|---|---|
| `STORAGE_BACKEND=minio`, `s3` of `postgres` | De uitvoer gaat naar die opslag. Zie [opslag-backends](storage-abstraction.md). |
| Streamlit-pagina "Versleutelen & uploaden" | De gekozen kolom wordt in je browser versleuteld. Alleen het versleutelde resultaat gaat naar de server, die het in MinIO en PostgreSQL opslaat. Je browser laadt daarvoor Pyodide van `cdn.jsdelivr.net`; je gegevens gaan niet naar die server. |
| Streamlit-pagina "Persoonsgegevens uploaden" | Gevoelige kolommen worden in je browser versleuteld. De server maakt er een UUID van met een sleutel uit `EENCIJFERHO_ENCRYPT_KEY`. Zonder die sleutel gebruikt de pagina een bekende demo-sleutel en toont ze een waarschuwing. Gebruik dat niet voor echte gegevens. |

Streamlit zelf staat zo ingesteld dat het geen gebruiksstatistieken verzamelt (`gatherUsageStats = false`).

## Het koppelbestand met studentnummers

Je instelling levert zelf een CSV (puntkomma-gescheiden) of Parquet-bestand met `burgerservicenummer` en `studentnummer`. Zie [Aan de slag](getting-started.md#studentnummer-koppeling-optioneel).

- Het koppelbestand moet een lokaal pad zijn. De tool leest het rechtstreeks van schijf.
- De tool voegt het studentnummer toe aan elk `EV*.csv`- en `VAKHAVW*.csv`-bestand. De BSN-kolom blijft staan.
- Heeft het koppelbestand dubbele BSN's, dan verandert het aantal rijen. De tool stopt dan zonder een bestand te wijzigen.
- De tool verwijdert het koppelbestand niet en bepaalt geen bewaartermijn. Dat regel je zelf.

## Wat regel je zelf?

De tool schrijft ongewijzigde nummers naar de uitvoermap. Regel dus zelf:

- waar de invoer- en uitvoermappen staan en wie erbij mag;
- hoe lang je de uitvoer en het koppelbestand bewaart en hoe je ze verwijdert;
- of en hoe je de uitvoer deelt;
- welke afspraken horen bij een opslag-backend die je instelt.

Leg dit voor in je instelling bij de privacyfunctionaris.

## Demo-gegevens

De map `data/01-input/DEMO/` is bedoeld om de tool uit te proberen. Het DEMO-koppelbestand `data/koppeling_bsn_studentnummer_DEMO.csv` bevat een fictief studentnummer voor elk BSN in de DEMO-bestanden. Zie het [voorbeeld met DEMO-gegevens](voorbeeld-demo.md).
