![Braille fonts](https://see.fontimg.com/api/rf5/DOeDd/MGE4NTM1Njg3NjZhNDZhZTgwNTE0MjE5YzUxMzA0OTgudHRm/VEVYVCBBTkFMWVNJUw/braille-cc0.png?r=dw&h=81&w=1250&fg=00B17E&bg=000000&s=65)

<div align="center">
  <h1>1CijferHO Tool</h1>

  <p>Maak 1cijferHO-data direct bruikbaar voor analyse en onderzoek</p>

  <p>
    <a href="#"><img src="https://custom-icon-badges.demolab.com/badge/Windows-0078D6?logo=windows11&logoColor=white" alt="Windows"></a>
    <a href="#"><img src="https://img.shields.io/badge/macOS-000000?logo=apple&logoColor=F0F0F0" alt="macOS"></a>
    <a href="#"><img src="https://img.shields.io/badge/Linux-FCC624?logo=linux&logoColor=black" alt="Linux"></a>
    <img src="https://badgen.net/github/last-commit/cedanl/1cijferho" alt="GitHub Last Commit">
    <img src="https://badgen.net/github/contributors/cedanl/1cijferho" alt="Contributors">
    <img src="https://img.shields.io/github/license/cedanl/1cijferho" alt="GitHub License">
    <a href="https://cedanl.github.io/1cijferho/"><img src="https://img.shields.io/badge/docs-mkdocs-teal" alt="Documentatie"></a>
  </p>

</div>

## Wat is het?

De 1CijferHO Tool automatiseert het verwerken van 1cijferHO-data, zoals:
- **ASCII-bestanden** zonder duidelijke scheiding tussen velden.
- **Metadata-bestanden** in ongestructureerde .txt-indeling.

Met deze tool kun je in enkele minuten grote hoeveelheden data verwerken, zonder risico op fouten of verlies van gegevens.

> ℹ️ Benieuwd wat er op de planning staat? Bekijk de [roadmap](ROADMAP.md).

### 📑 Congrespresentatie(s)

Tijdens de DAIR-conferentie in 2025 hebben we de 1CijferHO Tool gepresenteerd. Bekijk de slides van de presentatie hieronder:

[📂 Presentatie DAIR 2025](docs/presentatie-DAIR-2025.pdf)


## Waarom is dit belangrijk?

1cijferHO-data is essentieel voor beleidsvorming en onderzoek, maar het handmatig verwerken ervan kost veel tijd en brengt risico’s met zich mee. De 1CijferHO Tool biedt een oplossing die:
- **Tijd bespaart**: Verwerk gigabytes aan data in enkele minuten.
- **Betrouwbaar is**: Voorkomt fouten door automatische validatie.
- **Veilig werkt**: Anonimiseert gevoelige gegevens, zoals BSN’s.
- **Gebruiksklaar**: Levert schone CSV- of Parquet-bestanden voor directe analyse.


## Hoe werkt het?

1. **Start de tool** met één eenvoudige opdracht.
2. **Upload je bestanden** (data en metadata).
3. **Bekijk de resultaten**: Je krijgt direct schone, geoptimaliseerde bestanden.


## Aan de slag

### Stap 1: Vereisten
Zorg dat [uv](https://docs.astral.sh/uv/getting-started/installation/) is geïnstalleerd.

### Stap 2: Download de tool
```bash
git clone https://github.com/cedanl/1cijferho.git
cd 1cijferho
```
Of download het [ZIP-bestand](https://github.com/cedanl/1cijferho/archive/refs/heads/main.zip) en pak het uit.

### Stap 3: Installeer dependencies
```bash
uv sync --extra frontend
```

### Stap 4: Start de applicatie
```bash
uv run streamlit run src/main.py
```
De applicatie opent automatisch in je browser.


## Installeren als pakket

Gebruik je `eencijferho` als backend-bibliotheek in een bestaande Python-omgeving? Dan heb je de Streamlit-app niet nodig en kun je het pakket direct installeren:

```bash
# pip
pip install eencijferho

# poetry
poetry add eencijferho
```

Voor gebruik via de CLI na installatie:

```bash
# Volledige pipeline in één stap: van ruwe DUO-bestanden naar analyse-klare CSV/Parquet
eencijferho pipeline --input data/01-input --output data/02-output

# Of stap voor stap:

# Stap 1: Lees de .txt metadata-bestanden uit en sla ze op als JSON en Excel
eencijferho extract          --input data/01-input --output data/02-output

# Stap 2: Controleer of de metadata compleet is en of elk bestand een bijpassend metadata-bestand heeft
eencijferho validate         --input data/01-input --output data/02-output

# Stap 3: Converteer de fixed-width bestanden naar CSV/Parquet op basis van de gevalideerde metadata
eencijferho convert          --input data/01-input --output data/02-output

# Stap 3b: Decodeer outputbestanden met Dec_* opzoektabellen (draai na convert)
eencijferho decode           --input data/01-input --output data/02-output

# Stap 3c: Verrijk gedecodeerde bestanden met variable_metadata labels (draai na decode)
eencijferho enrich           --input data/01-input --output data/02-output

# Stap 4 (optioneel): Valideer de geconverteerde outputbestanden op kolomwaarden en DEC-codes
eencijferho validate-output  --input data/01-input --output data/02-output
```

---

## 🎬 Demo Video

Bekijk hieronder een korte demonstratie van hoe de 1CijferHO Tool werkt:

![Demo](src/assets/demo.gif)


## 🫂 Bijdragers

Dank aan alle mensen die hebben bijgedragen aan de ontwikkeling van de 1CijferHO Tool:

[![](https://github.com/asewnandan.png?size=50)](https://github.com/asewnandan)
[![](https://github.com/tin900.png?size=50)](https://github.com/tin900)
[![](https://github.com/Tomeriko96.png?size=50)](https://github.com/Tomeriko96)
[![](https://github.com/CorneeldH.png?size=50)](https://github.com/CorneeldH)
[![](https://github.com/oinkspook.png?size=50)](https://github.com/oinkspook)

