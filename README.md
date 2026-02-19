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

[📂 Presentatie DAIR 2025](presentatie-DAIR-2025.pdf)


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

### Stap 1: Download de tool
Je kunt de tool downloaden via GitHub:
- [Download ZIP-bestand](https://github.com/cedanl/1cijferho/archive/refs/heads/main.zip)
- Of gebruik Git:  
  ```bash
  git clone https://github.com/cedanl/1cijferho.git
  ```

### Stap 2: Installeer de tool
Voor installatie-instructies, zie de [Technische README](TECHNICAL_README.md).

### Stap 3: Start de applicatie
Open een terminal in de map waar je de tool hebt opgeslagen en voer het volgende commando uit:
```bash
uv run streamlit run src/main.py
```
De applicatie opent automatisch in je browser.


## 🎬 Demo Video

Bekijk hieronder een korte demonstratie van hoe de 1CijferHO Tool werkt:

![Demo](src/assets/demo.gif)


## 🫂 Bijdragers

Dank aan alle mensen die hebben bijgedragen aan de ontwikkeling van de 1CijferHO Tool:

[![](https://github.com/asewnandan.png?size=50)](https://github.com/asewnandan)
[![](https://github.com/tin900.png?size=50)](https://github.com/tin900)
[![](https://github.com/Tomeriko96.png?size=50)](https://github.com/Tomeriko96)

