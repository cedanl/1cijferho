# Technische README

Deze README bevat technische details over de installatie, configuratie en architectuur van de 1CijferHO Tool.

---

## Installatie

### Vereisten
- **Besturingssysteem**: Windows, macOS of Linux
- **Software**: [uv](https://docs.astral.sh/uv/getting-started/installation/)

### Stappen

1. **Installeer uv**  
   - **Linux/macOS**:  
     ```bash
     curl -LsSf https://astral.sh/uv/install.sh | sh
     ```
   - **Windows**:  
     ```powershell
     powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
     ```

2. **Controleer de installatie**  
   Voer het volgende commando uit om te controleren of uv correct is geïnstalleerd:
   ```bash
   uv self update
   ```

3. **Start de applicatie**  
   Ga naar de map waar de tool is opgeslagen en voer uit:
   ```bash
   uv run streamlit run src/main.py
   ```

---

## Architectuur

De tool is opgebouwd uit de volgende componenten:
- **Backend**: Verantwoordelijk voor dataverwerking en validatie.
- **Frontend**: Een gebruiksvriendelijke interface gebouwd met Streamlit.
- **Data**: Input- en outputbestanden, inclusief metadata.

### Belangrijke bestanden
- `src/main.py`: Startpunt van de applicatie.
- `src/backend/core`: Bevat de logica voor dataverwerking.
- `src/frontend`: Bevat de gebruikersinterface.

---


## Ondersteuning

Voor technische vragen kun je contact opnemen via:
- **E-mail**: a.sewnandan@hhs.nl | tomer.iwan@surf.nl
- **GitHub Issues**: [Probleem melden](https://github.com/cedanl/1cijferho/issues)