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

3. **Installeer frontend (Streamlit) dependencies**
Voordat je de frontend opstart, installeer je deze dependencies:
```bash
uv sync --extra frontend
```

4. **Start de applicatie**  
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
- `src/eencijferho/core`: Bevat de logica voor dataverwerking.
- `src/frontend`: Bevat de gebruikersinterface.

---

## Releases & PyPI-publish (automatisch)

Public release naar PyPI gebeurt automatisch via OIDC Trusted Publishing (geen API-token nodig). Zodra een tag in de vorm `v*.*.*` wordt gepusht, draait de workflow: eerst de tests, daarna de build, daarna publicatie naar PyPI — mits handmatige goedkeuring in de GitHub `pypi`-omgeving.

**Zie `.github/workflows/pypi-publish.yml` voor de pipeline details.**

*Stappen voor een release:*
1. Werk de versie bij in `pyproject.toml` en in het `VERSION`-bestand.
2. Commit en push naar main.
3. Maak een nieuwe tag (bijvoorbeeld `v0.1.3`):
   ```bash
   git tag v0.1.3
   git push origin v0.1.3
   ```
4. Maak een GitHub Release via de UI of CLI met deze tag.

De publicatie-workflow start automatisch en wacht op handmatige goedkeuring.


## Ondersteuning

Voor technische vragen kun je contact opnemen via:
- **E-mail**: a.sewnandan@hhs.nl | tomer.iwan@surf.nl
- **GitHub Issues**: [Probleem melden](https://github.com/cedanl/1cijferho/issues)