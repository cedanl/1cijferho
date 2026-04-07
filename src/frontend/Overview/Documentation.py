import streamlit as st

st.markdown("""
<style>
.doc-header {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 1.5rem 1.75rem;
    border-radius: 10px;
    color: white;
    margin-bottom: 1.5rem;
}
.doc-header h2 {
    margin: 0 0 0.25rem 0;
    font-size: 1.7rem;
    font-weight: 700;
}
.doc-header p {
    margin: 0;
    font-size: 0.95rem;
    opacity: 0.88;
}
</style>
<div class="doc-header">
    <h2>Documentatie</h2>
    <p>Alles over 1CijferHO — wat het doet, hoe je het gebruikt en hoe het werkt.</p>
</div>
""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["Overzicht", "Gebruik", "Technisch"])

with tab1:
    st.markdown("### Wat doet 1CijferHO?")
    st.markdown("""
    DUO levert onderwijsdata als vaste-breedte ASCII-bestanden — een formaat dat lastig te gebruiken is zonder voorbereiding:

    - Vaste-breedte strings: `821PL21PL506451B090    J2006Swo 50645...`
    - Decodeerbestanden: `5001Canada`, `5002Frankrijk`
    - Veldposities verstopt in `.txt`-bestanden

    **1CijferHO lost dit volledig automatisch op.** Het leest de metadata, splitst de velden correct en schrijft nette CSV- en Parquet-bestanden klaar voor gebruik in Python, R of Excel.
    """)

    st.markdown("### Voorbeeld: voor en na")
    st.code("""\
Voor:  821PL21PL506451B090    J20NLSwo 50645...

Na:    persoonsgebonden_nummer  type  landcode  ...
       821PL21PL506             J20   NL        ...\
""", language=None)

    st.markdown("### Wat levert het op?")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
**Standaard uitvoer**
- CSV met puntkomma-scheidingsteken
- Parquet (60–80% kleiner)
- Snake_case kolomnamen
""")
    with col2:
        st.markdown("""
**Optioneel**
- `_decoded` — codes vertaald naar omschrijvingen
- `_enriched` — waarden vervangen door leesbare labels
- Versleutelde kopie (BSN-anonimisering)
""")

with tab2:
    st.markdown("### Vereiste bestanden")
    st.markdown("""
Zorg dat u de volgende DUO-bestanden in uw invoermap plaatst:

| Type | Beschrijving |
|---|---|
| `EV_*.asc` / `VAKHAVW_*.asc` | Hoofdbestanden met vaste-breedte data |
| `Bestandsbeschrijving_*.txt` | Metadata met veldposities |
| `Dec_*.asc` | Decodeerbestanden voor waarde-omschrijvingen |
""")

    st.markdown("### Stap voor stap")
    st.markdown("""
1. **Bestanden plaatsen** — kopieer uw DUO-bestanden naar `data/01-input/`
2. **Metadata extraheren** — lees veldposities uit de `.txt`-bestanden
3. **Metadata valideren** — controleer structuur en koppel bestanden
4. **Turbo Conversie** — zet vaste-breedte data om naar CSV/Parquet
5. **Resultaten ophalen** — bestanden staan in `data/02-output/`
""")

    st.markdown("### Veelvoorkomende problemen")
    with st.expander("Geen tabellen gevonden in metadata-bestand"):
        st.markdown("Controleer of het `.txt`-bestand de kolommen `Startpositie` en `Aantal posities` bevat. Sommige DUO-bestanden gebruiken een afwijkend formaat.")

    with st.expander("Bestandskoppeling mislukt"):
        st.markdown("Controleer of de bestandsnamen van de hoofdbestanden overeenkomen met de namen in de bestandsbeschrijving.")

    with st.expander("Verwerking is traag"):
        st.markdown("Sluit onnodige applicaties. Controleer of er voldoende schijfruimte beschikbaar is (minimaal 3× de invoerbestandsgrootte).")

    with st.expander("Toegang geweigerd (permission error)"):
        st.markdown("Sluit eventuele Excel-bestanden in de metadata- of uitvoermap. Controleer schrijfrechten op de mappenstructuur.")

with tab3:
    st.markdown("### Technische stack")
    st.markdown("Python 3.13+ · Streamlit · Polars · uv")

    st.markdown("### Modules")
    st.markdown("""
| Module | Functie |
|---|---|
| `extractor.py` | Veldposities extraheren uit `.txt`-metadata |
| `converter.py` | Vaste-breedte naar CSV, multiprocessing |
| `decoder.py` | Decodeerbestanden verwerken |
| `converter_match.py` | Databestanden koppelen aan metadata |
| `converter_validation.py` | Rijtellingen en data-integriteit controleren |
| `compressor.py` | CSV naar Parquet |
| `encryptor.py` | SHA-256 hashing voor BSN-anonimisering |
""")

    st.markdown("### Datastroom")
    st.code("""\
Ruwe DUO-bestanden (data/01-input/)
    ↓ extractor.py
Metadata als JSON + Excel (data/00-metadata/)
    ↓ converter_match.py
Bestandskoppelingen vastgelegd in logs
    ↓ converter.py (parallel, n-1 CPU-kernen)
CSV-bestanden (data/02-output/)
    ↓ compressor.py
Parquet-bestanden
    ↓ encryptor.py
Geanonimiseerde kopie (BSN gehashed)\
""", language=None)

    st.markdown("### Prestaties")
    st.markdown("""
- Parallelle verwerking met n−1 CPU-kernen
- Streaming voor grote bestanden (geheugenefficiënt)
- Parquet-compressie: 60–80% kleiner dan CSV
- Cryptografische hashing voor privacy (SHA-256)
""")

st.divider()
col_a, col_b = st.columns([1, 2])
with col_a:
    if st.button("Begin met uploaden →", type="primary", use_container_width=True):
        st.switch_page("frontend/Files/Upload_Data.py")
with col_b:
    st.caption("Gemaakt door CEDA · a.sewnandan@hhs.nl · t.iwan@vu.nl")
