import streamlit as st

st.markdown("""
<style>
.hero-section {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 2rem;
    border-radius: 10px;
    color: white;
    margin-bottom: 2rem;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="hero-section">
    <h1>📚 Documentatie</h1>
</div>
""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["Overzicht", "Gebruik", "Technisch"])

with tab1:
    st.markdown("## Het probleem")
    
    st.markdown("""
    DUO levert onderwijsdata als ASCII-bestanden met vaste breedte, aparte decodeerbestanden en ongestructureerde metadata:
    
    - Vaste-breedte strings: `821PL21PL506451B090    J2006Swo 50645...`
    - Decodeerbestanden: `5001Canada`, `5002Frankrijk`
    - Veldposities verstopt in .txt-bestanden
    - Handmatige verwerking kost uren tot dagen
    """)
    
    st.markdown("## Wat doen wij?")
    
    st.markdown("""
    Automatisch DUO-bestanden omzetten naar CSV/Parquet-formaat, klaar voor analyse.
    """)
    
    st.code("""
Voor: 821PL21PL506451B090    J20NLSwo 50645...

Na:    Persoonsgebonden_nummer: 821PL21PL506
        Type: J20
        Landcode: NL
    """, language=None)

with tab2:
    st.markdown("## Vereiste bestanden")
    
    st.markdown("""
    - DUO hoofdbestanden (EV, VAKHAVW, CROHO, CROHO_VEST)
    - DUO bestandsbeschrijvingen
    - DUO dec-bestanden
    """)
    
    st.markdown("## Proces")
    
    st.markdown("""
    1. Upload uw DUO-bestanden naar `data/01-input/`
    2. Voer Extract Metadata uit
    3. Voer Validatie uit
    4. Voer Turbo Conversie uit
    5. Vind uw bestanden in `data/02-output/`
                
    """)
    
    st.markdown("## Problemen oplossen")
    
    with st.expander("Veelvoorkomende problemen"):
        st.markdown("""
        **Geen tabellen gevonden in metadata-bestand**  
        Controleer of het .txt-bestand "Startpositie" en "Aantal posities" bevat
        
        **Bestandskoppeling mislukt**  
        Controleer of de bestandsnamen overeenkomen
        
        **Verwerking is traag**  
        Sluit onnodige applicaties, controleer schijfruimte
        
        **Toegang geweigerd**  
        Sluit Excel-bestanden in de outputmap, controleer schrijfrechten
        """)

with tab3:
    st.markdown("## Stack")
    
    st.markdown("""
    Python 3.13+ • Streamlit • Polars • uv
    """)
    
    st.markdown("## Architectuur")
    
    st.markdown("""
    **Kernmodules**
    
    - `src/eencijferho/core/`
    - `extractor.py` — Extract field positions from metadata .txt files
    - `converter.py` — Convert fixed-width to CSV using multiprocessing
    - `decoder.py` — Process decode files (roadmap)
    
    **Hulpmiddelen**
    
    - `src/eencijferho/utils/`
    - `converter_match.py` — Match data files to metadata
    - `converter_validation.py` — Verify row counts and data integrity
    - `compressor.py` — Convert CSV to Parquet
    - `encryptor.py` — SHA256 hashing for BSN anonymization
    - `extractor_validation.py` — Validate metadata extraction
                
    **Frontend**
    
    - `src/frontend/`
    - `Overview/Home.py` — Home page
    - `Overview/Documentation.py` — Documentation page
    - `Files/Upload_Data.py` — Data upload page
    - `Modules/Extract_Metadata.py` — Extract metadata page
    - `Modules/Validate_Metadata.py` — Validate metadata page
    - `Modules/Turbo_Convert.py` — Turbo convert (parser) page
    - `Modules/Tip.py` — Tip page

    """)
    
    st.markdown("## Datastroom")
    
    st.code("""
Ruwe DUO-bestanden
    ↓
Metadata extractie (extractor.py)
    ↓
Tabelstructurering (JSON → Excel)
    ↓
Bestandskoppeling (converter_match.py)
    ↓
Parallelle conversie (converter.py)
    ↓
Validatie (converter_validation.py)
    ↓
Compressie (compressor.py)
    ↓
Anonimisering (encryptor.py)
    ↓
Onderzoeksklare output
    """, language=None)
    
    st.markdown("## Prestaties")
    
    st.markdown("""
    - Gebruikt n-1 CPU-kernen voor parallelle verwerking
    - Streamt grote bestanden om geheugen te beheren
    - Parquet-compressie verkleint bestanden met 60-80%
    - Cryptografische hashing voor privacy
    """)

st.markdown("---")
st.markdown("Gemaakt door CEDA • Vragen: a.sewnandan@hhs.nl | t.iwan@vu.nl")