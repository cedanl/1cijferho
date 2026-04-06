import streamlit as st
import os
import glob
from config import get_demo_mode, get_input_dir, get_decoder_input_dir

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def categorize_files() -> tuple[bool, dict[str, list[str]], int]:
    """Check if the input directory exists and categorize files found"""
    input_dir = get_input_dir()

    # Create directory if it doesn't exist
    os.makedirs(input_dir, exist_ok=True)
    
    if not os.path.exists(input_dir):
        return False, {}, 0
    
    # Get all files in the directory (any extension, excluding .zip files)
    all_files_paths = glob.glob(os.path.join(input_dir, "*"))
    # Also include decoder files from root input dir when in DEMO_MODE
    if get_demo_mode():
        all_files_paths += glob.glob(os.path.join(get_decoder_input_dir(), "Dec_*.asc"))
    
    # Filter out directories and .zip files, keep only regular files
    all_files = []
    seen = set()
    for file_path in all_files_paths:
        if os.path.isfile(file_path) and not file_path.lower().endswith('.zip'):
            basename = os.path.basename(file_path)
            if basename not in seen:
                seen.add(basename)
                all_files.append(basename)
    
    # Categorize files
    categorized_files = {
        "bestandsbeschrijvingen": [],
        "decodeer_files": [],
        "main_files": []
    }
    
    for filename in all_files:
        filename_lower = filename.lower()
        
        # Bestandsbeschrijvingen: .txt files with "bestandsbeschrijving" in name
        if filename_lower.endswith('.txt') and 'bestandsbeschrijving' in filename_lower:
            categorized_files["bestandsbeschrijvingen"].append(filename)
        
        # Decodeer Files: start with "Dec_"
        elif filename.startswith('Dec_'):
            categorized_files["decodeer_files"].append(filename)
        
        # Main Files: start with EV, VAKHAVW, Croho, or Croho_vest
        elif (filename.startswith('EV') or 
              filename.startswith('VAKHAVW') or 
              filename.startswith('Croho') or 
              filename.startswith('Croho_vest')):
            categorized_files["main_files"].append(filename)
    
    # Sort each category
    for category in categorized_files:
        categorized_files[category].sort()
    
    total_files = len(all_files)
    files_found = total_files > 0
    
    return files_found, categorized_files, total_files

# -----------------------------------------------------------------------------
# Header Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title("📂 Bestanden uploaden")

if get_demo_mode():
    st.info(f"🎯 **Demo modus actief** – demo-bestanden worden geladen uit `{get_input_dir()}`.")
    st.write("""
Volg deze stappen om te beginnen:

1. **Demo-bestanden** staan al klaar in de demo-map
2. **Ververs deze pagina** om de beschikbare bestanden per type te bekijken
""")
else:
    st.write(f"""
Volg deze stappen om te beginnen:

1. **Kopieer uw 1CHO-bestanden** naar de map `{get_input_dir()}` van deze applicatie
2. **Plaats bestanden direct** in de map (niet in submappen)
3. **Ververs deze pagina** om uw geüploade bestanden per type te bekijken
""")

# Side-by-side buttons for refresh and extract
col1, col2 = st.columns(2)

with col1:
    if st.button("🔄 Pagina verversen", type="primary", use_container_width=True):
        st.rerun()

with col2:
    # Check if files exist to enable/disable the extract button
    files_found, _, _ = categorize_files()
    
    if st.button("➡️ Ga door naar stap 1", type="secondary", disabled=not files_found, use_container_width=True):
        st.switch_page("frontend/Modules/Extract_Metadata.py")

# -----------------------------------------------------------------------------
# Example Directory Structure
# -----------------------------------------------------------------------------
with st.expander("📂 Voorbeeld mapstructuur"):
    st.write(f"""
    ### Voorbeeld mapstructuur
    
    Uw `{get_input_dir()}` map moet eruitzien zoals hieronder. Bestanden worden automatisch ingedeeld in drie types:

    - **📄 Bestandsbeschrijvingen**: .txt-bestanden met "bestandsbeschrijving" in de naam
    - **🔓 Decodeerbestanden**: Bestanden die beginnen met "Dec_"
    - **📊 Hoofdbestanden**: Bestanden die beginnen met "EV", "VAKHAVW", "Croho" of "Croho_vest"

    **Belangrijk:** Plaats bestanden direct in de map `{get_input_dir()}`, niet in submappen.
    """)
    
    # Path to the image (if it exists)
    if os.path.exists("src/assets/example_files.png"):
        st.image("src/assets/example_files.png")

# -----------------------------------------------------------------------------
# File Detection and Categorization Section
# -----------------------------------------------------------------------------
files_found, categorized_files, total_files = categorize_files()

if not files_found:
    st.error(f"""
    🚨 **Geen bestanden gevonden in de map `{get_input_dir()}`**

    Kopieer uw uitgepakte 1CHO-bestanden naar de map `{get_input_dir()}` en ververs deze pagina.
    """)
else:
    st.success(f"""
    ✅ **{total_files} bestanden gevonden in de map `{get_input_dir()}`**

    Bestanden zijn automatisch ingedeeld per type. Controleer hieronder of alle verwachte bestanden aanwezig zijn.
    """)

    st.markdown("---")
    
    # Display categorized files in uniform columns
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### 📄 Bestandsbeschrijvingen")
        count = len(categorized_files["bestandsbeschrijvingen"])
        st.metric("Count", count)
        
        if count > 0:
            with st.expander(f"Bekijk {count} bestanden"):
                for filename in categorized_files["bestandsbeschrijvingen"]:
                    st.write(f"• `{filename}`")
        else:
            st.info("Geen bestanden gevonden")
    
    with col2:
        st.markdown("#### 🔓 Decodeerbestanden")
        count = len(categorized_files["decodeer_files"])
        st.metric("Count", count)
        
        if count > 0:
            with st.expander(f"Bekijk {count} bestanden"):
                for filename in categorized_files["decodeer_files"]:
                    st.write(f"• `{filename}`")
        else:
            st.info("Geen bestanden gevonden")
    
    with col3:
        st.markdown("#### 📊 Hoofdbestanden")
        count = len(categorized_files["main_files"])
        st.metric("Count", count)
        
        if count > 0:
            with st.expander(f"Bekijk {count} bestanden"):
                for filename in categorized_files["main_files"]:
                    st.write(f"• `{filename}`")
        else:
            st.info("Geen bestanden gevonden")
