import polars as pl
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from backend.core import decoder
import streamlit as st
import glob
import subprocess
import backend.utils.converter_validation as cv
import backend.utils.compressor as co
import backend.utils.encryptor as en
import backend.utils.converter_headers as ch
import io
import contextlib

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
#st.set_page_config(
#    page_title="⚡ Turbo Convert",
#    layout="centered",
#    initial_sidebar_state="expanded"
#)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def get_matched_files():
    """Get all matched files from the validation logs"""
    logs_dir = "data/00-metadata/logs"
    if not os.path.exists(logs_dir):
        return [], []
    
    matching_log_files = glob.glob(os.path.join(logs_dir, "*file_matching_log_latest.json"))
    
    successful_pairs = []
    skipped_pairs = []
    
    if matching_log_files:
        try:
            import json
            with open(matching_log_files[0], 'r') as f:
                matching_data = json.load(f)
            
            for file_info in matching_data.get('processed_files', []):
                if file_info.get('status') == 'matched':
                    for match in file_info.get('matches', []):
                        pair_info = {
                            'input_file': file_info['input_file'],
                            'rows': file_info.get('row_count', 0),
                            'metadata_file': match['validation_file'].replace('Bestandsbeschrijving_', '').split('_')[0]
                        }
                        
                        if match.get('validation_status') == 'success':
                            successful_pairs.append(pair_info)
                        else:
                            skipped_pairs.append(pair_info)
        except (json.JSONDecodeError, FileNotFoundError):
            pass
    
    return successful_pairs, skipped_pairs

def clear_console_log():
    """Clear the console log in session state"""
    if 'convert_console_log' in st.session_state:
        del st.session_state['convert_console_log']

def get_output_files():
    """Get all files from the output directory"""
    output_dir = "data/02-output"
    if not os.path.exists(output_dir):
        return []
    
    files = []
    for file in os.listdir(output_dir):
        if os.path.isfile(os.path.join(output_dir, file)):
            file_path = os.path.join(output_dir, file)
            file_size = os.path.getsize(file_path)
            files.append({
                'name': file,
                'size': file_size,
                'size_formatted': format_file_size(file_size)
            })
    
    # Sort files by name
    files.sort(key=lambda x: x['name'])
    return files

def format_file_size(size_bytes):
    """Format file size in human readable format"""
    if size_bytes == 0:
        return "0 B"
    size_names = ["B", "KB", "MB", "GB"]
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_names[i]}"

def start_conversion():
    """Callback function to start the conversion process"""
    st.session_state.start_turbo_convert = True

# -----------------------------------------------------------------------------
# Initialize/Clear Console Log on Page Load
# -----------------------------------------------------------------------------
# Clear console log when page is loaded
clear_console_log()

# Initialize conversion trigger
if 'start_turbo_convert' not in st.session_state:
    st.session_state.start_turbo_convert = False

# Set page initialization flag
st.session_state.page_initialized_convert = True

# -----------------------------------------------------------------------------
# Main Content
# -----------------------------------------------------------------------------
st.title("⚡ Turbo Convert")

# Introductie
st.write("""
**Stap 3: Data omzetten en verwerken**

We gebruiken de gevalideerde metadata om je hoofd- en dec-bestanden om te zetten. Je vaste-breedte data wordt zo omgezet naar veilige, gecomprimeerde en direct bruikbare bestanden.

Wat gebeurt er:
- Bestanden omzetten naar CSV met de juiste velden
- Scheidingsteken: puntkomma ; Codering: Latin-1
- Resultaten controleren op fouten
- CSV-bestanden comprimeren naar Parquet
- Gevoelige gegevens versleutelen in een kopie (xxx_encrypted)
- snake_case kolomnamen toevoegen
- Alles opslaan in `data/02-output/` + ballonnen 🎈 als het klaar is!

Gaat er iets mis? Kijk dan hieronder in het log voor details.
""")

# Get files and display status
successful_pairs, skipped_pairs = get_matched_files()
total_pairs = len(successful_pairs) + len(skipped_pairs)

if total_pairs == 0:
    st.error("🚨 **Geen gekoppelde bestanden gevonden**")
    st.info("💡 Voer eerst de validatie uit zodat je bestanden klaar zijn voor conversie.")
else:
    st.success(f"✅ **{len(successful_pairs)} bestandparen klaar voor conversie** ({len(skipped_pairs)} overgeslagen validatie)")
    
    # Show file pairs in compact expander - closed by default
    if successful_pairs or skipped_pairs:
        with st.expander(f"📁 Bestanddetails ({len(successful_pairs)} klaar, {len(skipped_pairs)} overgeslagen)", expanded=False):
            tab1, tab2 = st.tabs([f"✅ Klaar ({len(successful_pairs)})", f"❌ Overgeslagen ({len(skipped_pairs)})"])
            
            with tab1:
                if successful_pairs:
                    st.write("**Bestanden klaar voor conversie:**")
                    for pair in successful_pairs:
                        st.write(f"• `{pair['input_file']}` ({pair['rows']:,} rijen)")
                else:
                    st.info("Geen bestanden klaar voor conversie.")
            
            with tab2:
                if skipped_pairs:
                    st.write("**Bestanden met validatiefouten - kijk bij 🛡️ Metadata valideren + logs (3) & (4) voor details:**")
                    for pair in skipped_pairs:
                        st.write(f"• `{pair['input_file']}` ({pair['rows']:,} rijen)")
                else:
                    st.info("Geen validatiefouten.")
    
    # Centered button - only show if there are successful pairs
    if successful_pairs:
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            # Use callback-based button (recommended Streamlit pattern)
            st.button("⚡ Start Turbo Convert ⚡", 
                     type="primary", 
                     use_container_width=True, 
                     key="turbo_convert_btn",
                     on_click=start_conversion)

        # Handle conversion logic using session state flag
        if st.session_state.start_turbo_convert:
            # Reset the flag immediately
            st.session_state.start_turbo_convert = False
            
            # Reset console log at the start of each conversion
            st.session_state.convert_console_log = ""
            
            # Create progress bar and status containers
            progress_bar = st.progress(0)
            status_text = st.empty()
            console_container = st.empty()
            
            def update_console():
                """Update the console display"""
                with console_container.container():
                    if st.session_state.convert_console_log:
                        st.code(st.session_state.convert_console_log, language=None)
                    else:
                        st.info("Start conversie process...")
            
            try:
                st.session_state.convert_console_log += "🔄 Start conversie pipeline...\n"
                update_console()
                progress_bar.progress(10)
                status_text.text("⚡ Stap 3: Converting fixed-width files...")
                
                # Step 3: Convert Files
                st.session_state.convert_console_log += "⚡ Stap 3: Converting fixed-width files...\n"
                update_console()
                result = subprocess.run(["uv", "run", "src/backend/core/converter.py"], 
                                      capture_output=True, text=True, cwd=".")
                if result.stdout:
                    st.session_state.convert_console_log += result.stdout
                if result.stderr:
                    st.session_state.convert_console_log += f"Warning: {result.stderr}\n"
                st.session_state.convert_console_log += "✅ File conversion completed\n"
                update_console()
                progress_bar.progress(30)

                # --- Decoding step for EV* and VAKHAVW* files ---
                st.session_state.convert_console_log += "🔤 Step 3b: Decoding main files (EV*, VAKHAVW*)...\n"
                update_console()
                dec_json = os.path.join("data/00-metadata/json/Bestandsbeschrijving_Dec-bestanden_DEMO.json")
                dec_dir = "data/02-output"
                decoded_count = 0
                for file in os.listdir(dec_dir):
                    if (file.startswith("EV") or file.startswith("VAKHAVW")) and file.endswith(".csv") and not file.endswith("_decoded.csv"):
                        file_path = os.path.join(dec_dir, file)
                        try:
                            main_df = pl.read_csv(file_path, separator=';', encoding='latin1')
                            dec_tables = decoder.load_dec_tables_from_metadata(dec_json, dec_dir)
                            def snake_case(name):
                                import re
                                name = name.lower()
                                name = re.sub(r'[^a-z0-9]+', '_', name)
                                name = re.sub(r'_+', '_', name).strip('_')
                                return name
                            decoded_df = decoder.decode_fields(main_df, dec_json, dec_tables, naming_func=snake_case)
                            # Debug: print shape and columns before writing
                            st.session_state.convert_console_log += f"[debug] Decoded DataFrame for {file}: shape={decoded_df.shape}, columns={decoded_df.columns}\n"
                            update_console()
                            # If empty, print join keys for diagnosis
                            if decoded_df.shape[0] == 0:
                                st.session_state.convert_console_log += f"[debug] Decoded DataFrame for {file} is EMPTY!\n"
                                st.session_state.convert_console_log += f"[debug] Main DataFrame columns: {main_df.columns}\n"
                                st.session_state.convert_console_log += f"[debug] Main DataFrame first 5 rows:\n{main_df.head(5)}\n"
                                update_console()
                            # Write decoded file with debug messages before and after
                            decoded_file = file_path.replace('.csv', '_decoded.csv')
                            st.session_state.convert_console_log += f"[debug] Attempting to write decoded CSV: {decoded_file} (shape={decoded_df.shape})\n"
                            update_console()
                            try:
                                # Write decoded CSV using a string buffer, then write to file with encoding='utf-8' (like originals)
                                import io
                                csv_buffer = io.StringIO()
                                decoded_df.write_csv(csv_buffer, separator=';')
                                csv_content = csv_buffer.getvalue()
                                with open(decoded_file, 'w', encoding='latin1', newline='') as f_out:
                                    f_out.write(csv_content)
                                st.session_state.convert_console_log += f"[debug] Successfully wrote decoded CSV: {decoded_file} (size={os.path.getsize(decoded_file)} bytes)\n"
                            except Exception as e:
                                st.session_state.convert_console_log += f"[debug] Failed to write decoded CSV: {decoded_file} ({e})\n"
                            update_console()
                            # Also write decoded Parquet file directly
                            decoded_parquet = file_path.replace('.csv', '_decoded.parquet')
                            st.session_state.convert_console_log += f"[debug] Attempting to write decoded Parquet: {decoded_parquet} (shape={decoded_df.shape})\n"
                            update_console()
                            try:
                                decoded_df.write_parquet(decoded_parquet)
                                st.session_state.convert_console_log += f"[debug] Successfully wrote decoded Parquet: {decoded_parquet} (size={os.path.getsize(decoded_parquet)} bytes)\n"
                            except Exception as e:
                                st.session_state.convert_console_log += f"[debug] Failed to write decoded Parquet: {decoded_parquet} ({e})\n"
                            update_console()
                            decoded_count += 1
                        except Exception as e:
                            st.session_state.convert_console_log += f"Warning: Decoding failed for {file}: {e}\n"
                            update_console()
                st.session_state.convert_console_log += f"✅ Decoding completed for {decoded_count} file(s)\n"
                update_console()
                progress_bar.progress(40)
                
                # Step 4: Validate Conversion
                status_text.text("🔍 Stap 4: Validating conversion results...")
                st.session_state.convert_console_log += "🔍 Stap 4: Validating conversion results...\n"
                update_console()
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    cv.converter_validation()
                st.session_state.convert_console_log += captured_output.getvalue()
                st.session_state.convert_console_log += "✅ Conversion validation completed\n"
                update_console()
                progress_bar.progress(50)
                
                # Step 5: Run Compressor
                status_text.text("🗜️ Stap 5: Compressing to Parquet format...")
                st.session_state.convert_console_log += "🗜️ Stap 5: Compressing to Parquet format...\n"
                update_console()
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    co.convert_csv_to_parquet()
                st.session_state.convert_console_log += captured_output.getvalue()
                st.session_state.convert_console_log += "✅ Compression completed\n"
                update_console()
                progress_bar.progress(75)

                # Step 6: Run Encryptor
                status_text.text("🔒 Stap 6: Encrypting final files...")
                st.session_state.convert_console_log += "🔒 Stap 6: Encrypting final files...\n"
                update_console()
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    en.encryptor()
                st.session_state.convert_console_log += captured_output.getvalue()
                st.session_state.convert_console_log += "✅ Encryptie afgerond\n"
                st.session_state.convert_console_log += "🎉 Complete processing pipeline succesvol afgerond!\n"
                update_console()
                progress_bar.progress(90)
                
                # Step 7: Run Converter Headers (snake_case)
                status_text.text("🔨 Stap 7: Converteer headers naar snake_case...")
                st.session_state.convert_console_log += "🔨 Stap 7: Converteer headers naar snake_case...\n"
                update_console()
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    ch.convert_csv_headers_to_snake_case()
                st.session_state.convert_console_log += captured_output.getvalue()
                st.session_state.convert_console_log += "✅ Header conversie afgerond\n"
                update_console()
                progress_bar.progress(100)


                status_text.text("✅ Verwerking succesvol voltooid!")
                
                st.success("✅ **Verwerking voltooid!** Bestanden geconverteerd, gevalideerd, gecomprimeerd en versleuteld. Resultaten opgeslagen in `data/02-output/`")
                
                # Show converted files
                output_files = get_output_files()
                if output_files:
                    with st.expander(f"📁 Converted Files ({len(output_files)} files)", expanded=True):
                        st.write("**Bestanden succesvol aangemaakt in `data/02-output/`:**")
                        
                        # Group files by type for better organization
                        csv_files = [f for f in output_files if f['name'].endswith('.csv') and not f['name'].endswith('_encrypted.csv') and not f['name'].endswith('_decoded.csv')]
                        decoded_files = [f for f in output_files if f['name'].endswith('_decoded.csv')]
                        parquet_files = [f for f in output_files if f['name'].endswith('.parquet')]
                        encrypted_files = [f for f in output_files if f['name'].endswith('_encrypted.csv')]

                        if csv_files:
                            st.write("**📄 CSV-bestanden (geconverteerd):**")
                            for file in csv_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")

                        if decoded_files:
                            st.write("**🔤 Decoded Files (Main files with decoded columns):**")
                            for file in decoded_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")

                        if parquet_files:
                            st.write("**🗜️ Parquet-bestanden (gecomprimeerd):**")
                            for file in parquet_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")

                        if encrypted_files:
                            st.write("**🔒 Versleutelde bestanden (definitief):**")
                            for file in encrypted_files:
                                st.write(f"• `{file['name']}` ({file['size_formatted']})")
                
                # Celebrate with balloons!
                st.balloons()
                
                # Clear the progress indicators after a moment
                import time
                time.sleep(300)
                progress_bar.empty()
                status_text.empty()
                console_container.empty()
                
                # Rerun to update any button states
                st.rerun()
                
            except Exception as e:
                st.session_state.convert_console_log += f"❌ Error: {str(e)}\n"
                update_console()
                progress_bar.progress(0)
                status_text.text("❌ Verwerking mislukt")
                st.error(f"❌ **Verwerking mislukt:** {str(e)}")
    else:
        st.warning("⚠️ Geen bestanden klaar voor conversie. Controleer de validatieresultaten.")

# Console Log expander
with st.expander("📋 Console Log", expanded=True):
    st.caption("💡 Let op: Meldingen zoals 'Could not determine dtype for column X, falling back to string' zijn onschuldig - dit is een eigenaardigheid van de Polars Excel-bibliotheek.")
    if 'convert_console_log' in st.session_state and st.session_state.convert_console_log:
        st.code(st.session_state.convert_console_log, language=None)
    else:
        st.info("Nog geen conversieproces gestart. Klik op 'Start Turbo Conversie' om te beginnen.")



# Show existing converted files (if any)
output_files = get_output_files()
if output_files:
    with st.expander(f"📁 Geconverteerde bestanden ({len(output_files)} files)", expanded=False):
        st.write("**Bestanden momenteel in `data/02-output/`:**")
        
        # Group files by type for better organization
        csv_files = [f for f in output_files if f['name'].endswith('.csv') and not f['name'].endswith('_encrypted.csv')]
        parquet_files = [f for f in output_files if f['name'].endswith('.parquet')]
        encrypted_files = [f for f in output_files if f['name'].endswith('_encrypted.csv')]
        
        if csv_files:
            st.write("**📄 CSV-bestanden (geconverteerd):**")
            for file in csv_files:
                st.write(f"• `{file['name']}` ({file['size_formatted']})")
        
        if parquet_files:
            st.write("**🗜️ Parquet-bestanden (gecomprimeerd):**")
            for file in parquet_files:
                st.write(f"• `{file['name']}` ({file['size_formatted']})")
        
        if encrypted_files:
            st.write("**🔒 Versleutelde bestanden (definitief):**")
            for file in encrypted_files:
                st.write(f"• `{file['name']}` ({file['size_formatted']})")
        
else:
    st.info("📁 Nog geen geconverteerde bestanden gevonden in `data/02-output/`.")

# Warning about existing files
if os.path.exists("data/02-output") and os.listdir("data/02-output"):
    st.warning("⚠️ Nieuwe conversie zal bestaande bestanden in `data/02-output/` overschrijven")