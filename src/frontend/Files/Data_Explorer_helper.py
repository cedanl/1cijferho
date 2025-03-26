import streamlit as st
import os
import polars as pl
import tkinter as tk
from tkinter import filedialog
import platform
import tempfile
import subprocess

def select_folder():
    # Different approach based on OS
    if platform.system() == "Darwin":  # macOS
        try:
            # Create a temporary AppleScript file
            script_file = tempfile.NamedTemporaryFile(delete=False, suffix='.scpt', mode='w')
            script_path = script_file.name
            
            # Write AppleScript to select folder
            script_file.write('''
            set folderPath to POSIX path of (choose folder with prompt "Select a folder:")
            return folderPath
            ''')
            script_file.close()
            
            # Execute the AppleScript
            result = subprocess.run(['osascript', script_path], 
                                   capture_output=True, 
                                   text=True, 
                                   check=True)
            
            # Clean up
            os.unlink(script_path)
            
            # Return the selected folder path
            folder_path = result.stdout.strip()
            return folder_path if folder_path else None
            
        except subprocess.CalledProcessError as e:
            # User probably canceled the dialog
            return None
        except Exception as e:
            st.error(f"Error selecting folder: {str(e)}")
            return None
            
    else:  # Windows and other systems
        import tkinter as tk
        from tkinter import filedialog
        
        root = tk.Tk()
        root.withdraw()
        folder_path = filedialog.askdirectory(master=root)
        root.destroy()
        return folder_path

def get_files_dataframe(folder_path):
    if not os.path.exists(folder_path):
        return None
    
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    
    if not files:
        return None
    
    # Calculate raw file sizes in MB
    sizes_mb = [os.path.getsize(os.path.join(folder_path, f)) / (1024 * 1024) for f in files]
    
    # Create the DataFrame with raw values
    df = pl.DataFrame({
        "Filename": files,
        "Extension": [os.path.splitext(f)[1] for f in files],
        "Size (MB)": sizes_mb
    })
    
    # Convert to Decimal datatype and round to 1 decimal place
    df = df.with_columns(pl.col("Size (MB)").cast(pl.Decimal(precision=10, scale=3)))
    
    return df

def categorize_files(df):
    if df is None:
        return None
        
    # Create a new column "Type" based on filename patterns
    return df.with_columns(
        pl.when(pl.col("Filename").str.contains("Bestandsbeschrijving"))
        .then(pl.lit("Bestandsbeschrijving"))
        .when(pl.col("Filename").str.contains("Dec_"))
        .then(pl.lit("Decodeer File"))
        .when(
            (pl.col("Filename").str.contains("EV") | 
             pl.col("Filename").str.contains("Croho") |
             pl.col("Filename").str.contains("Croho_vest") |
             pl.col("Filename").str.contains("VAKHAVW")) &
            ~pl.col("Extension").is_in([".txt", ".zip", ".xlsx"])
        )
        .then(pl.lit("Main File"))
        .otherwise(pl.lit("Other"))
        .alias("Type")  # This explicitly names the column "Type"
    )
