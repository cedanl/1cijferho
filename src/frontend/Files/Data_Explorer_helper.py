import streamlit as st
import os
import polars as pl

INPUT_FOLDER = "data/01-input"

def get_files_dataframe(folder_path):
    if not os.path.exists(folder_path):
        return None
    
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    
    if not files:
        return None
    
    df = pl.DataFrame({
        "Filename": files,
        "Extension": [os.path.splitext(f)[1] for f in files]
    })
    
    return df

# ---

def filter_bestandbeschrijving_txt(df):
    # Filter the DataFrame to only include Bestandbeschrijving.txt files
    mask = (
        df['Filename'].str.contains('Bestandbeschrijving') & 
        df['Extension'].str.contains('txt')
    )
    
    # Return the filtered DataFrame
    return df[mask]