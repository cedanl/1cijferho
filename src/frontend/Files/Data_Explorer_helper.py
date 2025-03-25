import streamlit as st
import os
import polars as pl

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

def get_bestandsbeschrijving_files(df):
    # Filter the dataframe to only include rows with "Bestandsbeschrijving" in the Filename
    return df.filter(pl.col("Filename").str.contains("Bestandsbeschrijving"))

def get_dec_files(df):
    # Filter the dataframe to only include rows with "Dec_" in the Filename
    return df.filter(pl.col("Filename").str.contains("Dec_"))

def get_main_files(df):
    # Filter for filenames containing any of the specified patterns
    pattern_filter = (
        pl.col("Filename").str.contains("EV") | 
        pl.col("Filename").str.contains("Croho") |
        pl.col("Filename").str.contains("Croho_vest") |
        pl.col("Filename").str.contains("VAKHAVW")
    )
    
    # Filter out files with specified extensions
    extension_filter = ~(
        pl.col("Extension").is_in([".txt", ".zip", ".xlsx"])
    )
    
    # Apply both filters
    return df.filter(pattern_filter & extension_filter)