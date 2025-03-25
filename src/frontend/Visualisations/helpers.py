import streamlit as st
import polars as pl
import plotly.express as px

def get_data_summary(data):
    """Get detailed information about the loaded dataset"""
    if data is None:
        return None

    summary = {}

    # Total records
    summary["total_records"] = data.shape[0]

    # Year range
    ## add conditional if exists Inschrijvingsjaar, or Diplomajaar

    if "Diplomajaar" in data.columns:
        years = data["Diplomajaar"].unique().sort()
    else:
        years = data["Inschrijvingsjaar"].unique().sort()
    
    summary["year_range"] = {"start": years[0], "end": years[-1]}

    # Gender distribution
    if "Geslacht" in data.columns:
        gender_dist = (
            data.group_by("Geslacht")
            .agg(pl.len().alias("Count"))
            .sort("Count", descending=True)
        )
        summary["gender_distribution"] = gender_dist

    # Education phase distribution
    if "OpleidingsfaseActueel" in data.columns:
        phase_dist = (
            data.group_by("OpleidingsfaseActueel")
            .agg(pl.len().alias("Count"))
            .sort("Count", descending=True)
        )
        summary["phase_distribution"] = phase_dist

    # Available columns
    summary["columns"] = data.columns

    return summary

def render_data_info(data):
    """Render the data information in Streamlit"""
    summary = get_data_summary(data)
    
    if summary is None:
        st.write("No data available.")
        return

    st.subheader("Dataset Summary")
    st.write(f"Total Records: {summary['total_records']}")
    st.write(f"Year Range: {summary['year_range']['start']} - {summary['year_range']['end']}")


    ## conditional display if Geslacht and OpleidingsfaseActueel exists
    if "Geslacht" in summary['columns']:
        st.subheader("Gender Distribution")
        st.write(summary['gender_distribution'])

    if "OpleidingsfaseActueel" in summary['columns']:
        st.subheader("Education Phase Distribution")
        st.write(summary['phase_distribution'])

    st.subheader("Available Columns")
    st.write(", ".join(summary['columns']))