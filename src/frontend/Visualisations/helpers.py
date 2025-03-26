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

    # Display schema information
    st.write("##### Data Schema")

    # Create schema table with example values
    schema_data = [] ## COMPUTATION
    for col in summary["columns"]:
        col_data = data.get_column(col)
        col_type = str(col_data.dtype)
        n_unique = len(col_data.unique())
        n_missing = col_data.null_count()

        # Get first non-null value, try up to first 1000 rows
        example = "No data"
        non_null_values = col_data.drop_nulls()
        if len(non_null_values) > 0:
            example = str(non_null_values[0])
            # If example is too long, truncate it
            if len(example) > 50:
                example = example[:47] + "..."

        schema_data.append(
            {
                "Column": col,
                "Type": col_type,
                "Unique Values": f"{n_unique:,}",
                "Missing Values": f"{n_missing:,} ({(n_missing / len(col_data) * 100):.1f}%)",
                "Example": example,
            }
        )

    # Convert to DataFrame and display
    schema_df = pl.DataFrame(schema_data)
    st.dataframe(
        schema_df,
        hide_index=True,
        column_config={
            "Column": st.column_config.TextColumn("Column Name", width="medium"),
            "Type": st.column_config.TextColumn("Data Type", width="small"),
            "Unique Values": st.column_config.TextColumn(
                "Unique Values", width="small"
            ),
            "Missing Values": st.column_config.TextColumn(
                "Missing Values", width="medium"
            ),
            "Example": st.column_config.TextColumn("Example Value", width="large"),
        },
    )

    # Add explanation of data types
    with st.expander("📖 Understanding Data Types"):
        st.markdown("""
        - **Utf8**: Text data (strings)
        - **Int64**: Integer numbers
        - **Float64**: Decimal numbers
        - **Date**: Date values
        - **Boolean**: True/False values
        """)
