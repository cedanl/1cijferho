from pathlib import Path
import plotly.express as px
import polars as pl
import streamlit as st

def find_and_load_ev_csv(directory):
    """Find and load first CSV file with 'EV' in name as Polars DataFrame."""
    file_path = next(Path(directory).glob("*EV*.parquet"))
    return pl.read_parquet(file_path)



def get_intake_visualization(df, stack_by=None):
    df = df.with_columns(pl.col('Inschrijvingsjaar').cast(pl.Utf8).alias('Year'))
    
    if stack_by:
        grouped = (df.group_by(['Year', stack_by])
                    .agg(pl.count().alias('count'))
                    .sort('Year'))
        fig = px.bar(grouped.to_pandas(), x='Year', y='count', color=stack_by, barmode='stack',
                    title='Student Intake Analysis',
                    labels={'Year': 'Enrollment Year', 'count': 'Number of Students'})
    else:
        grouped = (df.group_by('Year')
                    .agg(pl.count().alias('count'))
                    .sort('Year'))
        fig = px.bar(grouped.to_pandas(), x='Year', y='count',
                    title='Student Intake Analysis',
                    labels={'Year': 'Enrollment Year', 'count': 'Number of Students'})
    
    fig.update_xaxes(type='category')
    fig.update_layout(xaxis_title='Enrollment Year',
                    yaxis_title='Number of Students',
                    legend_title=stack_by if stack_by else None)
    return fig

def get_trends_visualization(data, gender_filter=None, phase_filter=None, stack_by=None):
    """Generate trends visualization showing enrollment trends over time"""
    if data is None:
        return None

    try:
        # Data preparation with filters
        trends_data = data
        if gender_filter and gender_filter != "All":
            trends_data = trends_data.filter(pl.col("Geslacht") == gender_filter)
        if phase_filter and phase_filter != "All":
            trends_data = trends_data.filter(pl.col("OpleidingsfaseActueel") == phase_filter)

        # Ensure year is properly formatted
        trends_data = trends_data.with_columns([pl.col("Inschrijvingsjaar").cast(pl.Utf8).alias("Year")])

        # Group by logic depending on stack_by parameter
        if stack_by and stack_by != "None":
            grouped = (trends_data.group_by(["Year", stack_by])
                    .agg(pl.count().alias("count"))
                    .sort(["Year", stack_by]))

            fig = px.line(grouped.to_pandas(), x="Year", y="count", color=stack_by,
                        title="Enrollment Trends Over Time",
                        labels={"Year": "Enrollment Year", "count": "Number of Students", stack_by: "Category"},
                        markers=True)
        else:
            grouped = (trends_data.group_by("Year")
                    .agg(pl.count().alias("count"))
                    .sort("Year"))

            fig = px.line(grouped.to_pandas(), x="Year", y="count",
                        title="Enrollment Trends Over Time",
                        labels={"Year": "Enrollment Year", "count": "Number of Students"},
                        markers=True)

        # Update layout for better readability
        fig.update_layout(
            xaxis_title="Enrollment Year",
            yaxis_title="Number of Students",
            plot_bgcolor="white",
            xaxis={"type": "category"},
            height=500,
            showlegend=True if stack_by and stack_by != "None" else False,
            margin=dict(t=30),
        )

        # Enhance grid and axes
        fig.update_traces(line=dict(width=2))
        fig.update_xaxes(gridcolor="lightgray", tickangle=45)
        fig.update_yaxes(gridcolor="lightgray", zeroline=True, zerolinecolor="gray")

        return fig
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        return None
