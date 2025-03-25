import streamlit as st
import frontend.Visualisations.EV_helper as helper

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="EV",
    layout="wide",  # This sets the layout to centered (not wide)
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Main Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title("EV")
st.caption("DEMO - EV Analytics")

# -----------------------------------------------------------------------------
# Filter/Stack Section
# -----------------------------------------------------------------------------
# Show filters and stack options to apply to the data

# Load EV File 
dfEV = helper.find_and_load_ev_csv("data/02-output")
st.dataframe(dfEV.head(1000))  # Show first 1000 rows

# Logic

# - Inladen + Types + PascalCase
# - intake_visualization

# -----------------------------------------------------------------------------
# Test

import streamlit as st
import polars as pl
import plotly.express as px

# def filter_data(df, gender_filter=None, phase_filter=None, opleiding_filter=None):
def filter_data(df, gender_filter=None, phase_filter=None):
    filtered_df = df
    if gender_filter:
        filtered_df = filtered_df[filtered_df["Geslacht"] == gender_filter]
    if phase_filter:
        filtered_df = filtered_df[filtered_df["OpleidingsfaseActueel"] == phase_filter]
    # if opleiding_filter:
        # filtered_df = filtered_df[filtered_df["NAAM_OPLEIDING"] == opleiding_filter]
    return filtered_df

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

# Filter/Stack Section
st.header("Filters and Stacking")

# Create filter options
gender_filter = st.selectbox("Filter by Gender", ["All"] + list(dfEV["Geslacht"].unique()))
phase_filter = st.selectbox("Filter by Phase", ["All"] + list(dfEV["OpleidingsfaseActueel"].unique()))
# opleiding_filter = st.selectbox("Filter by Opleiding", ["All"] + list(dfEV["NAAM_OPLEIDING"].unique()))

# Create stack option
# stack_by = st.selectbox("Stack by", ["None", "Geslacht", "OpleidingsfaseActueel", "NAAM_OPLEIDING"])
stack_by = st.selectbox("Stack by", ["None", "Geslacht", "OpleidingsfaseActueel"])

# Apply filters
filtered_df = filter_data(
    dfEV,
    gender_filter if gender_filter != "All" else None,
    phase_filter if phase_filter != "All" else None,
    # opleiding_filter if opleiding_filter != "All" else None
)

# Generate and display visualization
fig = get_intake_visualization(filtered_df, stack_by if stack_by != "None" else None)
st.plotly_chart(fig)


# - data summary -> if we have time and last tab
# - diploma types
# - performance visualization
# - score visualisation
# - trends visualisation