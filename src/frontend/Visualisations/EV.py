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

# Assuming dfEV is loaded elsewhere. If not, load it here:
# dfEV = pl.read_csv("your_data_file.csv")

@st.cache_data
def filter_data(_df, gender_filter, phase_filter):
    df = _df  # Create a new reference to avoid modifying the original
    if gender_filter != "All":
        df = df.filter(pl.col("Geslacht") == gender_filter)
    if phase_filter != "All":
        df = df.filter(pl.col("OpleidingsfaseActueel") == phase_filter)
    return df

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

# Streamlit app
st.title("Student Intake Analysis Dashboard")

# Filter/Stack Section
st.header("Filters and Stacking")

# Create filter options
gender_options = ["All"] + sorted(dfEV["Geslacht"].unique().to_list())
phase_options = ["All"] + sorted(dfEV["OpleidingsfaseActueel"].unique().to_list())

gender_filter = st.selectbox("Filter by Gender", gender_options)
phase_filter = st.selectbox("Filter by Phase", phase_options)

# Create stack option
stack_options = ["None", "Geslacht", "OpleidingsfaseActueel"]
stack_by = st.selectbox("Stack by", stack_options)

# Apply filters
filtered_df = filter_data(dfEV, gender_filter, phase_filter)

# Generate and display visualization
fig = get_intake_visualization(filtered_df, stack_by if stack_by != "None" else None)
st.plotly_chart(fig)

# Display data counts
st.write(f"Total records: {len(dfEV)}")
st.write(f"Filtered records: {len(filtered_df)}")

# Optional: Display filtered data
if st.checkbox("Show filtered data"):
    st.write(filtered_df)

# Display current filter and stack settings in the sidebar for clarity
st.sidebar.write("Current Settings:")
st.sidebar.write(f"Gender Filter: {gender_filter}")
st.sidebar.write(f"Phase Filter: {phase_filter}")
st.sidebar.write(f"Stack By: {stack_by}")


# - data summary -> if we have time and last tab
# - diploma types
# - performance visualization
# - score visualisation
# - trends visualisation