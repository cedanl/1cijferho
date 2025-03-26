import frontend.Visualisations.EV_helper as helper
import backend.analytics.EV_analytics as analytics_ev
import frontend.Visualisations.helpers as helpers
import streamlit as st
import polars as pl

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
dfEV = helper.find_and_load_ev_csv("data/012-output")

@st.cache_data
def filter_data(_df, gender_filter, phase_filter):
    df = _df.clone()  # Create a new reference to avoid modifying the original
    if gender_filter and gender_filter != "All":
        df = df.filter(pl.col("Geslacht") == gender_filter)
    if phase_filter and phase_filter != "All":
        df = df.filter(pl.col("OpleidingsfaseActueel") == phase_filter)
    return df


# Filter/Stack Section
st.header("Filters and Stacking")

# Create filter options
gender_filter = st.selectbox("Filter by Gender", ["All"] + sorted(dfEV["Geslacht"].unique().to_list()))
phase_filter = st.selectbox("Filter by Phase", ["All"] + sorted(dfEV["OpleidingsfaseActueel"].unique().to_list()))

# Create stack option
stack_by = st.selectbox("Stack by", ["None", "Geslacht", "OpleidingsfaseActueel"])

# Apply filters
filtered_df = filter_data(
    dfEV,
    gender_filter,
    phase_filter
)

# Create tabs for different sections of the dashboard
tab1, tab2, tab3, tab4, tab5 = st.tabs( 
    ["📊 Visualizations", "📈 Trends Analysis", "🏆 Performance", "🎓 Scores", "ℹ️ Data Info"]
)


# Logic


# - Inladen + Types + PascalCase
# - intake_visualization

# -----------------------------------------------------------------------------
# Test

with tab1:
    # Streamlit app
    st.title("Student Intake Analysis Dashboard")

    # Generate and display visualization
    fig = analytics_ev.get_intake_visualization(filtered_df, stack_by if stack_by != "None" else None)
    st.plotly_chart(fig)

with tab2:
    # Generate and display visualization
    trends_fig = analytics_ev.get_trends_visualization(filtered_df, gender_filter, phase_filter, stack_by)
    if trends_fig:
        st.plotly_chart(trends_fig)
    else:
        st.write("No data available for the selected filters.")

with tab3:
    per = analytics_ev.get_performance_visualization(filtered_df, gender_filter, phase_filter, stack_by)
    st.plotly_chart(per)

with tab4:
    st.title("Student Intake Analysis Dashboard")
    fig = analytics_ev.get_score_visualization(filtered_df, stack_by if stack_by != "None" else None)
    st.plotly_chart(fig)
    

with tab5:
    st.dataframe(dfEV.head(1000))  # Show first 1000 rows
    helpers.render_data_info(dfEV)

# - data summary -> if we have time and last tab
# - diploma types
# - performance visualization
# - score visualisation
# - trends visualisation