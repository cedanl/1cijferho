import streamlit as st
import frontend.Visualisations.VAKHAVW_helper as helper
import frontend.Visualisations.helpers as helpers
import backend.analytics.VAKHAVW_analytics as analytics_vakhavw
import polars as pl

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="VAKHAVW",
    layout="wide",  # This sets the layout to centered (not wide)
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Main Section
# -----------------------------------------------------------------------------
# Main header and subtitle
st.title("VAKHAVW")
st.caption("DEMO - VAKHAVW Analytics")

# Logic 
# Load EV File 
try:
    dfVAKHAVW = helper.find_and_load_vakhavw_csv("data/02-output")
except Exception as e:
    st.warning(f"Correct VAKHAVW not found, did you run the Magic Converter first?{str(e)}")
    
@st.cache_data
def filter_data(_df, diplomajaar_filter=None, afkortingvak_filter=None):
    """Filter the dataframe based on Diplomajaar and AfkortingVak."""
    df = _df  # Create a new reference to avoid modifying the original
    if diplomajaar_filter and diplomajaar_filter != "All":
        diplomajaar_filter = str(diplomajaar_filter)  # Ensure it's a string
        df = df.with_columns(pl.col("Diplomajaar").cast(pl.Utf8))  # Cast to string
        df = df.filter(pl.col("Diplomajaar") == diplomajaar_filter)
    if afkortingvak_filter and afkortingvak_filter != "All":
        df = df.filter(pl.col("AfkortingVak") == afkortingvak_filter)
    return df

# Filter/Stack Section
st.header("Filters and Stacking")

# Create filter options
diplomajaar_filter = st.selectbox(
    "Filter by Diploma Year", ["All"] + sorted(dfVAKHAVW["Diplomajaar"].unique().to_list())
)
afkortingvak_filter = st.selectbox(
    "Filter by Subject (AfkortingVak)", ["All"] + sorted(dfVAKHAVW["AfkortingVak"].unique().to_list())
)

# Create stack option
stack_by = st.selectbox(
    "Stack by", ["None", "Diplomajaar", "AfkortingVak"]
)

# Create metric selection
metric = st.selectbox(
    "Select Metric",
    options=[
        "CijferSchoolexamen",
        "CijferEersteCentraalExamen",
        "CijferTweedeCentraalExamen",
        "CijferDerdeCentraalExamen",
        "EersteEindcijfer",
        "TweedeEindcijfer",
        "DerdeEindcijfer",
        "CijferCijferlijst",
    ],
    key="metric_selectbox"
)

# Apply filters
filtered_df = filter_data(dfVAKHAVW, diplomajaar_filter, afkortingvak_filter)

# Create tabs for different sections of the dashboard
tab1, tab2, tab3 = st.tabs( ## tabjes aanmaken kan nu voor iedere "module" ipv hier
    ["📊 Visualizations", "ℹ️ Data Info", "📈 Trends Analysis"]
)
# Logic


# - Inladen + Types + PascalCase
# - intake_visualization

# -----------------------------------------------------------------------------
# Test

with tab1:
    st.title("Student Analysis Dashboard")

    # Generate and display visualization
    fig = analytics_vakhavw.get_AfkortingVak_visualization(filtered_df, stack_by if stack_by != "None" else None)
    st.plotly_chart(fig)

with tab2:
    st.dataframe(dfVAKHAVW.head(1000))  # Show first 1000 rows
    helpers.render_data_info(dfVAKHAVW)

with tab3:
    st.title("Trends Analysis Dashboard")
    trends_fig = analytics_vakhavw.get_trends_visualization(filtered_df, afkortingvak_filter, metric)
    if trends_fig:
        st.plotly_chart(trends_fig)
    else:
        st.write("No data available for the selected filters.")
    



# - Inladen + Types + PascalCase
# - data summary -> if we have time and last tab
# - AfkortingVak Visualization
# - Get trends visualization