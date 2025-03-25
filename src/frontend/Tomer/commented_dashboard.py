import streamlit as st
from backend.dashboard_analytics import DashboardAnalytics
from backend.vakhavw_dashboard_analytics import VAKHAVWDashboardAnalytics
from frontend.Modules.visualizations import render_visualizations
from frontend.Modules.data_info import render_data_info
from frontend.Modules.trends_analysis import render_trends_analysis


def render_dashboard():
    """Render the student intake dashboard"""
    # if "df" not in st.session_state:
    #     st.info("Please upload a data file to begin visualization.")
    #     return

    # df_columns = set(st.session_state.df.columns)
    # vakhavw_columns = {'CijferTweedeCentraalExamen', 'AfkortingVak', 'CijferCijferlijst', 'Diplomajaar', 'EersteEindcijfer', 'Vakcode', 'BrinnummerVoinstelling', 'Onderwijsnummer', 'CijferDerdeCentraalExamen', 'VooropleidingOorspronkelijkeCode', 'PersoonsgebondenNummer', 'Burgerservicenummer', 'AnderNiveau', 'VestigingsnummerVovestiging', 'TweedeEindcijfer', 'IndicatieDiplomavak', 'BeoordelingSchoolexamen', 'DerdeEindcijfer', 'CijferSchoolexamen', 'CijferEersteCentraalExamen', 'GemiddeldCijferCijferlijst'}

    # if vakhavw_columns.issubset(df_columns):
    #     analytics = VAKHAVWDashboardAnalytics()
    #     title = "VAKHAVW Dashboard"
    # else:
    #     analytics = DashboardAnalytics()
    #     title = "Student Intake Dashboard"

    # analytics.load_data(st.session_state.df)
    # st.title(title)

    # Create tabs for different sections of the dashboard
    tab1, tab2, tab3 = st.tabs( ## tabjes aanmaken kan nu voor iedere "module" ipv hier
        ["📊 Visualizations", "ℹ️ Data Info", "📈 Trends Analysis"]
    )

    # Render the visualizations tab
    with tab1:
        if isinstance(analytics, VAKHAVWDashboardAnalytics):
            diplomajaar_filter = st.selectbox( ## NODIG --> filter
                "Select Diplomajaar",
                options=st.session_state.df["Diplomajaar"].unique(),
                key="diplomajaar_filter",
            )
            metric = st.selectbox( ## NODIG --> staat gebruikers toe welke te gebruiken
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
                key="metric_selectbox_1",
            )
            fig = analytics.get_AfkortingVak_visualization(diplomajaar_filter, metric)
            st.plotly_chart(fig)
        else:
            filters = render_visualizations(analytics)

    # Render the data info tab
    with tab2:
        render_data_info(analytics) ## ## NODIG --> als je de gebruiker snelle inzichten wil laten hebben

    # Render the trends analysis tab
    with tab3:
        if isinstance(analytics, VAKHAVWDashboardAnalytics):
            AfkortingVak_filter = st.selectbox( ## NODIG --> filter
                "Select AfkortingVak",
                options=st.session_state.df["AfkortingVak"].unique().sort(),
                key="AfkortingVak_filter",
            )
            metric = st.selectbox( ## REPEAT van regel 41
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
                key="metric_selectbox_2",
            )
            render_trends_analysis(analytics, AfkortingVak_filter, metric)
        else:
            render_trends_analysis(
                analytics, filters if "filters" in locals() else None
            )


# if __name__ == "__main__":
#     render_dashboard()
