import streamlit as st
from backend.vakhavw_dashboard_analytics import VAKHAVWDashboardAnalytics


def render_trends_analysis(analytics, filters, metric=None):
    """Render the trends analysis tab"""
    st.subheader("Trends Analysis")

    if isinstance(analytics, VAKHAVWDashboardAnalytics):
        vakafkorting_filter = filters
        fig = analytics.get_trends_visualization(
            vakafkorting_filter=vakafkorting_filter, metric=metric
        )
    else:
        if filters:
            gender_filter = filters.get("gender_filter")
            phase_filter = filters.get("phase_filter")
            stack_by = filters.get("stack_by")
            opleiding_filter = filters.get("opleiding_filter")
        else:
            gender_filter = None
            phase_filter = None
            stack_by = None
            opleiding_filter = None

        fig = analytics.get_trends_visualization(
            gender_filter=gender_filter,
            phase_filter=phase_filter,
            stack_by=stack_by,
            opleiding_filter=opleiding_filter,
        )

    if fig:
        st.plotly_chart(fig)
    else:
        st.warning("No data available for trends visualization.")
