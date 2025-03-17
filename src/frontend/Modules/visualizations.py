import streamlit as st


def render_visualizations(analytics):
    """Render the visualizations tab"""
    st.header("Student Intake Analysis")

    # Create visualization controls
    st.subheader("Visualization Controls")

    col1, col2, col3 = st.columns(3)

    # Gender filter
    with col1:
        gender_options = ["None"] + sorted(
            analytics.data.get_column("Geslacht").unique().to_list()
        )
        selected_gender = st.selectbox(
            "Filter by Gender:", gender_options, key="gender_filter"
        )

    # Education phase filter
    with col2:
        phase_options = ["None"] + sorted(
            analytics.data.get_column("OpleidingsfaseActueel").unique().to_list()
        )
        selected_phase = st.selectbox(
            "Filter by Education Phase:", phase_options, key="phase_filter"
        )

    # Stack by variable option
    with col3:
        stack_options = [
            "None",
            "Geslacht",
            "Inschrijvingsvorm",
            "IndicatieInternationaleStudent",
            "OpleidingsfaseActueel",
            "Opleidingsvorm",
        ]
        selected_stack = st.selectbox("Stack by:", stack_options, key="stack_by")

    # Conditional filter for NAAM_OPLEIDING
    if "NAAM_OPLEIDING" in analytics.data.columns:
        col4 = st.columns(1)[0]  # Fix the context manager issue
        with col4:
            opleiding_options = ["None"] + sorted(
                analytics.data.get_column("NAAM_OPLEIDING").unique().to_list()
            )
            selected_opleiding = st.selectbox(
                "Filter by Opleiding:",
                opleiding_options,
                key="opleiding_filter",
            )
    else:
        selected_opleiding = None

    # Create dynamic filter description
    filter_desc = []
    if selected_gender != "None":
        filter_desc.append(f"Gender: {selected_gender}")
    if selected_phase != "None":
        filter_desc.append(f"Education Phase: {selected_phase}")
    if selected_stack != "None":
        filter_desc.append(f"Stacked by: {selected_stack}")
    if selected_opleiding and selected_opleiding != "None":
        filter_desc.append(f"Opleiding: {selected_opleiding}")

    if filter_desc:
        st.caption(f"🔍 Showing data filtered by: {' | '.join(filter_desc)}")

    # Apply filters
    filters = {
        "gender_filter": selected_gender if selected_gender != "None" else None,
        "phase_filter": selected_phase if selected_phase != "None" else None,
        "stack_by": selected_stack if selected_stack != "None" else None,
        "opleiding_filter": selected_opleiding
        if selected_opleiding != "None"
        else None,
    }

    # Get and display intake visualization
    intake_fig = analytics.get_intake_visualization(**filters)

    if intake_fig:
        st.plotly_chart(intake_fig, use_container_width=True)
    else:
        st.info("No data available for intake visualization.")

    # Performance Section
    st.markdown("---")
    st.header("Study Performance Analysis")

    # Add dynamic filter description for performance visualization
    if filter_desc:
        st.caption(f"🔍 Showing data filtered by: {' | '.join(filter_desc)}")

    st.write(
        "This visualization shows the study performance distribution across cohorts, categorizing students based on their graduation timeline."
    )

    # Get and display performance visualization using the same filters
    perf_fig = analytics.get_performance_visualization(**filters)

    if perf_fig:
        st.plotly_chart(perf_fig, use_container_width=True)

        # Add explanation
        with st.expander("📖 Understanding the Performance Categories"):
            st.markdown("""
            - **Diploma binnen 3 jaar**: Students who graduated within 3 years
            - **Diploma na 3 jaar**: Students who graduated after 3 years
            - **Geen diploma**: Students without a diploma
            """)
    else:
        st.info("No data available for performance visualization.")

    # Add Score Analysis Section
    st.markdown("---")
    st.header("Average Final Scores Analysis")

    # Add dynamic filter description for score visualization
    if filter_desc:
        st.caption(f"🔍 Showing data filtered by: {' | '.join(filter_desc)}")

    st.write("This visualization shows the average final scores of students over time.")

    # Get and display score visualization using the same filters
    score_fig = analytics.get_score_visualization(**filters)

    if score_fig:
        st.plotly_chart(score_fig, use_container_width=True)

        # Add explanation
        with st.expander("📖 Understanding the Score Analysis"):
            st.markdown("""
            - This chart shows the average final scores of students over different enrollment years
            - Higher scores indicate better academic performance
            - The scores are calculated from 'GemEindcijferVoVanDeHoogsteVooroplVoorHetHo'
            """)
    else:
        st.info("No data available for score visualization.")

    return filters
