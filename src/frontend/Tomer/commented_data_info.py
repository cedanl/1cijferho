import streamlit as st
import polars as pl


def render_data_info(analytics):
    """Render the data info tab"""
    st.subheader("Dataset Information")

    # Get data summary
    summary = analytics.get_data_summary() ## Gebruik maar geen Class

    if summary:
        # Display total records and time period in metrics
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Records", f"{summary['total_records']:,}")
        with col2:
            st.metric(
                "Time Period",
                f"{summary['year_range']['start']} - {summary['year_range']['end']}",
            )

        # Conditionally display gender distribution and phase distribution
        if hasattr(analytics, "get_intake_visualization"): ## CONDITIONAL: EV wel, VAKHAVW Niet
            # Display gender distribution in a small table
            st.write("##### Gender Distribution")
            gender_df = summary["gender_distribution"].to_pandas()
            st.dataframe(gender_df, hide_index=True)

            # Display education phase distribution in a small table
            st.write("##### Education Phase Distribution")
            phase_df = summary["phase_distribution"].to_pandas()
            st.dataframe(phase_df, hide_index=True)

        # Display schema information
        st.write("##### Data Schema")

        # Create schema table with example values
        schema_data = [] ## COMPUTATION
        for col in summary["columns"]:
            col_data = analytics.data.get_column(col)
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
    else:
        st.info("No data summary available.")
