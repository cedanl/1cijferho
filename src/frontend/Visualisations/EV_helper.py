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


def get_score_visualization(
    data,
    gender_filter=None,
    phase_filter=None,
    stack_by=None
):
    """Generate score visualization showing average final scores over time"""
    if data is None:
        return None

    try:
        # Data preparation with filters
        score_data = data
        if gender_filter:
            score_data = score_data.filter(pl.col("Geslacht") == gender_filter)
        if phase_filter:
            score_data = score_data.filter(pl.col("OpleidingsfaseActueel") == phase_filter)

        # Ensure score column is numeric and year is properly formatted
        score_data = score_data.with_columns(
            [
                pl.col("GemEindcijferVoVanDeHoogsteVooroplVRHetHo")
                .cast(pl.Float64)
                .alias("average_score"),
                pl.col("Inschrijvingsjaar").cast(pl.Utf8).alias("Year"),
            ]
        )

        # Group by logic depending on stack_by parameter
        if stack_by:
            grouped = (
                score_data.group_by(["Year", stack_by])
                .agg(pl.col("average_score").mean().alias("avg_score"))
                .sort(["Year", stack_by])
            )

            fig = px.line(
                grouped.to_pandas(),
                x="Year",
                y="avg_score",
                color=stack_by,
                title="Average Final Scores Over Time",
                labels={
                    "Year": "Enrollment Year",
                    "avg_score": "Average Final Score",
                    stack_by: "Category",
                },
                markers=True,
            )
        else:
            grouped = (
                score_data.group_by("Year")
                .agg(pl.col("average_score").mean().alias("avg_score"))
                .sort("Year")
            )

            fig = px.line(
                grouped.to_pandas(),
                x="Year",
                y="avg_score",
                title="Average Final Scores Over Time",
                labels={
                    "Year": "Enrollment Year",
                    "avg_score": "Average Final Score",
                },
                markers=True,
            )

        # Update layout for better readability
        fig.update_layout(
            xaxis_title="Enrollment Year",
            yaxis_title="Average Final Score",
            plot_bgcolor="white",
            xaxis={"type": "category"},
            height=500,
            showlegend=True if stack_by else False,
            margin=dict(t=30),
        )

        # Enhance grid and axes
        fig.update_traces(line=dict(width=2))
        fig.update_xaxes(gridcolor="lightgray", tickangle=45)
        fig.update_yaxes(gridcolor="lightgray", zeroline=True, zerolinecolor="gray")

        return fig

    except Exception as e:
        print(f"Error in score visualization: {str(e)}")
        return None


import polars as pl
import plotly.express as px
import streamlit as st

def get_performance_visualization(
    data,
    gender_filter=None,
    phase_filter=None,
    stack_by=None
    ):
    """Generate performance visualization using a line chart with markers"""
    if data is None:
        return None

    try:
        # Data preparation with filters
        performance_data = data
        if gender_filter and gender_filter != "All":
            performance_data = performance_data.filter(
                pl.col("Geslacht") == gender_filter
            )
        if phase_filter and phase_filter != "All":
            performance_data = performance_data.filter(
                pl.col("OpleidingsfaseActueel") == phase_filter
            )

        # Check if required columns exist
        required_columns = ["Inschrijvingsjaar", "VerblijfsjaarActueleInstelling", "SoortDiplomaInstelling"]
        missing_columns = [col for col in required_columns if col not in performance_data.columns]
        if missing_columns:
            st.error(f"Missing required columns: {', '.join(missing_columns)}")
            return None

        performance_data = (
            performance_data.filter(pl.col("Inschrijvingsjaar").is_not_null())
            .with_columns(
                [
                    pl.col("Inschrijvingsjaar").cast(pl.Int64).alias("eerste_jaar"),
                    pl.col("VerblijfsjaarActueleInstelling")
                    .cast(pl.Int64)
                    .fill_null(0)
                    .alias("verblijfsjaar"),
                    pl.col("SoortDiplomaInstelling")
                    .cast(pl.Utf8)
                    .fill_null("")
                    .alias("diploma_type"),
                ]
            )
            .filter(pl.col("eerste_jaar") > 0)
            .with_columns(
                [
                    pl.when(
                        pl.col("diploma_type").cast(pl.Utf8).fill_null("") == ""
                    )
                    .then(pl.lit("Geen diploma"))
                    .when(pl.col("verblijfsjaar") <= 3)
                    .then(pl.lit("Diploma binnen 3 jaar"))
                    .otherwise(pl.lit("Diploma na 3 jaar"))
                    .alias("rendement")
                ]
            )
        )

        # Group by logic depending on stack_by parameter
        group_cols = ["eerste_jaar", "rendement"]
        if stack_by and stack_by != "None" and stack_by in performance_data.columns:
            group_cols.append(stack_by)

        grouped = (
            performance_data.group_by(group_cols)
            .agg(pl.count().alias("count"))
            .sort(group_cols)
        )

        if stack_by and stack_by != "None" and stack_by in performance_data.columns:
            grouped = grouped.with_columns(
                [
                    (pl.col("rendement") + " - " + pl.col(stack_by)).alias(
                        "legend_name"
                    )
                ]
            )
        else:
            grouped = grouped.with_columns(
                [pl.col("rendement").alias("legend_name")]
            )

        if grouped.shape[0] > 0:
            # Create line chart with markers
            fig = px.line(
                grouped.to_pandas(),
                x="eerste_jaar",
                y="count",
                color="legend_name",
                title="Study Performance by Cohort",
                labels={
                    "eerste_jaar": "Enrollment Year",
                    "count": "Number of Students",
                    "legend_name": "Performance",
                },
                markers=True,
            )

            # Create a custom color map based on the performance categories
            if stack_by and stack_by != "None" and stack_by in performance_data.columns:
                base_colors = {
                    "Diploma binnen 3 jaar": "#2ecc71",
                    "Diploma na 3 jaar": "#3498db",
                    "Geen diploma": "#e74c3c",
                }
                # Create color variations for each stacked category
                color_map = {}
                for perf, base_color in base_colors.items():
                    for stack_val in grouped[stack_by].unique():
                        color_map[f"{perf} - {stack_val}"] = base_color

                fig.update_traces(opacity=0.7)
            else:
                color_map = {
                    "Diploma binnen 3 jaar": "#2ecc71",
                    "Diploma na 3 jaar": "#3498db",
                    "Geen diploma": "#e74c3c",
                }

            fig.update_traces(line=dict(width=2))

            # Update layout for better readability
            fig.update_layout(
                xaxis_title="Enrollment Year",
                yaxis_title="Number of Students",
                plot_bgcolor="white",
                legend_title="Performance Categories",
                xaxis={"type": "category"},
                height=500,
                showlegend=True,
                margin=dict(t=30),
            )

            # Enhance grid and axes
            fig.update_xaxes(gridcolor="lightgray", tickangle=45)
            fig.update_yaxes(
                gridcolor="lightgray", zeroline=True, zerolinecolor="gray"
            )

            return fig

        return None

    except Exception as e:
        st.error(f"Error in performance visualization: {str(e)}")
        return None


