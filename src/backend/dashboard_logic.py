import polars as pl
import plotly.express as px


class DashboardAnalytics:
    def __init__(self):
        self.data = None

    def load_data(self, df):
        # Ensure year is categorical and properly formatted
        self.data = df.with_columns(
            [
                pl.col("Inschrijvingsjaar").cast(pl.Utf8).alias("Year"),
                pl.col("GemEindcijferVoVanDeHoogsteVooroplVoorHetHo").cast(pl.Float64),
                pl.col("VerblijfsjaarActueleInstelling").cast(pl.Int64),
            ]
        )

    def get_intake_visualization(
        self,
        gender_filter=None,
        phase_filter=None,
        stack_by=None,
        opleiding_filter=None,
    ):
        if self.data is None:
            return None

        # Base query with filters
        query = self.data
        if gender_filter:
            query = query.filter(pl.col("Geslacht") == gender_filter)
        if phase_filter:
            query = query.filter(pl.col("OpleidingsfaseActueel") == phase_filter)
        if opleiding_filter:
            query = query.filter(pl.col("NAAM_OPLEIDING") == opleiding_filter)

        # Common plot parameters
        base_params = {
            "title": "Student Intake Analysis",
            "labels": {"Year": "Enrollment Year", "count": "Number of Students"},
            "template": "plotly_white",
        }

        # Data aggregation
        if stack_by:
            grouped = (
                query.group_by(["Year", stack_by])
                .agg(pl.len().alias("count"))
                .sort("Year")
                .to_pandas()
            )

            fig = px.bar(
                grouped,
                x="Year",
                y="count",
                color=stack_by,
                barmode="stack",
                **base_params,
            )
            fig.update_layout(title=f"Student Intake by Year and {stack_by}")
        else:
            grouped = (
                query.group_by("Year")
                .agg(pl.len().alias("count"))
                .sort("Year")
                .to_pandas()
            )

            fig = px.bar(grouped, x="Year", y="count", **base_params)
            fig.update_layout(title="Student Intake by Year")

        # Ensure categorical axis treatment
        fig.update_xaxes(type="category")
        fig.update_layout(
            xaxis_title="Enrollment Year",
            yaxis_title="Number of Students",
            legend_title=stack_by if stack_by else None,
        )

        return fig

    def get_data_summary(self):
        """Get detailed information about the loaded dataset"""
        if self.data is None:
            return None

        summary = {}

        # Total records
        summary["total_records"] = self.data.shape[0]

        # Year range
        years = self.data["Year"].unique().sort()
        summary["year_range"] = {"start": years[0], "end": years[-1]}

        # Gender distribution
        gender_dist = (
            self.data.group_by("Geslacht")
            .agg(pl.len().alias("Count"))
            .sort("Count", descending=True)
        )
        summary["gender_distribution"] = gender_dist

        # Education phase distribution
        phase_dist = (
            self.data.group_by("OpleidingsfaseActueel")
            .agg(pl.len().alias("Count"))
            .sort("Count", descending=True)
        )
        summary["phase_distribution"] = phase_dist

        # Available columns
        summary["columns"] = self.data.columns

        return summary

    @staticmethod
    def get_diploma_type(row):
        diploma_types = [
            "Hoofd-bachelor-diploma binnen de actuele instelling",
            "Neven-bachelor-diploma binnen de actuele instelling",
            "Hoofd-master-diploma binnen de actuele instelling",
            "Neven-master-diploma binnen de actuele instelling",
            "Hoofd-doctoraal-diploma binnen de actuele instelling",
            "Neven-doctoraal-diploma binnen de actuele instelling",
            "Hoofddiploma beroepsfase/voortgezet binnen de actuele instelling",
            "Nevendiploma beroepsfase/voortgezet binnen de actuele instelling",
            "Hoofddiploma associate degree binnen de actuele instelling",
            "Nevendiploma associate degree binnen de actuele instelling",
            "Hoofddiploma postinitiele master binnen de actuele instelling",
            "Nevendiploma postinitiele master binnen de actuele instelling",
        ]
        return (
            "Diploma behaald (excl. propedeuse)"
            if row["SoortDiplomaInstelling"] in diploma_types
            else "Geen diploma"
        )

    def get_performance_visualization(
        self,
        gender_filter=None,
        phase_filter=None,
        stack_by=None,
        opleiding_filter=None,
    ):
        """Generate performance visualization using a line chart with markers"""
        if self.data is None:
            return None

        try:
            # Data preparation with filters
            performance_data = self.data
            if gender_filter:
                performance_data = performance_data.filter(
                    pl.col("Geslacht") == gender_filter
                )
            if phase_filter:
                performance_data = performance_data.filter(
                    pl.col("OpleidingsfaseActueel") == phase_filter
                )
            if opleiding_filter:
                performance_data = performance_data.filter(
                    pl.col("NAAM_OPLEIDING") == opleiding_filter
                )

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
            if stack_by:
                grouped = (
                    performance_data.group_by(["eerste_jaar", "rendement", stack_by])
                    .agg(pl.count().alias("count"))
                    .sort(["eerste_jaar", "rendement", stack_by])
                )

                # Create combined legend names
                grouped = grouped.with_columns(
                    [
                        (pl.col("rendement") + " - " + pl.col(stack_by)).alias(
                            "legend_name"
                        )
                    ]
                )
            else:
                grouped = (
                    performance_data.group_by(["eerste_jaar", "rendement"])
                    .agg(pl.count().alias("count"))
                    .sort(["eerste_jaar", "rendement"])
                )
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
                if stack_by:
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
            print(f"Error in performance visualization: {str(e)}")
            return None

    def get_score_visualization(
        self,
        gender_filter=None,
        phase_filter=None,
        stack_by=None,
        opleiding_filter=None,
    ):
        """Generate score visualization showing average final scores over time"""
        if self.data is None:
            return None

        try:
            # Data preparation with filters
            score_data = self.data
            if gender_filter:
                score_data = score_data.filter(pl.col("Geslacht") == gender_filter)
            if phase_filter:
                score_data = score_data.filter(
                    pl.col("OpleidingsfaseActueel") == phase_filter
                )
            if opleiding_filter:
                score_data = score_data.filter(
                    pl.col("NAAM_OPLEIDING") == opleiding_filter
                )

            # Ensure score column is numeric and year is properly formatted
            score_data = score_data.with_columns(
                [
                    pl.col("GemEindcijferVoVanDeHoogsteVooroplVoorHetHo")
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
                    .to_pandas()
                )

                fig = px.line(
                    grouped,
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
                    .to_pandas()
                )

                fig = px.line(
                    grouped,
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

    def get_trends_visualization(
        self,
        gender_filter=None,
        phase_filter=None,
        stack_by=None,
        opleiding_filter=None,
    ):
        """Generate trends visualization showing enrollment trends over time"""
        if self.data is None:
            return None

        try:
            # Data preparation with filters
            trends_data = self.data
            if gender_filter:
                trends_data = trends_data.filter(pl.col("Geslacht") == gender_filter)
            if phase_filter:
                trends_data = trends_data.filter(
                    pl.col("OpleidingsfaseActueel") == phase_filter
                )
            if opleiding_filter:
                trends_data = trends_data.filter(
                    pl.col("NAAM_OPLEIDING") == opleiding_filter
                )

            # Ensure year is properly formatted
            trends_data = trends_data.with_columns(
                [pl.col("Inschrijvingsjaar").cast(pl.Utf8).alias("Year")]
            )

            # Group by logic depending on stack_by parameter
            if stack_by:
                grouped = (
                    trends_data.group_by(["Year", stack_by])
                    .agg(pl.len().alias("count"))
                    .sort(["Year", stack_by])
                    .to_pandas()
                )

                fig = px.line(
                    grouped,
                    x="Year",
                    y="count",
                    color=stack_by,
                    title="Enrollment Trends Over Time",
                    labels={
                        "Year": "Enrollment Year",
                        "count": "Number of Students",
                        stack_by: "Category",
                    },
                    markers=True,
                )
            else:
                grouped = (
                    trends_data.group_by("Year")
                    .agg(pl.len().alias("count"))
                    .sort("Year")
                    .to_pandas()
                )

                fig = px.line(
                    grouped,
                    x="Year",
                    y="count",
                    title="Enrollment Trends Over Time",
                    labels={"Year": "Enrollment Year", "count": "Number of Students"},
                    markers=True,
                )

            # Update layout for better readability
            fig.update_layout(
                xaxis_title="Enrollment Year",
                yaxis_title="Number of Students",
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
            print(f"Error in trends visualization: {str(e)}")
            return None
