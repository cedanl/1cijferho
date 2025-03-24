import polars as pl
import plotly.express as px


class VAKHAVWDashboardAnalytics:
    def __init__(self):
        self.data = None

    def load_data(self, df):
        self.data = df.with_columns(
            [
                pl.col("Diplomajaar").cast(pl.Utf8),
                pl.col("CijferSchoolexamen").cast(pl.Float64),
                pl.col("CijferEersteCentraalExamen").cast(pl.Float64),
                pl.col("CijferTweedeCentraalExamen").cast(pl.Float64),
                pl.col("CijferDerdeCentraalExamen").cast(pl.Float64),
                pl.col("EersteEindcijfer").cast(pl.Float64),
                pl.col("TweedeEindcijfer").cast(pl.Float64),
                pl.col("DerdeEindcijfer").cast(pl.Float64),
                pl.col("CijferCijferlijst").cast(pl.Float64),
            ]
        )

    def get_data_summary(self):
        """Get detailed information about the loaded dataset"""
        if self.data is None:
            return None

        summary = {}

        # Total records
        summary["total_records"] = self.data.shape[0]

        # Year range
        years = self.data["Diplomajaar"].unique().sort()
        summary["year_range"] = {"start": years[0], "end": years[-1]}

        # Available columns
        summary["columns"] = self.data.columns

        return summary

    def get_AfkortingVak_visualization(
        self, diplomajaar_filter=None, metric="CijferSchoolexamen"
    ):
        if self.data is None:
            return None

        query = self.data
        if diplomajaar_filter:
            diplomajaar_filter = str(diplomajaar_filter)  # Ensure the filter value is a string
            query = query.with_columns(pl.col("Diplomajaar").cast(pl.Utf8))
            query = query.filter(pl.col("Diplomajaar") == diplomajaar_filter)

        grouped = (
            query.group_by(["AfkortingVak", "Diplomajaar"])
            .agg(pl.col(metric).mean().alias("avg_score"))
            .sort(["AfkortingVak", "Diplomajaar"])
            .to_pandas()
        )

        fig = px.bar(
            grouped,
            x="AfkortingVak",
            y="avg_score",
            color="Diplomajaar",
            title=f"Average {metric} by Subject and Year",
            labels={"AfkortingVak": "Subject", "avg_score": f"Average {metric}"},
            barmode="group",
        )

        fig.update_layout(
            xaxis_title="Subject",
            yaxis_title=f"Average {metric}",
            plot_bgcolor="white",
            height=500,
            showlegend=True,
            margin=dict(t=30),
        )

        fig.update_xaxes(gridcolor="lightgray", tickangle=45)
        fig.update_yaxes(gridcolor="lightgray", zeroline=True, zerolinecolor="gray")

        return fig

    def get_trends_visualization(
        self, AfkortingVak_filter=None, metric="CijferSchoolexamen"
    ):
        """Generate trends visualization showing average school exam scores over time for a specific subject"""
        if self.data is None:
            return None

        try:
            # Data preparation with filters
            trends_data = self.data
            if AfkortingVak_filter:
                trends_data = trends_data.filter(
                    pl.col("AfkortingVak") == AfkortingVak_filter
                )

            # Ensure year is properly formatted
            trends_data = trends_data.with_columns(
                [pl.col("Diplomajaar").cast(pl.Utf8).alias("Year")]
            )

            # Group by year and calculate average score
            grouped = (
                trends_data.group_by("Year")
                .agg(pl.col(metric).mean().alias("avg_score"))
                .sort("Year")
                .to_pandas()
            )

            fig = px.line(
                grouped,
                x="Year",
                y="avg_score",
                title=f"Average {metric} Over Time for {AfkortingVak_filter}",
                labels={"Year": "Diploma Year", "avg_score": f"Average {metric}"},
                markers=True,
            )

            # Update layout for better readability
            fig.update_layout(
                xaxis_title="Diploma Year",
                yaxis_title=f"Average {metric}",
                plot_bgcolor="white",
                xaxis={"type": "category"},
                height=500,
                showlegend=False,
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
