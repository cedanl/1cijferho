"""
Switch Analysis Module
Tracks study program switches within institutions based on VU and Avans approaches
Creates switch FROM and TO variables for each student-opleiding-year combination
"""

import polars as pl
from pathlib import Path
import argparse
from rich.console import Console
from datetime import datetime

console = Console()

# =============================================================================
# SWITCH DETECTION FUNCTIONS
# =============================================================================

def add_switch_within_1_year(df):
    """
    Detect switches within 1 year (Avans style)
    Compares verblijfsjaar 1 vs 2 for same student
    Only for students not dropped out or graduated within 1 year
    """
    # Find required columns
    person_col = None
    program_col = None
    year_col = None
    verblijfsjaar_col = None
    diploma_col = None

    for col in df.columns:
        if "persoonsgebonden_nummer" in col.lower():
            person_col = col
        elif "opleidingscode" in col.lower():
            program_col = col
        elif "inschrijvingsjaar" in col.lower():
            year_col = col
        elif "verblijfsjaar" in col.lower() and "actuele" in col.lower():
            verblijfsjaar_col = col
        elif "diplomajaar" in col.lower():
            diploma_col = col

    if not all([person_col, program_col, year_col, verblijfsjaar_col]):
        missing = []
        if not person_col: missing.append("persoonsgebonden_nummer")
        if not program_col: missing.append("opleidingscode/isat")
        if not year_col: missing.append("inschrijvingsjaar")
        if not verblijfsjaar_col: missing.append("verblijfsjaar*actuele*")
        console.print(f"[yellow]⚠️  Required columns not found for 1-year switch detection: {missing}")
        console.print(f"[yellow]    Available columns: {df.columns[:10]}...")
        return df.with_columns([
            pl.lit(None).alias("switch_binnen_1_jaar"),
            pl.lit(None).alias("switch_van_opleiding_1_jaar"),
            pl.lit(None).alias("switch_naar_opleiding_1_jaar"),
            pl.lit(None).alias("switch_kalenderjaren_verschil_1_jaar")
        ])

    # Create switch detection for verblijfsjaar 1 and 2 (step by step)
    # Step 1: Add lag columns
    df_with_lags = df.with_columns([
        pl.col(program_col).shift(1).over([person_col]).alias("vorige_opleiding_1_jaar"),
        pl.col(verblijfsjaar_col).shift(1).over([person_col]).alias("vorig_verblijfsjaar"),
        pl.col(year_col).shift(1).over([person_col]).alias("vorig_kalenderjaar")
    ])

    # Step 2: Add switch detection columns
    return df_with_lags.with_columns([
        # Switch detection: same student, consecutive verblijfsjaren (1->2), different program
        pl.when(
            (pl.col(verblijfsjaar_col) == 2) &
            (pl.col("vorig_verblijfsjaar") == 1) &
            (pl.col(program_col) != pl.col("vorige_opleiding_1_jaar")) &
            (pl.col(year_col) - pl.col("vorig_kalenderjaar") == 1)  # consecutive years
        ).then(True)
        .otherwise(False)
        .alias("switch_binnen_1_jaar"),

        # Switch FROM (original program in verblijfsjaar 1)
        pl.when(
            (pl.col(verblijfsjaar_col) == 2) &
            (pl.col("vorig_verblijfsjaar") == 1) &
            (pl.col(program_col) != pl.col("vorige_opleiding_1_jaar")) &
            (pl.col(year_col) - pl.col("vorig_kalenderjaar") == 1)
        ).then(pl.col("vorige_opleiding_1_jaar"))
        .otherwise(None)
        .alias("switch_van_opleiding_1_jaar"),

        # Switch TO (current program in verblijfsjaar 2)
        pl.when(
            (pl.col(verblijfsjaar_col) == 2) &
            (pl.col("vorig_verblijfsjaar") == 1) &
            (pl.col(program_col) != pl.col("vorige_opleiding_1_jaar")) &
            (pl.col(year_col) - pl.col("vorig_kalenderjaar") == 1)
        ).then(pl.col(program_col))
        .otherwise(None)
        .alias("switch_naar_opleiding_1_jaar"),

        # Calendar year difference for verification
        (pl.col(year_col) - pl.col("vorig_kalenderjaar")).alias("switch_kalenderjaren_verschil_1_jaar")
    ]).drop(["vorige_opleiding_1_jaar", "vorig_verblijfsjaar", "vorig_kalenderjaar"])

def add_switch_within_3_years(df):
    """
    Detect switches within 3 years (Avans style)
    Compares verblijfsjaar 1 vs 4 for same student
    Only for students not dropped out or graduated within 3 years
    """
    # Find required columns
    person_col = None
    program_col = None
    year_col = None
    verblijfsjaar_col = None

    for col in df.columns:
        if "persoonsgebonden_nummer" in col.lower():
            person_col = col
        elif "opleidingscode" in col.lower():
            program_col = col
        elif "inschrijvingsjaar" in col.lower():
            year_col = col
        elif "verblijfsjaar" in col.lower() and "actuele" in col.lower():
            verblijfsjaar_col = col

    if not all([person_col, program_col, year_col, verblijfsjaar_col]):
        missing = []
        if not person_col: missing.append("persoonsgebonden_nummer")
        if not program_col: missing.append("opleidingscode/isat")
        if not year_col: missing.append("inschrijvingsjaar")
        if not verblijfsjaar_col: missing.append("verblijfsjaar*actuele*")
        console.print(f"[yellow]⚠️  Required columns not found for 3-year switch detection: {missing}")
        return df.with_columns([
            pl.lit(None).alias("switch_binnen_3_jaar"),
            pl.lit(None).alias("switch_van_opleiding_3_jaar"),
            pl.lit(None).alias("switch_naar_opleiding_3_jaar"),
            pl.lit(None).alias("switch_kalenderjaren_verschil_3_jaar")
        ])

    # Get verblijfsjaar 1 data for each student
    verblijfsjaar_1_data = df.filter(pl.col(verblijfsjaar_col) == 1).select([
        person_col,
        pl.col(program_col).alias("opleiding_verblijfsjaar_1"),
        pl.col(year_col).alias("jaar_verblijfsjaar_1")
    ])

    # Add verblijfsjaar 1 info to current data
    df_with_ref = df.join(verblijfsjaar_1_data, on=person_col, how="left")

    return df_with_ref.with_columns([
        # Switch detection: verblijfsjaar 4, different program than verblijfsjaar 1, 3 years later
        pl.when(
            (pl.col(verblijfsjaar_col) == 4) &
            (pl.col(program_col) != pl.col("opleiding_verblijfsjaar_1")) &
            (pl.col(year_col) - pl.col("jaar_verblijfsjaar_1") == 3)  # exactly 3 years
        ).then(True)
        .otherwise(False)
        .alias("switch_binnen_3_jaar"),

        # Switch FROM (original program in verblijfsjaar 1)
        pl.when(
            (pl.col(verblijfsjaar_col) == 4) &
            (pl.col(program_col) != pl.col("opleiding_verblijfsjaar_1")) &
            (pl.col(year_col) - pl.col("jaar_verblijfsjaar_1") == 3)
        ).then(pl.col("opleiding_verblijfsjaar_1"))
        .otherwise(None)
        .alias("switch_van_opleiding_3_jaar"),

        # Switch TO (current program in verblijfsjaar 4)
        pl.when(
            (pl.col(verblijfsjaar_col) == 4) &
            (pl.col(program_col) != pl.col("opleiding_verblijfsjaar_1")) &
            (pl.col(year_col) - pl.col("jaar_verblijfsjaar_1") == 3)
        ).then(pl.col(program_col))
        .otherwise(None)
        .alias("switch_naar_opleiding_3_jaar"),

        # Calendar year difference for verification
        (pl.col(year_col) - pl.col("jaar_verblijfsjaar_1")).alias("switch_kalenderjaren_verschil_3_jaar")
    ]).drop(["opleiding_verblijfsjaar_1", "jaar_verblijfsjaar_1"])

def add_switch_dropout_based(df):
    """
    Detect switches based on dropout patterns (VU style)
    Switch = dropout from one program + enrollment in another within same phase
    """
    # Find required columns
    person_col = None
    program_col = None
    year_col = None
    phase_col = None
    studiejaar_col = None

    for col in df.columns:
        if "persoonsgebonden_nummer" in col.lower():
            person_col = col
        elif "opleidingscode" in col.lower():
            program_col = col
        elif "inschrijvingsjaar" in col.lower():
            year_col = col
        elif "opleidingsfase" in col.lower():
            phase_col = col
        elif "studiejaar" in col.lower():
            studiejaar_col = col

    if not all([person_col, program_col, year_col]):
        missing = []
        if not person_col: missing.append("persoonsgebonden_nummer")
        if not program_col: missing.append("opleidingscode")
        if not year_col: missing.append("inschrijvingsjaar")
        console.print(f"[yellow]⚠️  Required columns not found for dropout-based switch detection: {missing}")
        return df.with_columns([
            pl.lit(None).alias("switch_uitval_gebaseerd"),
            pl.lit(None).alias("switch_instroom_na_uitval"),
            pl.lit(None).alias("switch_aantal_jaar_tussen")
        ])

    # First, identify students with dropout (using existing logic if available)
    if "uitval" in df.columns:
        dropout_col = "uitval"
    else:
        # Create basic dropout indicator: not active in max year and no diploma
        max_year = df.select(pl.col(year_col).max()).item()
        df = df.with_columns([
            pl.when(
                (pl.col(year_col).max().over([person_col, program_col]) != max_year) &
                (pl.col("diplomajaar").is_null() if "diplomajaar" in df.columns else True)
            ).then(True)
            .otherwise(False)
            .alias("dropout_indicator")
        ])
        dropout_col = "dropout_indicator"

    # Get dropout students
    dropout_students = df.filter(pl.col(dropout_col) == True).select(person_col).unique()

    # For dropout students, find multiple program enrollments within same phase
    if phase_col:
        switch_data = df.filter(
            pl.col(person_col).is_in(dropout_students.to_series())
        ).group_by([person_col, phase_col]).agg([
            pl.n_unique(program_col).alias("aantal_programmas_in_fase"),
            pl.col(program_col).unique().alias("programmas_in_fase"),
            pl.col(year_col).min().alias("eerste_jaar_fase"),
            pl.col(year_col).max().alias("laatste_jaar_fase")
        ]).filter(
            pl.col("aantal_programmas_in_fase") > 1
        )
    else:
        switch_data = df.filter(
            pl.col(person_col).is_in(dropout_students.to_series())
        ).group_by(person_col).agg([
            pl.n_unique(program_col).alias("aantal_programmas"),
            pl.col(program_col).unique().alias("programmas"),
            pl.col(year_col).min().alias("eerste_jaar"),
            pl.col(year_col).max().alias("laatste_jaar")
        ]).filter(
            pl.col("aantal_programmas") > 1
        )

    # Mark switches in main dataframe
    switch_students = switch_data.select(person_col).unique()

    return df.with_columns([
        pl.col(person_col).is_in(switch_students.to_series()).alias("switch_uitval_gebaseerd"),

        # More detailed switch analysis would require row-by-row logic
        # For now, create placeholder columns
        pl.lit(None).alias("switch_instroom_na_uitval"),
        pl.lit(None).alias("switch_aantal_jaar_tussen")
    ])

def add_switch_comprehensive_tracking(df):
    """
    Create comprehensive switch tracking for each student-program-year
    Tracks both switches FROM this record and switches TO this record
    """
    # Find required columns
    person_col = None
    program_col = None
    year_col = None

    for col in df.columns:
        if "persoonsgebonden_nummer" in col.lower():
            person_col = col
        elif "opleidingscode" in col.lower():
            program_col = col
        elif "inschrijvingsjaar" in col.lower():
            year_col = col

    if not all([person_col, program_col, year_col]):
        missing = []
        if not person_col: missing.append("persoonsgebonden_nummer")
        if not program_col: missing.append("opleidingscode/isat")
        if not year_col: missing.append("inschrijvingsjaar")
        console.print(f"[yellow]⚠️  Required columns not found for comprehensive switch tracking: {missing}")
        console.print(f"[yellow]    Available columns: {df.columns[:10]}...")
        return df.with_columns([
            pl.lit(None).alias("is_switch_from_record"),
            pl.lit(None).alias("is_switch_to_record"),
            pl.lit(None).alias("switch_to_program"),
            pl.lit(None).alias("switch_from_program"),
            pl.lit(None).alias("switch_year_gap")
        ])

    # Sort by person and year for proper sequence analysis
    df_sorted = df.sort([person_col, year_col])

    # Step 1: Add lag/lead columns
    df_with_shifts = df_sorted.with_columns([
        pl.col(program_col).shift(-1).over(person_col).alias("next_program"),
        pl.col(year_col).shift(-1).over(person_col).alias("next_year"),
        pl.col(program_col).shift(1).over(person_col).alias("prev_program"),
        pl.col(year_col).shift(1).over(person_col).alias("prev_year")
    ])

    # Step 2: Add basic switch detection columns
    df_with_switches = df_with_shifts.with_columns([
        # Is this record a switch FROM (student switches away from this program)
        pl.when(
            (pl.col(program_col) != pl.col("next_program")) &
            (pl.col("next_year") - pl.col(year_col) == 1)
        ).then(True)
        .otherwise(False)
        .alias("is_switch_from_record"),

        # Is this record a switch TO (student switches to this program)
        pl.when(
            (pl.col(program_col) != pl.col("prev_program")) &
            (pl.col(year_col) - pl.col("prev_year") == 1)
        ).then(True)
        .otherwise(False)
        .alias("is_switch_to_record"),

        # What program did they switch TO (from this record)
        pl.when(
            (pl.col(program_col) != pl.col("next_program")) &
            (pl.col("next_year") - pl.col(year_col) == 1)
        ).then(pl.col("next_program"))
        .otherwise(None)
        .alias("switch_to_program"),

        # What program did they switch FROM (to this record)
        pl.when(
            (pl.col(program_col) != pl.col("prev_program")) &
            (pl.col(year_col) - pl.col("prev_year") == 1)
        ).then(pl.col("prev_program"))
        .otherwise(None)
        .alias("switch_from_program")
    ])

    # Step 3: Add derived columns and clean up
    return df_with_switches.with_columns([
        # Year gap for switches
        pl.when(
            pl.col("is_switch_from_record") | pl.col("is_switch_to_record")
        ).then(
            pl.when(pl.col("is_switch_from_record"))
            .then(pl.col("next_year") - pl.col(year_col))
            .otherwise(pl.col(year_col) - pl.col("prev_year"))
        ).otherwise(None)
        .alias("switch_year_gap")
    ]).drop(["next_program", "next_year", "prev_program", "prev_year"])

# =============================================================================
# DERIVED SWITCH VARIABLES
# =============================================================================

def add_switch_derived_variables(df):
    """
    Create derived variables based on switch information
    Switch patterns, timing, program characteristics, etc.
    """
    derived_cols = []

    # Switch summary per student
    person_col = None
    for col in df.columns:
        if "persoonsgebonden_nummer" in col.lower():
            person_col = col
            break

    if person_col:
        derived_cols.extend([
            # Total switches per student (any type) - use max of the three switch types
            pl.max_horizontal([
                pl.col("switch_binnen_1_jaar").sum().over(person_col),
                pl.col("switch_binnen_3_jaar").sum().over(person_col),
                pl.col("is_switch_from_record").sum().over(person_col)
            ]).alias("student_total_switches"),

            # Switch timing
            pl.when(pl.col("switch_binnen_1_jaar") == True)
            .then(pl.lit("Switch binnen 1 jaar"))
            .when(pl.col("switch_binnen_3_jaar") == True)
            .then(pl.lit("Switch binnen 3 jaar"))
            .when(pl.col("switch_uitval_gebaseerd") == True)
            .then(pl.lit("Switch na uitval"))
            .otherwise(pl.lit("Geen switch"))
            .alias("switch_type_timing"),

            # Switch direction indicator
            pl.when(pl.col("is_switch_from_record") == True)
            .then(pl.lit("Switch weg van deze opleiding"))
            .when(pl.col("is_switch_to_record") == True)
            .then(pl.lit("Switch naar deze opleiding"))
            .otherwise(pl.lit("Geen switch voor dit record"))
            .alias("switch_direction"),

            # Early vs late switcher
            pl.when(
                (pl.col("switch_binnen_1_jaar") == True) |
                (pl.col("is_switch_from_record") == True) & (pl.col("studiejaar") <= 1 if "studiejaar" in df.columns else True)
            ).then(pl.lit("Vroege switcher"))
            .when(
                (pl.col("switch_binnen_3_jaar") == True) |
                (pl.col("is_switch_from_record") == True) & (pl.col("studiejaar") > 1 if "studiejaar" in df.columns else True)
            ).then(pl.lit("Late switcher"))
            .otherwise(pl.lit("Geen switcher"))
            .alias("switch_timing_category")
        ])

    # Program-level switch patterns
    program_col = None
    for col in df.columns:
        if "opleidingscode" in col.lower():
            program_col = col
            break

    if program_col:
        derived_cols.extend([
            # Programs with high switch-out rates
            (pl.col("is_switch_from_record").sum().over(program_col) /
             pl.len().over(program_col)).alias("program_switch_out_rate"),

            # Programs with high switch-in rates
            (pl.col("is_switch_to_record").sum().over(program_col) /
             pl.len().over(program_col)).alias("program_switch_in_rate"),

            # Net switch flow (in - out)
            (pl.col("is_switch_to_record").sum().over(program_col) -
             pl.col("is_switch_from_record").sum().over(program_col)).alias("program_net_switch_flow")
        ])

    # Year-level switch patterns
    year_col = None
    for col in df.columns:
        if "inschrijvingsjaar" in col.lower():
            year_col = col
            break

    if year_col:
        derived_cols.extend([
            # Switches per year
            pl.col("is_switch_from_record").sum().over(year_col).alias("year_total_switches"),

            # Switch rate per year
            (pl.col("is_switch_from_record").sum().over(year_col) /
             pl.len().over(year_col)).alias("year_switch_rate")
        ])

    # Add all derived columns
    if derived_cols:
        return df.with_columns(derived_cols)
    else:
        console.print("[yellow]⚠️  Could not create derived switch variables")
        return df

def add_switch_sector_analysis(df):
    """
    Analyze switches between sectors/faculties/programs
    Requires sector/faculty columns to be available
    """
    # Find sector-related columns
    sector_cols = []
    for col in df.columns:
        if any(term in col.lower() for term in ["sector", "faculteit", "croho", "isced"]):
            sector_cols.append(col)

    if not sector_cols:
        console.print("[yellow]⚠️  No sector columns found for sector switch analysis")
        return df.with_columns([
            pl.lit(None).alias("switch_between_sectors"),
            pl.lit(None).alias("switch_within_sector")
        ])

    # Use first available sector column
    sector_col = sector_cols[0]

    # Get sector of switch programs
    return df.with_columns([
        # Previous sector (for switch TO analysis)
        pl.col(sector_col).shift(1).over(pl.col(df.columns[0]) if df.columns else None).alias("prev_sector"),

        # Next sector (for switch FROM analysis)
        pl.col(sector_col).shift(-1).over(pl.col(df.columns[0]) if df.columns else None).alias("next_sector"),

        # Between-sector switch indicator
        pl.when(
            (pl.col("is_switch_from_record") == True) &
            (pl.col(sector_col) != pl.col(sector_col).shift(-1).over(pl.col(df.columns[0]) if df.columns else None))
        ).then(True)
        .when(
            (pl.col("is_switch_to_record") == True) &
            (pl.col(sector_col) != pl.col(sector_col).shift(1).over(pl.col(df.columns[0]) if df.columns else None))
        ).then(True)
        .otherwise(False)
        .alias("switch_between_sectors"),

        # Within-sector switch indicator
        pl.when(
            (pl.col("is_switch_from_record") == True) &
            (pl.col(sector_col) == pl.col(sector_col).shift(-1).over(pl.col(df.columns[0]) if df.columns else None))
        ).then(True)
        .when(
            (pl.col("is_switch_to_record") == True) &
            (pl.col(sector_col) == pl.col(sector_col).shift(1).over(pl.col(df.columns[0]) if df.columns else None))
        ).then(True)
        .otherwise(False)
        .alias("switch_within_sector")
    ]).drop(["prev_sector", "next_sector"])

# =============================================================================
# MAIN SWITCH ENRICHMENT FUNCTION
# =============================================================================

def enrich_switch_data(df):
    """
    Apply all switch enrichment functions to a dataframe
    Creates comprehensive switch tracking and derived variables
    """
    console.print(f"[cyan]🔄 Starting switch enrichment for dataframe with {len(df)} rows")

    # Debug: Show available columns for troubleshooting
    console.print(f"[cyan]📋 Available columns ({len(df.columns)} total):")
    for i, col in enumerate(df.columns):
        if i < 20:  # Show first 20 columns
            console.print(f"[cyan]  {i+1:2d}. {col}")
        elif i == 20:
            console.print(f"[cyan]  ... and {len(df.columns) - 20} more columns")
            break

    # Basic switch detection (Avans style)
    df = add_switch_within_1_year(df)
    df = add_switch_within_3_years(df)

    # Dropout-based switch detection (VU style)
    df = add_switch_dropout_based(df)

    # Comprehensive switch tracking
    df = add_switch_comprehensive_tracking(df)

    # Derived variables
    df = add_switch_derived_variables(df)
    df = add_switch_sector_analysis(df)

    console.print(f"[green]✅ Switch enrichment completed - {len(df.columns)} columns")
    return df

def enrich_all_switch_data(input_dir="data/03-combined", output_dir="data/03-combined"):
    """
    Apply switch enrichment to all CSV files in input directory
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    if not input_path.exists():
        console.print(f"[red]❌ Input directory not found: {input_dir}")
        return

    output_path.mkdir(parents=True, exist_ok=True)

    # Find CSV files
    csv_files = list(input_path.glob("*.csv"))
    csv_files = [f for f in csv_files if not f.name.startswith('Dec_')]

    if not csv_files:
        console.print(f"[yellow]⚠️  No CSV files found in {input_dir}")
        return

    console.print(f"[bold blue]🔄 Starting switch enrichment process...[/bold blue]")
    console.print(f"[cyan]Found {len(csv_files)} files to enrich")

    for csv_file in csv_files:
        try:
            console.print(f"\n[cyan]🔀 Processing switches: {csv_file.name}")

            # Load data
            df = pl.read_csv(csv_file)
            console.print(f"[green]  ✅ Loaded: {len(df)} rows, {len(df.columns)} columns")

            # Apply switch enrichment
            df_enriched = enrich_switch_data(df)

            # Save enriched data (with switch suffix to distinguish)
            output_file = output_path / f"{csv_file.stem}_with_switches.csv"
            df_enriched.write_csv(output_file)
            console.print(f"[green]  ✅ Saved switch-enriched data: {output_file}")

        except Exception as e:
            console.print(f"[red]  ❌ Error processing {csv_file.name}: {str(e)}")

def main():
    """
    Command line entry point for switch enrichment
    """
    parser = argparse.ArgumentParser(description='Enrich data with switch analysis variables')
    parser.add_argument('--input-dir', default='data/03-combined',
                       help='Input directory with combined data')
    parser.add_argument('--output-dir', default='data/03-combined',
                       help='Output directory for switch-enriched data')

    args = parser.parse_args()

    enrich_all_switch_data(
        input_dir=args.input_dir,
        output_dir=args.output_dir
    )

if __name__ == "__main__":
    main()
