"""
Data Enrichment Module
Adds calculated fields and standardized variables to combined 1CHO data
Based on VU and Avans analytics approaches
"""

import polars as pl
from pathlib import Path
import argparse
from rich.console import Console
from datetime import datetime
import re

console = Console()

# =============================================================================
# BASIC VARIABLES
# =============================================================================

def add_uitwonend(df):
    """
    Determine if student is living away from home by comparing postcodes
    VU style: if postcodes are equal -> living at home, if different -> away from home
    """
    postcode_student_col = None
    postcode_vooropl_col = None

    # Find postcode columns (flexible column naming)
    for col in df.columns:
        if "postcodecijfers_student_op_1_oktober" in col:
            postcode_student_col = col
        elif "postcodecijfers_van_de_hoogste_vooropl_voor_het_ho" in col:
            postcode_vooropl_col = col

    if postcode_student_col and postcode_vooropl_col:
        return df.with_columns([
            pl.when(
                pl.col(postcode_student_col) == pl.col(postcode_vooropl_col)
            ).then(False)
            .otherwise(True)
            .alias("student_uitwonend")
        ])
    else:
        console.print("[yellow]⚠️  Postcode columns not found, skipping uitwonend calculation")
        return df.with_columns([pl.lit(None).alias("student_uitwonend")])

def add_indicatie_voltijd(df):
    """
    Determine if enrollment is full-time based on opleidingsvorm
    VU style: opleidingsvorm_code == 1 -> full-time
    """
    # Find opleidingsvorm column
    opleidingsvorm_col = None
    for col in df.columns:
        if "opleidingsvorm" in col.lower():
            opleidingsvorm_col = col
            break

    if opleidingsvorm_col:
        return df.with_columns([
            pl.when(pl.col(opleidingsvorm_col) == 1)
            .then(True)
            .otherwise(False)
            .alias("indicatie_voltijd")
        ])
    else:
        console.print("[yellow]⚠️  Opleidingsvorm column not found, skipping voltijd calculation")
        return df.with_columns([pl.lit(None).alias("indicatie_voltijd")])

def add_dubbele_studie_instelling(df):
    """
    Detect double study within institution by counting unique programs per year
    """
    # Find person and program identifier columns
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

    if person_col and program_col and year_col:
        return df.with_columns([
            pl.n_unique(program_col).over([person_col, year_col]).alias("aantal_inschrijvingen_jaar_instelling"),
            (pl.n_unique(program_col).over([person_col, year_col]) > 1).alias("dubbele_studie_instelling")
        ])
    else:
        console.print("[yellow]⚠️  Required columns not found for dubbele studie calculation")
        return df.with_columns([
            pl.lit(None).alias("aantal_inschrijvingen_jaar_instelling"),
            pl.lit(None).alias("dubbele_studie_instelling")
        ])

# =============================================================================
# PROFILE STANDARDIZATION
# =============================================================================

def standardize_profiles(df):
    """
    Standardize VWO/HAVO profiles to NT/NG/EM/CM format
    Create individual profile variables based on profile codes
    """
    # Check if profile column exists
    profile_col = None
    for col in df.columns:
        if "profiel" in col.lower() and "voor_het_ho" in col.lower():
            profile_col = col
            break

    if profile_col is None:
        console.print("[yellow]⚠️  No profile column found, skipping profile standardization")
        return df.with_columns([
            pl.lit(None).alias("hoogste_vooropleiding_voor_ho_profiel_nt"),
            pl.lit(None).alias("hoogste_vooropleiding_voor_ho_profiel_ng"),
            pl.lit(None).alias("hoogste_vooropleiding_voor_ho_profiel_em"),
            pl.lit(None).alias("hoogste_vooropleiding_voor_ho_profiel_cm"),
            pl.lit(None).alias("hoogste_vooropleiding_voor_ho_profiel_is_combinatie")
        ])

    # Profile codes based on hoogste_vooropleiding_voor_het_ho_profiel.csv
    nt_codes = ["208", "209", "210", "211", "408", "409", "410", "411"]
    ng_codes = ["205", "206", "207", "211", "405", "406", "407", "411"]
    em_codes = ["203", "204", "207", "210", "403", "404", "407", "410"]
    cm_codes = ["202", "204", "206", "209", "402", "404", "406", "409"]
    combination_codes = ["204", "206", "207", "209", "210", "211", "404", "406", "407", "409", "410", "411"]

    return df.with_columns([
        # Individual profile flags based on codes
        pl.col(profile_col).is_in(nt_codes).alias("hoogste_vooropleiding_voor_ho_profiel_nt"),
        pl.col(profile_col).is_in(ng_codes).alias("hoogste_vooropleiding_voor_ho_profiel_ng"),
        pl.col(profile_col).is_in(em_codes).alias("hoogste_vooropleiding_voor_ho_profiel_em"),
        pl.col(profile_col).is_in(cm_codes).alias("hoogste_vooropleiding_voor_ho_profiel_cm"),

        # Combination profile detection (any code with & in the label)
        pl.col(profile_col).is_in(combination_codes).alias("hoogste_vooropleiding_voor_ho_profiel_is_combinatie")
    ])

# =============================================================================
# STUDY PROGRESS
# =============================================================================

def add_studiejaar(df):
    """
    Calculate study year as dense rank within each student+program combination
    VU style: dense_rank per student per opleiding
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

    if person_col and program_col and year_col:
        return df.with_columns([
            pl.col(year_col)
            .rank("dense", descending=False)
            .over([person_col, program_col])
            .alias("studiejaar")
        ])
    else:
        console.print("[yellow]⚠️  Required columns not found for studiejaar calculation")
        return df.with_columns([pl.lit(None).alias("studiejaar")])

def add_gap_years(df):
    """
    Detect gap years before B/M/P programs
    VU style gap year detection based on vooropleiding timing
    """
    # Find required columns
    vooropl_jaar_col = None
    inschrijf_jaar_col = None

    for col in df.columns:
        if "jaar_hoogste_vooropleiding" in col.lower():
            vooropl_jaar_col = col
        elif "inschrijvingsjaar" in col.lower():
            inschrijf_jaar_col = col

    if vooropl_jaar_col and inschrijf_jaar_col:
        return df.with_columns([
            # Direct enrollment check
            (
                (pl.col(vooropl_jaar_col) + 1) ==
                pl.col(inschrijf_jaar_col)
            ).alias("indicatie_instroom_direct"),

            # Gap year indicator (inverse of indicatie_instroom_direct)
            (
                (pl.col(vooropl_jaar_col) + 1) <
                pl.col(inschrijf_jaar_col)
            ).alias("indicatie_tussenjaar")
        ])
    else:
        console.print("[yellow]⚠️  Required columns not found for gap year calculation")
        return df.with_columns([
            pl.lit(None).alias("indicatie_instroom_direct"),
            pl.lit(None).alias("indicatie_tussenjaar")
        ])

def add_aansluiting(df):
    """
    Determine type of enrollment connection
    VU style aansluiting categories
    """
    # Check if required columns exist and are not all null
    if ("indicatie_instroom_direct" not in df.columns or
        "indicatie_tussenjaar" not in df.columns or
        df.select(pl.col("indicatie_instroom_direct").is_null().all()).item() or
        df.select(pl.col("indicatie_tussenjaar").is_null().all()).item()):
        console.print("[yellow]⚠️  Gap year indicators not available, skipping aansluiting calculation")
        return df.with_columns([pl.lit("Onbekend").alias("aansluiting")])

    # Find instelling column
    instelling_col = None
    for col in df.columns:
        if "instelling_van_de_hoogste_vooropleiding" in col.lower():
            instelling_col = col
            break

    if instelling_col:
        return df.with_columns([
            pl.when(pl.col("indicatie_instroom_direct") &
                    pl.col(instelling_col).str.contains("eigen|zelfde"))
            .then(pl.lit("Direct na diploma instelling"))
            .when(pl.col("indicatie_instroom_direct"))
            .then(pl.lit("Direct na extern diploma"))
            .when(pl.col("indicatie_tussenjaar"))
            .then(pl.lit("Tussenjaar"))
            .otherwise(pl.lit("Onbekend"))
            .alias("aansluiting")
        ])
    else:
        return df.with_columns([
            pl.when(pl.col("indicatie_instroom_direct"))
            .then(pl.lit("Direct na diploma"))
            .when(pl.col("indicatie_tussenjaar"))
            .then(pl.lit("Tussenjaar"))
            .otherwise(pl.lit("Onbekend"))
            .alias("aansluiting")
        ])

# =============================================================================
# STUDY SUCCESS
# =============================================================================

def add_aantal_inschrijvingen_in_opleiding(df):
    """
    Count total enrollments per student per program
    """
    # Find required columns
    person_col = None
    program_col = None

    for col in df.columns:
        if "persoonsgebonden_nummer" in col.lower():
            person_col = col
        elif "opleidingscode" in col.lower():
            program_col = col

    if person_col and program_col:
        return df.with_columns([
            pl.len().over([person_col, program_col]).alias("aantal_inschrijvingen_in_opleiding")
        ])
    else:
        console.print("[yellow]⚠️  Required columns not found for aantal inschrijvingen calculation")
        return df.with_columns([pl.lit(None).alias("aantal_inschrijvingen_in_opleiding")])

def add_uitval(df):
    """
    Determine dropout status
    Student drops out if: no diploma AND not active in max year
    """
    # Find required columns
    person_col = None
    program_col = None
    year_col = None
    diploma_col = None

    for col in df.columns:
        if "persoonsgebonden_nummer" in col.lower():
            person_col = col
        elif "opleidingscode" in col.lower():
            program_col = col
        elif "inschrijvingsjaar" in col.lower():
            year_col = col
        elif "diplomajaar" in col.lower():
            diploma_col = col

    if not all([person_col, program_col, year_col]):
        console.print("[yellow]⚠️  Required columns not found for uitval calculation")
        return df.with_columns([
            pl.lit(None).alias("inschrijvingsjaar_max"),
            pl.lit(None).alias("actief_in_max_jaar"),
            pl.lit(None).alias("uitval")
        ])

    # Get max enrollment year globally
    max_year = df.select(pl.col(year_col).max()).item()

    if diploma_col:
        return df.with_columns([
            pl.col(year_col).max().over([person_col, program_col]).alias("inschrijvingsjaar_max"),
            (pl.col(year_col).max().over([person_col, program_col]) == max_year).alias("actief_in_max_jaar"),

            # Uitval = no diploma AND not active in max year
            (
                pl.col(diploma_col).is_null() &
                (pl.col(year_col).max().over([person_col, program_col]) != max_year)
            ).alias("uitval")
        ])
    else:
        return df.with_columns([
            pl.col(year_col).max().over([person_col, program_col]).alias("inschrijvingsjaar_max"),
            (pl.col(year_col).max().over([person_col, program_col]) == max_year).alias("actief_in_max_jaar"),

            # Without diploma info, can't determine uitval
            pl.lit(None).alias("uitval")
        ])

def add_diploma_bij_instelling_info(df):
    """
    Add diploma timing variables based on Avans logic
    Creates diploma_status, eerste_diplomajaar, and verblijfsjaar_diploma
    Filters for valid diplomas (excludes propedeuse)
    """
    # Find required columns
    diploma_col = None
    person_col = None
    soort_diploma_col = None
    verblijfsjaar_col = None

    for col in df.columns:
        if "diplomajaar" in col.lower():
            diploma_col = col
        elif "persoonsgebonden_nummer" in col.lower():
            person_col = col
        elif "soort_diploma_instelling" in col.lower() or "soortdiplomainstelling" in col.lower():
            soort_diploma_col = col
        elif "verblijfsjaar" in col.lower() and "actuele" in col.lower():
            verblijfsjaar_col = col

    # Define valid diploma codes (excluding propedeuse: 01, 02)
    valid_diploma_codes = [
        "03", "04",  # bachelor
        "05", "06",  # master
        "07", "08",  # doctoraal
        "09", "10",  # beroepsfase/voortgezet
        "13", "14",  # associate degree
        "15", "16"   # postinitiele master
    ]

    if diploma_col and person_col:
        columns_to_add = [
            # Get first diploma year per student (for valid diplomas only)
            pl.when(
                pl.col(soort_diploma_col).is_in(valid_diploma_codes) if soort_diploma_col else True
            ).then(pl.col(diploma_col))
            .otherwise(None)
            .min().over([person_col]).alias("eerste_diplomajaar"),

            # Diploma status based on valid diplomas
            pl.when(
                pl.col(soort_diploma_col).is_in(valid_diploma_codes) if soort_diploma_col else
                pl.col(diploma_col).is_not_null()
            ).then(pl.lit("Diploma behaald"))
            .otherwise(pl.lit("Geen diploma"))
            .alias("diploma_status")
        ]

        # Add verblijfsjaar_diploma if available
        if verblijfsjaar_col:
            columns_to_add.append(
                pl.when(
                    pl.col(soort_diploma_col).is_in(valid_diploma_codes) if soort_diploma_col else
                    pl.col(diploma_col).is_not_null()
                ).then(pl.col(verblijfsjaar_col))
                .otherwise(None)
                .alias("verblijfsjaar_diploma")
            )
        else:
            columns_to_add.append(pl.lit(None).alias("verblijfsjaar_diploma"))

        return df.with_columns(columns_to_add)
    else:
        console.print("[yellow]⚠️  Required columns not found for diploma timing calculation")
        return df.with_columns([
            pl.lit(None).alias("eerste_diplomajaar"),
            pl.lit("Onbekend").alias("diploma_status"),
            pl.lit(None).alias("verblijfsjaar_diploma")
        ])

# =============================================================================
# AVANS STYLE ENRICHMENT
# =============================================================================

def add_studieduur(df):
    """
    Calculate study duration
    Avans style: diplomajaar - eerstejaar + 1
    """
    # Find required columns
    diploma_col = None
    person_col = None
    program_col = None
    year_col = None

    for col in df.columns:
        if "diplomajaar" in col.lower():
            diploma_col = col
        elif "persoonsgebonden_nummer" in col.lower():
            person_col = col
        elif "opleidingscode" in col.lower():
            program_col = col
        elif "inschrijvingsjaar" in col.lower():
            year_col = col

    if all([diploma_col, person_col, program_col, year_col]):
        return df.with_columns([
            # First enrollment year per student per program
            pl.col(year_col).min().over([person_col, program_col]).alias("eerste_inschrijvingsjaar"),

            # Study duration calculation
            pl.when(pl.col(diploma_col).is_not_null())
            .then(pl.col(diploma_col) - pl.col(year_col).min().over([person_col, program_col]) + 1)
            .otherwise(None)
            .alias("studieduur")
        ])
    else:
        console.print("[yellow]⚠️  Required columns not found for studieduur calculation")
        return df.with_columns([
            pl.lit(None).alias("eerste_inschrijvingsjaar"),
            pl.lit(None).alias("studieduur")
        ])


def add_rendement_instelling(df):
    """
    Add rendement variables based on Avans logic
    Creates rendement_instelling_3_jaar, _5_jaar, _8_jaar
    Based on eerste_diplomajaar and first enrollment year
    """
    # Find required columns
    diploma_jaar_col = "eerste_diplomajaar"  # Created by add_diploma_bij_instelling_info
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

    if not all([person_col, program_col, year_col]) or diploma_jaar_col not in df.columns:
        console.print("[yellow]⚠️  Required columns not found for rendement calculation")
        return df.with_columns([
            pl.lit(None).alias("rendement_instelling_3_jaar"),
            pl.lit(None).alias("rendement_instelling_5_jaar"),
            pl.lit(None).alias("rendement_instelling_8_jaar")
        ])

    return df.with_columns([
        # Get first enrollment year per student per program
        pl.col(year_col).min().over([person_col, program_col]).alias("eerstejaar_instelling"),

        # Rendement 3 jaar
        pl.when(
            (pl.col(diploma_jaar_col) >= pl.col(year_col).min().over([person_col, program_col])) &
            (pl.col(diploma_jaar_col) <= pl.col(year_col).min().over([person_col, program_col]) + 2)
        ).then(pl.lit("Diploma binnen 3 jaar"))
        .when(
            (pl.col(diploma_jaar_col) >= pl.col(year_col).min().over([person_col, program_col])) &
            (pl.col(diploma_jaar_col) > pl.col(year_col).min().over([person_col, program_col]) + 2)
        ).then(pl.lit("Diploma na 3 jaar"))
        .when(pl.col(diploma_jaar_col).is_null()).then(pl.lit("Geen diploma"))
        .when(pl.col(diploma_jaar_col) < pl.col(year_col).min().over([person_col, program_col]))
        .then(pl.lit("Onbekend, want diplomajaar ligt voor eerste jaar bij instelling"))
        .otherwise(pl.lit("Onbekend"))
        .alias("rendement_instelling_3_jaar"),

        # Rendement 5 jaar
        pl.when(
            (pl.col(diploma_jaar_col) >= pl.col(year_col).min().over([person_col, program_col])) &
            (pl.col(diploma_jaar_col) <= pl.col(year_col).min().over([person_col, program_col]) + 4)
        ).then(pl.lit("Diploma binnen 5 jaar"))
        .when(
            (pl.col(diploma_jaar_col) >= pl.col(year_col).min().over([person_col, program_col])) &
            (pl.col(diploma_jaar_col) > pl.col(year_col).min().over([person_col, program_col]) + 4)
        ).then(pl.lit("Diploma na 5 jaar"))
        .when(pl.col(diploma_jaar_col).is_null()).then(pl.lit("Geen diploma"))
        .when(pl.col(diploma_jaar_col) < pl.col(year_col).min().over([person_col, program_col]))
        .then(pl.lit("Onbekend, want diplomajaar ligt voor eerste jaar bij instelling"))
        .otherwise(pl.lit("Onbekend"))
        .alias("rendement_instelling_5_jaar"),

        # Rendement 8 jaar
        pl.when(
            (pl.col(diploma_jaar_col) >= pl.col(year_col).min().over([person_col, program_col])) &
            (pl.col(diploma_jaar_col) <= pl.col(year_col).min().over([person_col, program_col]) + 7)
        ).then(pl.lit("Diploma binnen 8 jaar"))
        .when(
            (pl.col(diploma_jaar_col) >= pl.col(year_col).min().over([person_col, program_col])) &
            (pl.col(diploma_jaar_col) > pl.col(year_col).min().over([person_col, program_col]) + 7)
        ).then(pl.lit("Diploma na 8 jaar"))
        .when(pl.col(diploma_jaar_col).is_null()).then(pl.lit("Geen diploma"))
        .when(pl.col(diploma_jaar_col) < pl.col(year_col).min().over([person_col, program_col]))
        .then(pl.lit("Onbekend, want diplomajaar ligt voor eerste jaar bij instelling"))
        .otherwise(pl.lit("Onbekend"))
        .alias("rendement_instelling_8_jaar")
    ])

def add_uitval_instelling(df):
    """
    Add uitval variables based on Avans logic
    Creates uitval_instelling_in_jaar, _1_jaar, _3_jaar, _5_jaar, _8_jaar
    Based on status (dropout/diploma/enrolled) and last enrollment year
    """
    # Find required columns
    person_col = None
    program_col = None
    year_col = None
    diploma_status_col = "diploma_status"  # Created by add_diploma_bij_instelling_info

    for col in df.columns:
        if "persoonsgebonden_nummer" in col.lower():
            person_col = col
        elif "opleidingscode" in col.lower():
            program_col = col
        elif "inschrijvingsjaar" in col.lower():
            year_col = col

    if not all([person_col, program_col, year_col]) or diploma_status_col not in df.columns:
        console.print("[yellow]⚠️  Required columns not found for uitval calculation")
        return df.with_columns([
            pl.lit(None).alias("uitval_instelling_in_jaar"),
            pl.lit(None).alias("uitval_instelling_1_jaar"),
            pl.lit(None).alias("uitval_instelling_3_jaar"),
            pl.lit(None).alias("uitval_instelling_5_jaar"),
            pl.lit(None).alias("uitval_instelling_8_jaar")
        ])

    # Get max year globally to determine current/recent year
    max_year = df.select(pl.col(year_col).max()).item() if year_col in df.columns else None

    return df.with_columns([
        # Get first enrollment year per student per program
        pl.col(year_col).min().over([person_col, program_col]).alias("eerstejaar_uitval"),

        # Get last enrollment year per student per program
        pl.col(year_col).max().over([person_col, program_col]).alias("laatste_jaar_uitval"),

        # Determine status: diploma, enrolled (active in max year), or dropped out
        pl.when(pl.col(diploma_status_col) == "Diploma behaald")
        .then(pl.lit("Diploma behaald"))
        .when(pl.col(year_col).max().over([person_col, program_col]) == max_year)
        .then(pl.lit("Zittend"))
        .otherwise(pl.lit("Uitgevallen"))
        .alias("status_uitval"),

        # Calculate years until dropout (only for dropouts)
        pl.when(
            (pl.col(diploma_status_col) != "Diploma behaald") &
            (pl.col(year_col).max().over([person_col, program_col]) != max_year)
        ).then(
            pl.col(year_col).max().over([person_col, program_col]) + 1 -
            pl.col(year_col).min().over([person_col, program_col])
        ).otherwise(None)
        .alias("uitval_instelling_in_jaar"),

        # Uitval within 1 year
        pl.when(
            (pl.col(diploma_status_col) != "Diploma behaald") &
            (pl.col(year_col).max().over([person_col, program_col]) != max_year) &
            (pl.col(year_col).max().over([person_col, program_col]) + 1 -
             pl.col(year_col).min().over([person_col, program_col]) == 1)
        ).then(pl.lit("Uitgevallen binnen 1 jaar"))
        .when(
            (pl.col(diploma_status_col) != "Diploma behaald") &
            (pl.col(year_col).max().over([person_col, program_col]) != max_year) &
            (pl.col(year_col).max().over([person_col, program_col]) + 1 -
             pl.col(year_col).min().over([person_col, program_col]) > 1)
        ).then(pl.lit("Na 1 jaar nog ingeschreven of diploma behaald"))
        .otherwise(pl.lit("Na 1 jaar nog ingeschreven of diploma behaald"))
        .alias("uitval_instelling_1_jaar"),

        # Uitval within 3 years
        pl.when(
            (pl.col(diploma_status_col) != "Diploma behaald") &
            (pl.col(year_col).max().over([person_col, program_col]) != max_year) &
            (pl.col(year_col).max().over([person_col, program_col]) + 1 -
             pl.col(year_col).min().over([person_col, program_col]) <= 3)
        ).then(pl.lit("Uitgevallen binnen 3 jaar"))
        .otherwise(pl.lit("Na 3 jaar nog ingeschreven of diploma behaald"))
        .alias("uitval_instelling_3_jaar"),

        # Uitval within 5 years
        pl.when(
            (pl.col(diploma_status_col) != "Diploma behaald") &
            (pl.col(year_col).max().over([person_col, program_col]) != max_year) &
            (pl.col(year_col).max().over([person_col, program_col]) + 1 -
             pl.col(year_col).min().over([person_col, program_col]) <= 5)
        ).then(pl.lit("Uitgevallen binnen 5 jaar"))
        .otherwise(pl.lit("Na 5 jaar nog ingeschreven of diploma behaald"))
        .alias("uitval_instelling_5_jaar"),

        # Uitval within 8 years
        pl.when(
            (pl.col(diploma_status_col) != "Diploma behaald") &
            (pl.col(year_col).max().over([person_col, program_col]) != max_year) &
            (pl.col(year_col).max().over([person_col, program_col]) + 1 -
             pl.col(year_col).min().over([person_col, program_col]) <= 8)
        ).then(pl.lit("Uitgevallen binnen 8 jaar"))
        .otherwise(pl.lit("Na 8 jaar nog ingeschreven of diploma behaald"))
        .alias("uitval_instelling_8_jaar")
    ])

# =============================================================================
# MAIN ENRICHMENT FUNCTION
# =============================================================================

def enrich_dataframe(df):
    """
    Apply all enrichment functions to a dataframe
    """
    console.print(f"[cyan]🔧 Starting enrichment for dataframe with {len(df)} rows")

    # Basic variables
    df = add_uitwonend(df)
    df = add_indicatie_voltijd(df)
    df = add_dubbele_studie_instelling(df)

    # Profile standardization
    df = standardize_profiles(df)

    # Study progress
    df = add_studiejaar(df)
    df = add_gap_years(df)
    df = add_aansluiting(df)

    # Study success
    df = add_aantal_inschrijvingen_in_opleiding(df)
    df = add_uitval(df)
    df = add_diploma_bij_instelling_info(df)

    # Avans style
    df = add_studieduur(df)

    # New Avans-style variables
    df = add_rendement_instelling(df)
    df = add_uitval_instelling(df)

    console.print(f"[green]✅ Enrichment completed - {len(df.columns)} columns")
    return df

def enrich_all_data(input_dir="data/03-combined", output_dir="data/03-combined"):
    """
    Apply enrichment to all CSV files in input directory
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

    console.print(f"[bold blue]🔄 Starting data enrichment process...[/bold blue]")
    console.print(f"[cyan]Found {len(csv_files)} files to enrich")

    for csv_file in csv_files:
        try:
            console.print(f"\n[cyan]📊 Processing: {csv_file.name}")

            # Load data
            df = pl.read_csv(csv_file)
            console.print(f"[green]  ✅ Loaded: {len(df)} rows, {len(df.columns)} columns")

            # Apply enrichment
            df_enriched = enrich_dataframe(df)

            # Save enriched data
            output_file = output_path / csv_file.name
            df_enriched.write_csv(output_file)
            console.print(f"[green]  ✅ Saved enriched data: {output_file}")

        except Exception as e:
            console.print(f"[red]  ❌ Error processing {csv_file.name}: {str(e)}")

def main():
    """
    Command line entry point
    """
    parser = argparse.ArgumentParser(description='Enrich combined data with calculated fields')
    parser.add_argument('--input-dir', default='data/03-combined',
                       help='Input directory with combined data')
    parser.add_argument('--output-dir', default='data/03-combined',
                       help='Output directory for enriched data')

    args = parser.parse_args()

    enrich_all_data(
        input_dir=args.input_dir,
        output_dir=args.output_dir
    )

if __name__ == "__main__":
    main()

