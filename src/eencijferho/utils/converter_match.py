# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Script that matches the input files with validation records from previously processed metadata files.

Functions:
    [x] load_input_files(input_folder)
        - Get all files from the input folder, excluding certain extensions
    [x] load_validation_log(log_path)
        - Load processed bestandbeschrijvingen validation log into a Polars dataframe
    [M] match_files(input_folder, log_path="data/00-metadata/logs/(3)_xlsx_validation_log_latest.json") -> Main function
        - Matches input files with metadata files and logs the results
"""
import os
import re

import polars as pl
from rich.console import Console
import datetime

from eencijferho.io.decorators import with_storage


@with_storage
def load_input_files(storage, input_folder: str) -> pl.DataFrame:
    """Get all files from the user input_folder (root level only), excluding .txt, .zip, and .xlsx extensions,
    and count the number of rows in each file."""
    files = []
    row_counts = []
    console = Console()

    all_files = storage.list_files(f"{input_folder}/*")
    for filepath in all_files:
        filename = os.path.basename(filepath)
        if filename.lower().endswith(('.txt', '.zip', '.xlsx', '.docx', '.csv')):
            continue
        files.append(filename)
        try:
            data = storage.read_bytes(filepath)
            row_count = data.count(b'\n')
            if data and not data.endswith(b'\n'):
                row_count += 1
            row_counts.append(row_count)
        except Exception as e:
            console.print(f"[red]Warning: Could not read file {filename}: {str(e)}")
            row_counts.append(-1)

    return pl.DataFrame({
        "input_file": files,
        "row_count": row_counts
    })


@with_storage
def load_validation_log(storage, log_path: str) -> pl.DataFrame:
    """Load processed bestandbeschrijvingen validation log and return a Polars dataframe with file and status columns"""
    data = storage.read_json(log_path)

    df = pl.DataFrame([
        {'file': item['file'], 'status': item['status']}
        for item in data.get('processed_files', [])
    ])

    return df


def _validation_year(validation_file: str) -> int | None:
    """Het jaartal uit de bestandsnaam van een bestandsbeschrijving, of None.

    De BB-bestanden dragen het jaartal in hun naam, bijvoorbeeld
    `Bestandsbeschrijving_1cyferho_2023_v1.1.txt`. De inhoud is geen bruikbare
    bron: een BB bevat het jaartal ook in waardeomschrijvingen (25 keer "2023"
    in de demobestand), dus daar lezen zou het verkeerde jaar kunnen opleveren.
    """
    match = re.search(r"(?<!\d)((?:19|20)\d{2})(?!\d)", validation_file)
    return int(match.group(1)) if match else None


def _input_year(input_file: str) -> int | None:
    """Het jaartal uit de naam van een EV-bestand, of None als het er niet staat.

    DUO schrijft het jaar als `XX<yy>`, bijvoorbeeld `EV299XX24` voor 2024. We
    leveren de laatste twee cijfers en niet het volledige jaar, zodat er niets
    naar de eeuw geraden hoeft te worden; `_year_matches` vergelijkt ze met de
    laatste twee cijfers van het BB-jaar. Bestanden zonder `XX<yy>` (zoals
    `VAKHAVW_99XX_DEMO.asc`) geven None en krijgen geen jaarfilter.
    """
    match = re.search(r"XX(\d{2})(?!\d)", os.path.splitext(input_file)[0])
    return int(match.group(1)) if match else None


def _year_matches(input_year: int, validation_year: int | None) -> bool:
    """Of een BB-jaar bij het tweecijferige jaar van het EV-bestand past."""
    return validation_year is not None and validation_year % 100 == input_year


@with_storage
def match_files(storage, input_folder: str, log_path: str = "data/00-metadata/logs/(3)_xlsx_validation_log_latest.json") -> dict[str, pl.DataFrame]:
    """Match input files with metadata files and log the results.

    Special matching rules:
    - Files starting with "EV" match with files containing "1cyferho", and the
      year in the file name must match the year in the metadata file name
    - Files containing "VAKHAVW" match with files containing "Vakgegevens"
    """

    log_folder = os.path.dirname(log_path)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_log_file = os.path.join(log_folder, f"file_matching_log_{timestamp}.json")
    latest_log_file = os.path.join(log_folder, "(4)_file_matching_log_latest.json")

    input_df = load_input_files(input_folder)
    validation_df = load_validation_log(log_path)

    console = Console()
    console.print("[green]Finding matches between input files and validation records")

    log_data = {
        "timestamp": timestamp,
        "input_folder": input_folder,
        "validation_log": log_path,
        "status": "started",
        "processed_files": [],
        "total_input_files": len(input_df),
        "matched_files": 0,
        "unmatched_files": 0,
        "total_validation_files": len(validation_df),
        "matched_validation_files": 0,
        "unmatched_validation_files": 0,
    }

    results = []

    # Keep track of which validation files have been matched
    matched_validation_files = set()

    for idx, row in enumerate(input_df.rows()):
        input_file = row[0]  # Filename
        row_count = row[1]   # Row count

        matches = None
        input_year = None
        year_mismatch_candidates = None

        if input_file.startswith("EV"):
            candidates = validation_df.filter(pl.col("file").str.contains("1cyferho", literal=True))
            input_year = _input_year(input_file)
            if input_year is None:
                matches = candidates
            else:
                matches = candidates.filter(
                    pl.col("file").map_elements(
                        lambda name: _year_matches(input_year, _validation_year(name)),
                        return_dtype=pl.Boolean,
                    )
                )
                if len(matches) == 0 and len(candidates) > 0:
                    year_mismatch_candidates = candidates
        elif "VAKHAVW" in input_file:
            matches = validation_df.filter(pl.col("file").str.contains("Vakgegevens", literal=True))
        else:
            matches = validation_df.filter(pl.col("file").str.contains(input_file, literal=True))

        file_log = {
            "input_file": input_file,
            "row_count": row_count,
            "status": "unmatched",
            "matches": []
        }

        if year_mismatch_candidates is not None:
            # Er is wel een 1cyferho-BB, maar niet voor dit jaar. Dat is geen
            # "geen match" maar een conflict: het bestand zou anders stilzwijgend
            # met de layout van een ander jaar worden gelezen. We markeren het
            # daarom expliciet en tonen welke jaren er wél beschikbaar zijn.
            file_log["status"] = "year_mismatch"
            file_log["year"] = 2000 + input_year
            file_log["matches"] = [
                {
                    "validation_file": name,
                    "validation_status": status,
                    "validation_year": _validation_year(name),
                }
                for name, status in year_mismatch_candidates.rows()
            ]
            available = ", ".join(
                str(_validation_year(n)) for n, _ in year_mismatch_candidates.rows()
            )
            console.print(
                f"[red]Jaarmismatch: {input_file} is uit {2000 + input_year}, maar de "
                f"gevonden 1cyferho-bestandsbeschrijving(en) zijn uit: {available}"
            )
            results.append({
                "input_file": input_file,
                "row_count": row_count,
                "validation_file": None,
                "status": None,
                "matched": False
            })
            log_data["processed_files"].append(file_log)
            continue

        if len(matches) > 0:
            file_log["status"] = "matched"

            for match_row in matches.rows():
                validation_file = match_row[0]
                matched_validation_files.add(validation_file)

                match_detail = {
                    "validation_file": validation_file,
                    "validation_status": match_row[1],
                    "validation_year": _validation_year(validation_file),
                }
                file_log["matches"].append(match_detail)

                results.append({
                    "input_file": input_file,
                    "row_count": row_count,
                    "validation_file": validation_file,
                    "status": match_row[1],
                    "matched": True
                })
        else:
            results.append({
                "input_file": input_file,
                "row_count": row_count,
                "validation_file": None,
                "status": None,
                "matched": False
            })

        log_data["processed_files"].append(file_log)

    result_df = pl.DataFrame(results)

    unmatched_validation = []
    for validation_row in validation_df.rows():
        validation_file = validation_row[0]
        if validation_file not in matched_validation_files:
            unmatched_validation.append({
                "validation_file": validation_file,
                "validation_status": validation_row[1],
                "matched": False
            })

    unmatched_validation_df = pl.DataFrame(unmatched_validation)


    log_data["status"] = "completed"
    log_data["matched_files"] = result_df.filter(pl.col('matched')).height
    log_data["unmatched_files"] = result_df.filter(~pl.col('matched')).height
    log_data["matched_validation_files"] = len(matched_validation_files)
    log_data["unmatched_validation_files"] = len(validation_df) - len(matched_validation_files)
    log_data["unmatched_validation"] = [
        {"validation_file": row["validation_file"], "validation_status": row["validation_status"]}
        for row in unmatched_validation
    ]

    storage.write_json(log_data, timestamped_log_file)
    storage.write_json(log_data, latest_log_file)

    console.print(f"[green]Total input files: {log_data['total_input_files']}  | Matched files: {log_data['matched_files']} [/green] | [red]Unmatched files: {log_data['unmatched_files']}[/red]")
    console.print(f"[green]Total validation files: {log_data['total_validation_files']} [/green] | [yellow]Unmatched validation files: {log_data['unmatched_validation_files']}[/yellow]")

    if log_data["unmatched_files"] > 0 or log_data["unmatched_validation_files"] > 0:
        console.print(f"\n[yellow]Perhaps a naming error? Manually fix in {input_folder} for input files or {os.path.dirname(log_path)} for validation files[/yellow]")

    if log_data["unmatched_files"] > 0:
        console.print("\n[red]Unmatched input files:[/red]")
        unmatched_input = result_df.filter(~pl.col('matched'))
        for row in unmatched_input.rows():
            input_file = row[0]
            console.print(f"[red]{input_file}[/red]")

    if log_data["unmatched_validation_files"] > 0:
        console.print("\n[yellow]Unmatched validation files:[/yellow]")
        for item in log_data["unmatched_validation"]:
            validation_file = item["validation_file"]
            console.print(f"[yellow]{validation_file}[/yellow]")

    console.print(f"\n[blue]Log saved to: {os.path.basename(latest_log_file)} and {os.path.basename(timestamped_log_file)} in {log_folder}[/blue]")

    return {
        "input_matches": result_df,
        "unmatched_validation": unmatched_validation_df
    }
