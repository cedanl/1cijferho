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
    [x] select_layout(input_file, candidates, keyword, layout_year=None)
        - Pick the one layout (bestandsbeschrijving) for an EV/VAKHAVW file by year
    [M] match_files(input_folder, log_path="data/00-metadata/logs/(3)_xlsx_validation_log_latest.json", layout_year=None) -> Main function
        - Matches input files with metadata files and logs the results
"""
import logging
import os
import re
from typing import NamedTuple

import polars as pl
from rich.console import Console
import datetime

from eencijferho.io.decorators import with_storage
from eencijferho.utils.run_log import log_failed

logger = logging.getLogger(__name__)


# Input files whose layout is chosen by year: name test -> keyword in the layout name.
_LAYOUT_FAMILIES = (
    (lambda name: name.startswith("EV"), "1cyferho"),
    (lambda name: "VAKHAVW" in name, "Vakgegevens"),
)

# "EV299XX24" -> 24: DUO puts the two-digit year after the XX placeholder.
_INPUT_TWO_DIGIT_YEAR = re.compile(r"XX(\d{2})(?!\d)")
# A four-digit year that is not part of a longer number ("EV2023.asc").
_FOUR_DIGIT_YEAR = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")

_LAYOUT_YEAR_HINT = "Kies de bestandsbeschrijving met --layout-year JJJJ."


class LayoutMatchError(ValueError):
    """An EV/VAKHAVW input file cannot be tied to exactly one layout.

    ``errors`` holds one dict per failed input file with the keys
    ``input_file``, ``reason`` and ``candidates``.
    """

    def __init__(self, errors: list[dict]):
        self.errors = errors
        header = (
            f"{len(errors)} invoerbestand(en) kunnen niet eenduidig aan een "
            f"bestandsbeschrijving worden gekoppeld:"
        )
        lines = [header]
        for err in errors:
            candidates = ", ".join(err["candidates"]) or "geen"
            lines.append(f"- {err['input_file']}: {err['reason']} Kandidaten: {candidates}.")
        super().__init__("\n".join(lines))


class LayoutChoice(NamedTuple):
    """Outcome of :func:`select_layout`. ``chosen`` is None when there is no valid choice."""

    chosen: str | None
    kind: str  # "ok", "forced", "no_candidates", "ambiguous", "year_mismatch", "not_success"
    reason: str
    candidates: list[str]


def _layout_keyword(input_file: str) -> str | None:
    """Return the layout keyword for files whose layout is chosen by year, else None."""
    for applies, keyword in _LAYOUT_FAMILIES:
        if applies(input_file):
            return keyword
    return None


def _year_from_input_name(name: str) -> int | None:
    """Year in an input file name: ``XXyy`` (2000 + yy), else a loose four-digit year."""
    match = _INPUT_TWO_DIGIT_YEAR.search(name)
    if match:
        return 2000 + int(match.group(1))
    match = _FOUR_DIGIT_YEAR.search(name)
    return int(match.group(1)) if match else None


def _year_from_layout_name(name: str, keyword: str) -> int | None:
    """Year in a layout name: four digits after ``<keyword>_``, else a loose four-digit year."""
    match = re.search(re.escape(keyword) + r"_((?:19|20)\d{2})(?!\d)", name)
    if match:
        return int(match.group(1))
    match = _FOUR_DIGIT_YEAR.search(name)
    return int(match.group(1)) if match else None


def select_layout(
    input_file: str,
    candidates: list[tuple[str, str]],
    keyword: str,
    layout_year: int | None = None,
) -> LayoutChoice:
    """Pick the one layout for an input file among ``candidates`` (file, validation status).

    The year in the input name, or ``layout_year`` when given, must point to exactly
    one candidate. A single candidate without a year is accepted as is. Anything else
    is an error: no candidates, several candidates for the same year, no year to
    choose between several candidates, or a year that no candidate has. The chosen
    layout must have validation status ``success``.
    """
    names = [name for name, _ in candidates]
    statuses = dict(candidates)

    def choose(name: str, kind: str, reason: str) -> LayoutChoice:
        if statuses[name] != "success":
            return LayoutChoice(
                None, "not_success",
                f"De gekozen bestandsbeschrijving {name} heeft validatiestatus "
                f"'{statuses[name]}'; herstel deze en valideer opnieuw.",
                names,
            )
        return LayoutChoice(name, kind, reason, names)

    if not candidates:
        return LayoutChoice(
            None, "no_candidates",
            f"Geen bestandsbeschrijving met '{keyword}' in de naam gevonden.", names,
        )

    years = {name: _year_from_layout_name(name, keyword) for name in names}
    input_year = _year_from_input_name(input_file)
    target = layout_year if layout_year is not None else input_year

    if target is None:
        if len(candidates) == 1:
            return choose(names[0], "ok", "Enige bestandsbeschrijving.")
        return LayoutChoice(
            None, "ambiguous",
            f"Geen jaar in de bestandsnaam en {len(candidates)} bestandsbeschrijvingen. "
            f"{_LAYOUT_YEAR_HINT}",
            names,
        )

    picked = [name for name in names if years[name] == target]
    if len(picked) == 1:
        if layout_year is not None:
            shown_year = input_year if input_year is not None else "onbekend"
            return choose(
                picked[0], "forced",
                f"Bestandsbeschrijving geforceerd op jaar {layout_year} via --layout-year "
                f"(jaar in bestandsnaam: {shown_year}).",
            )
        return choose(picked[0], "ok", f"Jaar {target} uit de bestandsnaam.")
    if len(picked) > 1:
        return LayoutChoice(
            None, "ambiguous",
            f"{len(picked)} bestandsbeschrijvingen voor jaar {target}; verwijder de overbodige.",
            names,
        )

    if len(candidates) == 1 and years[names[0]] is None:
        return choose(names[0], "ok", "Enige bestandsbeschrijving, zonder jaar.")

    known_years = sorted({year for year in years.values() if year is not None})
    available = ", ".join(str(year) for year in known_years) or "geen"
    if layout_year is not None:
        return LayoutChoice(
            None, "year_mismatch",
            f"Geen bestandsbeschrijving voor --layout-year {layout_year} "
            f"(beschikbare jaren: {available}).",
            names,
        )
    if all(year is not None for year in years.values()):
        return LayoutChoice(
            None, "year_mismatch",
            f"Jaar {input_year} in de bestandsnaam komt niet overeen met de "
            f"bestandsbeschrijving(en) voor {available}. {_LAYOUT_YEAR_HINT}",
            names,
        )
    return LayoutChoice(
        None, "ambiguous",
        f"Geen bestandsbeschrijving voor jaar {input_year} en {len(candidates)} kandidaten. "
        f"{_LAYOUT_YEAR_HINT}",
        names,
    )


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
        except Exception as e:  # catch-all: any storage backend error; the file gets row_count -1
            log_failed(
                logger, "input_row_count", e, file=filename,
                detail=f"Warning: Could not read file {filename}",
            )
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


@with_storage
def match_files(
    storage,
    input_folder: str,
    log_path: str = "data/00-metadata/logs/(3)_xlsx_validation_log_latest.json",
    layout_year: int | None = None,
) -> dict[str, pl.DataFrame]:
    """Match input files with metadata files and log the results.

    Special matching rules:
    - Files starting with "EV" match with exactly one file containing "1cyferho"
    - Files containing "VAKHAVW" match with exactly one file containing "Vakgegevens"
    For these two the year in the input name picks the layout (see
    :func:`select_layout`); ``layout_year`` forces the layout year instead.

    Raises:
        LayoutMatchError: when an EV/VAKHAVW file has no layout or no single one.
            The match log is written first, with status ``failed``.
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
        "layout_year": layout_year,
        "warnings": [],
        "errors": [],
    }

    results = []

    # Keep track of which validation files have been matched
    matched_validation_files = set()

    for idx, row in enumerate(input_df.rows()):
        input_file = row[0]  # Filename
        row_count = row[1]   # Row count

        file_log = {
            "input_file": input_file,
            "row_count": row_count,
            "status": "unmatched",
            "matches": []
        }

        keyword = _layout_keyword(input_file)
        if keyword is not None:
            candidates = validation_df.filter(pl.col("file").str.contains(keyword, literal=True))
            choice = select_layout(input_file, candidates.rows(), keyword, layout_year)
            file_log["candidates"] = choice.candidates
            file_log["reason"] = choice.reason
            if choice.chosen is None:
                file_log["status"] = "failed"
                file_log["error"] = choice.reason
                log_data["errors"].append({
                    "input_file": input_file,
                    "reason": choice.reason,
                    "candidates": choice.candidates,
                })
                log_data["processed_files"].append(file_log)
                results.append({
                    "input_file": input_file,
                    "row_count": row_count,
                    "validation_file": None,
                    "status": None,
                    "matched": False
                })
                continue
            if choice.kind == "forced":
                file_log["forced_layout_year"] = layout_year
                warning = f"{input_file}: {choice.reason} Gekozen: {choice.chosen}."
                log_data["warnings"].append(warning)
                console.print(f"[yellow]Waarschuwing: {warning}[/yellow]")
            matches = candidates.filter(pl.col("file") == choice.chosen)
        else:
            matches = validation_df.filter(pl.col("file").str.contains(input_file, literal=True))

        if len(matches) > 0:
            file_log["status"] = "matched"

            for match_row in matches.rows():
                validation_file = match_row[0]
                matched_validation_files.add(validation_file)

                match_detail = {
                    "validation_file": validation_file,
                    "validation_status": match_row[1]
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


    log_data["status"] = "failed" if log_data["errors"] else "completed"
    log_data["failed_files"] = len(log_data["errors"])
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

    if log_data["errors"]:
        error = LayoutMatchError(log_data["errors"])
        console.print(f"\n{error}", markup=False, style="red")
        raise error

    return {
        "input_matches": result_df,
        "unmatched_validation": unmatched_validation_df
    }
