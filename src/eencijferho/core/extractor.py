# -----------------------------------------------------------------------------
# Organization: CEDA
# Original Author: Ash Sewnandan
# Contributors: -
# License: MIT
# -----------------------------------------------------------------------------
"""
Extractor module for 1cijferho data. Extracts field tables from DUO bestandsbeschrijving
text files and converts them to JSON and Excel formats.

Public API:
    extract_tables_from_txt(txt_file, json_output_folder)
        Extracts tables from a .txt/.asc file and saves them as JSON.
    process_txt_folder(input_folder, json_output_folder)
        Processes all Bestandsbeschrijving*.txt and *.asc files in a folder.
    extract_excel_from_json(json_file, excel_output_folder)
        Converts a JSON metadata file to one Excel file per table.
    process_json_folder(json_input_folder, excel_output_folder)
        Converts all JSON files in a folder to Excel files.
    write_variable_metadata(input_dir, json_folder)
        Writes a consolidated variable_metadata.json from all input text files.
"""

import glob
import os
import json
import re
import datetime

import polars as pl
import xlsxwriter
from rich.console import Console
from typing import Any

from .parse_metadata import parse_metadata_file

_console = Console()

# Constants for table extraction
MAX_LOOKAHEAD_LINES_FOR_DECODING_SECTION = (
    20  # Lines to search for "Ten behoeve van de decodering" section
)
MAX_LOOKAHEAD_LINES_FOR_VARIABLE_LIST = 50  # Lines to search for bullet-pointed variable list (longer because list can be extended)


def _extract_decoding_variables(lines: list[str], start_index: int) -> list[str]:
    """
    Extracts decoding variables from lines starting at start_index.

    Looks for the "Ten behoeve van de decodering" or "Ten behoeve van de vertaling" section and extracts bullet-pointed variables.

    Args:
        lines (list[str]): List of text lines to search.
        start_index (int): Index to start searching from.

    Returns:
        list[str]: List of decoding variable names.

    Edge Cases:
        - Stops at section dividers or unrelated content.
        - Handles empty lines and notes.

    Example:
        >>> _extract_decoding_variables(lines, 10)
    """
    decoding_variables = []
    j = start_index

    # Look for "Ten behoeve van de decodering" or "Ten behoeve van de vertaling" section
    while j < len(lines) and j < start_index + MAX_LOOKAHEAD_LINES_FOR_DECODING_SECTION:
        current_line = lines[j].strip().lower()

        # Check if we've found the decoding/vertaling section
        if ("ten behoeve van de decodering" in current_line) or (
            "ten behoeve van de vertaling" in current_line
        ):
            # Now collect all the bullet points (lines starting with *)
            j += 1
            while (
                j < len(lines)
                and j < start_index + MAX_LOOKAHEAD_LINES_FOR_VARIABLE_LIST
            ):
                var_line = lines[j].strip()
                if var_line.startswith("*"):
                    # Extract the variable name after the asterisk
                    var_name = var_line[1:].strip()
                    if var_name:
                        decoding_variables.append(var_name)
                    j += 1
                elif not var_line:
                    # Empty line, continue to check for more
                    j += 1
                elif (
                    var_line.startswith("NB:")
                    or var_line.startswith("Opmerking:")
                    or var_line.startswith("Mogelijke")
                ):
                    # Stop when we hit notes or remarks
                    break
                else:
                    # Stop if we hit non-empty line that's not a bullet point or empty line
                    break
            break

        # Stop if we hit another table or section divider
        if current_line.startswith("==") or "startpositie" in current_line:
            break

        j += 1

    return decoding_variables


# ---------------------------------------------------------------------------
# Private helpers for extract_tables_from_txt
# ---------------------------------------------------------------------------


def _read_txt_latin1(path: str) -> str | None:
    """Read a text file using latin-1 encoding. Returns None on error."""
    try:
        with open(path, "r", encoding="latin-1") as fh:
            return fh.read()
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return None


def _find_title_above(lines: list[str], header_index: int) -> str:
    """Look backwards from header_index for a title above a '==' divider line."""
    for j in range(header_index - 1, max(0, header_index - 10), -1):
        if lines[j].strip().startswith("=="):
            if j > 0 and lines[j - 1].strip():
                return lines[j - 1].strip()
            break
    return ""


def _parse_tables(lines: list[str]) -> list[dict]:
    """Parse all tables from a list of lines from a bestandsbeschrijving file."""
    found = False
    table_title = ""
    table_content: list[str] = []
    table_start_idx = 0
    tables_found = 0
    all_tables: list[dict] = []

    for i, line in enumerate(lines):
        if "startpositie" in line.lower() and not found:
            found = True
            tables_found += 1
            table_start_idx = i
            table_content = [line]
            title = _find_title_above(lines, i)
            table_title = title if title else f"untitled_table_{tables_found}"
        elif found:
            if not line.strip():
                found = False
                decoding_variables = _extract_decoding_variables(lines, i + 1)
                all_tables.append({
                    "table_number": tables_found,
                    "table_title": table_title,
                    "content": table_content,
                    "decoding_variables": decoding_variables,
                })
                table_content = []
                continue
            table_content.append(line)

    if found and table_content:
        decoding_variables = _extract_decoding_variables(
            lines, table_start_idx + len(table_content)
        )
        all_tables.append({
            "table_number": tables_found,
            "table_title": table_title,
            "content": table_content,
            "decoding_variables": decoding_variables,
        })

    return all_tables


def _save_tables_json(
    all_tables: list[dict], txt_file: str, json_output_folder: str
) -> str:
    """Write all_tables to a JSON file named after txt_file. Returns the path."""
    base_filename = os.path.splitext(os.path.basename(txt_file))[0]
    json_path = os.path.join(json_output_folder, f"{base_filename}.json")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(
            {"filename": base_filename, "tables": all_tables},
            fh, indent=2, ensure_ascii=False,
        )
    return json_path


def extract_tables_from_txt(txt_file: str, json_output_folder: str) -> str | None:
    """
    Extracts tables from a .txt or .asc file and saves them as JSON.

    Args:
        txt_file (str): Path to the .txt or .asc file.
        json_output_folder (str): Folder to save the output JSON.

    Returns:
        str | None: Path to the saved JSON file, or None if extraction failed.

    Edge Cases:
        - Handles file read errors gracefully.
        - Creates output folder if missing.

    Example:
        >>> extract_tables_from_txt('Bestandsbeschrijving_1cyferho_2023_v1.1_DEMO.txt', 'data/00-metadata/json')
    """
    os.makedirs(json_output_folder, exist_ok=True)
    text = _read_txt_latin1(txt_file)
    if text is None:
        return None
    all_tables = _parse_tables(text.split("\n"))
    if not all_tables:
        return None
    return _save_tables_json(all_tables, txt_file, json_output_folder)


def process_txt_folder(
    input_folder: str, json_output_folder: str = "data/00-metadata/json"
) -> list[str]:
    """
    Finds all .txt files containing 'Bestandsbeschrijving' in the root directory and extracts tables from them.
    Also processes all .asc files in the root directory.

    Args:
        input_folder (str): Folder to search for .txt and .asc files.
        json_output_folder (str, optional): Output folder for JSON files. Defaults to 'data/00-metadata/json'.

    Returns:
        list[str]: Paths to all extracted JSON files.

    Edge Cases:
        - Removes any existing JSON files in the output folder.
        - Handles missing input folder gracefully.

    Example:
        >>> process_txt_folder('data/01-input')
    """
    os.makedirs(json_output_folder, exist_ok=True)

    # Remove any existing json files
    for file in os.listdir(json_output_folder):
        if file.endswith(".json"):
            os.remove(os.path.join(json_output_folder, file))

    # Setup logging — log folder is a sibling of json_output_folder
    log_folder = os.path.join(os.path.dirname(json_output_folder), "logs")
    os.makedirs(log_folder, exist_ok=True)

    # Create both timestamped and latest logs
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_log_file = os.path.join(
        log_folder, f"json_processing_log_{timestamp}.json"
    )
    latest_log_file = os.path.join(log_folder, "(1)_json_processing_log_latest.json")

    log_data = {
        "timestamp": timestamp,
        "input_folder": input_folder,
        "output_folder": json_output_folder,
        "status": "started",
        "processed_files": [],
        "total_files_processed": 0,
        "total_files_extracted": 0,
    }

    filter_keyword = "Bestandsbeschrijving"
    extracted_files = []

    # Only process files in the root directory, not subdirectories
    if os.path.exists(input_folder):
        for file in os.listdir(input_folder):
            file_path = os.path.join(input_folder, file)
            # Process Bestandsbeschrijving .txt files
            if (
                os.path.isfile(file_path)
                and file.endswith(".txt")
                and filter_keyword in file
            ):
                file_log = {"file": file, "status": "processing", "output": None}
                json_path = extract_tables_from_txt(file_path, json_output_folder)
                file_log["status"] = "success" if json_path else "no_tables_found"
                if json_path:
                    extracted_files.append(json_path)
                    file_log["output"] = os.path.basename(json_path)
                log_data["processed_files"].append(file_log)
            # Also process all .asc files (DEC files)
            elif os.path.isfile(file_path) and file.endswith(".asc"):
                file_log = {"file": file, "status": "processing", "output": None}
                json_path = extract_tables_from_txt(file_path, json_output_folder)
                file_log["status"] = "success" if json_path else "no_tables_found"
                if json_path:
                    extracted_files.append(json_path)
                    file_log["output"] = os.path.basename(json_path)
                log_data["processed_files"].append(file_log)

    # Update final log status
    log_data["status"] = "completed"
    log_data["total_files_processed"] = len(log_data["processed_files"])
    log_data["total_files_extracted"] = len(extracted_files)

    # Save log file to both locations
    with open(timestamped_log_file, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2)
    with open(latest_log_file, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2)

    # Print summary to console
    _console.print(f"[green]Processed {log_data['total_files_processed']} text files")
    _console.print(
        f"[green]Extracted tables to {log_data['total_files_extracted']} JSON files"
    )
    _console.print(
        f"[blue]Log saved to: {os.path.basename(latest_log_file)} and {os.path.basename(timestamped_log_file)} in {log_folder}"
    )

    # --- PATCH: Merge Dec_vakcode table from Vakkenbestanden JSON into Dec-bestanden JSON ---
    vakken_json = next((f for f in extracted_files if "Vakkenbestanden" in f), None)
    dec_json = next((f for f in extracted_files if "Dec-bestanden" in f), None)
    if vakken_json and dec_json and os.path.exists(vakken_json) and os.path.exists(dec_json):
        try:
            with open(vakken_json, "r", encoding="utf-8") as f_vak:
                vak_data = json.load(f_vak)
            with open(dec_json, "r", encoding="utf-8") as f_dec:
                dec_data = json.load(f_dec)
            # Find Dec_vakcode table in vak_data
            vakcode_tables = [
                t
                for t in vak_data.get("tables", [])
                if "vakcode" in t.get("table_title", "").lower()
            ]
            if vakcode_tables:
                # Only add if not already present in dec_data
                existing_titles = [
                    t.get("table_title", "").lower() for t in dec_data.get("tables", [])
                ]
                max_table_number = max(
                    [t.get("table_number", 0) for t in dec_data.get("tables", [])]
                    or [0]
                )
                for t in vakcode_tables:
                    # Set correct table_title and unique table_number
                    t["table_title"] = "Dec_vakcode.asc"
                    max_table_number += 1
                    t["table_number"] = max_table_number
                    # If decoding_variables is empty, set to ["Vakcode"]
                    if not t.get("decoding_variables"):
                        t["decoding_variables"] = ["Vakcode"]
                    if t["table_title"].lower() not in existing_titles:
                        dec_data["tables"].append(t)
                        existing_titles.append(t["table_title"].lower())
                # Save back
                with open(dec_json, "w", encoding="utf-8") as f_dec:
                    json.dump(dec_data, f_dec, indent=2, ensure_ascii=False)
                _console.print(
                    f"[cyan]Patched: Added Dec_vakcode table(s) from Vakkenbestanden JSON to Dec-bestanden JSON with correct title and table_number."
                )
        except Exception as e:
            _console.print(
                f"[red]Error patching Dec_vakcode into Dec-bestanden JSON: {e}"
            )

    return extracted_files


def write_variable_metadata(
    input_dir: str = "data/01-input",
    json_folder: str = "data/00-metadata/json",
    output_filename: str = "variable_metadata.json",
) -> None:
    """
    Scans all Bestandsbeschrijving*.txt files (recursively) from input_dir and writes a consolidated variable metadata JSON.

    Args:
        input_dir (str, optional): Folder to search for input text files. Defaults to 'data/01-input'.
        json_folder (str, optional): Output folder for JSON file. Defaults to 'data/00-metadata/json'.
        output_filename (str, optional): Name for output file. Defaults to 'variable_metadata.json'.

    Returns:
        None

    Edge Cases:
        - Handles missing input files or parser errors gracefully.
        - Avoids duplicate variables by name across all files.

    Example:
        >>> write_variable_metadata()
    """
    os.makedirs(json_folder, exist_ok=True)
    output_path = os.path.join(json_folder, output_filename)

    # Recursively find all Bestandsbeschrijving*.txt files in input_dir
    # Handles .../DEMO/ as well as any deeper future organization
    file_glob = os.path.join(input_dir, "**", "Bestandsbeschrijving*.txt")
    txt_files = glob.glob(file_glob, recursive=True)
    if not txt_files:
        _console.print(
            f"[yellow]No Bestandsbeschrijving*.txt files found in {input_dir}"
        )
        return

    all_vars = []
    seen_names = set()
    for txt_file in txt_files:
        try:
            vars_out = parse_metadata_file(txt_file)
            for variable in vars_out:
                name = variable.get("name")
                if name and name not in seen_names:
                    all_vars.append(variable)
                    seen_names.add(name)
        except Exception as e:
            _console.print(f"[red]Parser failed for {txt_file}: {e}")

    if all_vars:
        with open(output_path, "w", encoding="utf-8") as out_f:
            json.dump(all_vars, out_f, ensure_ascii=False, indent=2)
        _console.print(
            f"[blue]Consolidated {len(all_vars)} variables from {len(txt_files)} files into {output_path}"
        )
    else:
        _console.print(f"[yellow]No variables found in any file! Did parsing fail?")
    return


# ---------------------------------------------------------------------------
# Private helpers for extract_excel_from_json
# ---------------------------------------------------------------------------


def _sanitize_filename(filename: str) -> str:
    """Remove or replace characters that are invalid in filenames."""
    return re.sub(r'[\\/*?:"<>|]', "_", filename)


def _parse_data_line(
    line: str, start_pos_index: int, aantal_pos_index: int
) -> tuple[str, int, int, str] | None:
    """Parse one content line into (field_name, start_pos, aantal_pos, comment).

    Returns None when the line cannot produce a valid row.
    Handles two cases:
      1. Line contains both 'Startpositie' and 'Aantal posities' keywords (repeated header).
      2. Normal data line parsed by character position.
    """
    # Case 1: line repeats both header keywords
    if "Startpositie" in line and "Aantal posities" in line:
        modified = line.replace("Startpositie", "|Startpositie")
        modified = modified.replace("Aantal posities", "|Aantal posities|")
        parts = modified.split("|")
        if len(parts) >= 3:
            field_name = parts[0].strip()
            start_str = parts[1].replace("Startpositie", "").strip()
            aantal_str = parts[2].replace("Aantal posities", "").strip()
            comment = parts[3].strip() if len(parts) > 3 else ""
            if field_name and start_str.isdigit() and aantal_str.isdigit():
                return field_name, int(start_str), int(aantal_str), comment
        return None

    # Case 2: normal data line
    if len(line) <= start_pos_index:
        return None

    # Field name: everything before the first digit at/after start_pos_index
    pos_start = next(
        (j for j in range(start_pos_index, len(line)) if line[j].isdigit()), None
    )
    field_name = (
        line[:pos_start].rstrip() if pos_start is not None
        else line[:start_pos_index].rstrip()
    )

    # Start position digits
    i = start_pos_index
    while i < len(line) and not line[i].isdigit():
        i += 1
    start_digits = ""
    while i < len(line) and line[i].isdigit():
        start_digits += line[i]
        i += 1
    if not start_digits:
        return None

    # Aantal posities digits
    if len(line) <= aantal_pos_index:
        return None
    i = aantal_pos_index
    while i < len(line) and not line[i].isdigit():
        i += 1
    aantal_digits = ""
    while i < len(line) and line[i].isdigit():
        aantal_digits += line[i]
        i += 1
    if not aantal_digits:
        return None

    # Optional comment after aantal posities
    while i < len(line) and line[i].isspace():
        i += 1
    comment = line[i:].strip() if i < len(line) else ""

    if not field_name:
        return None
    return field_name, int(start_digits), int(aantal_digits), comment


def _build_table_rows(
    content_array: list[str], start_pos_index: int, aantal_pos_index: int
) -> tuple[list[list[Any]], int]:
    """Build Excel row data from a table's content_array.

    Returns (rows, valid_content_lines) where rows[0] is the header row
    and valid_content_lines counts successfully parsed data rows.
    """
    rows: list[list[Any]] = [
        ["ID", "Naam", "Startpositie", "Aantal posities", "Opmerking"]
    ]
    valid_content_lines = 0
    row_id = 1

    for line in content_array[1:]:
        if not line.strip():
            continue
        parsed = _parse_data_line(line, start_pos_index, aantal_pos_index)
        if parsed is None:
            _console.print(
                f"[yellow]Skipping row: could not parse line: {line[:80]!r}"
            )
            continue
        field_name, start_pos, aantal_pos, comment = parsed
        try:
            rows.append([row_id, field_name, start_pos, aantal_pos, comment])
            row_id += 1
            valid_content_lines += 1
        except Exception as e:
            _console.print(
                f"[red]Row creation error: {e} | field_name={field_name}, "
                f"start_pos={start_pos}, aantal_pos={aantal_pos}, comment={comment}"
            )

    return rows, valid_content_lines


def _write_table_excel(
    rows: list[list[Any]],
    decoding_variables: list[str],
    output_path: str,
) -> int:
    """Write rows (and optional decoding variables) to an Excel file.

    Returns the number of data rows written. Raises on write failure.
    """
    main_rows = [row for row in rows if isinstance(row[0], int)]
    columns = rows[0]
    df_main = pl.DataFrame(
        {col: [row[i] for row in main_rows] for i, col in enumerate(columns)}
    )

    if decoding_variables:
        df_dec = pl.DataFrame({"DecodingVariables": decoding_variables})
        workbook = xlsxwriter.Workbook(output_path)
        df_main.write_excel(workbook, worksheet="Table")
        df_dec.write_excel(workbook, worksheet="DecodingVariables")
        workbook.close()
    else:
        df_main.write_excel(output_path)

    return len(df_main)


def extract_excel_from_json(
    json_file: str, excel_output_folder: str
) -> tuple[list[dict[str, Any]], int, int]:
    """
    Extracts tables from a JSON file and saves them as Excel files.

    Includes ID column and a column for comments (Opmerkingen) after Aantal posities.
    Also extracts and stores decoding variables information in a separate sheet.
    Sets specific data types for Excel columns: ID (int), Naam (str), Startpositie (int),
    Aantal posities (int), Opmerking (str).

    Args:
        json_file (str): Path to the JSON file.
        excel_output_folder (str): Folder to save the Excel files.

    Returns:
        tuple[list[dict[str, Any]], int, int]: Processing results for table reporting.

    Edge Cases:
        - Handles missing or corrupt JSON files gracefully.
        - Ensures output folder exists.

    Example:
        >>> extract_excel_from_json('Bestandsbeschrijving_1cyferho_2023_v1.1_DEMO.json', 'data/00-metadata')
    """
    os.makedirs(excel_output_folder, exist_ok=True)
    results: list[dict[str, Any]] = []

    try:
        with open(json_file, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except json.JSONDecodeError as e:
        _console.print(f"[red]Error decoding JSON: {e}")
        return [], 0, 0
    except Exception as e:
        _console.print(f"[red]Error opening file: {e}")
        return [], 0, 0

    base_filename = data.get(
        "filename", os.path.splitext(os.path.basename(json_file))[0]
    )
    tables = data.get("tables", [])
    total_tables = len(tables)

    if total_tables == 0:
        _console.print("[yellow]Warning: No tables found in the JSON file.")
        return [], 0, 0

    files_created = 0

    try:
        for idx, table in enumerate(tables):
            table_number = table.get("table_number", idx + 1)
            table_title = table.get("table_title", f"Table_{table_number}")
            content_array = table.get("content", [])

            table_result: dict[str, Any] = {
                "table_number": table_number,
                "table_title": table_title,
                "status": "Processed",
                "rows": 0,
                "output_file": "",
                "notes": "",
            }

            if not content_array:
                table_result["status"] = "Skipped"
                table_result["notes"] = "Empty content"
                results.append(table_result)
                continue

            output_filename = (
                f"{base_filename}_{table_number}_{_sanitize_filename(table_title)}.xlsx"
            )
            output_path = os.path.join(excel_output_folder, output_filename)
            table_result["output_file"] = output_filename

            header = content_array[0]
            if "Startpositie" not in header or "Aantal posities" not in header:
                table_result["status"] = "Skipped"
                table_result["notes"] = "Missing required headers"
                results.append(table_result)
                continue

            start_pos_index = header.find("Startpositie")
            aantal_pos_index = header.find("Aantal posities")
            if start_pos_index == -1 or aantal_pos_index == -1:
                table_result["status"] = "Skipped"
                table_result["notes"] = "Could not locate positions for header columns"
                results.append(table_result)
                continue

            rows, valid_content_lines = _build_table_rows(
                content_array, start_pos_index, aantal_pos_index
            )

            if len(rows) <= 1:
                table_result["status"] = "Skipped"
                table_result["notes"] = "No data rows found"
                results.append(table_result)
                continue

            table_result["rows"] = len(rows) - 1

            decoding_variables = table.get("decoding_variables", [])
            if decoding_variables:
                table_result["decoding_variables"] = decoding_variables
                table_result["notes"] += (
                    f" Includes {len(decoding_variables)} decoding variable(s)."
                )

            try:
                df_row_count = _write_table_excel(rows, decoding_variables, output_path)
                if df_row_count != valid_content_lines:
                    _console.print(
                        f"[yellow]Warning: Row count mismatch for table {table_title}."
                    )
                    _console.print(
                        f"[yellow]Expected {valid_content_lines} rows, "
                        f"got {df_row_count} rows in DataFrame."
                    )
                    table_result["notes"] += (
                        f" Row count mismatch: {valid_content_lines} valid content "
                        f"lines vs {df_row_count} DataFrame rows."
                    )
                files_created += 1
                results.append(table_result)
            except PermissionError:
                table_result["status"] = "Error"
                table_result["notes"] = (
                    "File may be open in another program (e.g. Excel)"
                )
                results.append(table_result)
            except Exception as e:
                table_result["status"] = "Error"
                table_result["notes"] = f"Error: {str(e)}"
                results.append(table_result)

    except Exception as e:
        _console.print(f"[red]Error during processing: {str(e)}")
        return results, files_created, total_tables

    return results, files_created, total_tables


def get_fwf_params(txt_file: str, table_index: int = 0) -> dict:
    """Extract field names and column specs from a DUO bestandsbeschrijving .txt file.

    Returns a dict that can be passed directly to ``pandas.read_fwf()`` as
    keyword arguments, so no manual parsing of the fixed-width metadata is
    needed::

        import pandas as pd
        from eencijferho import get_fwf_params

        params = get_fwf_params("Bestandsbeschrijving_1cyferho_2023_v1.1.txt")
        df = pd.read_fwf("1cyferho_2023.asc", encoding="latin-1", header=None, **params)

    The ``colspecs`` values follow the pandas convention: 0-based, half-open
    intervals ``[start, end)``.  ``Startpositie`` from DUO is 1-based, so
    ``start = Startpositie - 1`` and ``end = Startpositie - 1 + Aantal posities``.

    Args:
        txt_file: Path to the DUO bestandsbeschrijving ``.txt`` (or ``.asc``) file.
        table_index: Which table to use when the file contains multiple tables.
            Defaults to 0 (the first / main table).

    Returns:
        ``{"names": list[str], "colspecs": list[tuple[int, int]]}``

    Raises:
        ValueError: When the file cannot be parsed, no tables are found, or
            ``table_index`` is out of range.

    Example::

        >>> params = get_fwf_params("Bestandsbeschrijving_1cyferho_2023_v1.1.txt")
        >>> params["names"][:3]
        ['Onderwijstype HO', 'BRIN-nummer', 'Opleidingscode (CROHO)']
        >>> params["colspecs"][:3]
        [(0, 3), (3, 7), (7, 12)]
    """
    text = _read_txt_latin1(txt_file)
    if text is None:
        raise ValueError(f"Kan bestand niet lezen: {txt_file}")

    tables = _parse_tables(text.split("\n"))
    if not tables:
        raise ValueError(f"Geen tabellen gevonden in {txt_file}")
    if table_index >= len(tables):
        raise ValueError(
            f"table_index {table_index} buiten bereik — bestand heeft {len(tables)} tabel(len)"
        )

    table = tables[table_index]
    content = table.get("content", [])
    if not content:
        raise ValueError(f"Tabel {table_index} heeft geen inhoud in {txt_file}")

    header = content[0]
    if "Startpositie" not in header or "Aantal posities" not in header:
        raise ValueError(
            f"Tabel {table_index} mist de verwachte kolomkoppen "
            f"'Startpositie' / 'Aantal posities' in {txt_file}"
        )

    start_pos_index = header.find("Startpositie")
    aantal_pos_index = header.find("Aantal posities")

    names: list[str] = []
    colspecs: list[tuple[int, int]] = []

    for line in content[1:]:
        if not line.strip():
            continue
        parsed = _parse_data_line(line, start_pos_index, aantal_pos_index)
        if parsed is None:
            continue
        field_name, start_pos, aantal_pos, _comment = parsed
        # DUO positions are 1-based; pandas.read_fwf expects 0-based half-open [start, end)
        names.append(field_name)
        colspecs.append((start_pos - 1, start_pos - 1 + aantal_pos))

    if not names:
        raise ValueError(f"Geen velden gevonden in tabel {table_index} van {txt_file}")

    return {"names": names, "colspecs": colspecs}


def list_fwf_tables(txt_file: str) -> list[str]:
    """List the table names found in a DUO bestandsbeschrijving .txt file.

    Useful for discovering which tables are available before calling
    :func:`get_fwf_params` with a specific ``table_index``::

        >>> list_fwf_tables("Bestandsbeschrijving_1cyferho_2023_v1.1.txt")
        ['1CijferHO 2023']

    Args:
        txt_file: Path to the DUO bestandsbeschrijving ``.txt`` (or ``.asc``) file.

    Returns:
        List of table titles, in order.  Returns an empty list when the file
        cannot be read or contains no tables.
    """
    text = _read_txt_latin1(txt_file)
    if text is None:
        return []
    tables = _parse_tables(text.split("\n"))
    return [t.get("table_title", f"untitled_table_{i}") for i, t in enumerate(tables)]


def process_json_folder(
    json_input_folder: str = "data/00-metadata/json",
    excel_output_folder: str = "data/00-metadata",
) -> None:
    """
    Processes all JSON files in a folder, converting tables to Excel files.

    Args:
        json_input_folder (str, optional): Folder containing JSON files. Defaults to 'data/00-metadata/json'.
        excel_output_folder (str, optional): Output folder for Excel files. Defaults to 'data/00-metadata'.

    Returns:
        None

    Edge Cases:
        - Removes any existing Excel files in the output folder.
        - Handles missing or empty input folder gracefully.

    Example:
        >>> process_json_folder('data/00-metadata/json', 'data/00-metadata')
    """
    os.makedirs(excel_output_folder, exist_ok=True)

    # Remove any existing Excel files
    for file in os.listdir(excel_output_folder):
        if file.endswith(".xlsx"):
            os.remove(os.path.join(excel_output_folder, file))

    # Setup logging — log folder is a subdirectory of excel_output_folder (the metadata dir)
    log_folder = os.path.join(excel_output_folder, "logs")
    os.makedirs(log_folder, exist_ok=True)

    # Create both a timestamped log and a latest log
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_log_file = os.path.join(
        log_folder, f"xlsx_processing_log_{timestamp}.json"
    )
    latest_log_file = os.path.join(log_folder, "(2)_xlsx_processing_log_latest.json")

    log_data = {
        "timestamp": timestamp,
        "input_folder": json_input_folder,
        "output_folder": excel_output_folder,
        "status": "started",
        "processed_files": [],
        "total_files_processed": 0,
        "total_files_extracted": 0,
        "row_count_mismatches": 0,  # Track files with row count mismatches
    }

    # Find all JSON files in the folder, but ignore the generated variable metadata
    json_files = [
        os.path.join(root, file)
        for root, _, files in os.walk(json_input_folder)
        for file in files
        if file.endswith(".json") and file != "variable_metadata.json"
    ]

    total_json_files = len(json_files)
    if total_json_files == 0:
        log_data["status"] = "completed"
        log_data["message"] = "No JSON files found"
        # Save to both log files
        with open(timestamped_log_file, "w", encoding="utf-8") as f:
            json.dump(log_data, f, indent=2)
        with open(latest_log_file, "w", encoding="utf-8") as f:
            json.dump(log_data, f, indent=2)
        return None

    # Process each JSON file
    total_excel_files = 0
    processed_json_files = 0
    total_row_mismatches = 0

    for json_file in json_files:
        file_name = os.path.basename(json_file)

        # Log file processing
        file_log = {"file": file_name, "status": "processing", "tables": []}

        # Extract tables from JSON file - now also gets detailed results
        table_results, files_created, tables_found = extract_excel_from_json(
            json_file, excel_output_folder
        )

        # Check for row count mismatches in any tables
        file_has_mismatch = False
        for table_result in table_results:
            if "Row count mismatch" in table_result.get("notes", ""):
                file_has_mismatch = True
                total_row_mismatches += 1

            # Add table results to file log
            file_log["tables"].append(table_result)

        # Update file status in log
        file_log["status"] = "success" if files_created > 0 else "no_tables_extracted"
        file_log["tables_found"] = tables_found
        file_log["files_created"] = files_created
        file_log["has_row_mismatch"] = file_has_mismatch

        log_data["processed_files"].append(file_log)

        # Update counters
        total_excel_files += files_created
        if files_created > 0:
            processed_json_files += 1

    # Update final log status
    log_data["status"] = "completed"
    log_data["total_files_processed"] = total_json_files
    log_data["total_files_extracted"] = processed_json_files
    log_data["row_count_mismatches"] = total_row_mismatches

    # Save log file to both locations
    with open(timestamped_log_file, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2)
    with open(latest_log_file, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2)

    # Print summary to console
    _console.print(f"[green]Processed {total_json_files} JSON files")
    _console.print(
        f"[green]Created {total_excel_files} Excel files from {processed_json_files} JSON files"
    )

    if total_row_mismatches > 0:
        _console.print(
            f"[yellow]Warning: {total_row_mismatches} tables had row count mismatches. Check logs for details."
        )
    else:
        _console.print(f"[green]All tables passed row count verification")

    _console.print(
        f"[blue]Log saved to: {os.path.basename(latest_log_file)} and {os.path.basename(timestamped_log_file)} in {log_folder}"
    )

    return None
