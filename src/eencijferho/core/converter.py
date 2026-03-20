"""
Fixed-width to CSV converter for 1CijferHO data files.

Public API:
    process_chunk(chunk_data)
        Converts a chunk of fixed-width lines to semicolon-delimited CSV lines.
    converter(input_file, metadata_file, output_dir)
        Converts a single fixed-width .asc file to CSV using multiprocessing.
    run_conversions_from_matches(input_folder, metadata_folder, match_log_file, output_folder)
        Runs converter for all matched file pairs from a match log.
    convert_dec_files(input_folder, metadata_folder, output_folder)
        Converts all Dec_*.asc files using their corresponding metadata.
"""

import sys
import os
import multiprocessing as mp
import json
import polars as pl
import datetime
from typing import Any
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from eencijferho.config import INPUT_DIR, OUTPUT_DIR, DECODER_INPUT_DIR

_console = Console()


def process_chunk(chunk_data: tuple[list[tuple[int, int]], list[str | bytes]]) -> list[str]:
    """
    Processes a chunk of lines from a fixed-width file and returns the converted output as CSV lines.

    Args:
        chunk_data: (positions, chunk) where positions is a list of (start, end) tuples
            and chunk is a list of lines (str or bytes) to process.

    Returns:
        list[str]: List of semicolon-delimited CSV strings.

    Edge Cases:
        - Handles both str and bytes input lines.
        - Skips empty lines.
        - Strips whitespace from each field.

    Example:
        >>> process_chunk(([(0, 5), (5, 10)], [b'abc  def  ']))
        ['abc;def']
    """
    positions, chunk = chunk_data
    output_lines = []
    for line in chunk:
        if isinstance(line, bytes):
            line = line.decode('latin1')
        if line.strip():
            fields = [line[start:end].strip() for start, end in positions]
            output_lines.append(';'.join(fields))
    return output_lines


# ---------------------------------------------------------------------------
# Private helpers for converter
# ---------------------------------------------------------------------------


def _resolve_output_path(input_file: str, output_dir: str) -> str:
    """Derive the output CSV path from the input file name and output directory."""
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    return os.path.join(output_dir, f"{base_name}.csv")


def _load_metadata(metadata_file: str) -> tuple[list[str], list[tuple[int, int]]]:
    """Load column names and field (start, end) positions from an Excel metadata file."""
    df = pl.read_excel(metadata_file)
    widths = [int(w) for w in df["Aantal posities"].to_list()]
    column_names = df["Naam"].to_list()
    positions = [(sum(widths[:i]), sum(widths[:i + 1])) for i in range(len(widths))]
    return column_names, positions


def _read_lines(input_file: str) -> list[str]:
    """Read all lines from a latin-1 encoded fixed-width file."""
    with open(input_file, 'r', encoding='latin1') as f:
        return f.readlines()


def _write_header(output_file: str, column_names: list[str]) -> None:
    """Write the semicolon-delimited header row, creating or overwriting output_file."""
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        f.write(';'.join(column_names) + '\n')


def _run_parallel(all_lines: list[str], positions: list[tuple[int, int]], output_file: str) -> None:
    """Convert all_lines using a multiprocessing pool, appending results to output_file."""
    num_processes = max(1, mp.cpu_count() - 1)
    chunk_size = max(1, len(all_lines) // (num_processes * 4))
    chunks = [all_lines[i:i + chunk_size] for i in range(0, len(all_lines), chunk_size)]
    chunk_data = [(positions, chunk) for chunk in chunks]
    with mp.Pool(processes=num_processes) as pool:
        with open(output_file, 'a', encoding='utf-8', newline='') as f_out:
            for result in pool.imap_unordered(process_chunk, chunk_data):
                if result:
                    f_out.write('\n'.join(result) + '\n')


def _run_serial(all_lines: list[str], positions: list[tuple[int, int]], output_file: str) -> None:
    """Convert all_lines serially (used in child processes), appending results to output_file."""
    result = process_chunk((positions, all_lines))
    with open(output_file, 'a', encoding='utf-8', newline='') as f_out:
        if result:
            f_out.write('\n'.join(result) + '\n')


def converter(input_file: str, metadata_file: str, output_dir: str | None = None) -> tuple[str, int]:
    """
    Converts a fixed-width ASCII file to CSV using metadata for field positions.

    Args:
        input_file (str): Path to the input .asc file.
        metadata_file (str): Path to the metadata Excel file (.xlsx).
        output_dir (str | None): Output directory. Defaults to OUTPUT_DIR from config.

    Returns:
        tuple[str, int]: (output CSV file path, total lines in input file).

    Edge Cases:
        - Uses multiprocessing in the main process; falls back to serial in child processes.
        - Creates output directory if missing.
        - Input read as latin-1; output written as utf-8.
        - Skips empty lines.

    Example:
        >>> out, n = converter('Dec_landcode.asc', 'Bestandsbeschrijving_Dec-bestanden_DEMO.xlsx')
    """
    if output_dir is None:
        output_dir = OUTPUT_DIR
    output_file = _resolve_output_path(input_file, output_dir)
    os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)

    column_names, positions = _load_metadata(metadata_file)
    _write_header(output_file, column_names)

    all_lines = _read_lines(input_file)
    total_lines = len(all_lines)

    if mp.current_process().name == 'MainProcess':
        _run_parallel(all_lines, positions, output_file)
    else:
        _run_serial(all_lines, positions, output_file)

    return output_file, total_lines


# ---------------------------------------------------------------------------
# Private helpers for run_conversions_from_matches
# ---------------------------------------------------------------------------


def _load_match_log(match_log_file: str) -> list[dict] | None:
    """Load processed_files from a match log JSON. Returns None on failure."""
    if not os.path.exists(match_log_file):
        _console.print(f"[red]Match log niet gevonden: {match_log_file}")
        return None
    try:
        with open(match_log_file, 'r') as f:
            return json.load(f)["processed_files"]
    except Exception as e:
        _console.print(f"[red]Fout bij laden match log: {e}")
        return None


def _convert_one(
    file_info: dict,
    input_folder: str,
    metadata_folder: str,
    output_folder: str | None,
) -> dict:
    """Process one entry from the match log. Returns a file_result dict."""
    input_file_name = file_info["input_file"]
    result: dict[str, Any] = {"input_file": input_file_name, "status": "skipped", "reason": ""}

    if file_info["status"] != "matched":
        result["reason"] = f"Bestandsstatus is {file_info['status']}"
        return result

    valid_matches = [m for m in file_info["matches"] if m["validation_status"] == "success"]
    if not valid_matches:
        result["reason"] = "Geen geldige validatiebestanden gevonden"
        return result

    input_path = os.path.join(input_folder, input_file_name)
    metadata_path = os.path.join(metadata_folder, valid_matches[0]["validation_file"])

    if not os.path.exists(input_path):
        _console.print(f"[red]Invoerbestand niet gevonden: {input_path}")
        result["status"] = "failed"
        result["reason"] = "Invoerbestand niet gevonden"
        return result

    if not os.path.exists(metadata_path):
        _console.print(f"[red]Metadatabestand niet gevonden: {metadata_path}")
        result["status"] = "failed"
        result["reason"] = "Metadatabestand niet gevonden"
        return result

    try:
        output_file, total_lines = converter(input_path, metadata_path, output_folder)
        result["status"] = "success"
        result["output_file"] = output_file
        result["total_lines"] = total_lines
    except Exception as e:
        result["status"] = "failed"
        result["reason"] = f"Fout tijdens omzetting: {e}"

    return result


def run_conversions_from_matches(
    input_folder: str,
    metadata_folder: str = "data/00-metadata",
    match_log_file: str = "data/00-metadata/logs/(4)_file_matching_log_latest.json",
    output_folder: str | None = None,
    skip_prefixes: list[str] | None = None,
) -> dict[str, Any]:
    """
    Runs conversion for all matched input/metadata file pairs based on a match log.

    Args:
        input_folder (str): Folder containing input files.
        metadata_folder (str): Folder with metadata files. Defaults to 'data/00-metadata'.
        match_log_file (str): Path to the match log JSON.
        output_folder (str | None): Output folder for converted CSVs.
        skip_prefixes (list[str] | None): File name prefixes to skip. When None
            all matched files are converted.  Example: ``["EV", "VAKHAVW"]``
            skips main data files and only converts Dec_* lookup files.

    Returns:
        dict[str, Any]: Summary with counts and per-file details.

    Edge Cases:
        - Handles missing or corrupt log files.
        - Logs and skips files that fail conversion.

    Example:
        >>> summary = run_conversions_from_matches('data/01-input')
        >>> print(summary['successful_conversions'])
    """
    _console.print(f"[cyan]Starting conversion based on match log: {match_log_file}")

    log_folder = os.path.dirname(match_log_file)
    os.makedirs(log_folder, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_log_file = os.path.join(log_folder, f"conversion_log_{timestamp}.json")
    latest_log_file = os.path.join(log_folder, "(5)_conversion_log_latest.json")

    processed_files = _load_match_log(match_log_file)
    if processed_files is None:
        return {"status": "failed", "reason": "Log file not found or invalid"}

    results: dict[str, Any] = {
        "timestamp": timestamp,
        "match_log_file": match_log_file,
        "total_files": 0,
        "successful_conversions": 0,
        "failed_conversions": 0,
        "skipped_files": 0,
        "details": [],
        "skipped_file_pairs": [],
    }

    valid_files = [
        f for f in processed_files
        if f["status"] == "matched"
        and any(m["validation_status"] == "success" for m in f["matches"])
    ]
    results["total_files"] = len(valid_files)

    # Use a fresh Console for Progress to avoid "Only one live display may be active
    # at once" when called repeatedly from Streamlit (module-level _console retains
    # live-display state across calls if a previous run exited uncleanly).
    with Progress(
        SpinnerColumn(),
        TextColumn("[cyan]Processing files..."),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=Console(),
    ) as progress:
        task = progress.add_task("", total=len(valid_files))

        for file_info in processed_files:
            if skip_prefixes and any(
                file_info["input_file"].startswith(p) for p in skip_prefixes
            ):
                results["skipped_files"] += 1
                results["skipped_file_pairs"].append({
                    "input_file": file_info["input_file"],
                    "reason": "Overgeslagen op basis van bestandsprefix-filter",
                })
                results["details"].append({
                    "input_file": file_info["input_file"],
                    "status": "skipped",
                    "reason": "Overgeslagen op basis van bestandsprefix-filter",
                })
                progress.update(task, advance=1)
                continue
            file_result = _convert_one(file_info, input_folder, metadata_folder, output_folder)

            if file_result["status"] == "success":
                results["successful_conversions"] += 1
            elif file_result["status"] == "failed":
                results["failed_conversions"] += 1
            else:
                results["skipped_files"] += 1
                results["skipped_file_pairs"].append({
                    "input_file": file_info["input_file"],
                    "reason": file_result["reason"],
                })

            results["details"].append(file_result)
            progress.update(task, advance=1)

    results["status"] = "completed"

    with open(timestamped_log_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    with open(latest_log_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    _console.print(f"[green]Omzetting voltooid")
    _console.print(f"[green]Totaal bestanden: {results['total_files']}")
    _console.print(f"[green]Succesvol omgezet: {results['successful_conversions']}")
    if results["failed_conversions"] > 0:
        _console.print(f"[red]Mislukte omzettingen: {results['failed_conversions']}")
    if results["skipped_files"] > 0:
        _console.print(f"[yellow]Overgeslagen bestanden: {results['skipped_files']}")
        for idx, skipped in enumerate(results["skipped_file_pairs"], 1):
            _console.print(f"[yellow] {idx}. {skipped['input_file']} — {skipped['reason']}[/yellow]")
    _console.print(f"[blue]Log opgeslagen: {os.path.basename(latest_log_file)} en conversion_log_{timestamp}.json in {log_folder}")

    return results


def convert_dec_files(
    input_folder: str,
    metadata_folder: str = "data/00-metadata",
    output_folder: str | None = None,
) -> None:
    """
    Converts all Dec_*.asc files in input_folder using their corresponding metadata.

    Args:
        input_folder (str): Folder containing Dec_*.asc files.
        metadata_folder (str): Folder with metadata files. Defaults to 'data/00-metadata'.
        output_folder (str | None): Output folder for converted CSVs.

    Edge Cases:
        - Skips Dec_* files with no matching metadata.
        - Prefers .xlsx metadata, falls back to .txt.

    Example:
        >>> convert_dec_files('data/01-input')
    """
    dec_files = [f for f in os.listdir(input_folder) if f.startswith("Dec_") and f.endswith(".asc")]
    for dec_file in dec_files:
        base = os.path.splitext(dec_file)[0]
        meta_candidates = [
            m for m in os.listdir(metadata_folder)
            if m.lower().startswith(f"bestandsbeschrijving_{base.lower()}")
            and (m.endswith(".xlsx") or m.endswith(".txt"))
        ]
        if not meta_candidates:
            print(f"[converter] Waarschuwing: geen metadata gevonden voor {dec_file}, overgeslagen.")
            continue
        meta_file = os.path.join(metadata_folder, meta_candidates[0])
        input_path = os.path.join(input_folder, dec_file)
        try:
            converter(input_path, meta_file, output_folder)
            print(f"[converter] Omgezet: {dec_file}")
        except Exception as e:
            print(f"[converter] Waarschuwing: kon {dec_file} niet omzetten: {e}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_folder = sys.argv[1]
    else:
        input_folder = INPUT_DIR

    if len(sys.argv) > 2:
        output_folder = sys.argv[2]
    else:
        output_folder = OUTPUT_DIR

    run_conversions_from_matches(input_folder, output_folder=output_folder)
    convert_dec_files(DECODER_INPUT_DIR, output_folder=output_folder)
