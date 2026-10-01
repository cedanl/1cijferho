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
import csv
from io import StringIO
import os
import multiprocessing as mp
import datetime
from typing import Any
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from eencijferho.config import INPUT_DIR, OUTPUT_DIR, DECODER_INPUT_DIR
from eencijferho.io.decorators import with_storage

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
    buffer = StringIO()
    writer = csv.writer(buffer, delimiter=";", lineterminator="\n")
    for line in chunk:
        if isinstance(line, bytes):
            line = line.decode('latin1')
        if line.strip():
            fields = [line[start:end].strip() for start, end in positions]
            buffer.seek(0)
            buffer.truncate(0)
            writer.writerow(fields)
            output_lines.append(buffer.getvalue().removesuffix("\n"))
    return output_lines


# ---------------------------------------------------------------------------
# Private helpers for converter
# ---------------------------------------------------------------------------


def _resolve_output_path(input_file: str, output_dir: str) -> str:
    """Derive the output CSV path from the input file name and output directory."""
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    return os.path.join(output_dir, f"{base_name}.csv")


@with_storage
def _load_metadata(storage, metadata_file: str) -> tuple[list[str], list[tuple[int, int]]]:
    """Load column names and field (start, end) positions from an Excel metadata file.

    ``Startpositie`` is the authority for where a field starts. DUO states it next
    to the width, and a field that does not begin where the previous one ended
    means the layout and the .asc file disagree about the format. Deriving the
    positions from the widths instead filled such a gap without a word, which
    shifted every following field and still reported status: success.

    A layout without a ``Startpositie`` column (a future DUO version) falls back to
    the running sum of the widths, with a warning.
    """
    df = storage.read_dataframe(metadata_file, format="excel")
    def layout_numbers(column: str, minimum: int = 1) -> list[int]:
        numbers = []
        for value in df[column].to_list():
            try:
                number = int(value)
                if isinstance(value, bool) or number < minimum or (
                    isinstance(value, float) and value != number
                ):
                    raise ValueError("invalid layout integer")
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(
                    f"Layout {os.path.basename(metadata_file)}: {column} moet gehele "
                    f"getallen >= {minimum} bevatten, gevonden {value!r}."
                ) from exc
            numbers.append(number)
        return numbers

    # Zero-width placeholders are valid; start positions remain one-based.
    widths = layout_numbers("Aantal posities", minimum=0)
    column_names = df["Naam"].to_list()

    if "Startpositie" not in df.columns:
        _console.print(
            f"[yellow]Layout {metadata_file} mist de kolom 'Startpositie'; "
            f"posities afgeleid uit de breedtes.[/]"
        )
        positions = [(sum(widths[:i]), sum(widths[:i + 1])) for i in range(len(widths))]
        return column_names, positions

    starts = layout_numbers("Startpositie")
    positions: list[tuple[int, int]] = []
    expected = 0
    for name, start, width in zip(column_names, starts, widths, strict=True):
        if start - 1 != expected:
            gap = start - 1 - expected
            raise ValueError(
                f"Layout-inconsistent in {os.path.basename(metadata_file)}: veld "
                f"'{name}' begint op Startpositie {start}, terwijl de vorige velden "
                f"tot {expected + 1} lopen. Gat van {gap} positie(s): velden na "
                f"'{name}' zouden {gap} te vroeg worden afgelezen."
            )
        positions.append((expected, expected + width))
        expected += width

    return column_names, positions


@with_storage
def _read_lines(storage, input_file: str) -> list[str]:
    """Read all lines from a latin-1 encoded fixed-width file."""
    text = storage.read_text(input_file, encoding='latin1')
    return text.splitlines(keepends=True)


def _run_parallel(all_lines: list[str], positions: list[tuple[int, int]]) -> list[str]:
    """Convert all_lines using a multiprocessing pool. Returns all CSV lines."""
    num_processes = max(1, mp.cpu_count() - 1)
    chunk_size = max(1, len(all_lines) // (num_processes * 4))
    chunks = [all_lines[i:i + chunk_size] for i in range(0, len(all_lines), chunk_size)]
    chunk_data = [(positions, chunk) for chunk in chunks]
    output_lines = []
    with mp.Pool(processes=num_processes) as pool:
        for result in pool.imap(process_chunk, chunk_data):
            if result:
                output_lines.extend(result)
    return output_lines


def _run_serial(all_lines: list[str], positions: list[tuple[int, int]]) -> list[str]:
    """Convert all_lines serially (used in child processes). Returns all CSV lines."""
    return process_chunk((positions, all_lines))


@with_storage
def converter(storage, input_file: str, metadata_file: str, output_dir: str | None = None) -> tuple[str, int]:
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
        - Input read as latin-1; output written as utf-8.
        - Skips empty lines.

    Example:
        >>> out, n = converter('Dec_landcode.asc', 'Bestandsbeschrijving_Dec-bestanden_DEMO.xlsx')
    """
    if output_dir is None:
        output_dir = OUTPUT_DIR
    output_file = _resolve_output_path(input_file, output_dir)

    column_names, positions = _load_metadata(metadata_file)
    all_lines = _read_lines(input_file)
    total_lines = len(all_lines)

    if mp.current_process().name == 'MainProcess':
        csv_lines = _run_parallel(all_lines, positions)
    else:
        csv_lines = _run_serial(all_lines, positions)

    # Build full CSV content: header + data lines
    header_buffer = StringIO()
    csv.writer(header_buffer, delimiter=";", lineterminator="\n").writerow(column_names)
    header = header_buffer.getvalue().removesuffix("\n")
    content = header + '\n' + '\n'.join(csv_lines) + '\n' if csv_lines else header + '\n'
    storage.write_text(content, output_file)

    return output_file, total_lines


# ---------------------------------------------------------------------------
# Private helpers for run_conversions_from_matches
# ---------------------------------------------------------------------------


@with_storage
def _load_match_log(storage, match_log_file: str) -> list[dict] | None:
    """Load processed_files from a match log JSON. Returns None on failure."""
    if not storage.exists(match_log_file):
        _console.print(f"[red]Match log niet gevonden: {match_log_file}")
        return None
    try:
        data = storage.read_json(match_log_file)
        return data["processed_files"]
    except Exception as e:
        _console.print(f"[red]Fout bij laden match log: {e}")
        return None


@with_storage
def _convert_one(
    storage,
    file_info: dict,
    input_folder: str,
    metadata_folder: str,
    output_folder: str | None,
) -> dict:
    """Process one entry from the match log. Returns a file_result dict."""
    input_file_name = file_info["input_file"]
    result: dict[str, Any] = {"input_file": input_file_name, "status": "skipped", "reason": ""}

    if file_info["status"] == "year_mismatch":
        # Er is een 1cyferho-bestandsbeschrijving, maar niet voor dit jaar. Converteren
        # zou het bestand met de layout van een ander jaar lezen, dus dit is een fout
        # die aandacht nodig heeft en geen bestand dat we stilletjes overslaan.
        available = sorted(
            {m["validation_year"] for m in file_info["matches"] if m.get("validation_year")}
        )
        years = ", ".join(str(year) for year in available) if available else "geen enkel"
        result["status"] = "failed"
        result["reason"] = (
            f"Jaarmismatch: {input_file_name} is uit {file_info.get('year')}, "
            f"maar alleen bestandsbeschrijving(en) uit {years} zijn aanwezig"
        )
        return result

    if file_info["status"] != "matched":
        result["reason"] = f"Bestandsstatus is {file_info['status']}"
        return result

    valid_matches = [m for m in file_info["matches"] if m["validation_status"] == "success"]
    if not valid_matches:
        result["reason"] = "Geen geldige validatiebestanden gevonden"
        return result

    valid_matches.sort(key=lambda match: match["validation_file"])
    if len(valid_matches) > 1:
        # The extractor also copies Dec_vakcode from Vakken into Dec-bestanden.
        # Only identical Dec definitions are interchangeable. EV/VAK layouts
        # remain ambiguous even if their positions happen to be the same.
        equivalent = False
        if input_file_name.startswith("Dec_"):
            try:
                layouts = [
                    _load_metadata(os.path.join(metadata_folder, match["validation_file"]))
                    for match in valid_matches
                ]
                equivalent = all(layout == layouts[0] for layout in layouts[1:])
            except Exception as exc:
                result["status"] = "failed"
                result["reason"] = f"Kon dubbele Dec-layouts niet vergelijken: {exc}"
                return result
        if not equivalent:
            names = ", ".join(match["validation_file"] for match in valid_matches)
            result["status"] = "failed"
            result["reason"] = (
                f"Meerdere geldige bestandsbeschrijvingen voor {input_file_name}: {names}. "
                "Verwijder of hernoem het overbodige layoutbestand; er wordt niet stilzwijgend een keuze gemaakt."
            )
            return result
        result["equivalent_layouts"] = [match["validation_file"] for match in valid_matches]

    result["validation_file"] = valid_matches[0]["validation_file"]
    input_path = os.path.join(input_folder, input_file_name)
    metadata_path = os.path.join(metadata_folder, result["validation_file"])

    if not storage.exists(input_path):
        _console.print(f"[red]Invoerbestand niet gevonden: {input_path}")
        result["status"] = "failed"
        result["reason"] = "Invoerbestand niet gevonden"
        return result

    if not storage.exists(metadata_path):
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


@with_storage
def run_conversions_from_matches(
    storage,
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

    storage.write_json(results, timestamped_log_file)
    storage.write_json(results, latest_log_file)

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


@with_storage
def convert_dec_files(
    storage,
    input_folder: str,
    metadata_folder: str = "data/00-metadata",
    output_folder: str | None = None,
    *,
    strict: bool = False,
) -> list[str]:
    """
    Converts all Dec_*.asc files in input_folder using their corresponding metadata.

    Args:
        input_folder (str): Folder containing Dec_*.asc files.
        metadata_folder (str): Folder with metadata files. Defaults to 'data/00-metadata'.
        output_folder (str | None): Output folder for converted CSVs.
        strict: Raise on failed/ambiguous conversions instead of only warning.

    Returns:
        Paths of Dec CSVs produced in this call (not stale output files).

    Edge Cases:
        - Skips Dec_* files with no matching metadata.
        - Prefers .xlsx metadata, falls back to .txt.

    Example:
        >>> convert_dec_files('data/01-input')
    """
    converted_files = []
    all_input = storage.list_files(f"{input_folder}/*")
    dec_files = [
        f for f in all_input
        if os.path.basename(f).startswith("Dec_") and f.endswith(".asc")
    ]
    all_meta = storage.list_files(f"{metadata_folder}/*")
    for dec_filepath in dec_files:
        dec_file = os.path.basename(dec_filepath)
        base = os.path.splitext(dec_file)[0]
        meta_candidates = [
            m for m in all_meta
            if os.path.basename(m).lower().startswith(f"bestandsbeschrijving_{base.lower()}")
            and (m.endswith(".xlsx") or m.endswith(".txt"))
        ]
        if not meta_candidates:
            print(f"[converter] Waarschuwing: geen metadata gevonden voor {dec_file}, overgeslagen.")
            continue
        file_info = {
            "input_file": dec_file, "status": "matched",
            "matches": [
                {"validation_file": os.path.basename(path), "validation_status": "success"}
                for path in meta_candidates
            ],
        }
        result = _convert_one(file_info, input_folder, metadata_folder, output_folder)
        if result["status"] == "success":
            converted_files.append(result["output_file"])
            print(f"[converter] Omgezet: {dec_file}")
        elif strict:
            raise ValueError(f"Kon {dec_file} niet omzetten: {result['reason']}")
        else:
            print(f"[converter] Waarschuwing: kon {dec_file} niet omzetten: {result['reason']}")
    return converted_files


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
