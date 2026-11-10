"""
Collector for the machine-readable decode log ``(7)_decoding_log_latest.json``.

The decoder records what it did into a :class:`DecodeReport` (when one is passed
in); the pipeline writes the result next to the other numbered logs. Recording
is idempotent per file: the dec-only and the enriched pass decode the same file
twice, and a finding is stored under a key, so the second pass overwrites the
first instead of counting it again.

The log holds no row data except a capped ``sample`` of codes from decoded
columns, and never a sample from a personal-identifier column.
"""

from __future__ import annotations

import datetime
import os
import re
from collections.abc import Iterable
from typing import Any

import polars as pl

from eencijferho.config import (
    DUO_BSN_COLUMN,
    DUO_ONDERWIJSNUMMER_COLUMN,
    DUO_PGN_COLUMN,
)

DECODING_LOG_LATEST = "(7)_decoding_log_latest.json"
DECODING_LOG_PREFIX = "decoding_log_"

SAMPLE_SIZE = 5

# Reason codes for a column that was not decoded.
NOT_IN_DECODE_SCOPE = "not_in_decode_scope"
NO_DEC_TABLE = "no_dec_table"
DEC_TABLE_NOT_LOADED = "dec_table_not_loaded"
JOIN_ERROR = "join_error"
NO_LABEL_COLUMNS = "no_label_columns"
NO_VALUES_MAPPED = "no_values_mapped"
MAPPING_ERROR = "mapping_error"

_PERSONAL_ID_PARTS = tuple(
    re.sub(r"[^a-z0-9]", "", name.lower())
    for name in (DUO_BSN_COLUMN, DUO_PGN_COLUMN, DUO_ONDERWIJSNUMMER_COLUMN)
) + ("bsn", "pgn")


def is_personal_id_column(name: str | None) -> bool:
    """True for a column that may hold a BSN, PGN or onderwijsnummer."""
    if not name:
        return False
    flat = re.sub(r"[^a-z0-9]", "", str(name).lower())
    return any(part in flat for part in _PERSONAL_ID_PARTS)


def _sorted_sample(values: Iterable[Any]) -> list[str]:
    """At most SAMPLE_SIZE distinct values, sorted for a stable log."""
    return sorted({"" if v is None else str(v) for v in values})[:SAMPLE_SIZE]


class _FileEntry:
    """Findings for one decoded main file."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.input_columns: list[str] = []
        self.decoded_via: dict[str, list[str]] = {}
        self.reasons: dict[str, str] = {}
        self.unmatched: dict[tuple[str, str], dict[str, Any]] = {}
        self.mappings: dict[tuple[str, str], dict[str, Any]] = {}
        self.not_found: dict[str, set[str]] = {}
        self.errors: dict[tuple[str, str], dict[str, str]] = {}
        self.status: dict[str, str] = {}

    def to_dict(self) -> dict[str, Any]:
        decoded = [c for c in self.input_columns if c in self.decoded_via]
        not_decoded = [
            {"column": c, "reason": self.reasons.get(c, NO_DEC_TABLE)}
            for c in self.input_columns
            if c not in self.decoded_via
        ]
        return {
            "file": self.name,
            **self.status,
            "columns_total": len(self.input_columns),
            "columns_decoded": decoded,
            "columns_not_decoded": not_decoded,
            "decoded_via": {c: self.decoded_via[c] for c in decoded},
            "unmatched_codes": [self.unmatched[k] for k in sorted(self.unmatched)],
            "variable_mappings": [self.mappings[k] for k in sorted(self.mappings)],
            # Variables named in the metadata but absent from this file: usually
            # variables of another file type, so grouped and compact.
            "variables_not_found_count": sum(len(v) for v in self.not_found.values()),
            "variables_not_found": {k: sorted(self.not_found[k]) for k in sorted(self.not_found)},
            "errors": [self.errors[k] for k in sorted(self.errors)],
        }


class DecodeReport:
    """Collects decode findings for one pipeline run.

    The decoder works on normalised column names; :meth:`start_pass` gives the
    report the mapping back to the original names, so the log speaks in the
    column names of the input file.
    """

    def __init__(self) -> None:
        self.decode_enabled = False
        self.enrich_enabled = False
        self.dec_metadata_file: str | None = None
        self.variable_metadata: dict[str, Any] | None = None
        self.dec_tables_total = 0
        self.dec_tables_loaded = 0
        self._not_loaded: dict[str, dict[str, str]] = {}
        self._files: dict[str, _FileEntry] = {}
        self._current: _FileEntry | None = None
        self._names: dict[str, str] = {}
        self.log_path: str | None = None

    # -- file context -------------------------------------------------------

    def begin_file(self, filename: str, columns: list[str] | None = None) -> None:
        """Make ``filename`` the file that following findings belong to.

        Args:
            columns: the input columns of the file, in file order.
        """
        if filename not in self._files:
            self._files[filename] = _FileEntry(filename)
        self._current = self._files[filename]
        self._names = {}
        if columns is not None and not self._current.input_columns:
            self._current.input_columns = list(columns)

    def set_file_status(self, **status: str) -> None:
        """Set status fields of the current file (e.g. ``enriched=...``)."""
        self._file().status.update(status)

    def start_pass(self, norm_map: dict[str, str], orig_columns: list[str]) -> None:
        """Start one decode pass over the current file.

        Args:
            norm_map: normalised name -> original column name.
            orig_columns: the input columns, in file order.
        """
        entry = self._file()
        self._names = dict(norm_map)
        if not entry.input_columns:
            entry.input_columns = list(orig_columns)

    def _file(self) -> _FileEntry:
        if self._current is None:
            self.begin_file("<dataframe>")
            return self._files["<dataframe>"]
        return self._current

    def _name(self, norm: str) -> str:
        return self._names.get(norm, norm)

    # -- Dec tables ---------------------------------------------------------

    def record_dec_tables_loaded(self, total: int, loaded: int) -> None:
        self.dec_tables_total = total
        self.dec_tables_loaded = loaded

    def record_dec_table_not_loaded(self, table_title: str, reason: str) -> None:
        if table_title in self._not_loaded:
            return
        self._not_loaded[table_title] = {
            "table_title": table_title,
            "file": table_title.replace(".asc", ".csv"),
            "reason": reason,
        }

    def record_variable_metadata(self, path: str | None, status: str, mappings: int) -> None:
        self.variable_metadata = {
            "file": os.path.basename(path) if path else None,
            "status": status,
            "mappings_loaded": mappings,
        }

    # -- per-column findings ------------------------------------------------

    def record_skip(self, column_norm: str, reason: str) -> None:
        """Record why a column was not decoded; the first reason wins."""
        self._file().reasons.setdefault(self._name(column_norm), reason)

    def record_decoded(self, column_norm: str, via: str) -> None:
        entry = self._file()
        sources = entry.decoded_via.setdefault(self._name(column_norm), [])
        if via not in sources:
            sources.append(via)

    def record_coverage(self, input_norm: list[str], result_columns: list[str]) -> None:
        """Mark input columns that got at least one ``<col>__<label>`` column."""
        added = [c for c in result_columns if c not in set(input_norm)]
        for col in input_norm:
            prefix = f"{col}__"
            if any(c.startswith(prefix) for c in added):
                self.record_decoded(col, "dec")

    def record_unmatched(
        self,
        key_columns: list[str],
        dec_table: str,
        unmatched: pl.DataFrame,
    ) -> None:
        """Aggregate unmatched codes of one Dec join.

        Args:
            key_columns: data columns of the join key (normalised); the last
                one is the decoded column.
            dec_table: Dec table title.
            unmatched: rows of the data without a match, restricted to
                ``key_columns``.
        """
        # An empty code never matches by design (see decoder._join_key): not a finding.
        unmatched = unmatched.filter(
            pl.col(key_columns[-1]).cast(pl.Utf8).str.strip_chars() != ""
        )
        if unmatched.height == 0:
            return
        column = self._name(key_columns[-1])
        names = [self._name(c) for c in key_columns]
        keys = unmatched.select(key_columns).unique()
        entry: dict[str, Any] = {
            "column": column,
            "dec_table": dec_table,
            "rows_unmatched": unmatched.height,
            "distinct_codes": keys.height,
        }
        if len(key_columns) > 1:
            entry["key_columns"] = names
        if any(is_personal_id_column(n) for n in names):
            entry["sample"] = []
            entry["sample_withheld"] = "personal_identifier"
        else:
            entry["sample"] = _sorted_sample(
                "|".join("" if v is None else str(v) for v in row)
                for row in keys.iter_rows()
            )
        self._file().unmatched[(column, dec_table)] = entry

    def record_mapping(
        self,
        column_norm: str,
        variable: str,
        mapped: int,
        total_non_null: int,
        unmapped: set,
        int_fallback_rows: int,
        int_fallback: set,
    ) -> None:
        column = self._name(column_norm)
        withhold = is_personal_id_column(column)
        entry: dict[str, Any] = {
            "column": column,
            "variable": variable,
            "mapped": mapped,
            "total_non_null": total_non_null,
            "unmapped_distinct": len(unmapped),
            "sample": [] if withhold else _sorted_sample(unmapped),
            "int_fallback_rows": int_fallback_rows,
            "int_fallback_distinct": len(int_fallback),
            "int_fallback_sample": [] if withhold else _sorted_sample(int_fallback),
        }
        if withhold:
            entry["sample_withheld"] = "personal_identifier"
        self._file().mappings[(column, variable)] = entry
        if mapped > 0:
            self.record_decoded(column_norm, "variable_metadata")
        else:
            self.record_skip(column_norm, NO_VALUES_MAPPED)

    def record_not_found(self, variable: str, source: str) -> None:
        """A variable from the metadata (``source``) that is not in the file."""
        self._file().not_found.setdefault(source, set()).add(variable)

    def record_error(self, column_norm: str, step: str, error: Exception) -> None:
        column = self._name(column_norm)
        self._file().errors[(column, step)] = {
            "column": column,
            "step": step,
            "error_type": type(error).__name__,
        }
        self.record_skip(column_norm, MAPPING_ERROR if step == "variable_mapping" else JOIN_ERROR)

    # -- output -------------------------------------------------------------

    def summary(self) -> dict[str, Any]:
        details = [f.to_dict() for f in self._files.values()]
        unmatched = [u for d in details for u in d["unmatched_codes"]]
        mappings = [m for d in details for m in d["variable_mappings"]]
        summary = {
            "files_decoded": len(details),
            "columns_total": sum(d["columns_total"] for d in details),
            "columns_decoded": sum(len(d["columns_decoded"]) for d in details),
            "columns_not_decoded": sum(len(d["columns_not_decoded"]) for d in details),
            "dec_tables_not_loaded": len(self._not_loaded),
            "total_unmatched_distinct_codes": sum(u["distinct_codes"] for u in unmatched),
            "total_unmatched_rows": sum(u["rows_unmatched"] for u in unmatched),
            "total_unmapped_distinct_values": sum(m["unmapped_distinct"] for m in mappings),
            "total_int_fallback_rows": sum(m["int_fallback_rows"] for m in mappings),
            "variables_not_found": sum(d["variables_not_found_count"] for d in details),
            "errors": sum(len(d["errors"]) for d in details),
        }
        summary["strict_violations"] = self._strict_violations(summary)
        return summary

    @staticmethod
    def _strict_violations(summary: dict[str, Any]) -> list[str]:
        """Findings that fail a ``--strict`` run.

        Unmapped variable_metadata values do not count: variable_metadata often
        lists only some values of a column (e.g. "leeg"), so they are reported
        but not enforced.
        """
        violations = []
        if summary["dec_tables_not_loaded"]:
            violations.append(f"{summary['dec_tables_not_loaded']} Dec-tabel(len) niet geladen")
        if summary["total_unmatched_distinct_codes"]:
            violations.append(
                f"{summary['total_unmatched_distinct_codes']} niet-gematchte code(s) in Dec-tabellen"
            )
        if summary["errors"]:
            violations.append(f"{summary['errors']} fout(en) bij decoderen")
        return violations

    def to_dict(self, timestamp: str | None = None) -> dict[str, Any]:
        details = [f.to_dict() for f in self._files.values()]
        return {
            "timestamp": timestamp or datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
            "status": "completed",
            "decode_enabled": self.decode_enabled,
            "enrich_enabled": self.enrich_enabled,
            "dec_metadata_file": self.dec_metadata_file,
            "variable_metadata": self.variable_metadata,
            "dec_tables_total": self.dec_tables_total,
            "dec_tables_loaded": self.dec_tables_loaded,
            "dec_tables_not_loaded": [self._not_loaded[k] for k in sorted(self._not_loaded)],
            "total_files": len(details),
            "details": details,
            "summary": self.summary(),
        }

    def write(self, storage, logs_dir: str) -> str:
        """Write the timestamped and the ``(7)`` latest log; return the latest path."""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        data = self.to_dict(timestamp)
        storage.write_json(data, os.path.join(logs_dir, f"{DECODING_LOG_PREFIX}{timestamp}.json"))
        latest = os.path.join(logs_dir, DECODING_LOG_LATEST)
        storage.write_json(data, latest)
        self.log_path = latest
        return latest
