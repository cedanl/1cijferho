#!/usr/bin/env python3
"""
Parse a metadata file into structured JSON.

This script reads a metadata description text file,
extracts variable names, descriptions, and 'Mogelijke waarden' sections,
and outputs them as structured JSON.
Handles key-value pairs, lists, and references.
Optionally, writes a git diff for review.
"""

import argparse
import json
import os
import re
import subprocess
import sys
from typing import List, Dict, Any

def parse_metadata_file(path: str) -> List[Dict[str, Any]]:
    """Parse metadata text file for variable descriptions and possible values."""
    with open(path, encoding="latin1") as f:
        lines = [ln.rstrip("\n") for ln in f]

    n = len(lines)
    i = 0
    vars_out = []

    def next_nonempty_index(start: int) -> int:
        j = start
        while j < n and lines[j].strip() == "":
            j += 1
        return j

    seen_names = set()
    while i < n:
        if lines[i].strip() == "":
            i += 1
            continue

        name_candidate = lines[i].strip()
        j = next_nonempty_index(i + 1)
        # Detect header separator line (dashes)
        if j < n and re.match(r'^-+\s*$', lines[j].strip()):
            name = name_candidate
            i = j + 1

            # Collect description lines until 'Mogelijke waarden:' section
            desc_lines = []
            found_values = False
            while i < n:
                s = lines[i].strip()
                if s.lower().startswith('mogelijke waarden:'):
                    found_values = True
                    i += 1
                    break
                k = next_nonempty_index(i)
                if k < n:
                    maybe_next = lines[k].strip()
                    kk = next_nonempty_index(k + 1)
                    if kk < n and re.match(r'^-+\s*$', lines[kk].strip()):
                        break
                desc_lines.append(lines[i].rstrip())
                i += 1

            if not found_values:
                i += 1
                continue

            # Parse values section: key-value pairs, lists, or references
            values = {}
            values_lines = []
            last_key = None
            notes_lines = []
            while i < n:
                raw = lines[i]
                s = raw.strip()
                # Check for next variable header (non-empty + next non-empty dashes)
                k = next_nonempty_index(i)
                kk = next_nonempty_index(k + 1)
                if k < n and kk < n and k == i and re.match(r'^-+\s*$', lines[kk].strip()):
                    break
                # End values section if we hit end of file
                if i >= n:
                    break
                # If line is empty, treat as possible separator, but do not break
                if s == "":
                    i += 1
                    continue
                # Skip lines starting with '*' (not a value, but a note)
                if s.startswith('*'):
                    notes_lines.append(s)
                    i += 1
                    continue
                m = re.match(r'^([^=<>`]+?)\s*=\s*(.+)$', s)
                if m:
                    key = m.group(1).strip()
                    val = m.group(2).strip()
                    # Special handling for Indicatie geboren, value 99
                    if name == "Indicatie geboren" and key == "99":
                        val = "Onbekend"
                    values[key] = val
                    last_key = key
                else:
                    # If previous key exists, treat as continuation
                    if last_key:
                        values[last_key] = values[last_key] + ' ' + s
                    else:
                        values_lines.append(s)
                i += 1
            # If no key-value pairs, but values_lines exist, store as 'reference' or 'list'
            if not values and values_lines:
                if len(values_lines) == 1 and ('Zie bestand' in values_lines[0] or 'Zie' in values_lines[0]):
                    values['reference'] = values_lines[0]
                else:
                    values['list'] = values_lines
            desc = ' '.join([ln.strip() for ln in desc_lines if ln.strip()])
            if notes_lines:
                desc = desc + ' ' + ' '.join(notes_lines)
            # Only add variable if not already seen
            if name not in seen_names:
                vars_out.append({'name': name, 'description': desc, 'values': values})
                seen_names.add(name)
            continue
        else:
            i += 1
    return vars_out

def run_git_diff(base: str = "main") -> str:
    """Return git diff output between base and HEAD."""
    try:
        diff = subprocess.run(["git", "diff", f"{base}...HEAD"],
                              capture_output=True, text=True, check=False)
        return diff.stdout or diff.stderr
    except Exception as e:
        return f"Error running git diff: {e}"

def main():
    parser = argparse.ArgumentParser(description="Parse metadata file into JSON format.")
    parser.add_argument(
        "infile", nargs="?",
        default=os.path.join("data", "01-input", "Bestandsbeschrijving_1cyferho_2023_v1.1_DEMO.txt"),
        help="Path to metadata input file"
    )
    parser.add_argument(
        "-o", "--output",
        default=os.path.join("data", "02-output", "variables_with_values.json"),
        help="Path to output JSON file"
    )
    parser.add_argument(
        "--no-git", action="store_true",
        help="Skip writing git diff output"
    )
    args = parser.parse_args()

    if not os.path.exists(args.infile):
        print(f"[ERROR] Input file not found: {args.infile}", file=sys.stderr)
        sys.exit(2)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    print(f"Parsing {args.infile} ...")
    parsed = parse_metadata_file(args.infile)

    with open(args.output, "w", encoding="utf-8") as w:
        json.dump(parsed, w, ensure_ascii=False, indent=2)

    print(f"[OK] Wrote {len(parsed)} variables to {args.output}")

    if not args.no_git:
        diff_out = run_git_diff()
        with open("git_diff_main...HEAD.txt", "w", encoding="utf8") as gf:
            gf.write(diff_out)
        print("[OK] Git diff written to git_diff_main...HEAD.txt")

if __name__ == "__main__":
    main()
