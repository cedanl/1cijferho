#!/usr/bin/env python3
"""
Parse metadata file into structured JSON (moved from dev/parse_metadata_to_json.py).
Provides `parse_metadata_file(path)` for programmatic use.
"""

import json
import os
import re
from typing import Any

from eencijferho.io.decorators import with_storage
from eencijferho.config import validate_safe_path


def _next_nonempty_index(lines: list[str], start: int) -> int:
    n = len(lines)
    j = start
    while j < n and lines[j].strip() == "":
        j += 1
    return j

def _is_separator(lines: list[str], idx: int) -> bool:
    if idx >= len(lines):
        return False
    return bool(re.match(r'^-+\s*$', lines[idx].strip()))

def _parse_description_section(lines: list[str], start: int) -> tuple[int, list[str]]:
    """Parse description lines until 'Mogelijke waarden:' or next variable header."""
    desc_lines = []
    i = start
    n = len(lines)

    while i < n:
        s = lines[i].strip()
        if s.lower().startswith('mogelijke waarden:'):
            return i + 1, desc_lines

        k = _next_nonempty_index(lines, i)
        if k < n:
            kk = _next_nonempty_index(lines, k + 1)
            if kk < n and _is_separator(lines, kk):
                break

        desc_lines.append(lines[i].rstrip())
        i += 1

    return i, desc_lines

def _is_long_key_continuation(raw: str, key: str) -> bool:
    eq_pos = raw.find('=')
    return (eq_pos >= 40) or (len(key) > 20 and not re.search(r'^[0-9]+$', key))

def _parse_values_section(lines: list[str], start: int, var_name: str) -> tuple[int, dict, list[str]]:
    """Parse values section (key=value pairs, lists, references)."""
    values = {}
    values_lines = []
    notes_lines = []
    last_key = None
    i = start
    n = len(lines)

    while i < n:
        raw = lines[i]
        s = raw.strip()

        k = _next_nonempty_index(lines, i)
        kk = _next_nonempty_index(lines, k + 1)
        if k < n and kk < n and k == i and _is_separator(lines, kk):
            break

        if i >= n:
            break

        if s == "":
            i += 1
            continue

        if s.startswith('*'):
            notes_lines.append(s)
            i += 1
            continue

        m = re.match(r'^([^=<>`]+?)\s*=\s*(.+?)$', s)
        if m:
            key = m.group(1).strip()
            val = m.group(2).strip()
            is_continuation = _is_long_key_continuation(raw, key)

            if var_name == "Indicatie geboren" and key == "99":
                values[key] = "Onbekend"
                last_key = None
            elif is_continuation and last_key:
                cont = re.sub(r'\s+', ' ', s).strip()
                values[last_key] = values[last_key].rstrip() + ' ' + cont
            else:
                values[key] = val
                last_key = key
        else:
            if last_key:
                cont = re.sub(r'\s+', ' ', s).strip()
                values[last_key] = values[last_key].rstrip() + ' ' + cont
            else:
                values_lines.append(s)

        i += 1

    if not values and values_lines:
        if len(values_lines) == 1 and any(x in values_lines[0] for x in ['Zie bestand', 'Zie']):
            values['reference'] = values_lines[0]
        else:
            values['list'] = values_lines

    return i, values, notes_lines

def _process_variable_block(lines: list[str], i: int, name: str, n: int) -> tuple[int, dict | None]:
    """Process a variable block and return next index and variable dict."""
    i, desc_lines = _parse_description_section(lines, i)
    found_values = i < n and lines[i - 1].strip().lower().startswith('mogelijke waarden:')

    if not found_values:
        return i, None

    i, values, notes_lines = _parse_values_section(lines, i, name)

    desc = ' '.join(ln.strip() for ln in desc_lines if ln.strip())
    if notes_lines:
        desc = desc + ' ' + ' '.join(notes_lines)

    return i, {'name': name, 'description': desc, 'values': values}

def _scan_for_variable_header(lines: list[str], i: int, n: int) -> tuple[int, str | None]:
    """Scan from current line for a variable header. Return next index and name or None."""
    s = lines[i].strip()
    if s == "":
        return i + 1, None

    name_candidate = s
    j = _next_nonempty_index(lines, i + 1)

    if j < n and _is_separator(lines, j):
        return j + 1, name_candidate

    return i + 1, None

@with_storage
def parse_metadata_file(storage, path: str) -> list[dict[str, Any]]:
    """Parse metadata text file for variable descriptions and possible values."""
    text = storage.read_text(path, encoding="latin-1")
    lines = text.split("\n")
    n = len(lines)

    vars_out = []
    seen_names = set()
    i = 0

    while i < n:
        i, name = _scan_for_variable_header(lines, i, n)
        if name is None:
            continue

        i, var_dict = _process_variable_block(lines, i, name, n)
        if var_dict and var_dict['name'] not in seen_names:
            vars_out.append(var_dict)
            seen_names.add(var_dict['name'])

    return vars_out


if __name__ == '__main__':
    # Provide a simple CLI for backward compatibility
    import argparse
    parser = argparse.ArgumentParser(description='Parse metadata file into JSON format.')
    parser.add_argument('infile', nargs='?', default=os.path.join('data', '01-input', 'Bestandsbeschrijving_1cyferho_2023_v1.1_DEMO.txt'))
    parser.add_argument('-o', '--output', default=os.path.join('data', '02-output', 'variables_with_values.json'))
    args = parser.parse_args()

    # Validate paths to prevent traversal attacks
    try:
        validate_safe_path(args.infile, base_dir='.')
        validate_safe_path(args.output, base_dir='.')
    except ValueError as e:
        print(f'Error: {e}', file=__import__('sys').stderr)
        __import__('sys').exit(1)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    parsed = parse_metadata_file(args.infile)
    with open(args.output, 'w', encoding='utf-8') as w:
        json.dump(parsed, w, ensure_ascii=False, indent=2)
    print(f'Wrote {len(parsed)} variables to {args.output}')
