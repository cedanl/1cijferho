#!/usr/bin/env python3
"""
Parse metadata file into structured JSON (moved from dev/parse_metadata_to_json.py).
Provides `parse_metadata_file(path)` for programmatic use.
"""

import json
import os
import re
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
                    # Decide whether this is a true key=value line or a
                    # continuation line that happens to contain an '=' inside
                    # parentheses or a long explanation. Use the position of
                    # '=' in the original raw line as a heuristic.
                    eq_pos = raw.find('=')
                    key = m.group(1).strip()
                    val = m.group(2).strip()

                    long_key_continuation = (eq_pos is not None and eq_pos >= 0 and eq_pos > 40) or (len(key) > 20 and not re.search(r'^[0-9]+$', key))

                    # Special handling for Indicatie geboren, value 99
                    if name == "Indicatie geboren" and key == "99":
                        values[key] = "Onbekend"
                        last_key = None
                    elif long_key_continuation and last_key:
                        # Treat this line as a continuation for the previous key
                        cont = re.sub(r'\s+', ' ', s).strip()
                        values[last_key] = values[last_key].rstrip() + ' ' + cont
                    else:
                        values[key] = val
                        last_key = key
                else:
                    # If previous key exists, treat as continuation
                    if last_key:
                        # Normalize whitespace and append as continuation
                        cont = re.sub(r'\s+', ' ', s).strip()
                        values[last_key] = values[last_key].rstrip() + ' ' + cont
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


if __name__ == '__main__':
    # Provide a simple CLI for backward compatibility
    import argparse
    parser = argparse.ArgumentParser(description='Parse metadata file into JSON format.')
    parser.add_argument('infile', nargs='?', default=os.path.join('data', '01-input', 'Bestandsbeschrijving_1cyferho_2023_v1.1_DEMO.txt'))
    parser.add_argument('-o', '--output', default=os.path.join('data', '02-output', 'variables_with_values.json'))
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    parsed = parse_metadata_file(args.infile)
    with open(args.output, 'w', encoding='utf-8') as w:
        json.dump(parsed, w, ensure_ascii=False, indent=2)
    print(f'Wrote {len(parsed)} variables to {args.output}')
