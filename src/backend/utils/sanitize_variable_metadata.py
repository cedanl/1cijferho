import json
import sys
import os

def sanitize_variable_metadata_json(json_path: str) -> None:
    with open(json_path, encoding='utf-8') as f:
        data = json.load(f)
    changed = False
    for item in data:
        values = item.get('values')
        if isinstance(values, dict):
            for k, v in values.items():
                if isinstance(v, str):
                    sanitized = v.replace(',', '').replace(';', '')
                    if sanitized != v:
                        values[k] = sanitized
                        changed = True
    if changed:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Sanitized: {json_path}")
    else:
        print(f"No changes needed: {json_path}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python sanitize_variable_metadata.py <path-to-variable_metadata.json>")
        sys.exit(1)
    sanitize_variable_metadata_json(sys.argv[1])
