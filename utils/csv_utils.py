import csv
from datetime import datetime
from pathlib import Path


def flatten_row(row: dict, parent_key: str = "", sep: str = "_") -> dict:
    items = {}

    for key, value in row.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, dict):
            items.update(flatten_row(value, new_key, sep=sep))

        elif isinstance(value, list):
            # Convert list values to pipe-separated string
            if all(isinstance(v, dict) for v in value):
                # If list of dicts → extract keys or stringify
                items[new_key] = "|".join(str(v.get("key", v)) for v in value)
            else:
                items[new_key] = "|".join(str(v) for v in value)

        else:
            items[new_key] = value

    return items


def json_to_csv(rows, filename: str) -> str:
    if not rows:
        print("No data found in Graph response")
        return ""

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = Path("test_scripts") / "graph_test_output"
    out_dir.mkdir(parents=True, exist_ok=True)
    file_path = out_dir / f"{filename}_{timestamp}.csv"

    # Flatten rows
    flattened_rows = [flatten_row(row) for row in rows]

    # Collect all keys
    all_keys = set()
    for row in flattened_rows:
        all_keys.update(row.keys())

    fieldnames = sorted(all_keys)

    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flattened_rows)

    print(f"CSV created successfully: {file_path}")
    return str(file_path)
