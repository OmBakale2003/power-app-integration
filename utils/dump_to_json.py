import json
import os
from datetime import datetime


def dump(data: list[dict], endpoint_key: str) -> None:
    """
    Saves extracted data to a local JSON file for debugging.
    Creates a 'debug_dumps' folder and writes the file inside.
    """
    if not data:
        print(f"[{endpoint_key}] No data to dump.")
        return

    # Create directory if it doesn't exist
    dump_dir = "debug_dumps"
    os.makedirs(dump_dir, exist_ok=True)

    # Generate filename with timestamp: e.g., devices_20260309_151730.json
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{endpoint_key}_{timestamp}.json"
    filepath = os.path.join(dump_dir, filename)

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"[{endpoint_key}] Successfully dumped {len(data)} records to {filepath}")
    except Exception as e:
        print(f"[{endpoint_key}] Error writing JSON file: {e}")
