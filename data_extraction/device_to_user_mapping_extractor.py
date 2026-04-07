from datetime import datetime, timezone, timedelta
import json
from data_extraction.graph_data_extractor import GraphDataExtractor
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

USER_TO_DEVICES_JSON_PATH = Path("./data_cache/user_to_devices.json")
USER_TO_DEVICES_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)

CACHE_TTL_HOURS = 48


def _load_cache() -> dict[str, str] | None:
    """Returns cached mapping if it exists and is still fresh, else None."""
    if not USER_TO_DEVICES_JSON_PATH.exists():
        return None

    try:
        with USER_TO_DEVICES_JSON_PATH.open("r") as f:
            cache = json.load(f)

        last_fetched = datetime.fromisoformat(cache["last_fetched"])
        age = datetime.now(timezone.utc) - last_fetched

        if age < timedelta(hours=CACHE_TTL_HOURS):
            logger.info(
                f"[user-device-mapping] Using cached data (age: {age.seconds // 3600}h {(age.seconds % 3600) // 60}m)"
            )
            return cache["mapping"]
        else:
            logger.info("[user-device-mapping] Cache expired, refetching...")
            return None

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning(f"[user-device-mapping] Cache invalid ({e}), refetching...")
        return None


def _save_cache(mapping: dict[str, str]) -> None:
    """Saves mapping with current UTC timestamp."""
    cache = {
        "last_fetched": datetime.now(timezone.utc).isoformat(),
        "mapping": mapping,
    }
    with USER_TO_DEVICES_JSON_PATH.open("w") as f:
        json.dump(cache, f, indent=2)
    logger.info("[user-device-mapping] Cache saved.")


def build_user_device_mapping() -> dict[str, str]:
    # Return cached data if fresh
    cached = _load_cache()
    if cached is not None:
        return cached

    # Otherwise fetch from API
    extractor = GraphDataExtractor()
    url = extractor._build_url(
        "users?$select=id&$top=999&$expand=registeredDevices($select=id;$top=50)"
    )
    mapping: dict[str, str] = {}
    page = 0

    while url:
        page += 1
        logger.info(f"[user-device-mapping] Fetching page {page}")

        from auth import get_graph_token

        headers = {
            "Authorization": f"Bearer {get_graph_token()}",
            "Accept": "application/json",
        }

        data = extractor._fetch_with_retry(url, headers, "user-device-mapping")

        for user in data.get("value", []):
            user_id = user.get("id")
            if not user_id:
                continue
            for device in user.get("registeredDevices", []):
                device_id = device.get("id")
                if device_id:
                    mapping[device_id] = user_id

        url = data.get("@odata.nextLink")

    logger.info(
        f"[user-device-mapping] Complete — {len(mapping)} device→user mappings built"
    )

    # Save fresh data to cache
    _save_cache(mapping)
    return mapping
