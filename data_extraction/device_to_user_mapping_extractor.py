from data_extraction.graph_data_extractor import GraphDataExtractor
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def build_user_device_mapping() -> dict[str, str]:
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
    return mapping
