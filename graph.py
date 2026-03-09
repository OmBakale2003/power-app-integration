import requests
from auth import get_graph_token
import logging


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def get_req_custom_url(append_url: str, additional_headers: dict | None = None):
    token = get_graph_token()

    url = "https://graph.microsoft.com/v1.0/" + append_url
    headers = {
        "Authorization": f"Bearer {token}",
    }

    if additional_headers is not None:
        for key, val in additional_headers.items():
            headers[key] = val

    logger.info(f"making a get request to url -->{url} \n with headers --> {headers}")

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_users():
    token = get_graph_token()

    url = "https://graph.microsoft.com/v1.0/users"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_devices():
    token = get_graph_token()

    print("printing access token -> ", token)

    url = "https://graph.microsoft.com/v1.0/devices"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_managed_devices():
    token = get_graph_token()

    print("printing access token -> ", token)

    url = "https://graph.microsoft.com/v1.0/devices"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()
