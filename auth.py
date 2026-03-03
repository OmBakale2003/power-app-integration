import msal
import config  
import requests

def get_dataverse_token() -> str:
    app = msal.ConfidentialClientApplication(
        client_id=config.CLIENT_ID,
        client_credential=config.CLIENT_SECRET,
        authority=f"https://login.microsoftonline.com/{config.TENANT_ID}",
    )

    scopes = [f"{config.ENV_URL.rstrip('/')}/.default"]
    result = app.acquire_token_for_client(scopes=scopes)

    access_token = (result or {}).get("access_token")
    if not access_token:
        raise RuntimeError(f"Failed to get token: {result}")
    return access_token

def get_graph_token() -> str:
    app = msal.ConfidentialClientApplication(
        client_id=config.CLIENT_ID,
        client_credential=config.CLIENT_SECRET,
        authority=f"https://login.microsoftonline.com/{config.TENANT_ID}",
    )

    scopes = ["https://graph.microsoft.com/.default"]
    result = app.acquire_token_for_client(scopes=scopes)

    access_token = (result or {}).get("access_token")
    if not access_token:
        raise RuntimeError(f"Failed to get token: {result}")
    return access_token


def whoami():
    token = get_dataverse_token()
    base = config.ENV_URL.rstrip('/')
    url = f"{base}/api/data/v9.2/WhoAmI"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.text[:500]}")
    resp.raise_for_status()
    return resp.json()
