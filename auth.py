import msal
import config
from fastapi.security import APIKeyHeader
from fastapi import HTTPException, Security, status
from config import API_KEY
import secrets


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


# API AUTH
# Tell FastAPI to look for an "X-API-Key" header
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)

assert API_KEY is not None, (
    "The API_KEY for the server is not set, configurate the .env file to include the API_KEY"
)


# The dependency function that checks the key
def get_api_key(api_key: str = Security(api_key_header)):
    # Use secrets.compare_digest to prevent timing attacks
    if not secrets.compare_digest(api_key, API_KEY):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
        )
    return api_key


""" def whoami():kj
    token = get_dataverse_token()
    base = config.ENV_URL.rstrip("/")
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
    return resp.json() """
