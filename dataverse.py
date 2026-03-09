# dataverse.py
import requests
from auth import get_dataverse_token
import config
import base64
import json
import math
import uuid
from pathlib import Path

BLOCK_SIZE = 5 * 1024 * 1024


def dataverse_headers(access_token: str):
    return {
        "Authorization": f"Bearer {access_token}",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def list_accounts(top: int = 10):
    token = get_dataverse_token()
    url = f"{config.ENV_URL}/api/data/v9.2/accounts"
    params = {"$select": "name,accountnumber", "$top": str(top)}
    headers = dataverse_headers(token)
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json().get("value", [])


""" def list_test_1(top: int = 5):
    #Fetch rows from the cr277_test_1 Dataverse table
    token = get_dataverse_token()
    base = config.ENV_URL.rstrip('/')

    # Use the EXACT EntitySetName from metadata
    entity_set = "cr277_test_1s"
    url = f"{base}/api/data/v9.2/{entity_set}"

    params = {
        "$top": top,
        "$select": "cr277_source_timestamp,cr277_json_dump"
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
    }

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()

    return resp.json().get("value", [])
 """


def create_dataverse_row(
    access_token: str,
    environment_url: str,
    entity_set_name: str,
    table_logical_name: str,
    row_data: dict,
) -> str:
    """
    Creates a Dataverse row and returns the row GUID.
    """

    url = f"{environment_url}/api/data/v9.2/{entity_set_name}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    response = requests.post(url, headers=headers, json=row_data)
    response.raise_for_status()

    return response.json()[f"{table_logical_name}id"]


# csv upload using simple patch http request
""" def upload_csv_file(entity_uri: str, csv_path: str,file_column: str):
    
    #Upload CSV to Dataverse using attachment navigation property.
    #Works even when File APIs / Attachments toggle are unavailable.
    
    token = get_dataverse_token()

    upload_url = f"{entity_uri}/{file_column}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
    }

    with open(csv_path, "rb") as f:
        resp = requests.patch(upload_url, headers=headers, data=f)

    resp.raise_for_status()
    print(" CSV uploaded successfully")
 """


# csv upload funtion using InitializeFileBlocksUpload action
def upload_file_to_dataverse(
    access_token: str,
    environment_url: str,  # https://org.crm.dynamics.com
    entity_logical_name: str,  # account
    primary_key_name: str,  # accountid
    entity_id: str,  # GUID
    file_column_name: str,  # sample_filecolumn (lowercase)
    file_path: Path,
    mime_type: str = "application/octet-stream",
    max_size_kb: int | None = None,
) -> str:
    """
    Uploads a file to a Dataverse file column using block upload APIs.
    Returns fileId (GUID).
    """

    headers = dataverse_headers(access_token)
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(file_path)

    file_size = file_path.stat().st_size
    if max_size_kb and (file_size / 1024) > max_size_kb:
        raise Exception("File too large for this Dataverse file column")

    # --------------------------------------------------
    # 1. Initialize upload
    # --------------------------------------------------
    init_url = f"{environment_url}/api/data/v9.2/InitializeFileBlocksUpload"

    init_payload = {
        "Target": {
            "@odata.type": f"Microsoft.Dynamics.CRM.{entity_logical_name}",
            primary_key_name: entity_id,
        },
        "FileAttributeName": file_column_name,
        "FileName": file_path.name,
    }

    init_resp = requests.post(init_url, headers=headers, json=init_payload)
    init_resp.raise_for_status()

    file_continuation_token = init_resp.json()["FileContinuationToken"]

    # --------------------------------------------------
    # 2. Upload blocks
    # --------------------------------------------------
    block_ids: list[str] = []

    upload_block_url = f"{environment_url}/api/data/v9.2/UploadBlock"

    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(BLOCK_SIZE)
            if not chunk:
                break

            block_id = base64.b64encode(str(uuid.uuid4()).encode("utf-8")).decode(
                "utf-8"
            )

            block_ids.append(block_id)

            upload_payload = {
                "BlockId": block_id,
                "BlockData": base64.b64encode(chunk).decode("utf-8"),
                "FileContinuationToken": file_continuation_token,
            }

            resp = requests.post(upload_block_url, headers=headers, json=upload_payload)
            resp.raise_for_status()

    # --------------------------------------------------
    # 3. Commit upload
    # --------------------------------------------------
    commit_url = f"{environment_url}/api/data/v9.2/CommitFileBlocksUpload"

    commit_payload = {
        "FileName": file_path.name,
        "MimeType": mime_type,
        "BlockList": block_ids,
        "FileContinuationToken": file_continuation_token,
    }

    commit_resp = requests.post(commit_url, headers=headers, json=commit_payload)
    commit_resp.raise_for_status()

    return commit_resp.json()["FileId"]


def download_csv_file(entity_uri: str, file_column: str, output_path: str):
    token = get_dataverse_token()

    download_url = f"{entity_uri}/{file_column}/$value"

    headers = {
        "Authorization": f"Bearer {token}",
    }

    resp = requests.get(download_url, headers=headers)
    resp.raise_for_status()

    with open(output_path, "wb") as f:
        f.write(resp.content)

    print("CSV downloaded successfully")
