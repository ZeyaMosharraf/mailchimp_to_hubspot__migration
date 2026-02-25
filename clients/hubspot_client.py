import requests
import json
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config.settings import load_settings

_session = None

def get_hubspot_session() -> requests.Session:

    global _session

    if _session is None:
        _session = requests.Session()

        retry_strategy = Retry(
            total=5,  
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PATCH"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)

        _session.mount("https://", adapter)
        _session.mount("http://", adapter)

    return _session

def batch_upsert_items(object_type: str, items: list[dict], id_property: str):

    settings = load_settings()
    token = settings.get("HUBSPOT_ACCESS_TOKEN")

    if not token:
        raise RuntimeError("HUBSPOT_ACCESS_TOKEN is missing")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    session = get_hubspot_session()

    url = f"https://api.hubapi.com/crm/v3/objects/{object_type}/batch/upsert"

    prepared_inputs = []

    for item in items:
        props = item.get("properties", {})
        unique_value = props.get(id_property)

        if not unique_value:
            continue

        prepared_inputs.append({
            "id": unique_value,
            "idProperty": id_property,
            "properties": props
        })

    payload = {
        "inputs": prepared_inputs
    }

    response = session.post(
        url,
        headers=headers,
        json=payload,
        timeout=30
    )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"HubSpot Batch Upsert Error: {e}. Details: {response.text}")
    
    response_json = response.json()
    if response.status_code == 207 and "errors" in response_json:
        print(f"⚠️ WARNING: Partial Batch Failure. {len(response_json['errors'])} items failed.")
        print(f"Error Details: {response_json['errors']}")
    return response_json

