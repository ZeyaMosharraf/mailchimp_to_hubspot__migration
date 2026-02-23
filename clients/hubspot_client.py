import requests
import json
import time
from config.settings import load_settings
from config.monday_columns import monday_company_columns

def upsert_item(board_id: str, unique_column_id: str, column_mapping: dict, item_data: dict):
    settings = load_settings()

    token = settings.get("MONDAY_API_TOKEN")
    board_id = settings.get("MONDAY_COMPANY_BOARD_ID")

    if not token:
        raise RuntimeError("MONDAY_API_TOKEN is missing")
    if not board_id:
        raise RuntimeError("MONDAY_COMPANY_BOARD_ID is missing")

    url = "https://api.monday.com/v2"
    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
        "API-Version": "2023-10"
    }

    column_values = {}

    for key, value in item_data["columns"].items():
        if value is None:
            continue

        column_id = column_mapping.get(key)

        if not column_id:
            raise ValueError(f"Column mapping missing for key: {key}")

        else:
            column_values[column_id] = value

    column_values = {k: v for k, v in column_values.items() if v is not None}


    session = requests.session()

    final_response = session.post(url, 
                                   headers=headers, 
                                   json=, 
                                   timeout=30)
    final_response.raise_for_status()
    result = final_response.json()

    if "errors" in result:
        raise RuntimeError(f"Monday Mutation Error: {result['errors']}")

    return result
