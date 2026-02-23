import requests
from config.settings import load_settings
from config.mailchimp_columns import mailchimp_properties

def fetch_object(list_id: str, fields: list[str], after: str, limit: int):
    
    settings = load_settings()
    mailchimp_api_key = settings.get("Mailchimp_API_TOKEN")

    if not mailchimp_api_key:
        raise RuntimeError("Mailchimp_API_TOKEN is missing")

    if not mailchimp_properties:
        raise RuntimeError("MAILCHIMP_PROPERTIES is empty")
    

    dc = mailchimp_api_key.split("-")[-1]

    url = f"https://{dc}.api.mailchimp.com/3.0/lists/{list_id}/members"

    params = {
        "count": limit,
        "offset": after,
        "fields": ",".join(mailchimp_properties)
    }

    session = requests.Session()

    response = session.get(
        url,
        auth=("anystring", mailchimp_api_key), 
        params=params,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    members = data.get("members", [])

    next_offset = after + limit

    return members, next_offset
