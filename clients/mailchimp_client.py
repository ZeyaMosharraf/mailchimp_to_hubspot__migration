import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config.settings import load_settings
from config.mailchimp_columns import mailchimp_properties


_session = None

def get_mailchimp_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()

        retry_strategy = Retry(
            total=5,  
            backoff_factor=1,  
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        _session.mount("https://", adapter)
        _session.mount("http://", adapter)
    return _session

def fetch_object(list_id: str, fields: list[str], offset: int, limit: int):
    settings = load_settings()
    mailchimp_api_key = settings.get("Mailchimp_API_TOKEN")

    if not mailchimp_api_key:
        raise RuntimeError("Mailchimp_API_TOKEN is missing")

    fields_to_use = fields if fields else mailchimp_properties
    if not fields_to_use:
        raise RuntimeError("Fields to fetch are empty")
        
    limit = min(limit, 1000)
    
    dc = mailchimp_api_key.split("-")[-1]

    url = f"https://{dc}.api.mailchimp.com/3.0/lists/{list_id}/members"

    params = {
        "count": limit,
        "offset": offset,
        "fields": ",".join(fields_to_use)
    }

    session = get_mailchimp_session()

    response = session.get(
        url,
        auth=("anystring", mailchimp_api_key), 
        params=params,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    members = data.get("members", [])

    if len(members) < limit:
        next_offset = None
    else:
        next_offset = offset + limit

    return members, next_offset
