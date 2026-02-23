from datetime import datetime

def transform_company(hubspot_company: dict) -> dict:
    props = hubspot_company.get("properties", {})

    raw_created = props.get("createdate")
    formatted_date = None

    if raw_created:
        try:
            dt = datetime.fromisoformat(raw_created.replace("Z", "+00:00"))
            formatted_date = dt.strftime("%Y-%m-%d")
        except Exception:
            formatted_date = None

    return {
        "item_name": props.get("name"),
        "unique_value": hubspot_company.get("id"),
        "columns": {
            "hubspot_id": hubspot_company.get("id"),
            "phone": props.get("phone"),
            "industry": props.get("industry"),
            "company_domain": props.get("domain"),
            "city": props.get("city"),
            "country": props.get("country"),
            "Created_date": formatted_date
        }
    }
