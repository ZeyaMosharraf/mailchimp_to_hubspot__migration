from datetime import datetime


def _parse_date(raw: str | None) -> str | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def transform_mailchimp(member: dict) -> dict | None:

    email = member.get("email_address")

    if not email:
        return None

    merge_fields = member.get("merge_fields", {}) or {}
    address = merge_fields.get("ADDRESS") or {}

    firstname = merge_fields.get("FNAME")
    lastname = merge_fields.get("LNAME")
    phone = merge_fields.get("PHONE")

    city = address.get("city")
    state = address.get("state")
    zip_code = address.get("zip")

    tags = member.get("tags", []) or []

    tag_list = []
    for tag in tags:
        if isinstance(tag, dict):
            tag_list.append(tag.get("name"))
        else:
            tag_list.append(tag)

    return {
        "properties": {
            "email": email,
            "firstname": firstname,
            "lastname": lastname,
            "phone": phone,
            "city": city,
            "state": state,
            "zip": zip_code,
            "tags": ";".join(tag_list) if tag_list else None
            # "mailchimp_signup_date": _parse_date(member.get("timestamp_signup")),
            # "mailchimp_last_changed": _parse_date(member.get("last_changed")),
        }
    }
