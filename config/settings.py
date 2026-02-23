from dotenv import load_dotenv
import os

load_dotenv(override=True)

def load_settings():
    return {
        "HUBSPOT_ACCESS_TOKEN": os.getenv("HUBSPOT_ACCESS_TOKEN"),
        "Mailchimp_API_TOKEN": os.getenv("Mailchimp_API_TOKEN"),
        "Mailchimp_PAGE_LIMIT": int(os.getenv("Mailchimp_PAGE_LIMIT", 100)),
        "Mailchimp_Audience_ID": os.getenv("Mailchimp_Audience_ID")
    }
