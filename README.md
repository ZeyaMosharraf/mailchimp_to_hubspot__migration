# Mailchimp → HubSpot Migration
![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python&logoColor=white)
![Status](https://img.shields.io/badge/Status-Production--Ready-brightgreen)
![APIs](https://img.shields.io/badge/APIs-Mailchimp%20%7C%20HubSpot-orange)


---

## Executive Summary

| | |
|---|---|
| **Business Problem** | Client needed to fully shut down Mailchimp and move all contact data into HubSpot CRM without manual effort or data loss |
| **Solution** | Automated Python pipeline that extracts contacts from Mailchimp via paginated API calls, transforms field mappings, and batch-upserts directly into HubSpot |
| **Throughput** | ~16.95 records/second |
| **Key Impact** | Full contact base migrated with tags, segmentation, and field mappings preserved — zero manual exports required |

**Next Steps:**
1. Extend transformer to support custom properties and additional Mailchimp merge fields per client
2. Add a validation report post-migration to diff Mailchimp source vs HubSpot destination counts
3. Package as a reusable CLI tool configurable via a single `config.yaml` for new client onboarding

---

## Business Problem

The client decided to fully decommission Mailchimp and consolidate their marketing operations into HubSpot as their single platform. A native Mailchimp → HubSpot migration path does not exist — HubSpot provides no built-in connector to import directly from Mailchimp with field mapping, tag preservation, or batch control.

Without this tool:
- Contact data, tags, and segmentation would have been lost or required slow, error-prone manual CSV exports
- There was no way to resume a failed migration mid-way without reprocessing from the beginning
- Scale would have been a blocker — manual imports have no retry logic or rate-limit handling

**Type:** One-time migration  
**Scope:** Part of a full platform switch from Mailchimp to HubSpot

---

## Solution & Methodology

A modular Python pipeline built across clearly separated layers — ingestion, transformation, and loading — with production-grade reliability features.

**APIs Used:**
- Mailchimp Marketing REST API — contact extraction with offset-based pagination
- HubSpot Contacts Batch REST API — bulk upsert (create or update) by email

**Authentication:**
- Mailchimp: API Key via `.env`
- HubSpot: Private App Access Token via `.env`

**Key Engineering Features:**

| Feature | Implementation |
|---|---|
| Pagination | Offset-based page fetching from Mailchimp to handle any audience size |
| Checkpoint / Resume | Offset saved to `state/checkpoint.json` after every batch — safe to stop and restart |
| Retry Logic | `urllib3` `HTTPAdapter` with automatic retries on transient failures for both APIs |
| Field Transformation | Dedicated transformer layer maps Mailchimp merge fields to HubSpot property schema |
| Batch Processing | 100 contacts per HubSpot API call — maximises throughput within API rate limits |
| Structured Logging | Centralised logger across all modules for clean, traceable run output |

---

## Project Structure

```
mailchimp_to_hubspot_migration/
├── main.py                        # Entry point — orchestrates the full migration
├── requirements.txt               # Python dependencies
├── .env                           # API credentials (excluded from version control)
│
├── clients/
│   ├── mailchimp_client.py        # Paginated contact extraction from Mailchimp
│   └── hubspot_client.py          # Batch upsert of contacts into HubSpot
│
├── transformers/
│   └── contact_mapping.py         # Maps Mailchimp fields → HubSpot property schema
│
├── config/
│   ├── settings.py                # Environment variable loader
│   ├── mailchimp_columns.py       # Mailchimp fields to extract
│   └── hubspot_columns.py         # HubSpot target property names
│
├── state/
│   ├── checkpoint.py              # Read/write migration offset to disk
│   └── checkpoint.json            # Auto-generated; tracks last processed offset
│
├── tests/
│   ├── test_mailchimp_client.py   # Unit tests — Mailchimp extraction
│   ├── test_hubspot_client.py     # Unit tests — HubSpot batch upsert
│   └── test_main.py               # Integration tests — full migration flow
│
└── utils/
    └── logger.py                  # Shared logger instance
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Create `.env` file

```env
HUBSPOT_ACCESS_TOKEN=your_hubspot_private_app_token
Mailchimp_API_TOKEN=your_mailchimp_api_key
Mailchimp_Audience_ID=your_mailchimp_list_id
Mailchimp_PAGE_LIMIT=100
```

> **Where to find these:**
> - HubSpot token: Settings → Integrations → Private Apps
> - Mailchimp API key: Account → Extras → API Keys
> - Mailchimp Audience ID: Audience → Settings → Audience name and defaults

---

## Running the Migration

```bash
python main.py
```

**Example output:**
```
Starting migration from offset: 0
Processed batch of 100. Next offset: 100
Processed batch of 100. Next offset: 200
...
===== MIGRATION SUMMARY =====
Total processed: <n>
Total time: <t> seconds
Records per second: ~16.95
Migration completed successfully.
```

> Throughput scales linearly. Each batch of 100 contacts completes in approximately 5–6 seconds depending on network latency. No hardcoded record limits — the pipeline runs until the full audience is exhausted.

---

## Resume After Failure

Progress is saved to `state/checkpoint.json` after every batch. If the migration is interrupted for any reason, simply re-run:

```bash
python main.py
```

It resumes from the last saved offset automatically — no duplicate processing.

**To restart from scratch:**
```bash
# macOS / Linux
rm state/checkpoint.json

# Windows
del state\checkpoint.json
```

---

## Field Mapping

| Mailchimp Field | HubSpot Property | Notes |
|---|---|---|
| `email_address` | `email` | Primary key for upsert |
| `merge_fields.FNAME` | `firstname` | |
| `merge_fields.LNAME` | `lastname` | |
| `merge_fields.PHONE` | `phone` | |
| `merge_fields.ADDRESS.city` | `city` | |
| `merge_fields.ADDRESS.state` | `state` | |
| `merge_fields.ADDRESS.zip` | `zip` | |
| `tags[].name` | `tags` | Joined as semicolon-separated string |

> To extend mappings for a new client, update `config/mailchimp_columns.py`, `config/hubspot_columns.py`, and `transformers/contact_mapping.py`.

---

## Testing

```bash
# Run all tests
python -m pytest tests/

# Run a specific module
python -m pytest tests/test_mailchimp_client.py
```

---

## Seeding Test Data (Development Only)

```bash
python scripts/seed_mailchimp.py
```

> ⚠️ Adjust `TOTAL_RECORDS` in the script to stay within your Mailchimp plan's contact limits.

---

## Results & Business Impact

| Metric | Result |
|---|---|
| **Throughput** | ~16.95 records/second |
| **Batch efficiency** | 100 contacts per API call |
| **Failure recovery** | Automatic resume from last checkpoint — no data reprocessing |
| **Data integrity** | Tags, field mappings, and segmentation data fully preserved |
| **Manual effort eliminated** | Zero CSV exports or manual field mapping required |

**Who benefits:**
- **Client** — complete, clean contact base available in HubSpot from day one, with no data loss
- **Implementation team** — reusable pipeline template adaptable for future Mailchimp → HubSpot migrations with minimal reconfiguration

---

## Future Reusability

This codebase is structured as a reusable migration template. Potential extensions for future client engagements:

1. **Config-driven onboarding** — Replace hardcoded column files with a single `config.yaml` so a new client migration requires only a config change, not code changes
2. **Extended field support** — Add support for custom Mailchimp merge fields and HubSpot custom properties to cover non-standard client schemas
3. **Post-migration validation report** — Auto-generate a diff report comparing Mailchimp source count vs HubSpot destination count per batch to confirm data integrity
4. **Multi-audience support** — Loop over multiple Mailchimp audience IDs in one run for clients with segmented lists
5. **Other CRM targets** — Swap the HubSpot client layer for a Salesforce or ActiveCampaign client to reuse the same extraction and transformation logic

---

## Security & Version Control

The following are excluded from version control via `.gitignore`:

| Path | Reason |
|---|---|
| `.env` | Contains API credentials — never commit |
| `tests/` | Internal test suite — not part of the production deployment |
| `state/checkpoint.json` | Auto-generated at runtime — environment specific |
| `__pycache__/` | Python bytecode — auto-generated |

> All API credentials are loaded via `python-dotenv` at runtime only and never hardcoded in source files.

---

## Skills & Technologies

| Category | Tools / Concepts |
|---|---|
| **Language** | Python 3.8+ |
| **APIs** | Mailchimp Marketing REST API, HubSpot Contacts Batch REST API |
| **Authentication** | API Key, Private App Bearer Token |
| **Engineering Patterns** | Pagination, Checkpoint/Resume, Retry Logic, Batch Processing, ETL Pipeline |
| **Libraries** | `requests`, `urllib3`, `python-dotenv` |
| **Testing** | `pytest` — unit + integration |
| **Dev Tools** | `.env` config, `.gitignore`, structured logging |

---

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| `requests` | 2.32.5 | HTTP calls to Mailchimp & HubSpot APIs |
| `urllib3` | 2.6.3 | Retry logic via `HTTPAdapter` |
| `python-dotenv` | 1.2.1 | Load API credentials from `.env` |