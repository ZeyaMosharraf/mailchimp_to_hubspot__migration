from config.settings import load_settings
from clients.hubspot_client import fetch_object
from transformers.company_mapper import transform_company
from clients.monday_client import upsert_item
from state.checkpoint import load_checkpoint, save_checkpoint
from config.monday_columns import monday_company_columns
from config.hubspot_columns import hubspot_properties
import time
from concurrent.futures import ThreadPoolExecutor

def process_record(hubspot_record, settings):
    mapped = transform_company(hubspot_record)

    upsert_item(
        board_id=settings["MONDAY_COMPANY_BOARD_ID"],
        unique_column_id=monday_company_columns["hubspot_id"],
        column_mapping=monday_company_columns,
        item_data=mapped
    )

def run_migration():
    settings = load_settings()

    after = load_checkpoint() or None

    total_processed = 0

    migration_start = time.time()

    print(f"Starting migration from cursor: {after}")

    while True:
        hubspot_records, next_after = fetch_object(
            object_type="companies",
            properties=hubspot_properties,
            after=after,
            limit=settings["HUBSPOT_PAGE_LIMIT"]
        )

        if not hubspot_records:
            print("No more companies to process.")
            break

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = executor.map(
                lambda record: process_record(record, settings),
                hubspot_records
            )
            list(futures)

        total_processed += len(hubspot_records)

        save_checkpoint(next_after)

        print(f"Processed batch of {len(hubspot_records)}. Next cursor: {next_after}")

        if not next_after:
            print("Reached end of HubSpot data.")
            break

        after = next_after

    migration_end = time.time()

    print("\n===== MIGRATION SUMMARY =====")
    print(f"Total processed: {total_processed}")
    print(f"Total time: {migration_end - migration_start:.2f} seconds")
    print(f"Records per second: {total_processed / (migration_end - migration_start):.2f}")
    print("Migration completed successfully.")

if __name__ == "__main__":
    run_migration()
