from config.settings import load_settings
from clients.mailchimp_client import fetch_object
from transformers.contact_mapping import transform_contact_mailchimp
from clients.hubspot_client import batch_upsert_items
from state.checkpoint import load_checkpoint, save_checkpoint
from config.mailchimp_columns import mailchimp_properties
import time


def run_migration():
    settings = load_settings()

    offset = load_checkpoint() or 0

    total_processed = 0

    migration_start = time.time()

    print(f"Starting migration from offset: {offset}")

    while True:
        mailchimp_records, next_offset = fetch_object(
            list_id=settings["Mailchimp_Audience_ID"],
            fields=mailchimp_properties,
            offset=offset,
            limit=settings["Mailchimp_PAGE_LIMIT"]
        )

        if not mailchimp_records:
            print("No more contacts to process.")
            break

        transformed = [
            transform_contact_mailchimp(record)
            for record in mailchimp_records
        ]

        batch_upsert_items(
            object_type="contacts",
            items=transformed,
            id_property="email"
        )

        total_processed += len(transformed)

        save_checkpoint(next_offset)

        print(f"Processed batch of {len(transformed)}. Next offset: {next_offset}")

        if not next_offset:
            print("Reached end of Mailchimp data.")
            break

        offset = next_offset

    migration_end = time.time()

    print("\n===== MIGRATION SUMMARY =====")
    print(f"Total processed: {total_processed}")
    print(f"Total time: {migration_end - migration_start:.2f} seconds")
    print(f"Records per second: {total_processed / (migration_end - migration_start):.2f}")
    print("Migration completed successfully.")


if __name__ == "__main__":
    run_migration()
