
import csv
from datetime import datetime, timezone
from pathlib import Path

SOURCE_FILE = Path("data/raw/ecommerce_transactions.csv")
BRONZE_FILE = Path("data/bronze/ecommerce_transactions_bronze.csv")
REJECTED_FILE = Path("data/bronze/rejected_records.csv")

REQUIRED_COLUMNS = [
    "order_id",
    "order_date",
    "customer_id",
    "product_id",
    "category",
    "quantity",
    "unit_price",
    "discount_pct",
    "payment_method",
    "region",
    "order_status",
]


def ingest_data():
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Source dataset not found: {SOURCE_FILE}")

    BRONZE_FILE.parent.mkdir(parents=True, exist_ok=True)

    ingestion_time = datetime.now(timezone.utc).isoformat()

    total_records = 0
    valid_records = 0
    rejected_records = 0

    with SOURCE_FILE.open("r", newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)

        if not reader.fieldnames:
            raise ValueError("Source CSV has no header.")

        missing_columns = set(REQUIRED_COLUMNS) - set(reader.fieldnames)

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {sorted(missing_columns)}"
            )

        bronze_columns = REQUIRED_COLUMNS + [
            "ingestion_timestamp",
            "source_file",
        ]

        with (
            BRONZE_FILE.open("w", newline="", encoding="utf-8") as bronze,
            REJECTED_FILE.open("w", newline="", encoding="utf-8") as rejected,
        ):
            bronze_writer = csv.DictWriter(
                bronze, fieldnames=bronze_columns
            )
            rejected_writer = csv.DictWriter(
                rejected,
                fieldnames=REQUIRED_COLUMNS + ["rejection_reason"],
            )

            bronze_writer.writeheader()
            rejected_writer.writeheader()

            for row in reader:
                total_records += 1

                missing_values = [
                    column
                    for column in REQUIRED_COLUMNS
                    if not row.get(column)
                    or not row[column].strip()
                ]

                if missing_values:
                    rejected_writer.writerow({
                        **{
                            column: row.get(column, "")
                            for column in REQUIRED_COLUMNS
                        },
                        "rejection_reason": (
                            "Missing values: " + ", ".join(missing_values)
                        ),
                    })
                    rejected_records += 1
                    continue

                bronze_writer.writerow({
                    **{column: row[column] for column in REQUIRED_COLUMNS},
                    "ingestion_timestamp": ingestion_time,
                    "source_file": SOURCE_FILE.name,
                })

                valid_records += 1

    print("=" * 55)
    print("BRONZE DATA INGESTION COMPLETED")
    print("=" * 55)
    print(f"Source records:       {total_records:,}")
    print(f"Bronze records:       {valid_records:,}")
    print(f"Rejected records:     {rejected_records:,}")
    print(f"Bronze output:        {BRONZE_FILE}")
    print(f"Rejected output:      {REJECTED_FILE}")
    print(f"Ingestion timestamp:  {ingestion_time}")


if __name__ == "__main__":
    ingest_data()
