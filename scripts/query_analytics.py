from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_PATH = PROJECT_ROOT / "data" / "gold"
SQL_FILE = PROJECT_ROOT / "sql" / "analytics.sql"

con = duckdb.connect()

tables = [
    "dim_customer",
    "dim_product",
    "dim_date",
    "fact_sales"
]

print("=" * 65)
print("DUCKDB - GOLD WAREHOUSE SQL ANALYTICS")
print("=" * 65)

# Register Gold Parquet tables as SQL views
for table in tables:
    parquet_path = GOLD_PATH / table / "*.parquet"

    con.execute(
        f"CREATE OR REPLACE VIEW {table} AS "
        f"SELECT * FROM read_parquet('{parquet_path}')"
    )

    count = con.execute(
        f"SELECT COUNT(*) FROM {table}"
    ).fetchone()[0]

    print(f"{table}: {count:,} records loaded")

# Read and execute analytics SQL
sql_content = SQL_FILE.read_text(encoding="utf-8")
queries = [
    query.strip()
    for query in sql_content.split(";")
    if query.strip()
]

print("\n" + "=" * 65)
print(f"EXECUTING {len(queries)} SQL ANALYTICS QUERIES")
print("=" * 65)

for index, query in enumerate(queries, start=1):
    print(f"\n{'=' * 65}")
    print(f"QUERY {index}")
    print("=" * 65)

    try:
        result = con.execute(query).fetchdf()

        print(f"Rows returned: {len(result):,}")

        if not result.empty:
            print(result.head(15).to_string(index=False))

            if len(result) > 15:
                print(f"... and {len(result) - 15:,} more rows")

    except Exception as error:
        print(f"QUERY FAILED: {error}")
        raise

print("\n" + "=" * 65)
print("ALL SQL ANALYTICS QUERIES EXECUTED SUCCESSFULLY")
print("=" * 65)

con.close()
