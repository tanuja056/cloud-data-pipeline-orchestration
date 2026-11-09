from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_PATH = PROJECT_ROOT / "data" / "gold"

spark = (
    SparkSession.builder
    .appName("GoldDataQualityTests")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("\n" + "=" * 65)
print("GOLD DATA WAREHOUSE - AUTOMATED DATA QUALITY TESTS")
print("=" * 65)

customers = spark.read.parquet(str(GOLD_PATH / "dim_customer"))
products = spark.read.parquet(str(GOLD_PATH / "dim_product"))
dates = spark.read.parquet(str(GOLD_PATH / "dim_date"))
facts = spark.read.parquet(str(GOLD_PATH / "fact_sales"))

results = []

def check(name, passed, details=""):
    status = "PASS" if passed else "FAIL"
    results.append((name, passed))
    print(f"[{status}] {name}")
    if details:
        print(f"       {details}")

# 1. Dimension primary-key uniqueness
check(
    "Customer keys are unique",
    customers.groupBy("customer_key").count()
    .filter(col("count") > 1).count() == 0
)

check(
    "Product keys are unique",
    products.groupBy("product_key").count()
    .filter(col("count") > 1).count() == 0
)

check(
    "Date keys are unique",
    dates.groupBy("date_key").count()
    .filter(col("count") > 1).count() == 0
)

# 2. Required fact keys are not null
fact_keys = ["order_id", "customer_key", "product_key", "date_key"]

null_key_count = facts.filter(
    " OR ".join(f"{key} IS NULL" for key in fact_keys)
).count()

check(
    "Fact table has no null business or dimension keys",
    null_key_count == 0,
    f"Null-key records: {null_key_count}"
)

# 3. Fact order IDs are unique
duplicate_orders = (
    facts.groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

check(
    "Order IDs are unique",
    duplicate_orders == 0,
    f"Duplicate order IDs: {duplicate_orders}"
)

# 4. Revenue and quantity validity
invalid_values = facts.filter(
    (col("quantity") <= 0)
    | (col("unit_price") < 0)
    | (col("gross_revenue") < 0)
    | (col("discount_amount") < 0)
    | (col("net_revenue") < 0)
).count()

check(
    "Quantity and revenue values are valid",
    invalid_values == 0,
    f"Invalid records: {invalid_values}"
)

# 5. Order status validity
valid_statuses = ["completed", "pending", "cancelled"]

invalid_statuses = facts.filter(
    ~lower(col("order_status")).isin(valid_statuses)
).count()

check(
    "Order statuses are valid",
    invalid_statuses == 0,
    f"Invalid status records: {invalid_statuses}"
)

# 6. Fact-to-dimension referential integrity
missing_customers = facts.join(
    customers.select("customer_key"),
    "customer_key",
    "left_anti"
).count()

missing_products = facts.join(
    products.select("product_key"),
    "product_key",
    "left_anti"
).count()

missing_dates = facts.join(
    dates.select("date_key"),
    "date_key",
    "left_anti"
).count()

check(
    "All fact customer keys exist in customer dimension",
    missing_customers == 0,
    f"Missing references: {missing_customers}"
)

check(
    "All fact product keys exist in product dimension",
    missing_products == 0,
    f"Missing references: {missing_products}"
)

check(
    "All fact date keys exist in date dimension",
    missing_dates == 0,
    f"Missing references: {missing_dates}"
)

# Final report
passed = sum(1 for _, result in results if result)
failed = len(results) - passed

print("\n" + "=" * 65)
print(f"TESTS PASSED: {passed}/{len(results)}")
print(f"TESTS FAILED: {failed}/{len(results)}")
print("=" * 65)

spark.stop()

if failed:
    raise SystemExit("GOLD DATA QUALITY TESTS FAILED")

print("ALL AUTOMATED GOLD DATA QUALITY TESTS PASSED!")
