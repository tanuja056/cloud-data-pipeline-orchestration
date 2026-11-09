
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    round,
    sum as spark_sum,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    TimestampType,
)

BRONZE_PATH = "data/bronze/ecommerce_transactions_bronze.csv"
SILVER_PATH = "data/silver/ecommerce_transactions"

def run_silver_transformation():

    spark = (
        SparkSession.builder
        .appName("EcommerceSilverTransformation")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    print("=" * 60)
    print("STARTING SILVER ETL TRANSFORMATION")
    print("=" * 60)

    # 1. Read Bronze data
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(BRONZE_PATH)
    )

    bronze_count = df.count()
    print(f"Bronze records: {bronze_count:,}")

    # 2. Apply correct data types
    df = (
        df
        .withColumn("order_date", col("order_date").cast(TimestampType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("unit_price", col("unit_price").cast(DoubleType()))
        .withColumn("discount_pct", col("discount_pct").cast(DoubleType()))
    )

    # 3. Remove duplicate order IDs
    df = df.dropDuplicates(["order_id"])

    # 4. Filter invalid records
    df = df.filter(
        col("order_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("product_id").isNotNull()
        & col("order_date").isNotNull()
        & col("quantity").isNotNull()
        & (col("quantity") > 0)
        & col("unit_price").isNotNull()
        & (col("unit_price") > 0)
        & col("discount_pct").isNotNull()
        & col("discount_pct").between(0, 100)
        & col("category").isNotNull()
        & col("region").isNotNull()
        & col("payment_method").isNotNull()
        & col("order_status").isin(
            "Completed", "Pending", "Cancelled"
        )
    )

    # 5. Calculate business metrics
    df = (
        df
        .withColumn(
            "gross_revenue",
            round(col("quantity") * col("unit_price"), 2)
        )
        .withColumn(
            "discount_amount",
            round(
                col("quantity")
                * col("unit_price")
                * col("discount_pct") / 100,
                2
            )
        )
        .withColumn(
            "net_revenue",
            round(
                col("quantity")
                * col("unit_price")
                * (1 - col("discount_pct") / 100),
                2
            )
        )
        .withColumn("processed_timestamp", current_timestamp())
    )

    # 6. Save Silver data in Parquet format
    silver_count = df.count()

    Path("data/silver").mkdir(parents=True, exist_ok=True)

    (
        df.write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )

    # 7. Generate quality summary
    print("\nSILVER DATA QUALITY SUMMARY")
    print("-" * 60)

    df.groupBy("order_status").agg(
        count("*").alias("record_count"),
        round(spark_sum("net_revenue"), 2).alias("net_revenue")
    ).show()

    print("=" * 60)
    print("SILVER TRANSFORMATION COMPLETED")
    print("=" * 60)
    print(f"Bronze records: {bronze_count:,}")
    print(f"Silver records: {silver_count:,}")
    print(f"Records removed: {bronze_count - silver_count:,}")
    print(f"Output format: Parquet")
    print(f"Output location: {SILVER_PATH}")

    spark.stop()


if __name__ == "__main__":
    run_silver_transformation()
