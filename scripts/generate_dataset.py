
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

OUTPUT = Path("data/raw/ecommerce_transactions.csv")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

products = {
    "Electronics": (50, 1500),
    "Clothing": (10, 200),
    "Home": (15, 500),
    "Books": (5, 80),
    "Beauty": (8, 150),
    "Sports": (12, 400),
}

regions = ["North", "South", "East", "West", "Central"]
payment_methods = ["Credit Card", "Debit Card", "UPI", "PayPal", "Bank Transfer"]
statuses = ["Completed", "Completed", "Completed", "Pending", "Cancelled"]

start_date = datetime(2024, 1, 1)
records = 50000

with OUTPUT.open("w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    writer.writerow([
        "order_id", "order_date", "customer_id",
        "product_id", "category", "quantity",
        "unit_price", "discount_pct", "payment_method",
        "region", "order_status"
    ])

    for i in range(1, records + 1):
        category = random.choice(list(products.keys()))
        min_price, max_price = products[category]

        order_date = start_date + timedelta(
            days=random.randint(0, 730),
            hours=random.randint(0, 23)
        )

        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(min_price, max_price), 2)

        writer.writerow([
            f"ORD{i:07d}",
            order_date.strftime("%Y-%m-%d %H:%M:%S"),
            f"CUST{random.randint(1, 10000):05d}",
            f"PROD{(list(products.keys()).index(category) * 1000 + random.randint(1, 1000)):04d}",
            category,
            quantity,
            unit_price,
            random.choice([0, 5, 10, 15, 20]),
            random.choice(payment_methods),
            random.choice(regions),
            random.choice(statuses)
        ])

print("=" * 55)
print("ECOMMERCE DATASET GENERATED")
print("=" * 55)
print(f"Total records: {records:,}")
print(f"Output: {OUTPUT}")
print("Dataset type: Synthetic transaction data")
