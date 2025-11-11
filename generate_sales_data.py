

import random
import csv
from datetime import datetime, timedelta
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "data" / "raw"
OUT.mkdir(parents=True, exist_ok=True)
FILE = OUT / "sales_data.csv"

NUM_ROWS = 10000  # change to 100k+ for stress testing

regions = ["North", "South", "East", "West", "Central"]
categories = {
    "Electronics": ["Laptop", "Smartphone", "Headphones", "Monitor"],
    "Furniture": ["Chair", "Table", "Desk", "Cabinet"],
    "Office Supplies": ["Paper", "Pen", "Notepad", "Stapler"],
    "Apparel": ["T-Shirt", "Jacket", "Shoes"],
    "Home": ["Cookware", "Bedding", "Decor"]
}
payment_methods = ["card", "paypal", "bank_transfer", "cash"]
start_date = datetime(2023, 1, 1)

def random_date(start, days_range=365):
    return (start + timedelta(days=random.randint(0, days_range-1))).strftime("%Y-%m-%d")

def main():
    with open(FILE, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "order_id", "order_date", "customer_id", "customer_name",
            "region", "category", "product", "unit_price", "quantity", "sales", "profit", "payment_method"
        ])
        for i in range(1, NUM_ROWS + 1):
            category = random.choice(list(categories.keys()))
            product = random.choice(categories[category])
            unit_price = round(random.uniform(5, 2000), 2)
            quantity = random.choices([1,2,3,4,5], weights=[60,20,10,7,3])[0]
            sales = round(unit_price * quantity, 2)
            profit = round(sales * random.uniform(0.05, 0.35), 2)
            region = random.choice(regions)
            order_date = random_date(start_date, days_range=730)
            cust_id = random.randint(1000, 9999)
            cust_name = f"Customer_{cust_id}"
            payment = random.choice(payment_methods)

            # Introduce some duplicates and nulls for cleaning practice
            if random.random() < 0.002:
                cust_name = ""  # missing name
            if random.random() < 0.001:
                product = None  # missing product

            writer.writerow([
                i, order_date, cust_id, cust_name, region, category, product,
                unit_price, quantity, sales, profit, payment
            ])
    print(f"Wrote {FILE}")

if __name__ == "__main__":
    main()
