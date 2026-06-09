"""
generate_sample_data.py
-----------------------
Generates realistic e-commerce retail datasets for the GCP ETL pipeline.
Outputs: customers.csv, products.csv, orders.csv, order_items.csv, returns.csv

Usage:
    python generate_sample_data.py --rows 100000 --output-dir ./raw/
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta

import numpy as np

# ── Config ─────────────────────────────────────────────────────────────────

CATEGORIES = {
    "Electronics":    ["Laptop", "Smartphone", "Tablet", "Headphones", "Smart Watch", "Charger", "Webcam", "Speaker"],
    "Clothing":       ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress", "Hoodie", "Socks", "Belt"],
    "Home & Kitchen": ["Blender", "Coffee Maker", "Air Fryer", "Cookware Set", "Knife Set", "Bedding", "Lamp"],
    "Sports":         ["Yoga Mat", "Resistance Bands", "Dumbbell Set", "Running Shoes", "Bicycle Helmet"],
    "Beauty":         ["Face Serum", "Moisturizer", "Lipstick", "Foundation", "Mascara", "Sunscreen"],
    "Books":          ["Fiction Novel", "Self Help Book", "Cookbook", "Tech Manual", "Biography"],
    "Toys":           ["Board Game", "Action Figure", "Building Blocks", "Puzzle", "RC Car"],
    "Grocery":        ["Protein Bars", "Coffee Beans", "Olive Oil", "Pasta", "Cereal"],
}

BRANDS = {
    "Electronics":    ["TechPro", "NexaVision", "CoreByte", "Luminary"],
    "Clothing":       ["UrbanThread", "PeakWear", "StyleCore", "NovaDenim"],
    "Home & Kitchen": ["CasaCraft", "HomeBase", "NestIQ", "KitchenElite"],
    "Sports":         ["ProFit", "ZenActive", "IronEdge", "SwiftGear"],
    "Beauty":         ["GlowLab", "PureSkin", "LuxeBeauty", "NaturaCo"],
    "Books":          ["Penguin", "HarperOne", "Scholastic", "RandomHouse"],
    "Toys":           ["PlayMax", "FunBuild", "JoyZone", "KidCraft"],
    "Grocery":        ["NutriPure", "FreshFarm", "OrganicPeak", "HealthRoot"],
}

PRICE_RANGES = {
    "Electronics":    (29.99, 1499.99),
    "Clothing":       (9.99, 249.99),
    "Home & Kitchen": (14.99, 349.99),
    "Sports":         (9.99, 399.99),
    "Beauty":         (7.99, 129.99),
    "Books":          (5.99, 49.99),
    "Toys":           (9.99, 149.99),
    "Grocery":        (2.99, 59.99),
}

US_STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
    "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "NV",
]

STATE_WEIGHTS = [
    0.12, 0.09, 0.07, 0.07, 0.04, 0.04, 0.04, 0.03, 0.03, 0.03,
    0.03, 0.03, 0.03, 0.03, 0.03, 0.02, 0.02, 0.02, 0.02, 0.02,
    0.02, 0.02, 0.02, 0.02, 0.02, 0.01, 0.01, 0.01, 0.01, 0.01,
]

CHANNELS = ["organic_search", "paid_search", "email", "social_media", "direct", "affiliate"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
ORDER_STATUSES = ["delivered", "delivered", "delivered", "delivered", "shipped", "cancelled", "returned"]


def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def generate_customers(n: int) -> list[dict]:
    print(f"  Generating {n:,} customers...")
    customers = []
    for i in range(n):
        created = random_date(datetime(2021, 1, 1), datetime(2024, 6, 1))
        customers.append({
            "customer_id":   str(uuid.uuid4()),
            "first_name":    random.choice(["Emma","Liam","Olivia","Noah","Ava","James","Isabella","Oliver","Sophia","Lucas"]),
            "last_name":     random.choice(["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Wilson","Moore"]),
            "email":         f"user_{i}_{random.randint(100,999)}@example.com",
            "state":         random.choices(US_STATES, weights=STATE_WEIGHTS)[0],
            "city":          random.choice(["Los Angeles","Houston","Miami","New York","Chicago","Phoenix","Seattle","Boston","Atlanta","Denver"]),
            "signup_channel":random.choice(CHANNELS),
            "is_loyalty_member": random.choice([True, True, False]),
            "created_at":    created.isoformat(),
        })
    return customers


def generate_products(n: int) -> list[dict]:
    print(f"  Generating {n:,} products...")
    products = []
    for _ in range(n):
        category = random.choice(list(CATEGORIES.keys()))
        name = random.choice(CATEGORIES[category])
        brand = random.choice(BRANDS[category])
        lo, hi = PRICE_RANGES[category]
        price = round(random.uniform(lo, hi), 2)
        cost = round(price * random.uniform(0.30, 0.65), 2)
        products.append({
            "product_id":   str(uuid.uuid4()),
            "product_name": f"{brand} {name}",
            "category":     category,
            "brand":        brand,
            "unit_price":   price,
            "unit_cost":    cost,
            "sku":          f"SKU-{random.randint(100000, 999999)}",
            "is_active":    random.choices([True, False], weights=[0.92, 0.08])[0],
            "created_at":   random_date(datetime(2020, 1, 1), datetime(2023, 1, 1)).isoformat(),
        })
    return products


def generate_orders_and_items(
    n_orders: int,
    customers: list[dict],
    products: list[dict],
) -> tuple[list[dict], list[dict]]:
    print(f"  Generating {n_orders:,} orders and line items...")
    orders = []
    items = []

    # Repeat customers to simulate returning buyers (Pareto distribution)
    customer_pool = random.choices(customers, k=n_orders)

    for customer in customer_pool:
        order_id = str(uuid.uuid4())
        order_date = random_date(datetime(2022, 1, 1), datetime(2024, 12, 31))
        n_items = random.choices([1, 2, 3, 4, 5], weights=[0.45, 0.30, 0.15, 0.07, 0.03])[0]
        status = random.choice(ORDER_STATUSES)

        order_items = random.choices(products, k=n_items)
        subtotal = 0.0

        for product in order_items:
            qty = random.randint(1, 3)
            unit_price = product["unit_price"]
            discount_pct = random.choices([0, 0.05, 0.10, 0.15, 0.20], weights=[0.50, 0.20, 0.15, 0.10, 0.05])[0]
            discounted_price = round(unit_price * (1 - discount_pct), 2)
            line_total = round(discounted_price * qty, 2)
            subtotal += line_total

            items.append({
                "order_item_id":  str(uuid.uuid4()),
                "order_id":       order_id,
                "product_id":     product["product_id"],
                "quantity":       qty,
                "unit_price":     unit_price,
                "discount_pct":   discount_pct,
                "line_total":     line_total,
            })

        shipping = round(random.uniform(0, 15.99), 2) if subtotal < 50 else 0.0
        tax = round(subtotal * 0.08, 2)
        order_total = round(subtotal + shipping + tax, 2)

        orders.append({
            "order_id":        order_id,
            "customer_id":     customer["customer_id"],
            "order_date":      order_date.date().isoformat(),
            "order_timestamp": order_date.isoformat(),
            "status":          status,
            "payment_method":  random.choice(PAYMENT_METHODS),
            "channel":         random.choice(CHANNELS),
            "subtotal":        round(subtotal, 2),
            "shipping_cost":   shipping,
            "tax":             tax,
            "order_total":     order_total,
            "state":           customer["state"],
            "city":            customer["city"],
        })

    return orders, items


def generate_returns(orders: list[dict], items: list[dict]) -> list[dict]:
    print("  Generating returns...")
    returned_orders = [o for o in orders if o["status"] in ("returned", "cancelled")]
    items_by_order: dict[str, list] = {}
    for item in items:
        items_by_order.setdefault(item["order_id"], []).append(item)

    returns = []
    for order in returned_orders:
        order_items = items_by_order.get(order["order_id"], [])
        if not order_items:
            continue
        n_returned = random.randint(1, max(1, len(order_items)))
        for item in random.sample(order_items, n_returned):
            return_date = datetime.fromisoformat(order["order_timestamp"]) + timedelta(days=random.randint(1, 30))
            returns.append({
                "return_id":      str(uuid.uuid4()),
                "order_id":       order["order_id"],
                "order_item_id":  item["order_item_id"],
                "product_id":     item["product_id"],
                "customer_id":    order["customer_id"],
                "return_date":    return_date.date().isoformat(),
                "return_reason":  random.choice(["defective", "wrong_item", "not_as_described", "changed_mind", "damaged_in_transit"]),
                "refund_amount":  item["line_total"],
                "status":         random.choice(["approved", "approved", "pending", "rejected"]),
            })
    return returns


def write_csv(records: list[dict], path: str) -> None:
    if not records:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print(f"  Wrote {len(records):,} rows → {path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=50000, help="Number of orders to generate")
    parser.add_argument("--customers", type=int, default=20000)
    parser.add_argument("--products", type=int, default=2000)
    parser.add_argument("--output-dir", default="./raw/")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    np.random.seed(args.seed)
    os.makedirs(args.output_dir, exist_ok=True)

    print("Generating e-commerce dataset...")
    customers = generate_customers(args.customers)
    products  = generate_products(args.products)
    orders, items = generate_orders_and_items(args.rows, customers, products)
    returns   = generate_returns(orders, items)

    write_csv(customers, os.path.join(args.output_dir, "customers.csv"))
    write_csv(products,  os.path.join(args.output_dir, "products.csv"))
    write_csv(orders,    os.path.join(args.output_dir, "orders.csv"))
    write_csv(items,     os.path.join(args.output_dir, "order_items.csv"))
    write_csv(returns,   os.path.join(args.output_dir, "returns.csv"))

    print(f"\nDone. {len(customers):,} customers | {len(products):,} products | "
          f"{len(orders):,} orders | {len(items):,} items | {len(returns):,} returns")


if __name__ == "__main__":
    main()
