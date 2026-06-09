"""
schema.py
---------
BigQuery table schemas for the retail ETL pipeline.
Used by both the Dataflow pipeline and Terraform provisioning.
"""

from google.cloud.bigquery import SchemaField

# ── RAW / STAGING SCHEMAS (Silver Layer) ──────────────────────────────────

CUSTOMERS_SCHEMA = [
    SchemaField("customer_id",       "STRING",    mode="REQUIRED"),
    SchemaField("first_name",        "STRING",    mode="NULLABLE"),
    SchemaField("last_name",         "STRING",    mode="NULLABLE"),
    SchemaField("email",             "STRING",    mode="NULLABLE"),
    SchemaField("state",             "STRING",    mode="NULLABLE"),
    SchemaField("city",              "STRING",    mode="NULLABLE"),
    SchemaField("signup_channel",    "STRING",    mode="NULLABLE"),
    SchemaField("is_loyalty_member", "BOOL",      mode="NULLABLE"),
    SchemaField("created_at",        "TIMESTAMP", mode="NULLABLE"),
    SchemaField("_ingested_at",      "TIMESTAMP", mode="NULLABLE"),
    SchemaField("_source_file",      "STRING",    mode="NULLABLE"),
]

PRODUCTS_SCHEMA = [
    SchemaField("product_id",   "STRING",    mode="REQUIRED"),
    SchemaField("product_name", "STRING",    mode="NULLABLE"),
    SchemaField("category",     "STRING",    mode="NULLABLE"),
    SchemaField("brand",        "STRING",    mode="NULLABLE"),
    SchemaField("unit_price",   "FLOAT64",   mode="NULLABLE"),
    SchemaField("unit_cost",    "FLOAT64",   mode="NULLABLE"),
    SchemaField("sku",          "STRING",    mode="NULLABLE"),
    SchemaField("is_active",    "BOOL",      mode="NULLABLE"),
    SchemaField("created_at",   "TIMESTAMP", mode="NULLABLE"),
    SchemaField("_ingested_at", "TIMESTAMP", mode="NULLABLE"),
    SchemaField("_source_file", "STRING",    mode="NULLABLE"),
]

ORDERS_SCHEMA = [
    SchemaField("order_id",         "STRING",    mode="REQUIRED"),
    SchemaField("customer_id",      "STRING",    mode="REQUIRED"),
    SchemaField("order_date",       "DATE",      mode="NULLABLE"),
    SchemaField("order_timestamp",  "TIMESTAMP", mode="NULLABLE"),
    SchemaField("status",           "STRING",    mode="NULLABLE"),
    SchemaField("payment_method",   "STRING",    mode="NULLABLE"),
    SchemaField("channel",          "STRING",    mode="NULLABLE"),
    SchemaField("subtotal",         "FLOAT64",   mode="NULLABLE"),
    SchemaField("shipping_cost",    "FLOAT64",   mode="NULLABLE"),
    SchemaField("tax",              "FLOAT64",   mode="NULLABLE"),
    SchemaField("order_total",      "FLOAT64",   mode="NULLABLE"),
    SchemaField("state",            "STRING",    mode="NULLABLE"),
    SchemaField("city",             "STRING",    mode="NULLABLE"),
    SchemaField("_ingested_at",     "TIMESTAMP", mode="NULLABLE"),
    SchemaField("_source_file",     "STRING",    mode="NULLABLE"),
]

ORDER_ITEMS_SCHEMA = [
    SchemaField("order_item_id",  "STRING",  mode="REQUIRED"),
    SchemaField("order_id",       "STRING",  mode="REQUIRED"),
    SchemaField("product_id",     "STRING",  mode="REQUIRED"),
    SchemaField("quantity",       "INT64",   mode="NULLABLE"),
    SchemaField("unit_price",     "FLOAT64", mode="NULLABLE"),
    SchemaField("discount_pct",   "FLOAT64", mode="NULLABLE"),
    SchemaField("line_total",     "FLOAT64", mode="NULLABLE"),
    SchemaField("_ingested_at",   "TIMESTAMP", mode="NULLABLE"),
    SchemaField("_source_file",   "STRING",  mode="NULLABLE"),
]

RETURNS_SCHEMA = [
    SchemaField("return_id",      "STRING",  mode="REQUIRED"),
    SchemaField("order_id",       "STRING",  mode="REQUIRED"),
    SchemaField("order_item_id",  "STRING",  mode="REQUIRED"),
    SchemaField("product_id",     "STRING",  mode="REQUIRED"),
    SchemaField("customer_id",    "STRING",  mode="REQUIRED"),
    SchemaField("return_date",    "DATE",    mode="NULLABLE"),
    SchemaField("return_reason",  "STRING",  mode="NULLABLE"),
    SchemaField("refund_amount",  "FLOAT64", mode="NULLABLE"),
    SchemaField("status",         "STRING",  mode="NULLABLE"),
    SchemaField("_ingested_at",   "TIMESTAMP", mode="NULLABLE"),
    SchemaField("_source_file",   "STRING",  mode="NULLABLE"),
]

# Registry for pipeline use
TABLE_SCHEMAS = {
    "customers":   CUSTOMERS_SCHEMA,
    "products":    PRODUCTS_SCHEMA,
    "orders":      ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "returns":     RETURNS_SCHEMA,
}
