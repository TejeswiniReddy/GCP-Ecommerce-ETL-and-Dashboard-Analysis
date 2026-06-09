"""
pipeline.py
-----------
Main Apache Beam / Cloud Dataflow pipeline for the GCP retail ETL.

Reads raw CSVs from GCS, cleans and validates each entity,
deduplicates, and writes to BigQuery (Silver / staging layer).

Run locally (DirectRunner):
    python pipeline.py \
        --project my-project \
        --input gs://my-bucket/raw/ \
        --bq_dataset retail_staging \
        --runner DirectRunner

Run on Dataflow:
    python pipeline.py \
        --project my-project \
        --region us-central1 \
        --input gs://my-bucket/raw/ \
        --bq_dataset retail_staging \
        --staging_location gs://my-bucket/staging/ \
        --temp_location gs://my-bucket/temp/ \
        --runner DataflowRunner \
        --setup_file ./setup.py
"""

from __future__ import annotations

import argparse
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions

from etl.schema import TABLE_SCHEMAS
from etl.transforms import (
    CleanCustomers,
    CleanOrders,
    CleanOrderItems,
    CleanProducts,
    CleanReturns,
    DeduplicateById,
    ReadCSVFromGCS,
    WritDeadLetterToGCS,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_pipeline(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",          required=True,  help="GCP project ID")
    parser.add_argument("--input",            required=True,  help="GCS path prefix for raw CSVs")
    parser.add_argument("--bq_dataset",       default="retail_staging")
    parser.add_argument("--dead_letter_path", default=None,   help="GCS path for rejected records")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = known_args.project

    dead_letter_base = (
        known_args.dead_letter_path
        or f"gs://{known_args.project}-retail/dead_letter/rejected"
    )

    bq_table = lambda name: f"{known_args.project}:{known_args.bq_dataset}.{name}"

    with beam.Pipeline(options=options) as p:

        # ── CUSTOMERS ────────────────────────────────────────────────────
        customers_raw = p | "Read Customers" >> ReadCSVFromGCS(
            gcs_pattern=f"{known_args.input.rstrip('/')}/customers*.csv",
            entity="customers"
        )
        customers_cleaned = (
            customers_raw
            | "Clean Customers" >> beam.ParDo(CleanCustomers()).with_outputs(
                "dead_letter", main="valid"
            )
        )
        (
            customers_cleaned.valid
            | "Dedup Customers" >> DeduplicateById("customer_id")
            | "Write Customers BQ" >> WriteToBigQuery(
                bq_table("customers"),
                schema={"fields": [
                    {"name": f.name, "type": f.field_type, "mode": f.mode}
                    for f in TABLE_SCHEMAS["customers"]
                ]},
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )
        customers_cleaned.dead_letter | "DL Customers" >> WritDeadLetterToGCS(
            f"{dead_letter_base}/customers"
        )

        # ── PRODUCTS ─────────────────────────────────────────────────────
        products_raw = p | "Read Products" >> ReadCSVFromGCS(
            gcs_pattern=f"{known_args.input.rstrip('/')}/products*.csv",
            entity="products"
        )
        products_cleaned = (
            products_raw
            | "Clean Products" >> beam.ParDo(CleanProducts()).with_outputs(
                "dead_letter", main="valid"
            )
        )
        (
            products_cleaned.valid
            | "Dedup Products" >> DeduplicateById("product_id")
            | "Write Products BQ" >> WriteToBigQuery(
                bq_table("products"),
                schema={"fields": [
                    {"name": f.name, "type": f.field_type, "mode": f.mode}
                    for f in TABLE_SCHEMAS["products"]
                ]},
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )
        products_cleaned.dead_letter | "DL Products" >> WritDeadLetterToGCS(
            f"{dead_letter_base}/products"
        )

        # ── ORDERS ───────────────────────────────────────────────────────
        orders_raw = p | "Read Orders" >> ReadCSVFromGCS(
            gcs_pattern=f"{known_args.input.rstrip('/')}/orders*.csv",
            entity="orders"
        )
        orders_cleaned = (
            orders_raw
            | "Clean Orders" >> beam.ParDo(CleanOrders()).with_outputs(
                "dead_letter", main="valid"
            )
        )
        (
            orders_cleaned.valid
            | "Dedup Orders" >> DeduplicateById("order_id")
            | "Write Orders BQ" >> WriteToBigQuery(
                bq_table("orders"),
                schema={"fields": [
                    {"name": f.name, "type": f.field_type, "mode": f.mode}
                    for f in TABLE_SCHEMAS["orders"]
                ]},
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )
        orders_cleaned.dead_letter | "DL Orders" >> WritDeadLetterToGCS(
            f"{dead_letter_base}/orders"
        )

        # ── ORDER ITEMS ──────────────────────────────────────────────────
        items_raw = p | "Read Order Items" >> ReadCSVFromGCS(
            gcs_pattern=f"{known_args.input.rstrip('/')}/order_items*.csv",
            entity="order_items"
        )
        items_cleaned = (
            items_raw
            | "Clean Items" >> beam.ParDo(CleanOrderItems()).with_outputs(
                "dead_letter", main="valid"
            )
        )
        (
            items_cleaned.valid
            | "Dedup Items" >> DeduplicateById("order_item_id")
            | "Write Items BQ" >> WriteToBigQuery(
                bq_table("order_items"),
                schema={"fields": [
                    {"name": f.name, "type": f.field_type, "mode": f.mode}
                    for f in TABLE_SCHEMAS["order_items"]
                ]},
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )
        items_cleaned.dead_letter | "DL Items" >> WritDeadLetterToGCS(
            f"{dead_letter_base}/order_items"
        )

        # ── RETURNS ──────────────────────────────────────────────────────
        returns_raw = p | "Read Returns" >> ReadCSVFromGCS(
            gcs_pattern=f"{known_args.input.rstrip('/')}/returns*.csv",
            entity="returns"
        )
        returns_cleaned = (
            returns_raw
            | "Clean Returns" >> beam.ParDo(CleanReturns()).with_outputs(
                "dead_letter", main="valid"
            )
        )
        (
            returns_cleaned.valid
            | "Dedup Returns" >> DeduplicateById("return_id")
            | "Write Returns BQ" >> WriteToBigQuery(
                bq_table("returns"),
                schema={"fields": [
                    {"name": f.name, "type": f.field_type, "mode": f.mode}
                    for f in TABLE_SCHEMAS["returns"]
                ]},
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )
        returns_cleaned.dead_letter | "DL Returns" >> WritDeadLetterToGCS(
            f"{dead_letter_base}/returns"
        )

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    build_pipeline()
