"""
transforms.py
-------------
Reusable Apache Beam PTransforms for the retail ETL pipeline.
Each transform is independently testable and handles one concern.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Any

import apache_beam as beam

logger = logging.getLogger(__name__)


# ── Utility ────────────────────────────────────────────────────────────────

def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes")


# ── CSV Reading ────────────────────────────────────────────────────────────

class ParseCSVLines(beam.DoFn):
    """
    Reads a CSV blob from GCS and yields one dict per row.
    Attaches `_source_file` and `_ingested_at` metadata fields.
    """

    def __init__(self, entity: str) -> None:
        self.entity = entity

    def process(self, element: str, file_name: str = "unknown"):
        reader = csv.DictReader(io.StringIO(element))
        for row in reader:
            row["_source_file"] = file_name
            row["_ingested_at"] = _now_utc()
            yield row


class ReadCSVFromGCS(beam.PTransform):
    """
    Reads all CSV files matching a GCS glob and parses each into dicts.
    """

    def __init__(self, gcs_pattern: str, entity: str) -> None:
        super().__init__()
        self.gcs_pattern = gcs_pattern
        self.entity = entity

    def expand(self, pcoll):
        return (
            pcoll.pipeline
            | f"Match {self.entity} Files" >> beam.io.fileio.MatchFiles(self.gcs_pattern)
            | f"Read {self.entity} Files" >> beam.io.fileio.ReadMatches()
            | f"Read {self.entity} Content" >> beam.Map(
                lambda f: (f.metadata.path, f.read_utf8())
            )
            | f"Parse {self.entity} CSV" >> beam.FlatMap(
                lambda kv: list(
                    csv.DictReader(io.StringIO(kv[1]),
                                   restkey="_extra", restval=None)
                )
            )
            | f"Tag {self.entity} Source" >> beam.Map(
                lambda row: {**row, "_ingested_at": _now_utc()}
            )
        )


# ── Cleaning Transforms ────────────────────────────────────────────────────

class CleanCustomers(beam.DoFn):
    """
    Validates and type-casts customer records.
    Emits valid rows to main output, invalid to 'dead_letter'.
    """

    def process(self, row: dict):
        errors = []

        customer_id = str(row.get("customer_id", "")).strip()
        if not customer_id:
            errors.append("missing customer_id")

        email = str(row.get("email", "")).strip().lower()
        if "@" not in email:
            errors.append(f"invalid email: {email}")

        if errors:
            yield beam.pvalue.TaggedOutput(
                "dead_letter",
                {"row": row, "errors": errors, "entity": "customers"}
            )
            return

        yield {
            "customer_id":       customer_id,
            "first_name":        str(row.get("first_name", "")).strip().title(),
            "last_name":         str(row.get("last_name", "")).strip().title(),
            "email":             email,
            "state":             str(row.get("state", "")).strip().upper(),
            "city":              str(row.get("city", "")).strip().title(),
            "signup_channel":    str(row.get("signup_channel", "unknown")).strip(),
            "is_loyalty_member": _safe_bool(row.get("is_loyalty_member", False)),
            "created_at":        str(row.get("created_at", "")).strip() or None,
            "_ingested_at":      row.get("_ingested_at"),
            "_source_file":      row.get("_source_file"),
        }


class CleanOrders(beam.DoFn):
    """
    Validates and type-casts order records.
    Rejects orders with negative totals or missing order/customer IDs.
    """

    def process(self, row: dict):
        errors = []

        order_id = str(row.get("order_id", "")).strip()
        customer_id = str(row.get("customer_id", "")).strip()

        if not order_id:
            errors.append("missing order_id")
        if not customer_id:
            errors.append("missing customer_id")

        order_total = _safe_float(row.get("order_total"))
        if order_total < 0:
            errors.append(f"negative order_total: {order_total}")

        if errors:
            yield beam.pvalue.TaggedOutput(
                "dead_letter",
                {"row": row, "errors": errors, "entity": "orders"}
            )
            return

        yield {
            "order_id":        order_id,
            "customer_id":     customer_id,
            "order_date":      str(row.get("order_date", "")).strip() or None,
            "order_timestamp": str(row.get("order_timestamp", "")).strip() or None,
            "status":          str(row.get("status", "unknown")).strip().lower(),
            "payment_method":  str(row.get("payment_method", "unknown")).strip().lower(),
            "channel":         str(row.get("channel", "unknown")).strip().lower(),
            "subtotal":        _safe_float(row.get("subtotal")),
            "shipping_cost":   _safe_float(row.get("shipping_cost")),
            "tax":             _safe_float(row.get("tax")),
            "order_total":     order_total,
            "state":           str(row.get("state", "")).strip().upper(),
            "city":            str(row.get("city", "")).strip().title(),
            "_ingested_at":    row.get("_ingested_at"),
            "_source_file":    row.get("_source_file"),
        }


class CleanProducts(beam.DoFn):

    def process(self, row: dict):
        errors = []

        product_id = str(row.get("product_id", "")).strip()
        if not product_id:
            errors.append("missing product_id")

        unit_price = _safe_float(row.get("unit_price"))
        unit_cost  = _safe_float(row.get("unit_cost"))

        if unit_price <= 0:
            errors.append(f"invalid unit_price: {unit_price}")
        if unit_cost < 0:
            errors.append(f"invalid unit_cost: {unit_cost}")
        if unit_cost > unit_price:
            errors.append("unit_cost exceeds unit_price")

        if errors:
            yield beam.pvalue.TaggedOutput(
                "dead_letter",
                {"row": row, "errors": errors, "entity": "products"}
            )
            return

        yield {
            "product_id":   product_id,
            "product_name": str(row.get("product_name", "")).strip(),
            "category":     str(row.get("category", "Unknown")).strip(),
            "brand":        str(row.get("brand", "Unknown")).strip(),
            "unit_price":   unit_price,
            "unit_cost":    unit_cost,
            "sku":          str(row.get("sku", "")).strip().upper(),
            "is_active":    _safe_bool(row.get("is_active", True)),
            "created_at":   str(row.get("created_at", "")).strip() or None,
            "_ingested_at": row.get("_ingested_at"),
            "_source_file": row.get("_source_file"),
        }


class CleanOrderItems(beam.DoFn):

    def process(self, row: dict):
        errors = []

        order_item_id = str(row.get("order_item_id", "")).strip()
        order_id      = str(row.get("order_id", "")).strip()
        product_id    = str(row.get("product_id", "")).strip()

        if not order_item_id:
            errors.append("missing order_item_id")
        if not order_id:
            errors.append("missing order_id")
        if not product_id:
            errors.append("missing product_id")

        quantity   = _safe_int(row.get("quantity", 1))
        line_total = _safe_float(row.get("line_total"))

        if quantity <= 0:
            errors.append(f"invalid quantity: {quantity}")
        if line_total < 0:
            errors.append(f"negative line_total: {line_total}")

        if errors:
            yield beam.pvalue.TaggedOutput(
                "dead_letter",
                {"row": row, "errors": errors, "entity": "order_items"}
            )
            return

        yield {
            "order_item_id": order_item_id,
            "order_id":      order_id,
            "product_id":    product_id,
            "quantity":      quantity,
            "unit_price":    _safe_float(row.get("unit_price")),
            "discount_pct":  _safe_float(row.get("discount_pct")),
            "line_total":    line_total,
            "_ingested_at":  row.get("_ingested_at"),
            "_source_file":  row.get("_source_file"),
        }


class CleanReturns(beam.DoFn):

    def process(self, row: dict):
        errors = []

        return_id = str(row.get("return_id", "")).strip()
        if not return_id:
            errors.append("missing return_id")

        refund = _safe_float(row.get("refund_amount"))
        if refund < 0:
            errors.append(f"negative refund_amount: {refund}")

        if errors:
            yield beam.pvalue.TaggedOutput(
                "dead_letter",
                {"row": row, "errors": errors, "entity": "returns"}
            )
            return

        yield {
            "return_id":     return_id,
            "order_id":      str(row.get("order_id", "")).strip(),
            "order_item_id": str(row.get("order_item_id", "")).strip(),
            "product_id":    str(row.get("product_id", "")).strip(),
            "customer_id":   str(row.get("customer_id", "")).strip(),
            "return_date":   str(row.get("return_date", "")).strip() or None,
            "return_reason": str(row.get("return_reason", "unknown")).strip().lower(),
            "refund_amount": refund,
            "status":        str(row.get("status", "pending")).strip().lower(),
            "_ingested_at":  row.get("_ingested_at"),
            "_source_file":  row.get("_source_file"),
        }


# ── Deduplication ──────────────────────────────────────────────────────────

class DeduplicateById(beam.PTransform):
    """
    Deduplicates records by a given key field, keeping the latest by _ingested_at.
    """

    def __init__(self, key_field: str) -> None:
        super().__init__()
        self.key_field = key_field

    def expand(self, pcoll):
        return (
            pcoll
            | "KeyBy ID" >> beam.Map(lambda r: (r[self.key_field], r))
            | "Group Dupes" >> beam.GroupByKey()
            | "Keep Latest" >> beam.Map(
                lambda kv: sorted(
                    kv[1],
                    key=lambda r: r.get("_ingested_at") or "",
                    reverse=True
                )[0]
            )
        )


# ── Dead Letter Sink ───────────────────────────────────────────────────────

class WritDeadLetterToGCS(beam.PTransform):
    """
    Writes rejected records to GCS as newline-delimited JSON for review.
    """

    def __init__(self, gcs_path: str) -> None:
        super().__init__()
        self.gcs_path = gcs_path

    def expand(self, pcoll):
        import json
        return (
            pcoll
            | "Serialize DL" >> beam.Map(json.dumps)
            | "Write DL" >> beam.io.WriteToText(
                self.gcs_path,
                file_name_suffix=".json",
                shard_name_template=""
            )
        )
