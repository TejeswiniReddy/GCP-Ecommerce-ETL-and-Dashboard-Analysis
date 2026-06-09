"""
tests/test_transforms.py
------------------------
Unit tests for custom Beam DoFns.
Run: pytest tests/ -v
"""

import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty

from etl.transforms import (
    CleanCustomers,
    CleanOrders,
    CleanProducts,
    CleanOrderItems,
    DeduplicateById,
)

NOW = "2024-01-01T00:00:00+00:00"


# ── Customer Tests ─────────────────────────────────────────────────────────

class TestCleanCustomers:

    def _make_row(self, **overrides):
        base = {
            "customer_id": "cust-001",
            "first_name": "jane",
            "last_name": "smith",
            "email": "jane@example.com",
            "state": "ca",
            "city": "los angeles",
            "signup_channel": "organic_search",
            "is_loyalty_member": "true",
            "created_at": "2023-06-15T10:00:00",
            "_ingested_at": NOW,
            "_source_file": "gs://bucket/customers.csv",
        }
        base.update(overrides)
        return base

    def test_valid_customer_passes(self):
        row = self._make_row()
        results = []
        dead_letters = []

        with TestPipeline() as p:
            output = (
                p
                | beam.Create([row])
                | beam.ParDo(CleanCustomers()).with_outputs("dead_letter", main="valid")
            )
            output.valid | beam.Map(results.append)
            output.dead_letter | beam.Map(dead_letters.append)

        assert len(results) == 1
        assert len(dead_letters) == 0
        assert results[0]["email"] == "jane@example.com"
        assert results[0]["first_name"] == "Jane"
        assert results[0]["state"] == "CA"

    def test_missing_customer_id_goes_to_dead_letter(self):
        row = self._make_row(customer_id="")
        results = []
        dead_letters = []

        with TestPipeline() as p:
            output = (
                p
                | beam.Create([row])
                | beam.ParDo(CleanCustomers()).with_outputs("dead_letter", main="valid")
            )
            output.valid | beam.Map(results.append)
            output.dead_letter | beam.Map(dead_letters.append)

        assert len(results) == 0
        assert len(dead_letters) == 1
        assert "missing customer_id" in dead_letters[0]["errors"]

    def test_invalid_email_goes_to_dead_letter(self):
        row = self._make_row(email="not-an-email")
        results = []
        dead_letters = []

        with TestPipeline() as p:
            output = (
                p
                | beam.Create([row])
                | beam.ParDo(CleanCustomers()).with_outputs("dead_letter", main="valid")
            )
            output.valid | beam.Map(results.append)
            output.dead_letter | beam.Map(dead_letters.append)

        assert len(results) == 0
        assert len(dead_letters) == 1

    def test_loyalty_member_bool_coercion(self):
        for val in ["True", "TRUE", "1", "yes"]:
            row = self._make_row(is_loyalty_member=val)
            results = []
            with TestPipeline() as p:
                (
                    p
                    | beam.Create([row])
                    | beam.ParDo(CleanCustomers()).with_outputs("dead_letter", main="valid")
                ).valid | beam.Map(results.append)
            assert results[0]["is_loyalty_member"] is True, f"Failed for value: {val}"


# ── Order Tests ────────────────────────────────────────────────────────────

class TestCleanOrders:

    def _make_row(self, **overrides):
        base = {
            "order_id": "ord-001",
            "customer_id": "cust-001",
            "order_date": "2024-03-15",
            "order_timestamp": "2024-03-15T14:22:00",
            "status": "delivered",
            "payment_method": "credit_card",
            "channel": "organic_search",
            "subtotal": "120.00",
            "shipping_cost": "0.00",
            "tax": "9.60",
            "order_total": "129.60",
            "state": "CA",
            "city": "Los Angeles",
            "_ingested_at": NOW,
            "_source_file": "gs://bucket/orders.csv",
        }
        base.update(overrides)
        return base

    def test_valid_order_passes(self):
        row = self._make_row()
        results = []
        with TestPipeline() as p:
            (
                p
                | beam.Create([row])
                | beam.ParDo(CleanOrders()).with_outputs("dead_letter", main="valid")
            ).valid | beam.Map(results.append)

        assert len(results) == 1
        assert results[0]["order_total"] == 129.60
        assert results[0]["status"] == "delivered"

    def test_negative_order_total_rejected(self):
        row = self._make_row(order_total="-10.00")
        dead_letters = []
        with TestPipeline() as p:
            (
                p
                | beam.Create([row])
                | beam.ParDo(CleanOrders()).with_outputs("dead_letter", main="valid")
            ).dead_letter | beam.Map(dead_letters.append)

        assert len(dead_letters) == 1
        assert any("negative" in e for e in dead_letters[0]["errors"])

    def test_missing_order_id_rejected(self):
        row = self._make_row(order_id="")
        dead_letters = []
        with TestPipeline() as p:
            (
                p
                | beam.Create([row])
                | beam.ParDo(CleanOrders()).with_outputs("dead_letter", main="valid")
            ).dead_letter | beam.Map(dead_letters.append)

        assert len(dead_letters) == 1


# ── Product Tests ──────────────────────────────────────────────────────────

class TestCleanProducts:

    def _make_row(self, **overrides):
        base = {
            "product_id": "prod-001",
            "product_name": "TechPro Laptop",
            "category": "Electronics",
            "brand": "TechPro",
            "unit_price": "999.99",
            "unit_cost": "450.00",
            "sku": "sku-123456",
            "is_active": "true",
            "created_at": "2022-01-01T00:00:00",
            "_ingested_at": NOW,
            "_source_file": "gs://bucket/products.csv",
        }
        base.update(overrides)
        return base

    def test_valid_product_passes(self):
        row = self._make_row()
        results = []
        with TestPipeline() as p:
            (
                p
                | beam.Create([row])
                | beam.ParDo(CleanProducts()).with_outputs("dead_letter", main="valid")
            ).valid | beam.Map(results.append)

        assert len(results) == 1
        assert results[0]["unit_price"] == 999.99
        assert results[0]["sku"] == "SKU-123456"

    def test_cost_exceeds_price_rejected(self):
        row = self._make_row(unit_cost="1200.00")
        dead_letters = []
        with TestPipeline() as p:
            (
                p
                | beam.Create([row])
                | beam.ParDo(CleanProducts()).with_outputs("dead_letter", main="valid")
            ).dead_letter | beam.Map(dead_letters.append)

        assert len(dead_letters) == 1

    def test_zero_price_rejected(self):
        row = self._make_row(unit_price="0")
        dead_letters = []
        with TestPipeline() as p:
            (
                p
                | beam.Create([row])
                | beam.ParDo(CleanProducts()).with_outputs("dead_letter", main="valid")
            ).dead_letter | beam.Map(dead_letters.append)

        assert len(dead_letters) == 1


# ── Dedup Tests ────────────────────────────────────────────────────────────

class TestDeduplicateById:

    def test_deduplication_keeps_latest(self):
        rows = [
            {"order_id": "ord-001", "_ingested_at": "2024-01-01T10:00:00", "status": "old"},
            {"order_id": "ord-001", "_ingested_at": "2024-01-01T12:00:00", "status": "latest"},
            {"order_id": "ord-002", "_ingested_at": "2024-01-01T09:00:00", "status": "only_one"},
        ]
        results = []
        with TestPipeline() as p:
            (
                p
                | beam.Create(rows)
                | DeduplicateById("order_id")
                | beam.Map(results.append)
            )

        assert len(results) == 2
        kept = {r["order_id"]: r for r in results}
        assert kept["ord-001"]["status"] == "latest"
        assert kept["ord-002"]["status"] == "only_one"
