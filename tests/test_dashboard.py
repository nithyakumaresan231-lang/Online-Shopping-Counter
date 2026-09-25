"""
Unit and integration tests for the Streamlit dashboard data loader and components.
Tests normal data, no-data, stale data, and error conditions.
"""

import os
import sys
import json
import time
import shutil
import tempfile
import unittest

import pandas as pd

# Add project root to sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dashboard.data_loader import (
    load_analytics_metrics,
    load_recent_transactions,
    get_default_metrics,
    normalize_transactions_df,
    format_currency,
    format_number,
    format_status_label,
    DISPLAY_COL_ORDER_ID,
    DISPLAY_COL_PRODUCT,
    DISPLAY_COL_CATEGORY,
    DISPLAY_COL_AMOUNT,
    DISPLAY_COL_CITY,
    DISPLAY_COL_STATUS,
    DISPLAY_TRANSACTION_COLUMNS,
)
from analytics.schemas import (
    COL_CATEGORY,
    COL_CITY,
    COL_PAYMENT_METHOD,
    COL_ORDER_STATUS,
    STATUS_COMPLETED,
    STATUS_RETURNED,
    STATUS_CANCELLED,
)


class TestDashboardDataLoader(unittest.TestCase):
    """Test suite for dashboard data loading, formatting, and edge cases."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.metrics_file = os.path.join(self.tmp_dir, "latest_metrics.json")
        self.transactions_file = os.path.join(self.tmp_dir, "recent_transactions.json")

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_default_metrics_structure(self):
        """Test default metrics structure returned when no data is available."""
        defaults = get_default_metrics()
        self.assertEqual(defaults["total_orders"], 0)
        self.assertEqual(defaults["completed_orders"], 0)
        self.assertEqual(defaults["returned_orders"], 0)
        self.assertEqual(defaults["completed_revenue"], 0.0)
        self.assertEqual(defaults["orders_in_last_minute"], 0)
        self.assertEqual(defaults["status"], "WAITING")
        self.assertFalse(defaults["is_live"])
        self.assertFalse(defaults["has_data"])

        # Check DataFrames have required columns
        self.assertIn(COL_CATEGORY, defaults["revenue_by_category"].columns)
        self.assertIn("revenue", defaults["revenue_by_category"].columns)
        self.assertIn(COL_CITY, defaults["orders_by_city"].columns)
        self.assertIn("order_count", defaults["orders_by_city"].columns)
        self.assertIn(COL_PAYMENT_METHOD, defaults["payment_method_distribution"].columns)
        self.assertIn(COL_ORDER_STATUS, defaults["order_status_distribution"].columns)

        for col in DISPLAY_TRANSACTION_COLUMNS:
            self.assertIn(col, defaults["recent_transactions"].columns)

    def test_no_data_condition(self):
        """Test loader when metrics file does not exist or is 0 bytes."""
        # Non-existent file
        res = load_analytics_metrics(self.metrics_file)
        self.assertEqual(res["total_orders"], 0)
        self.assertEqual(res["status"], "WAITING")
        self.assertFalse(res["is_live"])
        self.assertTrue(res["revenue_by_category"].empty)
        self.assertTrue(res["recent_transactions"].empty)

        # Empty 0-byte file
        with open(self.metrics_file, "w") as f:
            pass
        res_empty = load_analytics_metrics(self.metrics_file)
        self.assertEqual(res_empty["total_orders"], 0)
        self.assertEqual(res_empty["status"], "WAITING")

    def test_normal_data_condition(self):
        """Test loader with normal data matching analytics layer output."""
        sample_metrics = {
            "total_orders": 6,
            "completed_orders": 4,
            "returned_orders": 1,
            "completed_revenue": 6655.40,
            "orders_in_last_minute": 1,
            "revenue_by_category": [
                {"category": "Sports", "revenue": 3078.60},
                {"category": "Home & Kitchen", "revenue": 2077.80},
                {"category": "Accessories", "revenue": 1499.00},
                {"category": "Books", "revenue": 471.20},
            ],
            "orders_by_city": [
                {"city": "Salem", "order_count": 2},
                {"city": "Chennai", "order_count": 1},
                {"city": "Madurai", "order_count": 1},
                {"city": "Erode", "order_count": 1},
                {"city": "Trichy", "order_count": 1},
            ],
            "payment_method_distribution": [
                {"payment_method": "Credit Card", "order_count": 3},
                {"payment_method": "UPI", "order_count": 2},
                {"payment_method": "Debit Card", "order_count": 1},
            ],
            "order_status_distribution": [
                {"order_status": "Completed", "order_count": 4},
                {"order_status": "Returned", "order_count": 1},
                {"order_status": "Cancelled", "order_count": 1},
            ],
            "windowed_orders": [
                {"window_start": "2026-09-21 10:00:00", "window_end": "2026-09-21 10:01:00", "order_count": 5},
                {"window_start": "2026-09-21 10:02:00", "window_end": "2026-09-21 10:03:00", "order_count": 1},
            ],
            "recent_transactions": [
                {
                    "order_id": "ORD1002",
                    "product_name": "Cricket Bat",
                    "category": "Sports",
                    "total_amount": 3078.60,
                    "city": "Salem",
                    "order_status": "Completed",
                }
            ],
        }

        with open(self.metrics_file, "w", encoding="utf-8") as f:
            json.dump(sample_metrics, f)

        res = load_analytics_metrics(self.metrics_file)

        # Check KPIs
        self.assertEqual(res["total_orders"], 6)
        self.assertEqual(res["completed_orders"], 4)
        self.assertEqual(res["returned_orders"], 1)
        self.assertAlmostEqual(res["completed_revenue"], 6655.40, places=2)
        self.assertEqual(res["orders_in_last_minute"], 1)

        # Check Liveness
        self.assertTrue(res["is_live"])
        self.assertEqual(res["status"], "LIVE")
        self.assertIn("LIVE", res["status_badge"])

        # Check DataFrames
        self.assertEqual(len(res["revenue_by_category"]), 4)
        self.assertEqual(res["revenue_by_category"].iloc[0]["category"], "Sports")
        self.assertAlmostEqual(res["revenue_by_category"].iloc[0]["revenue"], 3078.60)

        self.assertEqual(len(res["orders_by_city"]), 5)
        self.assertEqual(res["orders_by_city"].iloc[0]["city"], "Salem")
        self.assertEqual(res["orders_by_city"].iloc[0]["order_count"], 2)

        self.assertEqual(len(res["payment_method_distribution"]), 3)
        self.assertEqual(len(res["order_status_distribution"]), 3)
        self.assertEqual(len(res["windowed_orders"]), 2)

        # Check Recent Transactions
        self.assertEqual(len(res["recent_transactions"]), 1)
        self.assertEqual(res["recent_transactions"].iloc[0][DISPLAY_COL_ORDER_ID], "ORD1002")
        self.assertEqual(res["recent_transactions"].iloc[0][DISPLAY_COL_PRODUCT], "Cricket Bat")
        self.assertEqual(res["recent_transactions"].iloc[0][DISPLAY_COL_STATUS], "Completed")

    def test_stale_data_condition(self):
        """Test pipeline status transitions to PAUSED when file is older than threshold."""
        sample_metrics = {"total_orders": 5, "completed_revenue": 1000.0}
        with open(self.metrics_file, "w", encoding="utf-8") as f:
            json.dump(sample_metrics, f)

        # Force modification time to 120 seconds in the past
        past_time = time.time() - 120
        os.utime(self.metrics_file, (past_time, past_time))

        res = load_analytics_metrics(self.metrics_file, stale_threshold_seconds=60)
        self.assertEqual(res["status"], "PAUSED")
        self.assertFalse(res["is_live"])
        self.assertIn("PAUSED", res["status_badge"])

    def test_corrupted_data_condition(self):
        """Test graceful error handling when file contains malformed JSON."""
        with open(self.metrics_file, "w", encoding="utf-8") as f:
            f.write("{ incomplete json: broken")

        res = load_analytics_metrics(self.metrics_file)
        self.assertEqual(res["status"], "ERROR")
        self.assertFalse(res["is_live"])
        self.assertIn("ERROR", res["status_badge"])
        self.assertIsNotNone(res["error"])

    def test_recent_transactions_from_standalone_file(self):
        """Test loading transactions from external output/recent_transactions.json."""
        transactions = [
            {
                "order_id": "ORD1001",
                "product_name": "Notebook",
                "category": "Books",
                "total_amount": 192.0,
                "city": "Erode",
                "order_status": "Returned",
            },
            {
                "order_id": "ORD1002",
                "product_name": "Cricket Bat",
                "category": "Sports",
                "total_amount": 3078.60,
                "city": "Salem",
                "order_status": "Completed",
            },
        ]
        with open(self.transactions_file, "w", encoding="utf-8") as f:
            json.dump(transactions, f)

        df = load_recent_transactions(self.transactions_file)
        self.assertEqual(len(df), 2)
        # Most recent transaction should appear first (ORD1002)
        self.assertEqual(df.iloc[0][DISPLAY_COL_ORDER_ID], "ORD1002")
        self.assertEqual(df.iloc[0][DISPLAY_COL_PRODUCT], "Cricket Bat")
        self.assertEqual(df.iloc[0][DISPLAY_COL_STATUS], "Completed")

    def test_formatting_functions(self):
        """Test presentation formatting functions."""
        self.assertEqual(format_currency(1234.5), "₹1,234.50")
        self.assertEqual(format_currency(0), "₹0.00")
        self.assertEqual(format_currency(None), "₹0.00")
        self.assertEqual(format_currency("invalid"), "₹0.00")

        self.assertEqual(format_number(1234567), "1,234,567")
        self.assertEqual(format_number(0), "0")
        self.assertEqual(format_number(None), "0")

        self.assertIn("Completed", format_status_label(STATUS_COMPLETED))
        self.assertIn("Returned", format_status_label(STATUS_RETURNED))
        self.assertIn("Cancelled", format_status_label(STATUS_CANCELLED))


if __name__ == "__main__":
    unittest.main()
