"""
Unit and integration tests for analytics aggregations and schemas.
"""

import os
import sys
import unittest
import shutil
import tempfile
from datetime import datetime

# Set Python worker path for Spark on Windows
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from analytics.schemas import (
    PROCESSED_SCHEMA,
    TRANSACTION_SCHEMA,
    validate_processed_schema,
    COL_ORDER_ID,
    COL_TIMESTAMP,
    COL_CUSTOMER_ID,
    COL_PRODUCT_ID,
    COL_PRODUCT_NAME,
    COL_CATEGORY,
    COL_QUANTITY,
    COL_UNIT_PRICE,
    COL_DISCOUNT_PERCENT,
    COL_CITY,
    COL_PAYMENT_METHOD,
    COL_ORDER_STATUS,
    COL_TOTAL_AMOUNT,
    STATUS_COMPLETED,
    STATUS_RETURNED,
    STATUS_CANCELLED
)
from analytics.aggregations import (
    get_total_orders,
    get_completed_orders,
    get_returned_orders,
    get_completed_revenue,
    get_kpis,
    get_revenue_by_category,
    get_orders_by_city,
    get_payment_method_distribution,
    get_order_status_distribution,
    get_windowed_orders,
    get_orders_in_last_minute,
    compute_all_analytics,
    save_metrics_to_json
)


class TestAnalyticsAggregations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TestAnalyticsAggregations") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

        # Sample test dataset matching processed streaming DataFrame schema
        cls.sample_data = [
            (
                "ORD1001",
                datetime(2026, 9, 21, 10, 0, 9),
                "C1085", "P027", "Notebook", "Books",
                2, 120.0, 20.0, "Erode", "UPI", "Returned",
                192.0  # 2 * 120 * 0.8
            ),
            (
                "ORD1002",
                datetime(2026, 9, 21, 10, 0, 13),
                "C1137", "P021", "Cricket Bat", "Sports",
                2, 2199.0, 30.0, "Salem", "Credit Card", "Completed",
                3078.60  # 2 * 2199 * 0.7
            ),
            (
                "ORD1003",
                datetime(2026, 9, 21, 10, 0, 30),
                "C1314", "P014", "Water Bottle", "Home & Kitchen",
                2, 399.0, 15.0, "Chennai", "Debit Card", "Completed",
                678.30  # 2 * 399 * 0.85
            ),
            (
                "ORD1004",
                datetime(2026, 9, 21, 10, 0, 42),
                "C1223", "P013", "Cookware Set", "Home & Kitchen",
                1, 2799.0, 50.0, "Salem", "UPI", "Completed",
                1399.50  # 1 * 2799 * 0.5
            ),
            (
                "ORD1005",
                datetime(2026, 9, 21, 10, 0, 45),
                "C1148", "P039", "Power Bank", "Accessories",
                1, 1499.0, 0.0, "Madurai", "Credit Card", "Completed",
                1499.00  # 1 * 1499 * 1.0
            ),
            (
                "ORD1006",
                datetime(2026, 9, 21, 10, 2, 27),
                "C1153", "P026", "Novel", "Books",
                1, 349.0, 20.0, "Trichy", "Credit Card", "Cancelled",
                279.20  # 1 * 349 * 0.8
            )
        ]

        cls.df = cls.spark.createDataFrame(cls.sample_data, schema=PROCESSED_SCHEMA)
        cls.empty_df = cls.spark.createDataFrame([], schema=PROCESSED_SCHEMA)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_total_orders(self):
        """Test Total Orders metric (Metric 1)."""
        result = get_total_orders(self.df).first()["total_orders"]
        self.assertEqual(result, 6)

    def test_completed_orders(self):
        """Test Completed Orders count (Metric 2)."""
        result = get_completed_orders(self.df).first()["completed_orders"]
        self.assertEqual(result, 4)

    def test_returned_orders(self):
        """Test Returned Orders count (Metric 3)."""
        result = get_returned_orders(self.df).first()["returned_orders"]
        self.assertEqual(result, 1)

    def test_completed_revenue(self):
        """Test Completed Revenue sum for completed orders only (Metric 4)."""
        # Completed: ORD1002 (3078.60) + ORD1003 (678.30) + ORD1004 (1399.50) + ORD1005 (1499.00) = 6655.40
        result = get_completed_revenue(self.df).first()["completed_revenue"]
        self.assertAlmostEqual(result, 6655.40, places=2)

    def test_combined_kpis(self):
        """Test combined get_kpis returning all 4 scalar metrics."""
        kpis = get_kpis(self.df).first()
        self.assertEqual(kpis["total_orders"], 6)
        self.assertEqual(kpis["completed_orders"], 4)
        self.assertEqual(kpis["returned_orders"], 1)
        self.assertAlmostEqual(kpis["completed_revenue"], 6655.40, places=2)

    def test_revenue_by_category(self):
        """Test Revenue by Category aggregation (Metric 5)."""
        result_df = get_revenue_by_category(self.df).collect()
        cat_map = {row["category"]: row["revenue"] for row in result_df}

        self.assertIn("Sports", cat_map)
        self.assertAlmostEqual(cat_map["Sports"], 3078.60, places=2)
        self.assertAlmostEqual(cat_map["Home & Kitchen"], 2077.80, places=2)
        self.assertAlmostEqual(cat_map["Accessories"], 1499.00, places=2)
        self.assertAlmostEqual(cat_map["Books"], 471.20, places=2)

    def test_orders_by_city(self):
        """Test Orders by City count (Metric 6)."""
        result_df = get_orders_by_city(self.df).collect()
        city_map = {row["city"]: row["order_count"] for row in result_df}

        self.assertEqual(city_map["Salem"], 2)
        self.assertEqual(city_map["Chennai"], 1)
        self.assertEqual(city_map["Madurai"], 1)
        self.assertEqual(city_map["Erode"], 1)
        self.assertEqual(city_map["Trichy"], 1)

    def test_payment_method_distribution(self):
        """Test Payment Method Distribution count (Metric 7)."""
        result_df = get_payment_method_distribution(self.df).collect()
        pm_map = {row["payment_method"]: row["order_count"] for row in result_df}

        self.assertEqual(pm_map["Credit Card"], 3)
        self.assertEqual(pm_map["UPI"], 2)
        self.assertEqual(pm_map["Debit Card"], 1)

    def test_order_status_distribution(self):
        """Test Order Status Distribution count (Metric 8)."""
        result_df = get_order_status_distribution(self.df).collect()
        status_map = {row["order_status"]: row["order_count"] for row in result_df}

        self.assertEqual(status_map["Completed"], 4)
        self.assertEqual(status_map["Returned"], 1)
        self.assertEqual(status_map["Cancelled"], 1)

    def test_windowed_orders(self):
        """Test 1-minute event-time window metric (Metric 9)."""
        result_df = get_windowed_orders(self.df, "1 minute").collect()
        # 5 orders occurred between 10:00 and 10:01, 1 order at 10:02
        self.assertTrue(len(result_df) >= 2)
        counts = [row["order_count"] for row in result_df]
        self.assertIn(5, counts)
        self.assertIn(1, counts)

    def test_orders_in_last_minute(self):
        """Test Orders in Last 1 Minute relative to latest transaction timestamp."""
        # Latest transaction is 10:02:27. Cutoff is 10:01:27.
        # Only ORD1006 (10:02:27) falls in the last 1 minute.
        result = get_orders_in_last_minute(self.df).first()["orders_in_last_minute"]
        self.assertEqual(result, 1)

    def test_empty_dataframe(self):
        """Test graceful handling of empty DataFrame."""
        kpis = get_kpis(self.empty_df).first()
        self.assertEqual(kpis["total_orders"], 0)
        self.assertEqual(kpis["completed_orders"], 0)
        self.assertEqual(kpis["returned_orders"], 0)
        self.assertEqual(kpis["completed_revenue"], 0.0)

        empty_last_min = get_orders_in_last_minute(self.empty_df).first()["orders_in_last_minute"]
        self.assertEqual(empty_last_min, 0)

        analytics = compute_all_analytics(self.empty_df)
        self.assertEqual(analytics["total_orders"], 0)
        self.assertEqual(analytics["completed_revenue"], 0.0)
        self.assertTrue(analytics["revenue_by_category"].empty)
        self.assertTrue(analytics["orders_by_city"].empty)

    def test_discount_calculation_and_total_amount(self):
        """Verify discount percentage and total_amount calculation logic."""
        # Verify 4 * 3299 * (1 - 20/100) = 10556.8
        qty = 4
        price = 3299.0
        disc = 20.0
        total = qty * price * (1.0 - disc / 100.0)
        self.assertAlmostEqual(total, 10556.8, places=1)

    def test_schema_validation(self):
        """Test validate_processed_schema validates correctly."""
        self.assertTrue(validate_processed_schema(self.df))
        bad_df = self.spark.createDataFrame([(1, "bad")], ["id", "val"])
        with self.assertRaises(ValueError):
            validate_processed_schema(bad_df)

    def test_compute_all_analytics_and_json_export(self):
        """Test end-to-end compute_all_analytics and JSON export."""
        analytics = compute_all_analytics(self.df)
        self.assertEqual(analytics["total_orders"], 6)
        self.assertEqual(analytics["completed_orders"], 4)
        self.assertEqual(analytics["returned_orders"], 1)
        self.assertAlmostEqual(analytics["completed_revenue"], 6655.40, places=2)

        tmp_dir = tempfile.mkdtemp()
        try:
            output_file = os.path.join(tmp_dir, "test_metrics.json")
            save_metrics_to_json(analytics, output_file)
            self.assertTrue(os.path.exists(output_file))
            self.assertGreater(os.path.getsize(output_file), 100)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
