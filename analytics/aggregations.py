"""
Real-time Analytics Aggregations for Online Shopping Streaming.

Provides Spark Structured Streaming-compatible transformations to compute:
1. Total Orders
2. Completed Orders
3. Returned Orders
4. Completed Revenue
5. Revenue by Category
6. Orders by City
7. Payment Method Distribution
8. Order Status Distribution
9. 1-Minute Window Metric (Orders in Last 1 Minute)
"""

import os
import sys
import json
from datetime import timedelta

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import pandas as pd
except ImportError:
    pd = None

# Ensure Spark Python worker uses current Python interpreter
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    sum,
    when,
    round,
    coalesce,
    lit,
    window,
    max as spark_max
)
from pyspark.sql.types import StructType, StructField, LongType

from analytics.schemas import (
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
    validate_processed_schema
)


# ==============================================================================
# Metric 1: Total Orders
# ==============================================================================
def get_total_orders(df: DataFrame) -> DataFrame:
    """
    Count all processed orders.
    Compatible with both streaming and batch DataFrames.
    """
    return df.groupBy().agg(count("*").alias("total_orders"))


# ==============================================================================
# Metric 2: Completed Orders
# ==============================================================================
def get_completed_orders(df: DataFrame) -> DataFrame:
    """
    Count records where order_status indicates Completed.
    Returns 0 if no completed orders exist.
    """
    return df.groupBy().agg(
        count(when(col(COL_ORDER_STATUS) == STATUS_COMPLETED, 1)).alias("completed_orders")
    )


# ==============================================================================
# Metric 3: Returned Orders
# ==============================================================================
def get_returned_orders(df: DataFrame) -> DataFrame:
    """
    Count records where order_status indicates Returned.
    Returns 0 if no returned orders exist.
    """
    return df.groupBy().agg(
        count(when(col(COL_ORDER_STATUS) == STATUS_RETURNED, 1)).alias("returned_orders")
    )


# ==============================================================================
# Metric 4: Completed Revenue
# ==============================================================================
def get_completed_revenue(df: DataFrame) -> DataFrame:
    """
    Sum total_amount for completed orders only.
    Returns 0.0 if no completed orders exist.
    """
    return df.groupBy().agg(
        coalesce(
            round(sum(when(col(COL_ORDER_STATUS) == STATUS_COMPLETED, col(COL_TOTAL_AMOUNT))), 2),
            lit(0.0)
        ).alias("completed_revenue")
    )


# ==============================================================================
# Combined Core KPIs (Metrics 1 to 4)
# ==============================================================================
def get_kpis(df: DataFrame) -> DataFrame:
    """
    Calculate the 4 core scalar KPIs in a single efficient aggregation:
    total_orders, completed_orders, returned_orders, completed_revenue.
    """
    return df.groupBy().agg(
        count("*").alias("total_orders"),
        count(when(col(COL_ORDER_STATUS) == STATUS_COMPLETED, 1)).alias("completed_orders"),
        count(when(col(COL_ORDER_STATUS) == STATUS_RETURNED, 1)).alias("returned_orders"),
        coalesce(
            round(sum(when(col(COL_ORDER_STATUS) == STATUS_COMPLETED, col(COL_TOTAL_AMOUNT))), 2),
            lit(0.0)
        ).alias("completed_revenue")
    )


# ==============================================================================
# Metric 5: Revenue by Category
# ==============================================================================
def get_revenue_by_category(df: DataFrame) -> DataFrame:
    """
    Group revenue by category, rounded to 2 decimal places.
    """
    agg_df = df.groupBy(COL_CATEGORY).agg(
        round(sum(COL_TOTAL_AMOUNT), 2).alias("revenue")
    )
    try:
        return agg_df.orderBy(col("revenue").desc())
    except Exception:
        return agg_df


# ==============================================================================
# Metric 6: Orders by City
# ==============================================================================
def get_orders_by_city(df: DataFrame) -> DataFrame:
    """
    Count orders grouped by city.
    """
    agg_df = df.groupBy(COL_CITY).agg(
        count("*").alias("order_count")
    )
    try:
        return agg_df.orderBy(col("order_count").desc())
    except Exception:
        return agg_df


# ==============================================================================
# Metric 7: Payment Method Distribution
# ==============================================================================
def get_payment_method_distribution(df: DataFrame) -> DataFrame:
    """
    Count orders grouped by payment_method.
    """
    agg_df = df.groupBy(COL_PAYMENT_METHOD).agg(
        count("*").alias("order_count")
    )
    try:
        return agg_df.orderBy(col("order_count").desc())
    except Exception:
        return agg_df


# ==============================================================================
# Metric 8: Order Status Distribution
# ==============================================================================
def get_order_status_distribution(df: DataFrame) -> DataFrame:
    """
    Count orders grouped by order_status.
    """
    agg_df = df.groupBy(COL_ORDER_STATUS).agg(
        count("*").alias("order_count")
    )
    try:
        return agg_df.orderBy(col("order_count").desc())
    except Exception:
        return agg_df


# ==============================================================================
# Metric 9: 1-Minute Window Metric
# ==============================================================================
def get_windowed_orders(df: DataFrame, window_duration: str = "1 minute", slide_duration: str = None) -> DataFrame:
    """
    Event-time streaming window aggregation: count orders per time window
    based on the transaction timestamp.
    """
    if slide_duration:
        win_col = window(col(COL_TIMESTAMP), window_duration, slide_duration)
    else:
        win_col = window(col(COL_TIMESTAMP), window_duration)

    agg_df = df.groupBy(win_col).agg(
        count("*").alias("order_count")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("order_count")
    )
    try:
        return agg_df.orderBy(col("window_start").desc())
    except Exception:
        return agg_df


def get_orders_in_last_minute(df: DataFrame) -> DataFrame:
    """
    Calculate orders in the last 1 minute relative to event time (transaction timestamp).
    For streaming DataFrames, returns the windowed aggregation.
    For batch / micro-batch DataFrames, calculates orders within 1 minute of the latest event time.
    """
    if df.isStreaming:
        return get_windowed_orders(df, "1 minute")

    max_row = df.select(spark_max(col(COL_TIMESTAMP))).first()
    if max_row is None or max_row[0] is None:
        schema = StructType([StructField("orders_in_last_minute", LongType(), False)])
        return df.sparkSession.createDataFrame([(0,)], schema)

    latest_ts = max_row[0]
    cutoff = latest_ts - timedelta(minutes=1)
    return df.filter(col(COL_TIMESTAMP) >= cutoff).groupBy().agg(
        count("*").alias("orders_in_last_minute")
    )


# ==============================================================================
# Pipeline & Dashboard Integration Utilities
# ==============================================================================
def compute_all_analytics(df: DataFrame) -> dict:
    """
    Evaluates all 9 metrics on a batch or collected micro-batch DataFrame
    and returns a clean, structured Python dictionary suitable for consumption
    by the Streamlit dashboard.
    Handles empty DataFrames safely.
    """
    validate_processed_schema(df)

    is_empty = False
    try:
        if df.isEmpty():
            is_empty = True
    except Exception:
        if df.count() == 0:
            is_empty = True

    if is_empty:
        return {
            "total_orders": 0,
            "completed_orders": 0,
            "returned_orders": 0,
            "completed_revenue": 0.0,
            "orders_in_last_minute": 0,
            "revenue_by_category": pd.DataFrame(columns=[COL_CATEGORY, "revenue"]) if pd is not None else [],
            "orders_by_city": pd.DataFrame(columns=[COL_CITY, "order_count"]) if pd is not None else [],
            "payment_method_distribution": pd.DataFrame(columns=[COL_PAYMENT_METHOD, "order_count"]) if pd is not None else [],
            "order_status_distribution": pd.DataFrame(columns=[COL_ORDER_STATUS, "order_count"]) if pd is not None else [],
            "windowed_orders": pd.DataFrame(columns=["window_start", "window_end", "order_count"]) if pd is not None else []
        }

    kpi_row = get_kpis(df).first()
    total_orders = int(kpi_row["total_orders"]) if kpi_row and kpi_row["total_orders"] is not None else 0
    completed_orders = int(kpi_row["completed_orders"]) if kpi_row and kpi_row["completed_orders"] is not None else 0
    returned_orders = int(kpi_row["returned_orders"]) if kpi_row and kpi_row["returned_orders"] is not None else 0
    completed_revenue = float(kpi_row["completed_revenue"]) if kpi_row and kpi_row["completed_revenue"] is not None else 0.0

    last_min_row = get_orders_in_last_minute(df).first()
    orders_in_last_minute = int(last_min_row["orders_in_last_minute"]) if last_min_row and "orders_in_last_minute" in last_min_row else 0

    rev_by_cat = get_revenue_by_category(df).toPandas() if pd is not None else [r.asDict() for r in get_revenue_by_category(df).collect()]
    orders_by_city = get_orders_by_city(df).toPandas() if pd is not None else [r.asDict() for r in get_orders_by_city(df).collect()]
    payment_dist = get_payment_method_distribution(df).toPandas() if pd is not None else [r.asDict() for r in get_payment_method_distribution(df).collect()]
    status_dist = get_order_status_distribution(df).toPandas() if pd is not None else [r.asDict() for r in get_order_status_distribution(df).collect()]
    win_orders = get_windowed_orders(df, "1 minute").toPandas() if pd is not None else [r.asDict() for r in get_windowed_orders(df, "1 minute").collect()]

    return {
        "total_orders": total_orders,
        "completed_orders": completed_orders,
        "returned_orders": returned_orders,
        "completed_revenue": completed_revenue,
        "orders_in_last_minute": orders_in_last_minute,
        "revenue_by_category": rev_by_cat,
        "orders_by_city": orders_by_city,
        "payment_method_distribution": payment_dist,
        "order_status_distribution": status_dist,
        "windowed_orders": win_orders
    }


def save_metrics_to_json(metrics: dict, output_path: str = None) -> str:
    """
    Saves computed metrics to a JSON file for the Streamlit dashboard to consume.
    """
    if output_path is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        output_path = os.path.join(base_dir, "output", "latest_metrics.json")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    serializable = {
        "total_orders": metrics["total_orders"],
        "completed_orders": metrics["completed_orders"],
        "returned_orders": metrics["returned_orders"],
        "completed_revenue": metrics["completed_revenue"],
        "orders_in_last_minute": metrics["orders_in_last_minute"],
        "revenue_by_category": metrics["revenue_by_category"].to_dict(orient="records") if (pd is not None and isinstance(metrics["revenue_by_category"], pd.DataFrame)) else metrics["revenue_by_category"],
        "orders_by_city": metrics["orders_by_city"].to_dict(orient="records") if (pd is not None and isinstance(metrics["orders_by_city"], pd.DataFrame)) else metrics["orders_by_city"],
        "payment_method_distribution": metrics["payment_method_distribution"].to_dict(orient="records") if (pd is not None and isinstance(metrics["payment_method_distribution"], pd.DataFrame)) else metrics["payment_method_distribution"],
        "order_status_distribution": metrics["order_status_distribution"].to_dict(orient="records") if (pd is not None and isinstance(metrics["order_status_distribution"], pd.DataFrame)) else metrics["order_status_distribution"],
    }

    if "windowed_orders" in metrics:
        if pd is not None and isinstance(metrics["windowed_orders"], pd.DataFrame):
            win_df = metrics["windowed_orders"].copy()
            if not win_df.empty:
                win_df["window_start"] = win_df["window_start"].astype(str)
                win_df["window_end"] = win_df["window_end"].astype(str)
            serializable["windowed_orders"] = win_df.to_dict(orient="records")
        elif isinstance(metrics["windowed_orders"], list):
            formatted_win = []
            for item in metrics["windowed_orders"]:
                c = dict(item)
                if "window_start" in c:
                    c["window_start"] = str(c["window_start"])
                if "window_end" in c:
                    c["window_end"] = str(c["window_end"])
                formatted_win.append(c)
            serializable["windowed_orders"] = formatted_win
        else:
            serializable["windowed_orders"] = metrics["windowed_orders"]

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, indent=2)

    return output_path


def process_streaming_batch(batch_df: DataFrame, batch_id: int, output_path: str = None):
    """
    PySpark foreachBatch sink processor:
    Computes analytics on each micro-batch and writes the latest aggregated summary
    to the output folder for the dashboard.
    """
    if batch_df.isEmpty():
        return

    metrics = compute_all_analytics(batch_df)
    save_path = save_metrics_to_json(metrics, output_path)
    print(f"Batch {batch_id}: Processed {metrics['total_orders']} orders. Metrics saved to {save_path}")


def start_analytics_stream(processed_df: DataFrame, checkpoint_location: str = "/tmp/spark_analytics_checkpoint", output_path: str = None):
    """
    Starts a streaming query on the processed DataFrame using foreachBatch to compute
    and export real-time analytics metrics for the dashboard.
    """
    return processed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, batch_id: process_streaming_batch(df, batch_id, output_path)) \
        .option("checkpointLocation", checkpoint_location) \
        .start()
