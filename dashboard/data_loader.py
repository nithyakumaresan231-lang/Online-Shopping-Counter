"""
Data loading and pipeline integration utilities for the Streamlit dashboard.
Preserves the project architecture by reading aggregated analytics from the
analytics layer output without recalculating business metrics.
"""

import os
import sys
import json
import time
from datetime import datetime
from typing import Optional, Dict, Any, List

import pandas as pd

# Add project root to sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from analytics.schemas import (
    COL_ORDER_ID,
    COL_PRODUCT_NAME,
    COL_CATEGORY,
    COL_TOTAL_AMOUNT,
    COL_CITY,
    COL_ORDER_STATUS,
    COL_PAYMENT_METHOD,
    STATUS_COMPLETED,
    STATUS_RETURNED,
    STATUS_CANCELLED,
)

# Standard directory and file paths
DEFAULT_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
DEFAULT_METRICS_PATH = os.path.join(DEFAULT_OUTPUT_DIR, "latest_metrics.json")
DEFAULT_TRANSACTIONS_PATH = os.path.join(DEFAULT_OUTPUT_DIR, "recent_transactions.json")

# Standard transaction table column names for presentation
DISPLAY_COL_ORDER_ID = "Order ID"
DISPLAY_COL_PRODUCT = "Product"
DISPLAY_COL_CATEGORY = "Category"
DISPLAY_COL_AMOUNT = "Amount"
DISPLAY_COL_CITY = "City"
DISPLAY_COL_STATUS = "Status"

DISPLAY_TRANSACTION_COLUMNS = [
    DISPLAY_COL_ORDER_ID,
    DISPLAY_COL_PRODUCT,
    DISPLAY_COL_CATEGORY,
    DISPLAY_COL_AMOUNT,
    DISPLAY_COL_CITY,
    DISPLAY_COL_STATUS,
]


def format_currency(value: Any) -> str:
    """Format numeric value into readable currency string (₹)."""
    if value is None:
        return "₹0.00"
    try:
        val_float = float(value)
        return f"₹{val_float:,.2f}"
    except (ValueError, TypeError):
        return "₹0.00"


def format_number(value: Any) -> str:
    """Format numeric value into comma-separated integer string."""
    if value is None:
        return "0"
    try:
        val_int = int(value)
        return f"{val_int:,}"
    except (ValueError, TypeError):
        return "0"


def format_status_label(status: Any) -> str:
    """
    Format order status for presentation with clear textual labels
    and accessible visual indicators (not relying on color alone).
    """
    if not status or not isinstance(status, str):
        return "Unknown"
    cleaned = status.strip()
    if cleaned.lower() == STATUS_COMPLETED.lower():
        return "✅ Completed"
    elif cleaned.lower() == STATUS_RETURNED.lower():
        return "↩️ Returned"
    elif cleaned.lower() == STATUS_CANCELLED.lower():
        return "❌ Cancelled"
    return cleaned


def get_default_metrics() -> Dict[str, Any]:
    """
    Returns default empty metrics structure when no streaming data
    has reached the dashboard yet or when the output file is absent.
    Prevents dashboard crashes and ensures a clean no-data state.
    """
    return {
        "total_orders": 0,
        "completed_orders": 0,
        "returned_orders": 0,
        "completed_revenue": 0.0,
        "orders_in_last_minute": 0,
        "revenue_by_category": pd.DataFrame(columns=[COL_CATEGORY, "revenue"]),
        "orders_by_city": pd.DataFrame(columns=[COL_CITY, "order_count"]),
        "payment_method_distribution": pd.DataFrame(columns=[COL_PAYMENT_METHOD, "order_count"]),
        "order_status_distribution": pd.DataFrame(columns=[COL_ORDER_STATUS, "order_count"]),
        "windowed_orders": pd.DataFrame(columns=["window_start", "window_end", "order_count"]),
        "recent_transactions": pd.DataFrame(columns=DISPLAY_TRANSACTION_COLUMNS),
        "status": "WAITING",
        "status_badge": "⚪ WAITING",
        "status_message": "Waiting for streaming data...",
        "last_updated": None,
        "last_updated_ts": None,
        "is_live": False,
        "error": None,
        "has_data": False,
    }


def normalize_transactions_df(raw_df: pd.DataFrame, limit: int = 20) -> pd.DataFrame:
    """
    Normalizes a raw transactions DataFrame into the required presentation schema:
    [Order ID, Product, Category, Amount, City, Status].
    """
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=DISPLAY_TRANSACTION_COLUMNS)

    df = raw_df.copy()

    # Column mapping dictionary
    mapping = {
        COL_ORDER_ID: DISPLAY_COL_ORDER_ID,
        "orderId": DISPLAY_COL_ORDER_ID,
        COL_PRODUCT_NAME: DISPLAY_COL_PRODUCT,
        "product": DISPLAY_COL_PRODUCT,
        "product_id": DISPLAY_COL_PRODUCT,
        COL_CATEGORY: DISPLAY_COL_CATEGORY,
        COL_TOTAL_AMOUNT: DISPLAY_COL_AMOUNT,
        "amount": DISPLAY_COL_AMOUNT,
        "unit_price": DISPLAY_COL_AMOUNT,
        COL_CITY: DISPLAY_COL_CITY,
        COL_ORDER_STATUS: DISPLAY_COL_STATUS,
        "status": DISPLAY_COL_STATUS,
    }

    # Rename existing columns
    renames = {col: mapping[col] for col in df.columns if col in mapping}
    df = df.rename(columns=renames)

    # Ensure required columns exist
    for col in DISPLAY_TRANSACTION_COLUMNS:
        if col not in df.columns:
            df[col] = "—"

    # Coerce Amount to float where possible
    try:
        df[DISPLAY_COL_AMOUNT] = pd.to_numeric(df[DISPLAY_COL_AMOUNT], errors="coerce").fillna(0.0)
    except Exception:
        pass

    # Coerce string types
    for str_col in [DISPLAY_COL_ORDER_ID, DISPLAY_COL_PRODUCT, DISPLAY_COL_CATEGORY, DISPLAY_COL_CITY, DISPLAY_COL_STATUS]:
        df[str_col] = df[str_col].astype(str)

    # Return top N records
    result = df[DISPLAY_TRANSACTION_COLUMNS].tail(limit)
    # Reverse so most recent transaction appears first
    return result.iloc[::-1].reset_index(drop=True)


def load_recent_transactions(transactions_path: Optional[str] = None, limit: int = 20) -> pd.DataFrame:
    """
    Loads recent transactions from output/recent_transactions.json or related candidate files.
    Returns a normalized DataFrame with columns [Order ID, Product, Category, Amount, City, Status].
    Returns an empty DataFrame if no file is present.
    """
    candidates = []
    if transactions_path:
        candidates.append(transactions_path)
    candidates.extend([
        DEFAULT_TRANSACTIONS_PATH,
        os.path.join(DEFAULT_OUTPUT_DIR, "latest_transactions.json"),
        os.path.join(DEFAULT_OUTPUT_DIR, "transactions.json"),
        os.path.join(DEFAULT_OUTPUT_DIR, "recent_transactions.csv"),
    ])

    for path in candidates:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            continue

        try:
            if path.endswith(".csv"):
                raw_df = pd.read_csv(path)
                return normalize_transactions_df(raw_df, limit)
            else:
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                if not content:
                    continue

                try:
                    data = json.loads(content)
                    if isinstance(data, list):
                        raw_df = pd.DataFrame(data)
                        return normalize_transactions_df(raw_df, limit)
                    elif isinstance(data, dict):
                        items = data.get("transactions") or data.get("recent_transactions") or [data]
                        raw_df = pd.DataFrame(items)
                        return normalize_transactions_df(raw_df, limit)
                except json.JSONDecodeError:
                    # Attempt line-by-line NDJSON parsing
                    lines = [json.loads(line) for line in content.splitlines() if line.strip()]
                    if lines:
                        raw_df = pd.DataFrame(lines)
                        return normalize_transactions_df(raw_df, limit)

        except Exception:
            # Continue checking next candidate without crashing
            continue

    return pd.DataFrame(columns=DISPLAY_TRANSACTION_COLUMNS)


def load_analytics_metrics(
    metrics_path: Optional[str] = None,
    transactions_path: Optional[str] = None,
    stale_threshold_seconds: int = 60
) -> Dict[str, Any]:
    """
    Loads the latest analytics metrics computed by the analytics/Spark layer.
    Preserves existing analytics layer aggregations (from output/latest_metrics.json)
    and handles missing, corrupt, or empty files gracefully.

    Parameters:
        metrics_path: Path to latest_metrics.json (defaults to output/latest_metrics.json)
        transactions_path: Optional path to recent transactions file
        stale_threshold_seconds: Seconds of inactivity before pipeline is marked PAUSED

    Returns:
        Dictionary containing all 5 KPIs, 4 distribution DataFrames, recent transactions,
        and pipeline health/live indicator metadata.
    """
    if metrics_path is None:
        metrics_path = os.environ.get("ANALYTICS_OUTPUT_PATH", DEFAULT_METRICS_PATH)

    if transactions_path is None:
        transactions_path = os.environ.get("TRANSACTIONS_OUTPUT_PATH", DEFAULT_TRANSACTIONS_PATH)

    # 1. No-data state check
    if not os.path.exists(metrics_path) or os.path.getsize(metrics_path) == 0:
        default_res = get_default_metrics()
        # Also check if recent transactions exist independently
        tx_df = load_recent_transactions(transactions_path)
        if not tx_df.empty:
            default_res["recent_transactions"] = tx_df
        return default_res

    # 2. Safely read metrics JSON
    raw_data = None
    mtime = os.path.getmtime(metrics_path)

    for attempt in range(2):
        try:
            with open(metrics_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
            if not content:
                return get_default_metrics()
            raw_data = json.loads(content)
            break
        except (json.JSONDecodeError, OSError) as e:
            if attempt == 0:
                time.sleep(0.05)  # Brief delay to resolve file write race condition
                continue
            err_res = get_default_metrics()
            err_res["status"] = "ERROR"
            err_res["status_badge"] = "🔴 ERROR"
            err_res["status_message"] = f"Error reading analytics output: {str(e)}"
            err_res["error"] = str(e)
            return err_res

    if not isinstance(raw_data, dict):
        err_res = get_default_metrics()
        err_res["status"] = "ERROR"
        err_res["status_badge"] = "🔴 ERROR"
        err_res["status_message"] = "Invalid analytics format (expected JSON object)"
        return err_res

    # 3. Extract core KPIs
    try:
        total_orders = int(raw_data.get("total_orders", 0))
    except (ValueError, TypeError):
        total_orders = 0

    try:
        completed_orders = int(raw_data.get("completed_orders", 0))
    except (ValueError, TypeError):
        completed_orders = 0

    try:
        returned_orders = int(raw_data.get("returned_orders", 0))
    except (ValueError, TypeError):
        returned_orders = 0

    try:
        completed_revenue = float(raw_data.get("completed_revenue", 0.0))
    except (ValueError, TypeError):
        completed_revenue = 0.0

    try:
        orders_in_last_minute = int(raw_data.get("orders_in_last_minute", 0))
    except (ValueError, TypeError):
        orders_in_last_minute = 0

    # 4. Extract Category Revenue DataFrame
    raw_cat = raw_data.get("revenue_by_category", [])
    if isinstance(raw_cat, list) and len(raw_cat) > 0:
        cat_df = pd.DataFrame(raw_cat)
        if COL_CATEGORY not in cat_df.columns:
            cat_df[COL_CATEGORY] = "Unknown"
        if "revenue" not in cat_df.columns:
            cat_df["revenue"] = 0.0
        cat_df["revenue"] = pd.to_numeric(cat_df["revenue"], errors="coerce").fillna(0.0)
        cat_df = cat_df.sort_values(by="revenue", ascending=False).reset_index(drop=True)
    else:
        cat_df = pd.DataFrame(columns=[COL_CATEGORY, "revenue"])

    # 5. Extract City Orders DataFrame
    raw_city = raw_data.get("orders_by_city", [])
    if isinstance(raw_city, list) and len(raw_city) > 0:
        city_df = pd.DataFrame(raw_city)
        if COL_CITY not in city_df.columns:
            city_df[COL_CITY] = "Unknown"
        if "order_count" not in city_df.columns:
            city_df["order_count"] = 0
        city_df["order_count"] = pd.to_numeric(city_df["order_count"], errors="coerce").fillna(0).astype(int)
        city_df = city_df.sort_values(by="order_count", ascending=False).reset_index(drop=True)
    else:
        city_df = pd.DataFrame(columns=[COL_CITY, "order_count"])

    # 6. Extract Payment Method Distribution DataFrame
    raw_pm = raw_data.get("payment_method_distribution", [])
    if isinstance(raw_pm, list) and len(raw_pm) > 0:
        pm_df = pd.DataFrame(raw_pm)
        if COL_PAYMENT_METHOD not in pm_df.columns:
            pm_df[COL_PAYMENT_METHOD] = "Unknown"
        if "order_count" not in pm_df.columns:
            pm_df["order_count"] = 0
        pm_df["order_count"] = pd.to_numeric(pm_df["order_count"], errors="coerce").fillna(0).astype(int)
        pm_df = pm_df.sort_values(by="order_count", ascending=False).reset_index(drop=True)
    else:
        pm_df = pd.DataFrame(columns=[COL_PAYMENT_METHOD, "order_count"])

    # 7. Extract Order Status Distribution DataFrame
    raw_status = raw_data.get("order_status_distribution", [])
    if isinstance(raw_status, list) and len(raw_status) > 0:
        status_df = pd.DataFrame(raw_status)
        if COL_ORDER_STATUS not in status_df.columns:
            status_df[COL_ORDER_STATUS] = "Unknown"
        if "order_count" not in status_df.columns:
            status_df["order_count"] = 0
        status_df["order_count"] = pd.to_numeric(status_df["order_count"], errors="coerce").fillna(0).astype(int)
        status_df = status_df.sort_values(by="order_count", ascending=False).reset_index(drop=True)
    else:
        status_df = pd.DataFrame(columns=[COL_ORDER_STATUS, "order_count"])

    # 8. Extract Windowed Orders DataFrame
    raw_win = raw_data.get("windowed_orders", [])
    if isinstance(raw_win, list) and len(raw_win) > 0:
        win_df = pd.DataFrame(raw_win)
        for col in ["window_start", "window_end", "order_count"]:
            if col not in win_df.columns:
                win_df[col] = "—" if col != "order_count" else 0
        win_df["order_count"] = pd.to_numeric(win_df["order_count"], errors="coerce").fillna(0).astype(int)
    else:
        win_df = pd.DataFrame(columns=["window_start", "window_end", "order_count"])

    # 9. Extract Recent Transactions
    if "recent_transactions" in raw_data and isinstance(raw_data["recent_transactions"], list):
        tx_df = normalize_transactions_df(pd.DataFrame(raw_data["recent_transactions"]))
    else:
        tx_df = load_recent_transactions(transactions_path)

    # 10. Compute Pipeline Liveness & Status
    now = time.time()
    age_seconds = max(0.0, now - mtime)
    last_updated_dt = datetime.fromtimestamp(mtime)
    last_updated_str = last_updated_dt.strftime("%Y-%m-%d %H:%M:%S")

    has_data = total_orders > 0

    if not has_data:
        status = "WAITING"
        status_badge = "⚪ WAITING"
        status_message = "Waiting for streaming data..."
        is_live = False
    elif age_seconds <= stale_threshold_seconds:
        status = "LIVE"
        status_badge = "🟢 LIVE • Kafka + Spark Streaming"
        status_message = f"LIVE • Streaming active (updated {int(age_seconds)}s ago)"
        is_live = True
    else:
        status = "PAUSED"
        status_badge = "🟡 PAUSED • Stream Inactive"
        status_message = f"PAUSED • Stream inactive (last active: {last_updated_str})"
        is_live = False

    return {
        "total_orders": total_orders,
        "completed_orders": completed_orders,
        "returned_orders": returned_orders,
        "completed_revenue": completed_revenue,
        "orders_in_last_minute": orders_in_last_minute,
        "revenue_by_category": cat_df,
        "orders_by_city": city_df,
        "payment_method_distribution": pm_df,
        "order_status_distribution": status_df,
        "windowed_orders": win_df,
        "recent_transactions": tx_df,
        "status": status,
        "status_badge": status_badge,
        "status_message": status_message,
        "last_updated": last_updated_str,
        "last_updated_ts": mtime,
        "age_seconds": age_seconds,
        "is_live": is_live,
        "error": None,
        "has_data": has_data,
        "raw_json": raw_data,
    }
