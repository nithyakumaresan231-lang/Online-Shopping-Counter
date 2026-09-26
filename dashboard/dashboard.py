"""
Streamlit Real-Time Analytics Dashboard.
Phase 5 of Online Shopping Streaming Analytics.

Displays streaming KPIs, distribution charts, and recent transactions
directly from the analytics layer output (output/latest_metrics.json).
"""

import os
import sys
import time
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from data_loader import (
    load_analytics_metrics,
    format_currency,
    format_number,
    format_status_label,
    DISPLAY_COL_ORDER_ID,
    DISPLAY_COL_PRODUCT,
    DISPLAY_COL_CATEGORY,
    DISPLAY_COL_AMOUNT,
    DISPLAY_COL_CITY,
    DISPLAY_COL_STATUS,
    DEFAULT_METRICS_PATH,
    DEFAULT_TRANSACTIONS_PATH,
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

# -----------------------------------------------------------------------------
# Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Online Shopping Streaming Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------------------------------------------------------
# CSS Styling (Accessible, modern cards, subtle shadows, clean typography)
# -----------------------------------------------------------------------------
st.markdown(
    """
    <style>
    /* Metric Card Styling */
    div[data-testid="stMetric"] {
        background-color: var(--secondary-background-color, #f8f9fa);
        border: 1px solid rgba(128, 128, 128, 0.2);
        padding: 14px 18px;
        border-radius: 10px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    div[data-testid="stMetric"]:hover {
        box-shadow: 0 4px 10px rgba(0, 0, 0, 0.08);
    }
    div[data-testid="stMetricLabel"] p {
        font-size: 0.88rem !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: var(--text-color, #495057);
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.75rem !important;
        font-weight: 700 !important;
        color: var(--text-color, #212529);
    }

    /* Live Status Badge Styling */
    .status-badge {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 6px 14px;
        border-radius: 20px;
        font-size: 0.88rem;
        font-weight: 600;
        letter-spacing: 0.3px;
    }
    .status-live {
        background-color: #d4edda;
        color: #155724;
        border: 1px solid #c3e6cb;
    }
    .status-paused {
        background-color: #fff3cd;
        color: #856404;
        border: 1px solid #ffeeba;
    }
    .status-waiting {
        background-color: #e2e3e5;
        color: #383d41;
        border: 1px solid #d6d8db;
    }
    .status-error {
        background-color: #f8d7da;
        color: #721c24;
        border: 1px solid #f5c6cb;
    }

    /* Section Subheadings */
    .section-title {
        font-size: 1.15rem;
        font-weight: 600;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
        padding-bottom: 0.25rem;
        border-bottom: 2px solid rgba(128, 128, 128, 0.15);
        color: var(--text-color, #212529);
    }

    /* Sidebar info block */
    .pipeline-info-box {
        background-color: var(--secondary-background-color, #f8f9fa);
        border: 1px solid rgba(128, 128, 128, 0.2);
        padding: 12px;
        border-radius: 8px;
        font-size: 0.82rem;
        margin-top: 10px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# Sidebar: Controls & Navigation
# -----------------------------------------------------------------------------
with st.sidebar:
    st.title("🛒 Shopping Stream")
    st.caption("Apache Kafka • Spark Structured Streaming")

    st.markdown("---")
    nav_option = st.radio(
        "Navigation",
        options=["📊 Dashboard", "⚡ Live Stream", "📈 Analytics", "📋 Transactions"],
        index=0,
    )

    st.markdown("---")
    st.subheader("Streaming Controls")

    auto_refresh = st.checkbox(
        "Auto-refresh",
        value=True,
        help="Automatically refresh analytics on new streaming batches",
    )

    refresh_interval = st.select_slider(
        "Refresh Interval",
        options=[1, 2, 3, 5, 10],
        value=2,
        format_func=lambda x: f"{x} sec{'s' if x > 1 else ''}",
    )

    if st.button("🔄 Refresh Now", use_container_width=True):
        st.rerun()

    st.markdown("---")
    st.subheader("Pipeline Information")
    st.markdown(
        f"""
        <div class="pipeline-info-box">
            <b>Kafka Topic:</b> <code>shopping-transactions</code><br>
            <b>Broker:</b> <code>localhost:9092</code><br>
            <b>Processor:</b> <code>Spark Streaming</code><br>
            <b>Analytics Sink:</b> <code>output/latest_metrics.json</code><br>
            <b>Current Time:</b> {datetime.now().strftime('%H:%M:%S')}
        </div>
        """,
        unsafe_allow_html=True,
    )

# -----------------------------------------------------------------------------
# Chart Rendering Functions
# -----------------------------------------------------------------------------
def render_revenue_by_category_chart(df: pd.DataFrame):
    """Render horizontal bar chart for revenue by category."""
    st.markdown('<div class="section-title">Revenue by Category</div>', unsafe_allow_html=True)
    if df.empty or df["revenue"].sum() == 0:
        st.info("Waiting for category revenue data...")
        return

    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusEnd=4, height=22)
        .encode(
            x=alt.X("revenue:Q", title="Completed Revenue (₹)", axis=alt.Axis(format="~s")),
            y=alt.Y(f"{COL_CATEGORY}:N", title="", sort="-x"),
            color=alt.Color(f"{COL_CATEGORY}:N", legend=None, scale=alt.Scale(scheme="tableau10")),
            tooltip=[
                alt.Tooltip(f"{COL_CATEGORY}:N", title="Category"),
                alt.Tooltip("revenue:Q", title="Revenue", format=", .2f"),
            ],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)


def render_orders_by_city_chart(df: pd.DataFrame):
    """Render vertical bar chart for orders by city."""
    st.markdown('<div class="section-title">Orders by City</div>', unsafe_allow_html=True)
    if df.empty or df["order_count"].sum() == 0:
        st.info("Waiting for city order data...")
        return

    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadius=4)
        .encode(
            x=alt.X(f"{COL_CITY}:N", title="", sort="-y"),
            y=alt.Y("order_count:Q", title="Orders", axis=alt.Axis(tickMinStep=1)),
            color=alt.Color(f"{COL_CITY}:N", legend=None, scale=alt.Scale(scheme="category20b")),
            tooltip=[
                alt.Tooltip(f"{COL_CITY}:N", title="City"),
                alt.Tooltip("order_count:Q", title="Orders", format=","),
            ],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)


def render_payment_method_chart(df: pd.DataFrame):
    """Render donut chart for payment method distribution."""
    st.markdown('<div class="section-title">Payment Methods</div>', unsafe_allow_html=True)
    if df.empty or df["order_count"].sum() == 0:
        st.info("Waiting for payment method data...")
        return

    chart = (
        alt.Chart(df)
        .mark_arc(innerRadius=45, stroke="#fff", strokeWidth=1)
        .encode(
            theta=alt.Theta("order_count:Q", title="Orders"),
            color=alt.Color(
                f"{COL_PAYMENT_METHOD}:N",
                title="Payment Method",
                scale=alt.Scale(scheme="set2"),
                legend=alt.Legend(orient="bottom", columns=2),
            ),
            tooltip=[
                alt.Tooltip(f"{COL_PAYMENT_METHOD}:N", title="Method"),
                alt.Tooltip("order_count:Q", title="Orders", format=","),
            ],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)


def render_order_status_chart(df: pd.DataFrame):
    """Render donut chart for order status distribution with accessible status colors."""
    st.markdown('<div class="section-title">Order Status Distribution</div>', unsafe_allow_html=True)
    if df.empty or df["order_count"].sum() == 0:
        st.info("Waiting for order status data...")
        return

    # Semantic color mapping for statuses
    status_colors = {
        STATUS_COMPLETED: "#28a745",  # Green
        STATUS_RETURNED: "#fd7e14",   # Orange
        STATUS_CANCELLED: "#dc3545",  # Red
    }
    domain = list(status_colors.keys())
    range_colors = list(status_colors.values())

    chart = (
        alt.Chart(df)
        .mark_arc(innerRadius=45, stroke="#fff", strokeWidth=1)
        .encode(
            theta=alt.Theta("order_count:Q", title="Orders"),
            color=alt.Color(
                f"{COL_ORDER_STATUS}:N",
                title="Status",
                scale=alt.Scale(domain=domain, range=range_colors),
                legend=alt.Legend(orient="bottom", columns=3),
            ),
            tooltip=[
                alt.Tooltip(f"{COL_ORDER_STATUS}:N", title="Status"),
                alt.Tooltip("order_count:Q", title="Orders", format=","),
            ],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)

def render_recent_transactions_table(df: pd.DataFrame):
        """Render Recent Transactions table with styled amount and readable status."""
        st.markdown(
            '<div class="section-title">Recent Transactions</div>',
            unsafe_allow_html=True
        )

        if df.empty:
            st.info("Waiting for streaming data... (No transactions recorded yet)")

            empty_placeholder = pd.DataFrame(columns=[
                DISPLAY_COL_ORDER_ID,
                DISPLAY_COL_PRODUCT,
                DISPLAY_COL_CATEGORY,
                DISPLAY_COL_AMOUNT,
                DISPLAY_COL_CITY,
                DISPLAY_COL_STATUS,
            ])

            st.dataframe(
                empty_placeholder,
                use_container_width=True,
                hide_index=True
            )
            return

        display_df = df.copy()

        # -----------------------------------------------------------------
        # Fix duplicate column names before passing DataFrame to Streamlit
        # -----------------------------------------------------------------
        display_df = display_df.loc[:, ~display_df.columns.duplicated()]

        # Format status labels
        if DISPLAY_COL_STATUS in display_df.columns:
            display_df[DISPLAY_COL_STATUS] = display_df[
                DISPLAY_COL_STATUS
            ].apply(format_status_label)

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                DISPLAY_COL_ORDER_ID: st.column_config.TextColumn(
                    "Order ID",
                    width="small"
                ),
                DISPLAY_COL_PRODUCT: st.column_config.TextColumn(
                    "Product",
                    width="medium"
                ),
                DISPLAY_COL_CATEGORY: st.column_config.TextColumn(
                    "Category",
                    width="small"
                ),
                DISPLAY_COL_AMOUNT: st.column_config.NumberColumn(
                    "Amount",
                    format="₹%.2f",
                    width="small"
                ),
                DISPLAY_COL_CITY: st.column_config.TextColumn(
                    "City",
                    width="small"
                ),
                DISPLAY_COL_STATUS: st.column_config.TextColumn(
                    "Status",
                    width="small"
                ),
            },
        )

# -----------------------------------------------------------------------------
# Main Dashboard Body (Fragment for auto-refreshing without page flicker)
# -----------------------------------------------------------------------------
def render_dashboard_content():
    """Renders the main content of the Streamlit dashboard."""
    # 1. Load latest analytics output
    metrics = load_analytics_metrics()

    # 2. Header & Live Indicator Bar
    status_class_map = {
        "LIVE": "status-live",
        "PAUSED": "status-paused",
        "WAITING": "status-waiting",
        "ERROR": "status-error",
    }
    badge_class = status_class_map.get(metrics["status"], "status-waiting")
    last_up_display = metrics["last_updated"] if metrics["last_updated"] else "Never"

    col_title, col_status = st.columns([3, 2])
    with col_title:
        st.title("Online Shopping Streaming Analytics")
        st.caption("Continuous Transaction Ingestion & Analytics with Apache Kafka & Spark Structured Streaming")
    with col_status:
        st.markdown(
            f"""
            <div style="text-align: right; padding-top: 15px;">
                <span class="status-badge {badge_class}">
                    {metrics['status_badge']}
                </span>
                <div style="font-size: 0.8rem; color: gray; margin-top: 4px;">
                    Last updated: <b>{last_up_display}</b>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # 3. Informational / Error Banners
    if metrics["status"] == "WAITING":
        st.info("ℹ️ **Waiting for streaming data...** Start Kafka, Python Producer, and Spark Streaming to see real-time metrics.")
    elif metrics["status"] == "ERROR":
        st.error(f"⚠️ **Analytics Error:** {metrics['error'] or metrics['status_message']}")

    # 4. REQUIRED 5 KPI CARDS
    kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5 = st.columns(5)
    with kpi_col1:
        st.metric(
            label="Total Orders",
            value=format_number(metrics["total_orders"]),
            help="Total transaction orders processed so far",
        )
    with kpi_col2:
        st.metric(
            label="Completed Orders",
            value=format_number(metrics["completed_orders"]),
            help="Orders successfully completed",
        )
    with kpi_col3:
        st.metric(
            label="Returned Orders",
            value=format_number(metrics["returned_orders"]),
            help="Orders marked as returned",
        )
    with kpi_col4:
        st.metric(
            label="Completed Revenue",
            value=format_currency(metrics["completed_revenue"]),
            help="Total revenue generated by completed transactions",
        )
    with kpi_col5:
        st.metric(
            label="Orders in Last 1 Min",
            value=format_number(metrics["orders_in_last_minute"]),
            help="Event-time 1-minute window transaction volume",
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # 5. Route views based on Sidebar Navigation
    if nav_option == "📊 Dashboard":
        # All-in-One Presentation View
        # Top Row Charts: Revenue by Category, Orders by City, Payment Method Distribution
        c1, c2, c3 = st.columns([1.2, 1.2, 1.0])
        with c1:
            render_revenue_by_category_chart(metrics["revenue_by_category"])
        with c2:
            render_orders_by_city_chart(metrics["orders_by_city"])
        with c3:
            render_payment_method_chart(metrics["payment_method_distribution"])

        st.markdown("<br>", unsafe_allow_html=True)

        # Lower Section: Order Status Distribution + Recent Transactions Table
        col_status_dist, col_table = st.columns([1.1, 1.9])
        with col_status_dist:
            render_order_status_chart(metrics["order_status_distribution"])
        with col_table:
            render_recent_transactions_table(metrics["recent_transactions"])

        # Optional Collapsible Details
        with st.expander("🔍 Pipeline Health & Event-Time Window Analytics", expanded=False):
            w_col1, w_col2 = st.columns(2)
            with w_col1:
                st.write("**1-Minute Streaming Windows:**")
                win_df = metrics["windowed_orders"]
                if not win_df.empty:
                    st.dataframe(win_df, use_container_width=True, hide_index=True)
                else:
                    st.caption("No window metrics collected yet.")
            with w_col2:
                st.write("**Raw Analytics Output JSON:**")
                if metrics.get("raw_json"):
                    st.json(metrics["raw_json"])
                else:
                    st.caption("No JSON output file found on disk.")

    elif nav_option == "⚡ Live Stream":
        st.subheader("Live Streaming Activity")
        l_col1, l_col2 = st.columns([1, 1])
        with l_col1:
            st.write("### Real-Time Window Metrics")
            st.metric(
                label="Orders in Last 1 Minute",
                value=format_number(metrics["orders_in_last_minute"]),
            )
            win_df = metrics["windowed_orders"]
            if not win_df.empty:
                st.write("#### Event-Time Window Distribution")
                win_chart = (
                    alt.Chart(win_df)
                    .mark_bar(cornerRadius=4)
                    .encode(
                        x=alt.X("window_start:N", title="Window Start Time"),
                        y=alt.Y("order_count:Q", title="Orders"),
                        color=alt.value("#17a2b8"),
                        tooltip=["window_start", "window_end", "order_count"],
                    )
                    .properties(height=260)
                )
                st.altair_chart(win_chart, use_container_width=True)
            else:
                st.info("Waiting for streaming window data...")

        with l_col2:
            st.write("### Stream Status Details")
            st.write(f"- **Pipeline Status:** {metrics['status_badge']}")
            st.write(f"- **Status Detail:** {metrics['status_message']}")
            st.write(f"- **Last Output File Update:** {metrics['last_updated'] or 'None'}")
            st.write(f"- **Completed Transactions Ratio:** {metrics['completed_orders']} / {metrics['total_orders']} ({((metrics['completed_orders']/metrics['total_orders'])*100):.1f}%)" if metrics['total_orders'] > 0 else "- **Completed Transactions:** 0")
            st.write(f"- **Returned Transactions Ratio:** {metrics['returned_orders']} / {metrics['total_orders']} ({((metrics['returned_orders']/metrics['total_orders'])*100):.1f}%)" if metrics['total_orders'] > 0 else "- **Returned Transactions:** 0")

        st.markdown("---")
        render_recent_transactions_table(metrics["recent_transactions"])

    elif nav_option == "📈 Analytics":
        st.subheader("Aggregated Analytics Dimensions")
        a_col1, a_col2 = st.columns(2)
        with a_col1:
            render_revenue_by_category_chart(metrics["revenue_by_category"])
            if not metrics["revenue_by_category"].empty:
                st.dataframe(metrics["revenue_by_category"], use_container_width=True, hide_index=True)
        with a_col2:
            render_orders_by_city_chart(metrics["orders_by_city"])
            if not metrics["orders_by_city"].empty:
                st.dataframe(metrics["orders_by_city"], use_container_width=True, hide_index=True)

        st.markdown("---")
        b_col1, b_col2 = st.columns(2)
        with b_col1:
            render_payment_method_chart(metrics["payment_method_distribution"])
            if not metrics["payment_method_distribution"].empty:
                st.dataframe(metrics["payment_method_distribution"], use_container_width=True, hide_index=True)
        with b_col2:
            render_order_status_chart(metrics["order_status_distribution"])
            if not metrics["order_status_distribution"].empty:
                st.dataframe(metrics["order_status_distribution"], use_container_width=True, hide_index=True)

    elif nav_option == "📋 Transactions":
        st.subheader("Recent Transactions Explorer")
        tx_df = metrics["recent_transactions"]

        if not tx_df.empty:
            f_col1, f_col2, f_col3 = st.columns([1, 1, 2])
            with f_col1:
                status_filter = st.selectbox(
                    "Filter by Status",
                    options=["All"] + sorted(list(tx_df[DISPLAY_COL_STATUS].unique())),
                )
            with f_col2:
                city_filter = st.selectbox(
                    "Filter by City",
                    options=["All"] + sorted(list(tx_df[DISPLAY_COL_CITY].unique())),
                )
            with f_col3:
                search_term = st.text_input("Search Product / Order ID", "")

            filtered_df = tx_df.copy()
            if status_filter != "All":
                filtered_df = filtered_df[filtered_df[DISPLAY_COL_STATUS] == status_filter]
            if city_filter != "All":
                filtered_df = filtered_df[filtered_df[DISPLAY_COL_CITY] == city_filter]
            if search_term.strip():
                term = search_term.strip().lower()
                filtered_df = filtered_df[
                    filtered_df[DISPLAY_COL_ORDER_ID].str.lower().str.contains(term)
                    | filtered_df[DISPLAY_COL_PRODUCT].str.lower().str.contains(term)
                ]

            render_recent_transactions_table(filtered_df)
        else:
            render_recent_transactions_table(tx_df)


# Dynamic fragment decorator allows auto-refresh without full-page reloads
run_every_val = f"{refresh_interval}s" if auto_refresh else None
fragment_runner = st.fragment(run_every=run_every_val)(render_dashboard_content)
fragment_runner()
