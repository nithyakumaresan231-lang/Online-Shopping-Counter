"""
Schema definitions and constants for the Online Shopping Streaming Analytics project.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)

# Column Name Constants
COL_ORDER_ID = "order_id"
COL_TIMESTAMP = "timestamp"
COL_CUSTOMER_ID = "customer_id"
COL_PRODUCT_ID = "product_id"
COL_PRODUCT_NAME = "product_name"
COL_CATEGORY = "category"
COL_QUANTITY = "quantity"
COL_UNIT_PRICE = "unit_price"
COL_DISCOUNT_PERCENT = "discount_percent"
COL_CITY = "city"
COL_PAYMENT_METHOD = "payment_method"
COL_ORDER_STATUS = "order_status"
COL_TOTAL_AMOUNT = "total_amount"

# Order Status Constants
STATUS_COMPLETED = "Completed"
STATUS_RETURNED = "Returned"
STATUS_CANCELLED = "Cancelled"

# Incoming Raw JSON Transaction Schema (matches producer and Kafka message)
TRANSACTION_SCHEMA = StructType([
    StructField(COL_ORDER_ID, StringType(), True),
    StructField(COL_TIMESTAMP, StringType(), True),
    StructField(COL_CUSTOMER_ID, StringType(), True),
    StructField(COL_PRODUCT_ID, StringType(), True),
    StructField(COL_PRODUCT_NAME, StringType(), True),
    StructField(COL_CATEGORY, StringType(), True),
    StructField(COL_QUANTITY, IntegerType(), True),
    StructField(COL_UNIT_PRICE, DoubleType(), True),
    StructField(COL_DISCOUNT_PERCENT, DoubleType(), True),
    StructField(COL_CITY, StringType(), True),
    StructField(COL_PAYMENT_METHOD, StringType(), True),
    StructField(COL_ORDER_STATUS, StringType(), True)
])

# Processed DataFrame Schema (after timestamp parsing and total_amount calculation)
PROCESSED_SCHEMA = StructType([
    StructField(COL_ORDER_ID, StringType(), True),
    StructField(COL_TIMESTAMP, TimestampType(), True),
    StructField(COL_CUSTOMER_ID, StringType(), True),
    StructField(COL_PRODUCT_ID, StringType(), True),
    StructField(COL_PRODUCT_NAME, StringType(), True),
    StructField(COL_CATEGORY, StringType(), True),
    StructField(COL_QUANTITY, IntegerType(), True),
    StructField(COL_UNIT_PRICE, DoubleType(), True),
    StructField(COL_DISCOUNT_PERCENT, DoubleType(), True),
    StructField(COL_CITY, StringType(), True),
    StructField(COL_PAYMENT_METHOD, StringType(), True),
    StructField(COL_ORDER_STATUS, StringType(), True),
    StructField(COL_TOTAL_AMOUNT, DoubleType(), True)
])

REQUIRED_PROCESSED_COLUMNS = [
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
    COL_TOTAL_AMOUNT
]

def validate_processed_schema(df):
    """
    Validates that the input DataFrame contains all required columns
    for analytics processing.
    """
    missing_cols = [c for c in REQUIRED_PROCESSED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"DataFrame is missing required columns: {missing_cols}")
    return True
