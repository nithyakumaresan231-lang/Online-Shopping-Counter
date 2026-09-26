import sys
sys.path.insert(0, "/app")

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

from analytics.aggregations import start_analytics_stream


def get_transaction_schema():
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_percent", DoubleType(), True),
        StructField("city", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_status", StringType(), True)
    ])


def main():

    bootstrap_servers = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    )

    topic = os.environ.get(
        "KAFKA_TOPIC",
        "shopping-transactions"
    )

    analytics_checkpoint = os.environ.get(
        "ANALYTICS_CHECKPOINT_LOCATION",
        "/tmp/spark_analytics_checkpoint"
    )

    spark = (
        SparkSession.builder
        .appName("OnlineShoppingStreaming")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # ---------------------------------------------------------
    # Read Kafka stream
    # ---------------------------------------------------------

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # ---------------------------------------------------------
    # Parse JSON
    # ---------------------------------------------------------

    schema = get_transaction_schema()

    parsed_df = (
        df
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    # ---------------------------------------------------------
    # Add timestamp and total_amount
    # ---------------------------------------------------------

    processed_df = (
        parsed_df
        .withColumn(
            "timestamp",
            to_timestamp(
                col("timestamp"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        .withColumn(
            "total_amount",
            col("quantity")
            * col("unit_price")
            * (1 - col("discount_percent") / 100.0)
        )
    )

    # ---------------------------------------------------------
    # Start analytics stream
    # ---------------------------------------------------------

    analytics_query = start_analytics_stream(
        processed_df,
        checkpoint_location=analytics_checkpoint
    )

    print("Spark analytics streaming started...")
    print("Waiting for Kafka transactions...")

    analytics_query.awaitTermination()


if __name__ == "__main__":
    main()