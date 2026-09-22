# Technical Requirements Document (TRD)

## 1. Technology Stack

  Layer                Technology
  -------------------- -----------------------------------------
  Operating System     Windows
  Language             Python
  Input                CSV
  Streaming Producer   Python
  Message Broker       Apache Kafka 4.3.1
  Kafka Topic          `shopping-transactions`
  Stream Processing    Apache Spark 4.2.0
  Processing API       Spark Structured Streaming / DataFrames
  Dashboard            Streamlit
  Version Control      Git + GitHub

Java 21 is required by the current local Spark/Kafka setup.

## 2. Architecture

``` text
shopping_data.csv
       |
       v
Python Streaming Producer
       |
       v
Apache Kafka
topic: shopping-transactions
       |
       v
Spark Structured Streaming
       |
       +--> parsing
       +--> validation
       +--> transformation
       +--> aggregation
       |
       v
Analytics Output
       |
       v
Streamlit Dashboard
```

## 3. Kafka Configuration

-   Bootstrap server: `localhost:9092`
-   Topic: `shopping-transactions`
-   One Kafka message represents one transaction.
-   Messages use JSON representation of the transaction.

## 4. Spark Requirements

Spark must: 1. Read the Kafka stream. 2. Deserialize the Kafka message
value. 3. Parse JSON using an explicit schema. 4. Convert numeric fields
to appropriate numeric types. 5. Convert `timestamp` to a Spark
timestamp. 6. Calculate `total_amount`. 7. Perform streaming
aggregations. 8. Include a time-window aggregation.

## 5. Derived Metric

``` text
total_amount =
quantity * unit_price * (1 - discount_percent / 100)
```

Completed revenue should be based on completed transactions.

## 6. Python Producer Requirements

The producer must: - Read `data/shopping_data.csv`. - Send records
sequentially. - Convert each record to JSON. - Publish to Kafka. - Use a
configurable delay, approximately 1 second by default. - Print useful
logs. - Stop cleanly with Ctrl+C. - Handle connection errors without
silently failing.

## 7. Dashboard Requirements

The Streamlit dashboard should contain: - Total Orders KPI. - Completed
Orders KPI. - Returned Orders KPI. - Completed Revenue KPI. - Orders in
Last 1 Minute KPI. - Revenue by Category chart. - Orders by City
chart. - Payment Method Distribution chart. - Order Status Distribution
chart. - Recent Transactions table. - Live Kafka + Spark streaming
indicator.

## 8. Project Structure

``` text
OnlineShoppingStreaming/
├── data/
│   └── shopping_data.csv
├── producer/
│   └── producer.py
├── streaming/
│   └── spark_stream.py
├── dashboard/
│   └── dashboard.py
├── output/
├── docs/
│   ├── 01_PRD.md
│   ├── 02_TRD.md
│   ├── 03_APP_FLOW.md
│   ├── 04_UI_UX.md
│   ├── 05_DATA_SCHEMA.md
│   └── 06_IMPLEMENTATION_PLAN.md
├── tests/
├── .gitignore
├── README.md
└── requirements.txt
```

## 9. Environment Variables

No mandatory environment variables are required for the first local
version.

If configuration is externalized later, keep values out of Git.

## 10. Technical Constraints

-   Must run locally on Windows.
-   Must use Kafka for event ingestion.
-   Must use Spark Structured Streaming for stream processing.
-   Must keep the architecture simple enough for a college mini-project.
-   Do not add unrelated infrastructure.
