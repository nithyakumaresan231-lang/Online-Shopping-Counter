# Application Flow

## 1. System Entry

The project is primarily a local data pipeline rather than a
conventional user-authenticated web application.

The operational flow is:

``` text
Start Kafka
   ↓
Create/verify topic
   ↓
Start Spark Structured Streaming
   ↓
Start Python Producer
   ↓
Transactions enter Kafka
   ↓
Spark processes transactions
   ↓
Analytics are updated
   ↓
Streamlit Dashboard displays results
```

## 2. Component Flow

### A. Dataset

The existing `shopping_data.csv` is the source of transaction records.

### B. Producer

The producer reads one transaction at a time and publishes it to Kafka.

### C. Kafka

Kafka receives and stores the transaction event in
`shopping-transactions`.

### D. Spark

Spark continuously consumes the Kafka stream and transforms the events.

### E. Analytics

Spark calculates order, revenue, category, city, payment, status, and
time-window metrics.

### F. Dashboard

Streamlit displays the processed metrics and recent transaction
information.

## 3. Dashboard Navigation

The dashboard can use a simple sidebar:

-   Dashboard
-   Live Stream
-   Analytics
-   Transactions

A single-page dashboard is acceptable for the first version. Additional
pages should only be added if they improve the demonstration.

## 4. Main User Journey

1.  Start Kafka.
2.  Start the Spark streaming job.
3.  Start the Python producer.
4.  Observe transactions being sent.
5.  Observe Spark processing incoming events.
6.  Open the Streamlit dashboard.
7.  Watch order and revenue metrics update.
8.  Inspect category, city, payment, status, and recent-transaction
    views.

## 5. Empty State

If no transactions have reached the dashboard yet: - Show zero/empty KPI
values. - Show a clear message such as
`Waiting for streaming data...`. - Do not show fake data.

## 6. Error State

If Kafka or Spark is unavailable: - Show a clear error/log message. - Do
not silently report the system as live.

## 7. Loading State

While waiting for the first batch/window: - Display
`Waiting for streaming data...`. - Keep the dashboard usable.

## 8. Stop Flow

The producer and Spark process should be stoppable with Ctrl+C/normal
process termination without corrupting project files.

## 9. No Authentication Flow

This version does not contain signup, login, user accounts, or protected
routes.
