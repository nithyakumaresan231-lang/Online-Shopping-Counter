# Product Requirements Document (PRD)

## 1. Project Name

**Online Shopping Streaming Analytics**

## 2. Tagline

Real-time online shopping transaction analytics using Apache Kafka and
Spark Structured Streaming.

## 3. Problem

Traditional CSV analysis processes a complete dataset after the data has
already been collected. This project demonstrates how online shopping
transactions can be handled as a continuous stream, processed as they
arrive, and converted into real-time analytics.

## 4. Target User

The primary user is a student/project evaluator who needs to observe and
understand a Big Data streaming pipeline. The dashboard should make the
incoming transactions and resulting analytics easy to understand during
a live demonstration.

## 5. Core Value

The project demonstrates the complete flow from transaction ingestion to
streaming processing and visualization:

**CSV Dataset → Python Streaming Producer → Kafka → Spark Structured
Streaming → Analytics → Streamlit Dashboard**

The existing CSV is replayed one transaction at a time to simulate a
live shopping transaction source.

## 6. Must-Have Features

1.  Read the existing shopping transaction CSV.
2.  Stream transactions one at a time using Python.
3.  Publish transactions to Kafka topic `shopping-transactions`.
4.  Consume Kafka data using Spark Structured Streaming.
5.  Parse and validate incoming transaction data.
6.  Calculate `total_amount` from quantity, unit price, and discount.
7.  Calculate total orders.
8.  Calculate completed orders.
9.  Calculate returned orders.
10. Calculate completed revenue.
11. Calculate revenue by category.
12. Calculate orders by city.
13. Calculate payment-method distribution.
14. Calculate order-status distribution.
15. Include at least one time-window metric.
16. Display the analytics in a Streamlit dashboard.
17. Show recent transactions.
18. Provide clear run instructions.
19. Maintain the project in Git.

## 7. Nice-to-Have Features

-   Configurable producer delay.
-   Live transaction counter.
-   Automatic dashboard refresh.
-   Basic data-quality/error logging.
-   Additional charts if time permits.

## 8. Out of Scope

The current version will NOT include: - User authentication. - User
accounts. - A production database. - React frontend. - Cloud
deployment. - Docker/Kubernetes. - Hadoop/HDFS. - Machine learning. -
Real e-commerce APIs. - Payment processing. - Production-scale
distributed deployment.

## 9. User Stories

-   As a project evaluator, I want to see transactions arriving
    continuously so that I can understand the streaming concept.
-   As a project evaluator, I want to see Kafka receive transaction
    events so that the ingestion layer is visible.
-   As a project evaluator, I want Spark to process the incoming events
    so that the stream-processing layer can be demonstrated.
-   As a project evaluator, I want to see live order and revenue
    analytics so that the effect of incoming transactions is visible.
-   As a project evaluator, I want a dashboard so that the processed
    information is easy to interpret.

## 10. Success Criteria

The project is successful when: 1. A transaction can be published from
the CSV through the Python producer. 2. Kafka receives the transaction.
3. Spark Structured Streaming consumes and processes it. 4. Derived
metrics are calculated correctly. 5. New transactions change the
analytics. 6. The Streamlit dashboard displays the processed results. 7.
The complete pipeline can be demonstrated locally on Windows.
