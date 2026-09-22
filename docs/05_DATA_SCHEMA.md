# Data Schema

## 1. Source Dataset

File:

`data/shopping_data.csv`

## 2. Columns

  -----------------------------------------------------------------------
  Column                  Expected Type           Description
  ----------------------- ----------------------- -----------------------
  `order_id`              string                  Unique
                                                  transaction/order
                                                  identifier

  `timestamp`             timestamp/string before Transaction timestamp
                          parsing                 

  `customer_id`           string                  Customer identifier

  `product_id`            string                  Product identifier

  `product_name`          string                  Product name

  `category`              string                  Product category

  `quantity`              integer                 Number of units
                                                  purchased

  `unit_price`            numeric                 Price per unit

  `discount_percent`      numeric                 Discount percentage

  `city`                  string                  Customer/order city

  `payment_method`        string                  Payment method

  `order_status`          string                  Order state
  -----------------------------------------------------------------------

## 3. Example

``` text
ORD1002
2026-09-21 10:00:13
C1137
P021
Cricket Bat
Sports
2
2199
30
Salem
Credit Card
Completed
```

## 4. Kafka Message Schema

Each Kafka value should contain one JSON object with the same source
fields.

Example:

``` json
{
  "order_id": "ORD1002",
  "timestamp": "2026-09-21 10:00:13",
  "customer_id": "C1137",
  "product_id": "P021",
  "product_name": "Cricket Bat",
  "category": "Sports",
  "quantity": 2,
  "unit_price": 2199,
  "discount_percent": 30,
  "city": "Salem",
  "payment_method": "Credit Card",
  "order_status": "Completed"
}
```

## 5. Derived Field

`total_amount`

Calculation:

``` text
quantity * unit_price * (1 - discount_percent / 100)
```

Example:

``` text
2 * 2199 * (1 - 30/100)
= 3078.60
```

## 6. Validation Rules

-   Required columns must exist.
-   `quantity` should be numeric and non-negative.
-   `unit_price` should be numeric and non-negative.
-   `discount_percent` should be numeric and within a sensible
    percentage range.
-   `timestamp` should be parseable.
-   `order_id` should not be empty.
-   Invalid records should be logged/handled without crashing the entire
    stream.

## 7. Analytics Dimensions

The streaming application should support aggregation by: - category -
city - payment_method - order_status - time window

## 8. No Database Schema

The first version does not require a relational or NoSQL database.
