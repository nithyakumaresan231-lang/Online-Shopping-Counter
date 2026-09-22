# UI/UX Design Brief

## 1. Design Direction

Professional, clean, modern analytics dashboard suitable for a Big Data
project demonstration.

The interface should prioritize: - readability, - live status
visibility, - clear KPIs, - simple charts, - transaction visibility.

## 2. Dashboard Title

**🛒 Online Shopping Streaming Analytics**

## 3. Header

Show: - Project title. - `LIVE • Kafka + Spark Streaming` indicator. -
Last updated time.

The live indicator must represent actual application state and must not
be hardcoded as live when the pipeline is disconnected.

## 4. Layout

### Sidebar

-   Dashboard
-   Live Stream
-   Analytics
-   Transactions

### KPI Row

Display: 1. Total Orders 2. Completed Orders 3. Returned Orders 4.
Completed Revenue 5. Orders in Last 1 Minute

### Main Charts

-   Revenue by Category
-   Orders by City
-   Payment Method Distribution

### Lower Section

-   Order Status Distribution
-   Recent Transactions

## 5. Recent Transactions Table

Columns: - Order ID - Product - Category - Amount - City - Status

Use readable status indicators for Completed, Returned, and Cancelled.

## 6. Visual Style

-   Clean light dashboard.
-   Dark/navy header or sidebar is acceptable.
-   White/light cards.
-   Rounded cards with subtle shadows.
-   Consistent spacing.
-   Clear chart titles.
-   Avoid unnecessary decorative elements.

## 7. Responsive Behavior

The dashboard should remain usable on common laptop screen sizes.

## 8. Accessibility

-   Use readable font sizes.
-   Use labels in addition to color.
-   Maintain sufficient text/background contrast.
-   Do not communicate status using color alone.

## 9. Important UI Rule

Do not hardcode example analytics into the final application. All
displayed values must come from the actual streaming pipeline.
