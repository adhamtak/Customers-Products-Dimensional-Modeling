# Customers and Products Dimensional Modeling

## 📌 Project Overview

This project builds an end-to-end **e-commerce data warehouse** using
the Brazilian **Olist marketplace dataset**.

The main business goal is to track **product price history**, analyze
**customer purchasing behavior using RFM segmentation**, and provide
**order-item-level sales analytics** for business intelligence and
dashboard reporting.

The solution follows a **Medallion Architecture** and a **Star Schema**,
using distributed storage and processing technologies.

------------------------------------------------------------------------

## 🎯 Business Problem

Olist is a multi-seller e-commerce marketplace where the same product
can be sold by different sellers at different prices over time.

The business needs a data warehouse that can:

-   Track product price history accurately.
-   Analyze customer purchasing behavior.
-   Segment customers using RFM analysis.
-   Analyze sales over time.
-   Analyze sales by product category and geography.
-   Provide data for BI dashboards and business decision-making.

------------------------------------------------------------------------

## 📊 Data Sources

The project uses the following Olist datasets:

  -----------------------------------------------------------------------
  Dataset                             Purpose
  ----------------------------------- -----------------------------------
  `olist_products_dataset.csv`        Product catalog and category
                                      information

  `olist_orders_dataset.csv`          Order information and purchase
                                      timestamps

  `olist_order_items_dataset.csv`     Line-item prices and freight values

  Customer dataset                    Customer identity and geographic
                                      information
  -----------------------------------------------------------------------

### Important Business Keys

-   **`product_id`** --- identifies a product and is used as the
    business key for product price history.
-   **`customer_id`** --- Olist generates a new value for every order.
-   **`customer_unique_id`** --- stable person-level identifier used for
    accurate repeat-purchase and RFM analysis.
-   **`order_id`** --- identifies an order.
-   **`order_purchase_timestamp`** --- provides the time anchor for
    order and price observations.

------------------------------------------------------------------------

# 🏗️ Architecture

The project follows a **Medallion Architecture**:

``` text
                Olist Source Data
                       │
                       ▼
              ┌─────────────────┐
              │  Apache NiFi    │
              │    Ingestion    │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │  BRONZE / RAW   │
              │     HDFS        │
              │     Parquet     │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │     SILVER      │
              │  PySpark Clean  │
              │   & Validate    │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │      GOLD       │
              │   Star Schema   │
              │     Parquet     │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │      Hive       │
              │ SQL / Metastore │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │     Power BI    │
              │    Dashboard    │
              └─────────────────┘
```

## 🛠️ Technologies

-   **Apache NiFi** --- batch data ingestion and flow orchestration.
-   **HDFS** --- distributed storage for Raw, Silver, and Gold zones.
-   **Apache Spark / PySpark** --- data cleaning, validation, and
    dimensional transformations.
-   **Apache Hive** --- SQL querying and metastore layer over Gold
    Parquet data.
-   **Parquet** --- columnar storage format.
-   **Power BI** --- business intelligence and dashboard reporting.

------------------------------------------------------------------------

# 🔄 Data Ingestion

Each source dataset is ingested through an Apache NiFi flow:

``` text
GetFile
   ↓
ConvertRecord
   ↓
UpdateAttribute
   ↓
PutHDFS
```

The ingestion process is **batch-based**. Each CSV source file is loaded
as a complete file rather than being consumed as a continuous real-time
stream.

### Ingestion Validation

Before transformation, the datasets were validated for:

-   Null values
-   Duplicate records
-   Invalid values
-   Record counts
-   Required columns and identifiers

Examples:

-   Products: `0` null `product_id` and `0` duplicate `product_id`.
-   Orders: `0` null `order_id`, `customer_id`, or
    `order_purchase_timestamp`.
-   Order items: `0` invalid prices and `0` invalid freight values.

### Ingestion Issue Fixed

During development, Apache NiFi repeatedly picked up the same source
file because:

``` text
Keep Source File = true
```

The issue was fixed by:

``` text
Keep Source File = false
```

Queued FlowFiles were also cleared before restarting the flow.

------------------------------------------------------------------------

# 🥉 Bronze / Raw Layer

The Bronze layer preserves the ingested source data.

### Characteristics

-   Source data is not transformed before loading.
-   CSV files are converted to Parquet during ingestion.
-   Data is stored in HDFS.
-   The layer provides the starting point for downstream processing.

``` text
Bronze
├── Products
├── Orders
├── Order Items
└── Customers
```

------------------------------------------------------------------------

# 🥈 Silver Layer

The Silver layer contains **cleaned, validated, and standardized
datasets** produced with PySpark.

### Cleaning & Standardization

The Silver layer includes:

-   Null handling.
-   Duplicate checks.
-   Invalid-value checks.
-   ZIP code standardization.
-   Customer city cleaning.
-   Customer state standardization.
-   Validation after cleaning.

### Products

There were:

``` text
32,951 total products
610 products with null product_category_name
```

The missing product categories were replaced with:

``` text
Unknown
```

using PySpark `coalesce()` logic.

### Silver Grain

The Silver layer keeps source-level grain:

-   One row per product in Silver Products.
-   One row per order item in Silver Order Items.
-   One row per source record in the other cleaned datasets.

------------------------------------------------------------------------

# 🥇 Gold Layer

The Gold layer contains the final **business-ready Star Schema**.

``` text
                         Dim_Date
                            │
                            │
                            ▼
Dim_Product ───────► Fact_Order_Items ◄────── Dim_Customer
```

## Gold Tables

  ----------------------------------------------------------------------------
  Table                Grain                             Rows Purpose
  -------------------- ---------------- --------------------- ----------------
  `Dim_Customer`       One row per                     96,096 Customer
                       customer                               segmentation and
                                                              RFM

  `Dim_Product`        One row per                     43,147 Product price
                       product version                        history

  `Dim_Date`           One row per date                   774 Time-based
                                                              analysis

  `Fact_Order_Items`   One row per                    112,650 Sales analysis
                       order item                             
  ----------------------------------------------------------------------------

------------------------------------------------------------------------

# ⭐ Star Schema

The Star Schema was selected because dimensions connect directly to the
central fact table.

Benefits:

-   Simplifies analytical queries.
-   Reduces join complexity.
-   Makes relationships easier to understand.
-   Works well with BI tools.
-   Makes analytics easier for business users.

------------------------------------------------------------------------

# 📦 Dim_Product --- SCD Type 2

`Dim_Product` uses **Slowly Changing Dimension Type 2 (SCD Type 2)** to
preserve product price history.

### Grain

> One row per product price version.

### Key Design

  Field             Description
  ----------------- ------------------------
  `product_key`     Surrogate primary key
  `product_id`      Business key
  `category`        Product category
  `product_price`   Observed product price
  `version_no`      Product version
  `dw_start_date`   Version start date
  `dw_end_date`     Version end date
  `is_current`      Current-version flag

### SCD2 Logic

A new version is created when the observed product price changes.

The project uses **individual sale events as price observations** rather
than averaging prices by day.

This is important because Olist is a multi-seller marketplace, so the
same `product_id` can have different prices over time.

The final result contains:

``` text
43,147 Dim_Product rows
```

and each product has exactly one current version.

------------------------------------------------------------------------

# 👥 Dim_Customer --- RFM Analysis

`Dim_Customer` contains customer attributes and RFM metrics.

### Grain

> One row per customer.

### Key Design

  Field                        Description
  ---------------------------- ---------------------------------
  `cust_key`                   Surrogate primary key
  `customer_unique_id`         Stable customer business key
  `customer_zip_code_prefix`   Standardized ZIP code
  `customer_city`              Standardized city
  `customer_state`             Standardized state
  `Recency`                    Days since most recent purchase
  `Frequency`                  Number of distinct orders
  `Monetary`                   Total customer spending
  `RFM_Score`                  Combined 1--5 RFM score

## RFM Metrics

### Recency

Measures how recently a customer made an order.

-   Lower value = more recent purchase.
-   Higher R score = more recent customer.

### Frequency

Measures how often the customer places orders.

-   Calculated using distinct orders.
-   Higher value = more frequent customer.

### Monetary

Measures total customer spending.

-   Higher value = higher customer value.

Each metric receives a score from **1 to 5**, and the scores are
combined into:

``` text
RFM_Score
```

------------------------------------------------------------------------

# 🐛 Important Bug Found & Fixed

A major issue was discovered during RFM development.

Initially, RFM aggregation was grouped by:

``` text
customer_id
```

However, Olist generates a new `customer_id` for every order.

This caused many repeat customers to appear as if they had:

``` text
Frequency = 1
```

### Solution

The RFM aggregation was re-keyed using:

``` text
customer_unique_id
```

which is the stable person-level identifier.

### Validation

``` text
Raw customer_id count       : 99,441
Real customer_unique_id     : 96,096
Repeat-customer orders fixed: 3,345
```

This correction made the customer segmentation accurately represent
repeat-purchase behavior.

------------------------------------------------------------------------

# 📅 Dim_Date

`Dim_Date` is a conformed date dimension generated from the actual order
date range.

### Grain

> One row per calendar date.

### Date Range

``` text
2016-09-04 → 2018-10-17
```

### Total Rows

``` text
774
```

### Attributes

-   `date_key`
-   `full_date`
-   `day`
-   `month`
-   `quarter`
-   `year`
-   `day_of_week`
-   `day_name`

------------------------------------------------------------------------

# 🧾 Fact_Order_Items

The fact table connects:

``` text
Dim_Product
Dim_Customer
Dim_Date
```

### Grain

> One row per order item / line item.

Every row represents one product purchased within an order.

### Main Fields

  Field              Role
  ------------------ --------------------------------
  `order_item_key`   Surrogate primary key
  `order_id`         Degenerate / identifying field
  `order_item_id`    Line-item identifier
  `product_key`      Foreign key to Dim_Product
  `cust_key`         Foreign key to Dim_Customer
  `date_key`         Foreign key to Dim_Date
  `price`            Sales measure
  `freight_value`    Shipping measure
  `order_status`     Order status

### Point-in-Time Product Join

Because `Dim_Product` is SCD Type 2, each fact row is linked to the
**correct product version for the order date**.

The join uses the product version whose:

``` text
dw_start_date <= order_date
AND
order_date <= dw_end_date
```

This prevents historical sales from being incorrectly linked to the
current product price.

### Fact Validation

``` text
Fact rows             : 112,650
Unmatched product_key : 0
Unmatched cust_key    : 0
Unmatched date_key    : 0
```

The fact row count exactly matches the source `order_items` row count.

------------------------------------------------------------------------

# 📈 Business Analytics & KPIs

The Gold layer is queried through Hive to produce business KPIs for BI
reporting.

## 1. Overall Sales Performance

**Business Question:**

> What is the overall sales performance of the business?

``` sql
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT cust_key) AS total_customers,
    SUM(price) AS total_sales,
    SUM(freight_value) AS total_freight,
    ROUND(
        SUM(price) / COUNT(DISTINCT order_id),
        2
    ) AS average_order_value
FROM fact_order_items;
```

------------------------------------------------------------------------

## 2. Monthly Sales Trend

**Business Question:**

> How do sales and order volume change over time?

``` sql
SELECT
    d.year,
    d.month,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.price) AS total_sales
FROM fact_order_items f
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY
    d.year,
    d.month
ORDER BY
    d.year,
    d.month;
```

**Recommended visualization:** Line Chart.

------------------------------------------------------------------------

## 3. Sales by Customer State

**Business Question:**

> Which states generate the highest sales?

``` sql
SELECT
    c.customer_state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.price) AS total_sales
FROM fact_order_items f
JOIN dim_customer c
    ON f.cust_key = c.cust_key
GROUP BY
    c.customer_state
ORDER BY
    total_sales DESC;
```

**Recommended visualization:** Bar Chart / Filled Map.

------------------------------------------------------------------------

## 4. Revenue by Product Category

**Business Question:**

> Which product categories generate the highest sales?

``` sql
SELECT
    p.category,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(*) AS items_sold,
    SUM(f.price) AS total_sales
FROM fact_order_items f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY
    p.category
ORDER BY
    total_sales DESC;
```

**Recommended visualization:** Bar Chart.

------------------------------------------------------------------------

## 5. Top 10 Products

**Business Question:**

> Which products contribute the most to total sales?

``` sql
SELECT
    p.product_id,
    p.category,
    SUM(f.price) AS total_sales,
    COUNT(*) AS items_sold,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM fact_order_items f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY
    p.product_id,
    p.category
ORDER BY
    total_sales DESC
LIMIT 10;
```

**Recommended visualization:** Top-10 Bar Chart or Table.

------------------------------------------------------------------------

## 6. Average Order Value

**Business Question:**

> How does average order value change over time?

``` sql
SELECT
    d.year,
    d.month,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.price) AS total_sales,
    ROUND(
        SUM(f.price) / COUNT(DISTINCT f.order_id),
        2
    ) AS average_order_value
FROM fact_order_items f
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY
    d.year,
    d.month
ORDER BY
    d.year,
    d.month;
```

**Recommended visualization:** Line Chart.

------------------------------------------------------------------------

## 7. Customer RFM Segmentation

**Business Question:**

> How are customers distributed across RFM segments?

``` sql
SELECT
    rfm_score,
    COUNT(*) AS customer_count,
    SUM(monetary) AS total_customer_value
FROM dim_customer
GROUP BY rfm_score
ORDER BY rfm_score DESC;
```

**Recommended visualization:** Donut Chart / Matrix.

------------------------------------------------------------------------

## 8. Top Customers by Monetary Value

**Business Question:**

> Who are the highest-value customers?

``` sql
SELECT
    customer_unique_id,
    customer_city,
    customer_state,
    frequency,
    monetary,
    recency,
    rfm_score
FROM dim_customer
ORDER BY monetary DESC
LIMIT 10;
```

**Recommended visualization:** Table.

------------------------------------------------------------------------

## 9. Customers at Risk

**Business Question:**

> Which high-value customers have not purchased recently?

``` sql
SELECT
    customer_unique_id,
    customer_city,
    customer_state,
    recency,
    frequency,
    monetary,
    rfm_score
FROM dim_customer
WHERE recency >= 300
  AND monetary > 0
ORDER BY monetary DESC
LIMIT 20;
```

**Recommended visualization:** At-Risk Customer Table.

------------------------------------------------------------------------

## 10. Product Category Pricing

**Business Question:**

> What is the price distribution across product categories?

``` sql
SELECT
    category,
    COUNT(DISTINCT product_id) AS products_count,
    ROUND(AVG(product_price), 2) AS average_price,
    MIN(product_price) AS minimum_price,
    MAX(product_price) AS maximum_price
FROM dim_product
WHERE is_current = TRUE
GROUP BY category
ORDER BY average_price DESC;
```

**Recommended visualization:** Bar Chart.

------------------------------------------------------------------------

# 📊 Power BI Dashboard

The Gold Star Schema is exposed through Hive and connected to Power BI
for dashboard reporting.

## Dashboard Structure

### 1. Executive Performance

KPI Cards:

-   Total Sales
-   Total Orders
-   Total Customers
-   Average Order Value
-   Total Freight

Charts:

-   Monthly Sales Trend
-   Monthly Order Trend
-   Sales Evolution by Category

------------------------------------------------------------------------

### 2. Product Portfolio

Visuals:

-   Revenue by Category
-   Units Sold by Category
-   Top 10 Products
-   Category Pricing Spread
-   Product Price History

------------------------------------------------------------------------

### 3. Customer Insights

Visuals:

-   RFM Score Distribution
-   Customer Segments
-   Top Customers
-   At-Risk Customers
-   Geographic Sales by State

------------------------------------------------------------------------

### 4. Logistics & Operations

Metrics:

-   Total Freight
-   Freight-to-Revenue Ratio
-   Category-level freight burden
-   Margin dilution indicators

------------------------------------------------------------------------

## Recommended Dashboard Layout

``` text
┌─────────────────────────────────────────────────────────────┐
│                  E-COMMERCE SALES DASHBOARD                │
├─────────────┬─────────────┬─────────────┬─────────────────┤
│ Total Sales │ Total Orders│ Customers   │ Avg Order Value │
├─────────────┴─────────────┴─────────────┴─────────────────┤
│                                                             │
│                 MONTHLY SALES TREND                        │
│                                                             │
├──────────────────────────────┬──────────────────────────────┤
│                              │                              │
│     SALES BY CATEGORY        │       SALES BY STATE         │
│                              │                              │
├──────────────────────────────┴──────────────────────────────┤
│                                                             │
│                TOP 10 PRODUCTS                              │
│                                                             │
├──────────────────────────────┬──────────────────────────────┤
│                              │                              │
│     RFM SEGMENTS             │     AT-RISK CUSTOMERS        │
│                              │                              │
└──────────────────────────────┴──────────────────────────────┘
```

### Recommended Filters

-   Year
-   Month
-   Customer State
-   Product Category

These filters allow business users to interactively analyze sales and
customer behavior.

------------------------------------------------------------------------

# 💡 Business Insights Supported

The dashboard can support decisions around:

### Financial Oversight

-   Monitor revenue and order-volume trends.
-   Identify monthly revenue shortfalls.
-   Track average order value.

### Product & Pricing

-   Identify high-performing categories.
-   Identify top-selling products.
-   Analyze category price ranges.
-   Monitor historical product price changes.

### Customer Retention

-   Identify high-value customers.
-   Detect dormant customers.
-   Support targeted win-back campaigns.
-   Use RFM segments for customer targeting.

### Logistics Optimization

-   Monitor freight-to-revenue ratio.
-   Identify categories with high shipping costs.
-   Investigate shipping-related margin pressure.

### Regional Strategy

-   Identify high-performing states.
-   Compare geographic revenue distribution.
-   Support regional marketing and expansion decisions.

------------------------------------------------------------------------

# 🧪 Data Quality & Validation

Validation was performed throughout the pipeline.

Key checks included:

-   Null checks.
-   Duplicate checks.
-   Invalid value checks.
-   Record count validation.
-   Foreign-key matching.
-   SCD2 current-version validation.
-   Manual tracing of selected product price histories.

### Final Validation

``` text
Dim_Customer     : 96,096 rows
Dim_Product      : 43,147 rows
Dim_Date         : 774 rows
Fact_Order_Items : 112,650 rows
```

------------------------------------------------------------------------

# 🚀 Key Engineering Decisions

### 1. SCD Type 2 for Products

Product prices can change over time, so historical versions are
preserved instead of overwriting the current price.

### 2. `customer_unique_id` for RFM

The stable customer identifier is required because Olist's `customer_id`
can change across orders.

### 3. Order-Item Grain

The fact table uses:

``` text
One row = One order item
```

This preserves the required level of detail for product and sales
analysis.

### 4. Star Schema

The Star Schema keeps analytical relationships simple and BI-friendly.

### 5. Parquet on HDFS

Parquet provides columnar storage for the Raw, Silver, and Gold zones.

### 6. Hive over Gold Parquet

Hive provides the SQL/metastore layer over the Gold Parquet data using a
schema-on-read approach.

------------------------------------------------------------------------

# 📁 Suggested Repository Structure

``` text
customers-products-dimensional-modeling/
│
├── README.md
│
├── data/
│   └── README.md
│
├── nifi/
│   └── ingestion-flow.md
│
├── spark/
│   ├── silver/
│   │   ├── silver_products.py
│   │   ├── silver_orders.py
│   │   ├── silver_order_items.py
│   │   └── silver_customers.py
│   │
│   └── gold/
│       ├── dim_product_scd2.py
│       ├── dim_customer_rfm.py
│       ├── dim_date.py
│       └── fact_order_items.py
│
├── hive/
│   ├── dim_product.sql
│   ├── dim_customer.sql
│   ├── dim_date.sql
│   └── fact_order_items.sql
│
├── queries/
│   └── business_queries.sql
│
└── dashboard/
    └── powerbi/
```

------------------------------------------------------------------------

# 🏁 Final Outcome

This project demonstrates an end-to-end Data Engineering workflow:

``` text
Raw Olist Data
      ↓
Apache NiFi
      ↓
HDFS + Parquet
      ↓
PySpark
      ↓
Data Cleaning & Validation
      ↓
Gold Dimensional Model
      ↓
SCD Type 2 + RFM
      ↓
Hive SQL
      ↓
Business KPIs
      ↓
Power BI Dashboard
```

The final warehouse provides:

-   Historical product pricing.
-   Customer RFM segmentation.
-   Time-based analytics.
-   Order-item-level sales analysis.
-   Geographic sales analysis.
-   Product and category performance.
-   Business intelligence and dashboard capabilities.

------------------------------------------------------------------------

## 👨‍💻 Project Team

**Adham · Ahmed · Salma · Reem · Omar**

------------------------------------------------------------------------

## 📌 Project Summary

**Architecture:** Medallion Architecture + Star Schema\
**Processing:** Apache Spark / PySpark\
**Ingestion:** Apache NiFi\
**Storage:** HDFS + Parquet\
**SQL Layer:** Apache Hive\
**BI:** Power BI\
**Key Concepts:** SCD Type 2, RFM Analysis, Dimensional Modeling, Data
Quality, Batch ETL/ELT
