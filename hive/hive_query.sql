CREATE EXTERNAL TABLE fact_order_items (
    order_item_key INT,
    order_id STRING,
    order_item_id INT,
    product_key INT,
    cust_key INT,
    date_key INT,
    price FLOAT,
    freight_value double,
    order_status STRING
)
STORED AS PARQUET
LOCATION '/gold_zone/fact_order_items';



CREATE EXTERNAL TABLE IF NOT EXISTS dim_product (
    product_key    INT,
    product_id     STRING,
    category       STRING,
    version_no     BIGINT,
    product_price  FLOAT,
    dw_start_date  DATE,
    dw_end_date    DATE,
    is_current     BOOLEAN
)
STORED AS PARQUET
LOCATION '/gold_zone/dim_product';



CREATE EXTERNAL TABLE IF NOT EXISTS dim_customer (
    cust_key                  INT,
    customer_unique_id        STRING,
    customer_zip_code_prefix  STRING,
    customer_city             STRING,
    customer_state            STRING,
    recency                   INT,
    frequency                 BIGINT,
    monetary                  DOUBLE,
    rfm_score                 STRING
)
STORED AS PARQUET
LOCATION '/gold_zone/dim_customer';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
    date_key     INT,
    full_date    DATE,
    day          INT,
    month        INT,
    quarter      INT,
    year         INT,
    day_of_week  INT,
    day_name     STRING
)
STORED AS PARQUET
LOCATION '/gold_zone/dim_date';
 



-- 1. Overall Sales Performance
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT cust_key) AS total_customers,
    SUM(price) AS total_sales,
    SUM(freight_value) AS total_freight,
    ROUND(SUM(price) / COUNT(DISTINCT order_id), 2) AS average_order_value
FROM fact_order_items;

-- 2. Monthly Sales Trend
SELECT
    d.year,
    d.month,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.price) AS total_sales
FROM fact_order_items f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
 
-- 3. Sales by Customer State
SELECT
    c.customer_state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.price) AS total_sales
FROM fact_order_items f
JOIN dim_customer c ON f.cust_key = c.cust_key
GROUP BY c.customer_state
ORDER BY total_sales DESC;
 
-- 4. Top Product Categories
SELECT
    p.category,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(*) AS items_sold,
    SUM(f.price) AS total_sales
FROM fact_order_items f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_sales DESC;
 
-- 5. Top 10 Products by Sales
SELECT
    p.product_id,
    p.category,
    SUM(f.price) AS total_sales,
    COUNT(*) AS items_sold,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM fact_order_items f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_id, p.category
ORDER BY total_sales DESC
LIMIT 10;
 
-- 6. Average Order Value by Month
SELECT
    d.year,
    d.month,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.price) AS total_sales,
    ROUND(SUM(f.price) / COUNT(DISTINCT f.order_id), 2) AS average_order_value
FROM fact_order_items f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
 
-- 7. Customer RFM Segmentation
SELECT
    rfm_score,
    COUNT(*) AS customer_count,
    SUM(monetary) AS total_customer_value
FROM dim_customer
GROUP BY rfm_score
ORDER BY rfm_score DESC;
 
-- 8. Top Customers by Monetary Value
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
 
-- 9. Customers at Risk
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
 
-- 10. Product Category Price Analysis
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
 
-- 11. Product Price History -- SCD Type 2
SELECT
    product_id,
    category,
    version_no,
    product_price,
    dw_start_date,
    dw_end_date,
    is_current
FROM dim_product
WHERE product_id = 'd1c427060a0f73f6b889a5c7c61f2ac4'
ORDER BY version_no;
 
-- 12. Sales by Category Over Time
SELECT
    d.year,
    d.month,
    p.category,
    SUM(f.price) AS total_sales
FROM fact_order_items f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, p.category
ORDER BY d.year, d.month, total_sales DESC;
 
 
--=========================================================
--  VALIDATING SCD TYPE 2 (Dim_Product)
--=========================================================

-- 1. Multiple versions per natural key
SELECT product_id, COUNT(*) AS version_count
FROM dim_product
GROUP BY product_id
HAVING COUNT(*) > 1;

--Expected: Only products with attribute changes should appear.

--2. Current flag consistency
SELECT product_id, COUNT(*) AS current_count
FROM dim_product
WHERE is_current = TRUE
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Expected: Zero rows. Each product should have exactly one current record.

-- 3. Date range integrity
SELECT product_id
FROM dim_product
WHERE dw_start_date >= dw_end_date;

-- Expected: Zero rows. Start date must always be before end date.

-- 4. No overlaps (only one open-ended record per product)
SELECT product_id
FROM dim_product
GROUP BY product_id
HAVING SUM(CASE WHEN dw_end_date IS NULL THEN 1 ELSE 0 END) > 1;

-- Expected: Zero rows. Each product should have at most one open-ended record.


-- =========================================================
-- VALIDATING RFM (Dim_Customer)
-- =========================================================

-- 1. Non-null scores
SELECT COUNT(*) AS null_rfm
FROM dim_customer
WHERE rfm_score IS NULL;

-- Expected: Ideally zero. If >0, those customers have no orders.

-- 2. Score range check
SELECT MIN(recency_score) AS min_recency, MAX(recency_score) AS max_recency,
       MIN(frequency_score) AS min_frequency, MAX(frequency_score) AS max_frequency,
       MIN(monetary_score) AS min_monetary, MAX(monetary_score) AS max_monetary
FROM dim_customer;

-- Expected: Values between 1 and 5 (or your defined scale).

-- 3. Distribution validation
SELECT rfm_score, COUNT(*) AS customer_count
FROM dim_customer
GROUP BY rfm_score
ORDER BY customer_count DESC;

-- Expected: Reasonably balanced distribution. If one score dominates, check thresholds.

-- 4. Cross-check raw metrics
SELECT customer_id, COUNT(DISTINCT order_id) AS freq_check
FROM fact_orders
GROUP BY customer_id;

-- Expected: freq_check should align with frequency_score logic in dim_customer.


-- =========================================================
--  GENERAL TABLE CHECKS
-- =========================================================

-- Row counts
SELECT COUNT(*) AS dim_product_count FROM dim_product;
SELECT COUNT(*) AS dim_customer_count FROM dim_customer;

-- Expected: Counts should match source data volume after transformations.

-- Surrogate key uniqueness
SELECT cust_key, COUNT(*) AS dup_count
FROM dim_customer
GROUP BY cust_key
HAVING COUNT(*) > 1;

SELECT product_key, COUNT(*) AS dup_count
FROM dim_product
GROUP BY product_key
HAVING COUNT(*) > 1;

-- Expected: Zero rows. Surrogate keys must be unique.


-- =========================================================
-- FINAL SUMMARY: RFM Valid vs Missing
-- =========================================================

SELECT
  SUM(CASE WHEN rfm_score IS NOT NULL THEN 1 ELSE 0 END) AS valid_customers,
  SUM(CASE WHEN rfm_score IS NULL THEN 1 ELSE 0 END) AS missing_customers,
  ROUND(100.0 * SUM(CASE WHEN rfm_score IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_valid,
  ROUND(100.0 * SUM(CASE WHEN rfm_score IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_missing
FROM dim_customer;

-- Expected: pct_valid should be close to 100%. pct_missing > 0 means customers exist with no orders.
