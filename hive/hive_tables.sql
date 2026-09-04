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
