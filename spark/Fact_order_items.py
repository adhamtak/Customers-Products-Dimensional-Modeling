#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("FactOrderItems").getOrCreate()

# ---------------------------------------------------------------------------
# LOAD -- silver-layer sources + all three gold-layer dimensions
# ---------------------------------------------------------------------------
order_items = spark.read.parquet("hdfs:///silver_zones/order_items_dataset")
orders = spark.read.parquet("hdfs:///silver_zones/orders_dataset")
customers = spark.read.parquet("hdfs:///silver_zones/customers_dataset")

dim_product = spark.read.parquet("hdfs:///gold_zone/dim_product")
dim_customer = spark.read.parquet("hdfs:///gold_zone/dim_customer")
dim_date = spark.read.parquet("hdfs:///gold_zone/dim_date")

# ---------------------------------------------------------------------------
# BUILD base grain: one row per order item, with order date + customer identity
# ---------------------------------------------------------------------------
orders_clean = orders.withColumn(
    "order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp")
).withColumn(
    "order_date", F.to_date("order_purchase_timestamp")
)

base = (
    order_items
    .join(
        orders_clean.select("order_id", "customer_id", "order_date", "order_status"),
        "order_id", "inner"
    )
    .join(
        customers.select("customer_id", "customer_unique_id"),
        "customer_id", "inner"
    )
)

# ---------------------------------------------------------------------------
# JOIN to Dim_Product -- point-in-time correct version via SCD2 date range
# ---------------------------------------------------------------------------
fact_with_product = base.join(
    dim_product.select(
        "product_key",
        dim_product.product_id.alias("dp_product_id"),
        "dw_start_date",
        "dw_end_date",
    ),
    (base.product_id == F.col("dp_product_id")) &
    (base.order_date >= F.col("dw_start_date")) &
    (base.order_date <= F.col("dw_end_date")),
    "left"
).drop("dp_product_id", "dw_start_date", "dw_end_date")

# ---------------------------------------------------------------------------
# JOIN to Dim_Customer (one row per real customer, no SCD2 needed)
# ---------------------------------------------------------------------------
fact_with_customer = fact_with_product.join(
    dim_customer.select("cust_key", "customer_unique_id"),
    "customer_unique_id",
    "left"
)

# ---------------------------------------------------------------------------
# JOIN to Dim_Date
# ---------------------------------------------------------------------------
fact_with_date = fact_with_customer.withColumn(
    "date_key", F.date_format("order_date", "yyyyMMdd").cast("int")
).join(
    dim_date.select("date_key"),
    "date_key",
    "left"
)

# ---------------------------------------------------------------------------
# FINALIZE: select columns, assign surrogate key
# ---------------------------------------------------------------------------
w_key = Window.orderBy("order_id", "order_item_id")

fact_order_items = (
    fact_with_date
    .withColumn("order_item_key", F.row_number().over(w_key))
    .select(
        "order_item_key",
        "order_id",
        "order_item_id",
        "product_key",
        "cust_key",
        "date_key",
        F.round(F.col("price"), 2).alias("price"),
        F.round(F.col("freight_value"), 2).alias("freight_value"),
        "order_status",
    )
)

# ---------------------------------------------------------------------------
# VALIDATE before saving -- these counts matter
# ---------------------------------------------------------------------------
print("Total fact rows:", fact_order_items.count())
print("Rows with unmatched product_key:", fact_order_items.filter(F.col("product_key").isNull()).count())
print("Rows with unmatched cust_key:", fact_order_items.filter(F.col("cust_key").isNull()).count())
print("Rows with unmatched date_key:", fact_order_items.filter(F.col("date_key").isNull()).count())

fact_order_items.show(20, truncate=False)

# ---------------------------------------------------------------------------
# SAVE to gold layer
# ---------------------------------------------------------------------------
fact_order_items.write.mode("overwrite").parquet("hdfs:///gold_zone/fact_order_items")


# In[2]:


fact_order_items.printSchema()


# In[ ]:




