#!/usr/bin/env python
# coding: utf-8

# In[5]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, trim, initcap, upper, lpad

spark = SparkSession.builder.appName("OlistTransformations").getOrCreate()

customers_df = spark.read.parquet("hdfs:///raw_zones/customers")

# Handle missing values
customers_df = customers_df.dropna(subset=["customer_id", "customer_unique_id"])

# Replace missing city/state
customers_df = customers_df.fillna({
    "customer_city": "unknown",
    "customer_state": "unknown"
})

# Remove duplicates
customers_df = customers_df.dropDuplicates(["customer_id"])

# Clean ZIP code
customers_df = customers_df.withColumn(
    "customer_zip_code_prefix",
    lpad(col("customer_zip_code_prefix").cast("string"), 5, "0")
)

# Clean city
customers_df = customers_df.withColumn(
    "customer_city", initcap(trim(col("customer_city")))
)

# Clean state
customers_df = customers_df.withColumn(
    "customer_state", upper(trim(col("customer_state")))
)

# Diagnostics
customers_df.select([
    count(when(col(c).isNull(), c)).alias(c) for c in customers_df.columns
]).show()

customers_df.groupBy("customer_id").count().filter(col("count") > 1).show()

# SAVE to silver layer -- this was missing
customers_df.coalesce(4).write.mode("overwrite").parquet("hdfs:///silver_zones/customers_dataset")


# In[6]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder.appName("OlistTransformations").getOrCreate()

# ---------------------------------------------------------------------------
# LOAD (already-cleaned silver data -- customers was cleaned in a prior step)
# ---------------------------------------------------------------------------
customers_df = spark.read.parquet("hdfs:///silver_zones/customers_dataset")
orders_df = spark.read.parquet("hdfs:///silver_zones/orders_dataset")
order_items_df = spark.read.parquet("hdfs:///silver_zones/order_items_dataset")

# ---------------------------------------------------------------------------
# TRANSFORM: RFM (keyed on customer_unique_id, not customer_id)
# ---------------------------------------------------------------------------
orders_df = orders_df.withColumn(
    "order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))
)

# Bring in customer_unique_id BEFORE aggregating -- customer_id resets per
# order in Olist, so RFM must be computed on the stable customer_unique_id
orders_rfm_df = (
    orders_df.select("order_id", "customer_id", "order_purchase_timestamp")
    .join(
        customers_df.select("customer_id", "customer_unique_id"),
        on="customer_id",
        how="inner",
    )
)

order_items_rfm_df = order_items_df.select("order_id", "price")

order_data_df = orders_rfm_df.join(order_items_rfm_df, on="order_id", how="inner")

# Total order value
order_value_df = order_data_df.groupBy(
    "order_id", "customer_unique_id", "order_purchase_timestamp"
).agg(F.sum("price").alias("order_value"))

# Reference/snapshot date
reference_date = order_value_df.select(F.max("order_purchase_timestamp")).collect()[0][0]

# RFM metrics, grouped by the real stable customer identity
rfm_df = order_value_df.groupBy("customer_unique_id").agg(
    F.datediff(F.lit(reference_date), F.max("order_purchase_timestamp")).alias("Recency"),
    F.countDistinct("order_id").alias("Frequency"),
    F.sum("order_value").alias("Monetary"),
)

# RFM scores (quintiles, 5 = best)
rfm_scored_df = rfm_df.withColumn(
    "R_Score", F.ntile(5).over(Window.orderBy(F.col("Recency").desc()))
)
rfm_scored_df = rfm_scored_df.withColumn(
    "F_Score", F.ntile(5).over(Window.orderBy(F.col("Frequency")))
)
rfm_scored_df = rfm_scored_df.withColumn(
    "M_Score", F.ntile(5).over(Window.orderBy(F.col("Monetary")))
)

rfm_scored_df = rfm_scored_df.withColumn(
    "RFM_Score",
    F.concat(
        F.col("R_Score").cast("string"),
        F.col("F_Score").cast("string"),
        F.col("M_Score").cast("string"),
    ),
)

# ---------------------------------------------------------------------------
# Join RFM back to one row per real customer, assign surrogate key
# ---------------------------------------------------------------------------
customers_unique_df = customers_df.dropDuplicates(["customer_unique_id"])

dim_customer_df = customers_unique_df.join(
    rfm_scored_df, on="customer_unique_id", how="left"
)

dim_customer_df = dim_customer_df.select(
    "customer_unique_id",
    *[c for c in customers_unique_df.columns if c not in ("customer_id", "customer_unique_id")],
    "Recency",
    "Frequency",
    "Monetary",
    "RFM_Score",
)

window_spec = Window.orderBy("customer_unique_id")
dim_customer_df = dim_customer_df.withColumn("cust_key", F.row_number().over(window_spec))
dim_customer_df = dim_customer_df.select(
    "cust_key", *[c for c in dim_customer_df.columns if c != "cust_key"]
)

# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------
print("Customers count (deduped by unique id):", customers_unique_df.count())
print("RFM count:", rfm_df.count())
print("RFM Scored count:", rfm_scored_df.count())
print("Dim_Customer count:", dim_customer_df.count())
print("Customers with no matching orders (null RFM):",
      dim_customer_df.filter(F.col("RFM_Score").isNull()).count())

dim_customer_df.show(20, truncate=False)

# ---------------------------------------------------------------------------
# SAVE to gold layer
# ---------------------------------------------------------------------------
dim_customer_df.write.mode("overwrite").parquet("hdfs:///gold_zone/dim_customer")


# In[7]:


dim_customer_df.printSchema()


# In[ ]:




