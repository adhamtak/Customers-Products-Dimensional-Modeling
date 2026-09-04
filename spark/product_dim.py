#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window

price_events = (
    order_items
    .join(orders.select("order_id", "order_purchase_timestamp"), "order_id", "inner")
    .select("product_id", "order_purchase_timestamp", "price")
    .filter(F.col("product_id").isNotNull() & F.col("price").isNotNull())
)

w = Window.partitionBy("product_id").orderBy("order_purchase_timestamp")

price_flagged = price_events.withColumn(
    "prev_price", F.lag("price").over(w)
).withColumn(
    "is_new_version",
    F.when(F.col("prev_price").isNull() | (F.col("price") != F.col("prev_price")), 1).otherwise(0)
)

price_versioned = price_flagged.withColumn(
    "version_no", F.sum("is_new_version").over(w)
)

versions = (
    price_versioned
    .groupBy("product_id", "version_no")
    .agg(
        F.min("order_purchase_timestamp").alias("dw_start_ts"),
        F.first("price").alias("product_price"),
    )
    .withColumn("dw_start_date", F.to_date("dw_start_ts"))
    .drop("dw_start_ts")
)

w_next = Window.partitionBy("product_id").orderBy("version_no")

versions = versions.withColumn(
    "next_start_date", F.lead("dw_start_date").over(w_next)
).withColumn(
    "dw_end_date",
    F.when(F.col("next_start_date").isNotNull(), F.date_sub(F.col("next_start_date"), 1))
     .otherwise(F.lit("9999-12-31").cast("date"))
).withColumn(
    "is_current", F.col("next_start_date").isNull()
).drop("next_start_date")

products_clean = products.select(
    "product_id",
    F.col("product_category_name").alias("category"),
)

dim_product_facts = versions.join(products_clean, "product_id", "left")

w_key = Window.orderBy("product_id", "dw_start_date")

dim_product = (
    dim_product_facts
    .withColumn("product_key", F.row_number().over(w_key))
    .select(
        "product_key",
        "product_id",
        "category",
        "version_no",
        "product_price",
        "dw_start_date",
        "dw_end_date",
        "is_current",
    )
    .orderBy("product_id", "dw_start_date")
)

dim_product.show(200, truncate=False)


# In[2]:


products = spark.read.parquet(f"{RAW_PATH}/products_dataset/olist_products_dataset.parquet")
order_items = spark.read.parquet(f"{RAW_PATH}/order_items_dataset/olist_order_items_dataset.parquet")
orders = spark.read.parquet(f"{RAW_PATH}/orders_dataset/olist_orders_dataset.parquet")


# In[14]:


from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Data quality checks on products (diagnostic — run before transformation)
# ---------------------------------------------------------------------------
print("Null product_id count:", products.filter(F.col("product_id").isNull()).count())

print("Duplicate product_ids:")
products.groupBy("product_id").count().filter(F.col("count") > 1).show()

products.select(
    F.count("*").alias("total_rows"),
    F.count("product_id").alias("product_id_not_null"),
    F.count("product_category_name").alias("category_not_null"),
    F.countDistinct("product_id").alias("distinct_product_ids"),
).show()

products.select(
    F.sum(F.col("product_id").isNull().cast("int")).alias("null_product_id"),
    F.sum(F.col("product_category_name").isNull().cast("int")).alias("null_category"),
).show()

# ---------------------------------------------------------------------------
# Clean products: fill missing category instead of leaving it null
# ---------------------------------------------------------------------------
products_clean = products.select(
    "product_id",
    F.coalesce(F.col("product_category_name"), F.lit("Unknown")).alias("category"),
)

# ---------------------------------------------------------------------------
# (Optional) Date dimension — only needed if your project wants a shared
# calendar table; not required for Dim_Product's SCD2 logic itself
# ---------------------------------------------------------------------------
min_date = orders.select(F.min("order_purchase_timestamp")).first()[0]
max_date = orders.select(F.max("order_purchase_timestamp")).first()[0]

date_df = spark.sql(f"""
    SELECT sequence(
        to_date('{min_date}'),
        to_date('{max_date}'),
        interval 1 day
    ) AS date_array
""")

date_df = date_df.select(F.explode("date_array").alias("full_date"))

date_df = date_df.select(
    F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
    "full_date",
    F.day("full_date").alias("day"),
    F.month("full_date").alias("month"),
    F.quarter("full_date").alias("quarter"),
    F.year("full_date").alias("year"),
    F.dayofweek("full_date").alias("day_of_week"),
    F.date_format("full_date", "EEEE").alias("day_name"),
)


# In[4]:


from pyspark.sql import functions as F

silver_order_items = order_items.select(
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "price",
    "freight_value"
)

silver_order_items.select(
    F.count("*").alias("total_rows"),
    F.count("order_id").alias("order_id_not_null"),
    F.count("product_id").alias("product_id_not_null"),
    F.count("price").alias("price_not_null"),
    F.sum((F.col("price") <= 0).cast("int")).alias("invalid_price"),
    F.sum((F.col("freight_value") < 0).cast("int")).alias("invalid_freight")
).show()

# Drop rows with an invalid price -- a zero/negative price would corrupt
# the SCD2 version-detection logic (a fake "price change" that isn't real)
silver_order_items = silver_order_items.filter(
    F.col("price") > 0
).filter(
    F.col("freight_value") >= 0
)


# In[7]:


from pyspark.sql import SparkSession

# 1. Ensure SparkSession is initialized
spark = SparkSession.builder.getOrCreate()

# 2. Define the base path variable
RAW_PATH = "/raw_zones"  # Replace with your actual path, e.g., "/raw_zones"

# 3. Read the Parquet datasets
products = spark.read.parquet(f"{RAW_PATH}/products_dataset/olist_products_dataset.parquet")
order_items = spark.read.parquet(f"{RAW_PATH}/order_items_dataset/olist_order_items_dataset.parquet")
orders = spark.read.parquet(f"{RAW_PATH}/orders_dataset/olist_orders_dataset.parquet")


# In[8]:


RAW_PATH = "/raw_zones"

products = spark.read.parquet(f"{RAW_PATH}/products/olist_products_dataset.parquet")
order_items = spark.read.parquet(f"{RAW_PATH}/order_items/olist_order_items_dataset.parquet")
orders = spark.read.parquet(f"{RAW_PATH}/orders/olist_orders_dataset.parquet")


# In[15]:


from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Data quality checks on products (diagnostic — run before transformation)
# ---------------------------------------------------------------------------
print("Null product_id count:", products.filter(F.col("product_id").isNull()).count())

print("Duplicate product_ids:")
products.groupBy("product_id").count().filter(F.col("count") > 1).show()

products.select(
    F.count("*").alias("total_rows"),
    F.count("product_id").alias("product_id_not_null"),
    F.count("product_category_name").alias("category_not_null"),
    F.countDistinct("product_id").alias("distinct_product_ids"),
).show()

products.select(
    F.sum(F.col("product_id").isNull().cast("int")).alias("null_product_id"),
    F.sum(F.col("product_category_name").isNull().cast("int")).alias("null_category"),
).show()

# ---------------------------------------------------------------------------
# Clean products: fill missing category instead of leaving it null
# ---------------------------------------------------------------------------
products_clean = products.select(
    "product_id",
    F.coalesce(F.col("product_category_name"), F.lit("Unknown")).alias("category"),
)

# ---------------------------------------------------------------------------
# (Optional) Date dimension — only needed if your project wants a shared
# calendar table; not required for Dim_Product's SCD2 logic itself
# ---------------------------------------------------------------------------
min_date = orders.select(F.min("order_purchase_timestamp")).first()[0]
max_date = orders.select(F.max("order_purchase_timestamp")).first()[0]

date_df = spark.sql(f"""
    SELECT sequence(
        to_date('{min_date}'),
        to_date('{max_date}'),
        interval 1 day
    ) AS date_array
""")

date_df = date_df.select(F.explode("date_array").alias("full_date"))

date_df = date_df.select(
    F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
    "full_date",
    F.dayofmonth("full_date").alias("day"),
    F.month("full_date").alias("month"),
    F.quarter("full_date").alias("quarter"),
    F.year("full_date").alias("year"),
    F.dayofweek("full_date").alias("day_of_week"),
    F.date_format("full_date", "EEEE").alias("day_name"),
)
products_clean.write.mode("overwrite").parquet("hdfs:///silver_zones/products_dataset")


# In[11]:


from pyspark.sql import functions as F

silver_order_items = order_items.select(
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "price",
    "freight_value"
)

silver_order_items.select(
    F.count("*").alias("total_rows"),
    F.count("order_id").alias("order_id_not_null"),
    F.count("product_id").alias("product_id_not_null"),
    F.count("price").alias("price_not_null"),
    F.sum((F.col("price") <= 0).cast("int")).alias("invalid_price"),
    F.sum((F.col("freight_value") < 0).cast("int")).alias("invalid_freight")
).show()

# Drop rows with an invalid price -- a zero/negative price would corrupt
# the SCD2 version-detection logic (a fake "price change" that isn't real)
silver_order_items = silver_order_items.filter(
    F.col("price") > 0
).filter(
    F.col("freight_value") >= 0
)


# In[12]:


silver_orders = orders.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp"
)

silver_orders.select(
    F.count("*").alias("total_rows"),
    F.count("order_id").alias("order_id_not_null"),
    F.count("customer_id").alias("customer_id_not_null"),
    F.count("order_purchase_timestamp").alias("purchase_date_not_null"),
    F.countDistinct("order_id").alias("distinct_orders")
).show()

# Drop rows with no purchase timestamp -- your SCD2 logic can't place
# a price observation in time without a date
silver_orders = silver_orders.filter(
    F.col("order_purchase_timestamp").isNotNull()
)


# In[13]:


silver_order_items.write.mode("overwrite").parquet("hdfs:///silver_zones/order_items_dataset")
silver_orders.write.mode("overwrite").parquet("hdfs:///silver_zones/orders_dataset")


# In[17]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window

products = spark.read.parquet("hdfs:///silver_zones/products_dataset")
order_items = spark.read.parquet("hdfs:///silver_zones/order_items_dataset")
orders = spark.read.parquet("hdfs:///silver_zones/orders_dataset")

price_events = (
    order_items
    .join(orders.select("order_id", "order_purchase_timestamp"), "order_id", "inner")
    .select("product_id", "order_purchase_timestamp", "price")
    .filter(F.col("product_id").isNotNull() & F.col("price").isNotNull())
)

w = Window.partitionBy("product_id").orderBy("order_purchase_timestamp")

price_flagged = price_events.withColumn(
    "prev_price", F.lag("price").over(w)
).withColumn(
    "is_new_version",
    F.when(F.col("prev_price").isNull() | (F.col("price") != F.col("prev_price")), 1).otherwise(0)
)

price_versioned = price_flagged.withColumn(
    "version_no", F.sum("is_new_version").over(w)
)

versions = (
    price_versioned
    .groupBy("product_id", "version_no")
    .agg(
        F.min("order_purchase_timestamp").alias("dw_start_ts"),
        F.first("price").alias("product_price"),
    )
    .withColumn("dw_start_date", F.to_date("dw_start_ts"))
    .drop("dw_start_ts")
)

w_next = Window.partitionBy("product_id").orderBy("version_no")

versions = versions.withColumn(
    "next_start_date", F.lead("dw_start_date").over(w_next)
).withColumn(
    "dw_end_date",
    F.when(F.col("next_start_date").isNotNull(), F.date_sub(F.col("next_start_date"), 1))
     .otherwise(F.lit("9999-12-31").cast("date"))
).withColumn(
    "is_current", F.col("next_start_date").isNull()
).drop("next_start_date")

products_clean = products.select(
    "product_id",
    F.coalesce(F.col("category"), F.lit("Unknown")).alias("category"),
)

dim_product_facts = versions.join(products_clean, "product_id", "left")

w_key = Window.orderBy("product_id", "dw_start_date")

dim_product = (
    dim_product_facts
    .withColumn("product_key", F.row_number().over(w_key))
    .select(
        "product_key",
        "product_id",
        "category",
        "version_no",
        "product_price",
        "dw_start_date",
        "dw_end_date",
        "is_current",
    )
    .orderBy("product_id", "dw_start_date")
)

dim_product.show(200, truncate=False)
print("Total rows:", dim_product.count())


# In[18]:


dim_product.write.mode("overwrite").parquet("hdfs:///gold_zone/dim_product")


# In[19]:


dim_product.printSchema()


# In[ ]:




