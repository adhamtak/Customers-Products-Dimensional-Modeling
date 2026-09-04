#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("DimDate").getOrCreate()

# ---------------------------------------------------------------------------
# LOAD -- only need orders to know the real date range in the data
# ---------------------------------------------------------------------------
orders_df = spark.read.parquet("hdfs:///silver_zones/orders_dataset")

orders_df = orders_df.withColumn(
    "order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp")
)

min_date = orders_df.select(F.min("order_purchase_timestamp")).first()[0]
max_date = orders_df.select(F.max("order_purchase_timestamp")).first()[0]

print("Date range:", min_date, "to", max_date)

# ---------------------------------------------------------------------------
# BUILD -- one row per calendar day across the real order date range
# ---------------------------------------------------------------------------
date_df = spark.sql(f"""
    SELECT sequence(
        to_date('{min_date}'),
        to_date('{max_date}'),
        interval 1 day
    ) AS date_array
""")

date_df = date_df.select(F.explode("date_array").alias("full_date"))

dim_date = date_df.select(
    F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
    "full_date",
    F.dayofmonth("full_date").alias("day"),
    F.month("full_date").alias("month"),
    F.quarter("full_date").alias("quarter"),
    F.year("full_date").alias("year"),
    F.dayofweek("full_date").alias("day_of_week"),
    F.date_format("full_date", "EEEE").alias("day_name"),
)

# ---------------------------------------------------------------------------
# CHECKS
# ---------------------------------------------------------------------------
print("Total dim_date rows:", dim_date.count())
dim_date.show(10, truncate=False)

# date_key should be unique -- sanity check
print("Distinct date_keys:", dim_date.select("date_key").distinct().count())

# ---------------------------------------------------------------------------
# SAVE to gold layer
# ---------------------------------------------------------------------------
dim_date.write.mode("overwrite").parquet("hdfs:///gold_zone/dim_date")


# In[2]:


dim_date.printSchema()


# In[ ]:




