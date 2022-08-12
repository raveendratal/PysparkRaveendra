# Databricks notebook source
df = spark.read.csv("dbfs:/databricks-datasets/asa/airlines/1988.csv",header=True,inferSchema=True)

# COMMAND ----------

display(df.limit(100))

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("airlines")

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/asa/airlines/