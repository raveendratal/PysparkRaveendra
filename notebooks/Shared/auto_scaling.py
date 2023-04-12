# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/asa/airlines

# COMMAND ----------

df_airlines = spark.read.csv("/databricks-datasets/asa/airlines",header=True)

# COMMAND ----------

df_airlines.write.format("delta").mode("append").saveAsTable("airlines")