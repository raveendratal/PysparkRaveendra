# Databricks notebook source
# MAGIC %python
# MAGIC df = spark.read.option("nullValue","null").csv("/FileStore/tables/emp.csv",header=True,inferSchema=True)
# MAGIC df.write.format("delta").saveAsTable("emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp
