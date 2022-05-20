# Databricks notebook source
# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %fs ls /users/hive/warehouse

# COMMAND ----------

# MAGIC %sh ls

# COMMAND ----------

# MAGIC %python
# MAGIC df_emp_csv = spark.read.csv("/FileStore/tables/emp.csv",header=True)
# MAGIC display(df_emp_csv)