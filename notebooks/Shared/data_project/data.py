# Databricks notebook source
# MAGIC %md
# MAGIC ## this notebook we are using for to read csv file and create delta table

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading CSV File 

# COMMAND ----------

# MAGIC %python
# MAGIC df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/emp.csv",inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing into delta table

# COMMAND ----------

df1.write.format("delta").mode("append").saveAsTable("emp")

# COMMAND ----------

# MAGIC %md
# MAGIC # Validating written data using sql Query

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   emp

# COMMAND ----------

display(df1.limit(5))