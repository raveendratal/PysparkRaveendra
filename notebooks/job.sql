-- Databricks notebook source
print("this is sample print statement")

-- COMMAND ----------

-- MAGIC %python

-- COMMAND ----------

-- MAGIC %fs ls 

-- COMMAND ----------

spark.sql("select * from emp")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from emp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1 = spark.read.format("csv").option("nullValue","null").load("dbfs:/FileStore/tables/emp.csv",header=True,inferSchema=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1.write.format("delta").mode("append").saveAsTable("emp")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from emp
