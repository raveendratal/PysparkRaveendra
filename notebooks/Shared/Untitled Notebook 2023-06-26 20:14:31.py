# Databricks notebook source
# MAGIC %fs ls /

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/emp.csv")
df2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/dept3.csv")

# COMMAND ----------

df1.write.format("delta").saveAsTable("emp_delta")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select deptno,sum(sal) from emp_delta group by deptno