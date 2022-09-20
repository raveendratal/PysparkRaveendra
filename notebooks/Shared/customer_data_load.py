# Databricks notebook source
df = spark.read.csv("dbfs:/FileStore/tables/employeess.csv",header=True,sep=",",inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.dropDuplicates(["employee_id"]).dropna(how='all')

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("employees")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees where country_id='US'

# COMMAND ----------


