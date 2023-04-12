# Databricks notebook source
# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %fs ls file:/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/dept_pipe.csv

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/dept_pipe.csv",header=True,inferSchema=True,sep="|")
display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("dept")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dept

# COMMAND ----------

