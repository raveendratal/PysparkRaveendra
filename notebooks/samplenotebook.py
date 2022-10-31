# Databricks notebook source
print('this is my first python example')

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

display(df) -- df.collect()

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

