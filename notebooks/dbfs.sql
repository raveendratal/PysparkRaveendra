-- Databricks notebook source
-- MAGIC %fs ls /

-- COMMAND ----------

-- MAGIC %fs mkdirs /batch28

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.mounts())

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select
-- MAGIC   *
-- MAGIC from
-- MAGIC   emp
-- MAGIC where
-- MAGIC   deptno = 10

-- COMMAND ----------

