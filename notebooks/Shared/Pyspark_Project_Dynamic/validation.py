# Databricks notebook source
# MAGIC %md
# MAGIC #### How to handle corrupted Parquet files with different schema
# MAGIC * Let’s say you have a large list of essentially independent Parquet files, with a variety of different schemas. You want to read only those files that match a specific schema and skip the files that don’t match.
# MAGIC 
# MAGIC * One solution could be to read the files in sequence, identify the schema, and union the DataFrames together. However, this approach is impractical when there are hundreds of thousands of files.
# MAGIC * Set the Apache Spark property spark.sql.files.ignoreCorruptFiles to true and then read the files with the desired schema. Files that don’t match the specified schema are ignored. The resultant dataset contains only data from those files that match the specified schema.
# MAGIC 
# MAGIC * Set the Spark property using spark.conf.set:  `spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ignore Missing Files
# MAGIC * Spark allows you to use spark.sql.files.ignoreMissingFiles to ignore missing files while reading data from files. Here, missing file really means the deleted file under directory after you construct the DataFrame. When set to true, the Spark jobs will continue to run when encountering missing files and the contents that have been read will still be returned.
# MAGIC * enable ignore corrupt files
# MAGIC * `spark.sql("set spark.sql.files.ignoreCorruptFiles=true")`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Recursive File Lookup
# MAGIC * __`recursiveFileLookup`__ is used to recursively load files and it disables partition inferring. Its default value is false. If data source explicitly specifies the partitionSpec when recursiveFileLookup is true, exception will be thrown.
# MAGIC 
# MAGIC * To load all files recursively, you can use:
# MAGIC 
# MAGIC ```
# MAGIC recursive_loaded_df = spark.read.format("parquet")\
# MAGIC     .option("recursiveFileLookup", "true")\
# MAGIC     .load("examples/src/main/resources/dir1")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Path Global Filter
# MAGIC * __`pathGlobFilter`__ is used to only include files with file names matching the pattern. The syntax follows org.apache.hadoop.fs.GlobFilter. It does not change the behavior of partition discovery.
# MAGIC ```
# MAGIC df = spark.read.load("examples/src/main/resources/dir1",
# MAGIC                      format="parquet", pathGlobFilter="*.parquet")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Modification Time Path Filters
# MAGIC * __`modifiedBefore`__ and __`modifiedAfter`__ are options that can be applied together or separately in order to achieve greater granularity over which files may load during a Spark batch query. (Note that Structured Streaming file sources don’t support these options.)
# MAGIC 
# MAGIC * modifiedBefore: an optional timestamp to only include files with modification times occurring before the specified time. The provided timestamp must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
# MAGIC * modifiedAfter: an optional timestamp to only include files with modification times occurring after the specified time. The provided timestamp must be in the following format: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)
# MAGIC * When a timezone option is not provided, the timestamps will be interpreted according to the Spark session timezone (spark.sql.session.timeZone).
# MAGIC 
# MAGIC ```
# MAGIC # Only load files modified before 07/1/2050 @ 08:30:00
# MAGIC df = spark.read.load("examples/src/main/resources/dir1",
# MAGIC                      format="parquet", modifiedBefore="2050-07-01T08:30:00")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Surrogate Key we will use `SHA2` hash function in big data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sha2('what is spark', 256) as key union all
# MAGIC SELECT md5('what is spark') as key;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) , 'stg_customers' from stg_sales.stg_customers UNION ALL
# MAGIC select count(*) , 'stg_countries' from stg_sales.stg_countries UNION ALL
# MAGIC select count(*) , 'stg_product' from stg_sales.stg_product UNION ALL
# MAGIC select count(*) , 'stg_channels' from stg_sales.stg_channels UNION ALL
# MAGIC select count(*) , 'stg_times' from stg_sales.stg_times UNION ALL
# MAGIC select count(*) , 'stg_promotions' from stg_sales.stg_promotions UNION ALL
# MAGIC select count(*) , 'stg_sales_transaction' from stg_sales.stg_sales_transaction UNION ALL
# MAGIC select count(*) , 'stg_cost_transaction' from stg_sales.stg_costs_transaction;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) , 'curation_customers' from curation_sales.curation_customers UNION ALL
# MAGIC select count(*) , 'curation_countries' from curation_sales.curation_countries UNION ALL
# MAGIC select count(*) , 'curation_product' from curation_sales.curation_product UNION ALL
# MAGIC select count(*) , 'curation_channels' from curation_sales.curation_channels UNION ALL
# MAGIC select count(*) , 'curation_times' from curation_sales.curation_times UNION ALL
# MAGIC select count(*) , 'curation_promotions' from curation_sales.curation_promotions UNION ALL
# MAGIC select count(*) , 'sales_transaction' from curation_sales.curation_sales_transaction UNION ALL
# MAGIC select count(*) , 'costs_transaction' from curation_sales.curation_costs_transaction;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) , 'dim_customers' from dw_sales.dim_customers UNION ALL
# MAGIC select count(*) , 'dim_countries' from dw_sales.dim_countries UNION ALL
# MAGIC select count(*) , 'dim_product' from dw_sales.dim_product UNION ALL
# MAGIC select count(*) , 'dim_channels' from dw_sales.dim_channels UNION ALL
# MAGIC select count(*) , 'dim_times' from dw_sales.dim_times UNION ALL
# MAGIC select count(*) , 'dim_promotions' from dw_sales.dim_promotions UNION ALL
# MAGIC select count(*) , 'fact_sales_transaction' from dw_sales.fact_sales_transaction UNION ALL
# MAGIC select count(*) , 'fact_cost_transaction' from dw_sales.fact_costs_transaction;

# COMMAND ----------

# MAGIC %sql
# MAGIC select sha2('52770',512)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dw_sales.fact_costs_transaction  -- N/A -- Null String -- EMPTY 

# COMMAND ----------

# MAGIC %sql
# MAGIC select sha2("52769",256),md5("pyspark"),sha2("pyspark",256)
# MAGIC -- every 100k or 1m records

# COMMAND ----------

# MAGIC %sql
# MAGIC use log_db;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find Jobs Audit Information

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from log_db.job_audit_log

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bad data into this table..

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from log_db.job_bad_data_log where input_file_name='dbfs:/mnt/landing/sales/channels/channels06-12-2020-10-20-50.csv'