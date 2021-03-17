# Databricks notebook source
# MAGIC %md
# MAGIC ###Overview
# MAGIC 
# MAGIC This is a caller notebook used to perform Custom Loggin in all the python notebooks.
# MAGIC 
# MAGIC ###Details
# MAGIC | Detail Tag | Information
# MAGIC | - | - | - | - |
# MAGIC | Originally Created By | Raveendra  
# MAGIC | Object Name | Channels dimention processing
# MAGIC | File Type | channels .csv file
# MAGIC | Target Location | Databricks Mount Path External Table 
# MAGIC 
# MAGIC ###History
# MAGIC |Date | Developed By | comments
# MAGIC |----|-----|----
# MAGIC |05/12/2020|Ravendra| Initial Version

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calling Common utility functions notebook here using `%run` notebook name `functions`

# COMMAND ----------

# MAGIC %run Shared/Pyspark_Project_Code_Examples/utils/functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calling Common utility global variables notebook here using `%run` notebook name `global_variables`

# COMMAND ----------

# MAGIC %run /Shared/Pyspark_Project_Code_Examples/utils/global_variables

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calling Custom Loggin Defination script for storing Pyspark level custom log in file.
# MAGIC * calling `Custom_Logging` script notebook.

# COMMAND ----------

# MAGIC %run /Shared/Pyspark_Project_Code_Examples/utils/Custom_Logging

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Defining Custom Schema while Reading file and creating dataframe.
# MAGIC * `inferSchema` is a costly operation. we should not use this on big data files. for creating datatypes it will read full data and dump into driver node.
# MAGIC * To avoid that load on `Driver Node` we are going to use `Customer Schema Defination using pyspark.sql.types`

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType,LongType,IntegerType
channels_schema = StructType([StructField('CHANNEL_ID', IntegerType(), False),
                     StructField('CHANNEL_DESC', StringType(), False),
                     StructField('CHANNEL_CLASS', StringType(), False),
                     StructField('CHANNEL_CLASS_ID', IntegerType(), False),
                     StructField('CHANNEL_TOTAL', StringType(), False),
                     StructField('CHANNEL_TOTAL_ID', IntegerType(), False),
                     StructField('_corrupt_record', StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading Source .csv files and creating dataframe.
# MAGIC * we are using `read_csv` function for reading files and creating `dataframe`
# MAGIC * we are using `Try and Exception` block to track if any errors at runtime.

# COMMAND ----------

try:
    df_channels = func_read_csv(gv_source_path+gv_channels_tbl+'/*.csv',gv_bad_path+gv_channels_tbl,channels_schema)
    logger.info('Channels File is available in this location :{0}'.format(gv_source_path+gv_channels_tbl))
except Exception as e:
    logger.error(e)
    logger.error('Channels File is not  available in this location :{0}'.format(gv_source_path+gv_channels_tbl))        
    logging.shutdown()
    dbutils.notebook.exit('Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Adding adition columns into `Dataframe`
# MAGIC * below command we are going to create two aditional two columns in `dataframe`
# MAGIC * `Key Column` which is generated using `md5` based on `Id Column` from source table
# MAGIC * Adding `load_date` column and inserting `current_timestamp()`

# COMMAND ----------

from pyspark.sql.functions import md5,col,concat,lit,current_timestamp

df_channels = df_channels.withColumn("channels_key",md5(concat(df_channels.CHANNEL_ID)))\
              .withColumn('load_date',lit(current_timestamp()))



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating `DataFrame` from target table data based on `single ID Column`
# MAGIC * we are using below command to get existing data from target table to compare incremental data from source.

# COMMAND ----------

# Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the referenced columns only include the internal corrupt record column (named _corrupt_record by default).
# For example: spark.read.schema(schema).csv(file).filter($"_corrupt_record".isNotNull).count() and spark.read.schema(schema).csv(file).select("_corrupt_record").show().
# Instead, you can cache or save the parsed results and then send the same query.
# Remove corrupt records
df_channels.cache()
df_channels = df_channels.filter((col("_corrupt_record").isNull() )).drop(col("_corrupt_record"))
df_channels.createOrReplaceTempView('df_channels_v')
df_channels_old = spark.sql('select channel_id from  sales.channels')

# COMMAND ----------

df_channels.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Finding incremental data using `PYSPARK LEFT ANTI JOIN` joining with target table.
# MAGIC * below command can find the only incremental data which is not available in target table using `LEFT ANTI JOIN`

# COMMAND ----------

df_channels_incremental = df_channels.join(df_channels_old,df_channels['CHANNEL_ID'] == df_channels_old['CHANNEL_ID'],'anti')

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Inserting new data which is not available in target table. 
# MAGIC * above command is created new `dataframe - df_channel_incremental` created for incremental data.
# MAGIC * directly inserting into target table using pyspark `write with append mode`

# COMMAND ----------


if bool(df_channels_incremental.head(1)):
  df_channels_incremental.write.format('delta').mode('append').saveAsTable('sales.channels')
  logger.info('No of Incremental records are inserted : {}'.format(df_channels_incremental.count()))
  logging.shutdown()
else:
  logger.info('No Incremental Data for channels table from source ')
  logging.shutdown()

# COMMAND ----------

df_channels.unpersist()

# COMMAND ----------

df_channels_incremental.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Using SQL Merge Script we can update the existing data when data is matching based on condition

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO sales.channels t 
# MAGIC USING df_channels_v s
# MAGIC ON t.CHANNEL_ID = s.CHANNEL_ID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Verify newly inserted data is available in target table based on `load_date as Today's Date current_date()`

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from sales.channels where to_date(load_date,'YYYY-MM-dd')=current_Date()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Validation in target table if its having any duplicate rows. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select channels_key,count(*) from df_channels_v group by channels_key having count(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales.channels

# COMMAND ----------

# MAGIC %sql 
# MAGIC select channels_key,count(*) from sales.channels group by channels_key having count(*)>1

# COMMAND ----------

# MAGIC %md
# MAGIC *  Exit the Databricks notebook with `SUCCESS STATUS` which we can use for validation this notebook is processed successfully or failed in between.
# MAGIC *  If notebook failed inbetween, this last command wont be executed and we can say its failed inbetween.

# COMMAND ----------

dbutils.notebook.exit('CHANNELSSUCCESS')