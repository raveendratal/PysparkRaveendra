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
# MAGIC | Target Location | Databricks Delta Table 
# MAGIC 
# MAGIC ###History
# MAGIC |Date | Developed By | comments
# MAGIC |----|-----|----
# MAGIC |05/12/2020|Ravendra| Initial Version
# MAGIC | Find more Videos | Youtube   | <a href="https://www.youtube.com/watch?v=FpxkiGPFyfM&list=PL50mYnndduIHRXI2G0yibvhd3Ck5lRuMn" target="_blank"> Youtube link </a>|

# COMMAND ----------

# MAGIC %md
# MAGIC """
# MAGIC dbutils.widgets.text("landing_zone_file_path","/mnt/landing/sales")
# MAGIC dbutils.widgets.text("landing_zone_folder_name","channels")
# MAGIC dbutils.widgets.text("landing_zone_file_name","channels")
# MAGIC dbutils.widgets.text("landing_zone_file_type","csv")
# MAGIC dbutils.widgets.text("staging_zone_database_name","stg_sales")
# MAGIC dbutils.widgets.text("staging_zone_table_name","stg_channels")
# MAGIC dbutils.widgets.text("staging_zone_table_pk_column","CHANNEL_ID")
# MAGIC dbutils.widgets.text("curation_zone_database_name","curation_sales")
# MAGIC dbutils.widgets.text("curation_zone_table_name","curation_channels")
# MAGIC dbutils.widgets.text("curation_zone_table_pk_column","CHANNEL_ID")
# MAGIC dbutils.widgets.text("dw_zone_database_name","dw_sales")
# MAGIC dbutils.widgets.text("dw_zone_table_name","dw_channels")
# MAGIC dbutils.widgets.text("dw_zone_table_pk_column","CHANNEL_ID")
# MAGIC """

# COMMAND ----------

# MAGIC %run "/Shared/Pyspark_Project_Dynamic/utils/libraries"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calling Common utility functions notebook here using `%run` notebook name `functions`

# COMMAND ----------

# MAGIC %run "/Shared/Pyspark_Project_Dynamic/utils/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calling Common utility `Pyspark Schema` notebook here using `%run` notebook name `pyspark_schema`

# COMMAND ----------

# MAGIC %run "/Shared/Pyspark_Project_Dynamic/utils/pyspark_schema"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating parameters using `widgets` function `func_create_widgets((list))`
# MAGIC #### This is onetime activity

# COMMAND ----------

func_create_widgets(("staging_zone_database_name","staging_zone_table_name","staging_zone_table_pk_column","curation_zone_database_name","curation_zone_table_name","curation_zone_table_pk_column","dw_zone_database_name","dw_zone_table_name","dw_zone_table_pk_column","job_id","job_name"))

# COMMAND ----------

df_staging_table = spark.sql('select * from '+staging_zone_database_name +'.'+staging_zone_table_name)
#display(df_staging_table)

# COMMAND ----------

try:
  # Calling validate null data function
  func_validate_nulldata('Null Data',df_staging_table)
  # Calling Validate duplicate data function
  func_validate_duplicatedata('Duplicate Data',df_staging_table)
  # Calling remove null Data and duplicate data validation
  df_staging_valid_data = func_remove_null_duplicate(df_staging_table,staging_zone_table_pk_column)
  curation_rowcount = df_staging_valid_data.count()
except Exception as e:
  func_register_log("ERROR", "Error While validating staging table data :" +str(sys.exc_info()[1]))
  raise dbutils.notebook.exit(e) #raise the exception

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating view from `dataframe` to update data in target table

# COMMAND ----------

df_staging_valid_data.createOrReplaceTempView("df_staging_valid_data_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_staging_valid_data_v

# COMMAND ----------

# MAGIC %md
# MAGIC #### Updating Curating table from staging table after data validations

# COMMAND ----------

try:
  curation_updated_df = spark.sql("""MERGE INTO {0}.{1} t 
USING df_staging_valid_data_v s
ON t.PROD_ID = s.PROD_ID
AND t.TIME_ID = s.TIME_ID 
AND t.CHANNEL_ID = s.CHANNEL_ID 
AND t.PROMO_ID = s.PROMO_ID
WHEN MATCHED THEN
  UPDATE SET *
WHEN Not MATCHED THEN
  INSERT * """.format(curation_zone_database_name,curation_zone_table_name))
except Exception as e:
  func_register_log("ERROR", "Error While validating staging table data :" +str(sys.exc_info()[1]))
  raise dbutils.notebook.exit(json.dumps({"status":"FAILED","error":str(e)})) #raise the exception

# COMMAND ----------

curation_output_rows = curation_updated_df.toJSON().collect()[0]
curation_rowcount = json.loads(curation_output_rows)
print('curation stage completed with : ' ,curation_rowcount)

# COMMAND ----------

try:
  df_curation_table = spark.sql('select * from '+curation_zone_database_name +'.'+curation_zone_table_name)
  
  #display(df_curation_table
except Exception as e:
  func_register_log("ERROR", "Error While validating staging table data :" +str(sys.exc_info()[1]))
  raise dbutils.notebook.exit(json.dumps({"status":"FAILED","error":str(e)})) #raise the exception

# COMMAND ----------

# MAGIC %md
# MAGIC #### Generating Surrogate key using __`sha2-256`__ or `md5`

# COMMAND ----------

cur_cols = curation_zone_table_pk_column.split(",")

# COMMAND ----------

try:
  from pyspark.sql.functions import md5,sha2,col,concat,lit,current_timestamp,upper,concat
  df_curation_table = df_curation_table.withColumn("COST_FACT_KEY",sha2(concat_ws(",",col("PROD_ID"),col("TIME_ID"),col("CHANNEL_ID"),col("PROMO_ID")),256))
  #df_curation_table.withColumn("COST_FACT_KEY",sha2(concat(*cur_cols),256))
  #display(df_curation_table)
except Exception as e:
  func_register_log("ERROR", "Error While validating staging table data :" +str(sys.exc_info()[1]))
  raise dbutils.notebook.exit(json.dumps({"status":"FAILED","error":str(e)})) #raise the exception

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating view for updating in merge

# COMMAND ----------

df_curation_table.createOrReplaceTempView("df_curation_table_v")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calling final merge script to update in final table

# COMMAND ----------

try:
  dwh_updates_df = spark.sql("""MERGE INTO {0}.{1} t 
USING (select
  fact.cost_fact_key,
  nvl(dp.prod_key,-1) as prod_key,
  nvl(dch.channel_key,-1) as channel_key,
  nvl(dt.time_key,-1) as time_key,
  nvl(dpro.promo_key,-1) as promo_key,
  fact.UNIT_COST,
  fact.UNIT_PRICE,
  fact.created_date,
  fact.created_by,
  fact.updated_date,
  fact.updated_by
from
  df_curation_table_v fact
  left join dw_sales.dim_product dp on fact.prod_id = dp.prod_id
  left join dw_sales.dim_channels dch on fact.channel_id = dch.channel_id
  left join dw_sales.dim_times dt on fact.time_id = dt.time_id
  left join dw_sales.dim_promotions dpro  on fact.promo_id = dpro.promo_id) s
ON  t.COST_FACT_KEY = s.COST_FACT_KEY
WHEN MATCHED THEN
  UPDATE SET *
WHEN Not MATCHED THEN
  INSERT * """.format(dw_zone_database_name,dw_zone_table_name))
except Exception as e:
  func_register_log("ERROR", "Error While validating staging table data :" +str(sys.exc_info()[1]))
  raise dbutils.notebook.exit(json.dumps({"status":"FAILED","error":str(e)})) #raise the exception

# COMMAND ----------

dwh_output_rows = dwh_updates_df.toJSON().collect()[0]
dwh_rowcount = json.loads(dwh_output_rows)
print('dwh stage completed with : ' ,dwh_rowcount)

# COMMAND ----------

# Calling final Audit log function to append logs
func_final_log_append()

# COMMAND ----------

# MAGIC %md
# MAGIC *  Exit the Databricks notebook with `SUCCESS STATUS` which we can use for validation this notebook is processed successfully or failed in between.
# MAGIC *  If notebook failed inbetween, this last command wont be executed and we can say its failed inbetween.

# COMMAND ----------

#dbutils.notebook.exit('SUCCESS')
import json
dbutils.notebook.exit(json.dumps({"curationRows":curation_rowcount,"dwhRows":dwh_rowcount,"jobName":job_name,"jobId":job_id,"status":"SUCCESS"}))

# COMMAND ----------

print('new line added')