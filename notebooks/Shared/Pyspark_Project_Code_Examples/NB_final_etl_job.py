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
# MAGIC | Object Name | Final ETL Job 
# MAGIC | File Type | Processing all files..
# MAGIC | Target Location | Databricks Mount Path External Table 
# MAGIC 
# MAGIC ###History
# MAGIC |Date | Developed By | comments
# MAGIC |----|-----|----
# MAGIC |05/12/2020|Ravendra| Initial Version

# COMMAND ----------

# MAGIC %run Shared/Pyspark_Project_Code_Examples/utils/functions

# COMMAND ----------

# MAGIC %md
# MAGIC * Shared/Pyspark_Project_Code_Examples/NB_channels_dim

# COMMAND ----------

func_create_widgets(["cust_botebook_path","product_botebook_path","countries_botebook_path","times_botebook_path","channels_botebook_path","promotions_botebook_path"])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Process All Dimenstion notebooks in parallel using python `Threadpool`
# MAGIC * Python Multiprocessing can be triggered multiple notebooks parallely.

# COMMAND ----------

from multiprocessing.pool import ThreadPool
pool = ThreadPool(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Calling all dimention notebooks using `dbutils.notebook.run() method` can trigger jobs (workflows) multiple in parallel.
# MAGIC * `dbutils.notebook.run()` 1st parameter is `notebook path` and 2nd parameter `timeout in seconds` , 3rd parameter arguments. which is notebooks.

# COMMAND ----------

[channels_status,countries_status,customer_status,product_status,promotions_status,times_status] = pool.map(lambda path: dbutils.notebook.run(path,timeout_seconds=6000,arguments = {"input-data": path}),[getArgument("channels_botebook_path"),getArgument("countries_botebook_path"),getArgument("cust_botebook_path"),getArgument("product_botebook_path"),getArgument("promotions_botebook_path"),getArgument("times_botebook_path")])

# COMMAND ----------

channels_status

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling SalesFact load after completion of all dimentions...
# MAGIC * Getting each dimension load status into variable and based on `SUCCESS` status we can run fact load.

# COMMAND ----------

if customer_status=='CUSTOMERSSUCCESS' and product_status=='PRODUCTSUCCESS' and countries_status=='COUNTRIESSUCCESS' and times_status=='TIMESSUCCESS' and promotions_status=='PROMOTIONSSUCCESS' and channels_status=='CHANNELSSUCCESS':
    
  dbutils.notebook.run('/Shared/Pyspark_Project_Code_Examples/NB_sales_fact',300) 
  
else:
  
  print("Dimension loads are failed. please verify.. job log..")

# COMMAND ----------

# MAGIC %md
# MAGIC #### IF you want to process sequential run we can use below list running one by one..

# COMMAND ----------

"""
dbutils.notebook.run('./process_times_data',300)
dbutils.notebook.run('./process_customers_data',300)
dbutils.notebook.run('./process_product_data',300)
dbutils.notebook.run('./process_countries_data',300)
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales.sales