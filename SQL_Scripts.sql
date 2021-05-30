
select * from [dbo].[JOB_LIST]
GO

CREATE TABLE [dbo].[JOB_LIST](
[id] [varchar](100) NULL,
[jobname] [varchar](100) NULL,
[src_folderpath] [varchar](100) NULL,
[tgt_folderpath] [varchar](100) NULL,
[directory] [varchar](200) NULL,
[status] [varchar](100) NULL,
[processedon] [datetime] NULL
)
GO

/***********************************************************
**  INSERT SCRIPT FOR LIST OF JOBS
************************************************************/

insert into [dbo].[JOB_LIST] values(1,'channels_dim','/staging','/sales/projectz/landing','channels','',GETDATE());
insert into [dbo].[JOB_LIST] values(2,'countries_dim','/staging','/sales/projectz/landing','countries','',GETDATE());
insert into [dbo].[JOB_LIST] values(3,'customers_dim','/staging','/sales/projectz/landing','customers','',GETDATE());
insert into [dbo].[JOB_LIST] values(4,'product_dim','/staging','/sales/projectz/landing','product','',GETDATE());
insert into [dbo].[JOB_LIST] values(5,'promotions_dim','/staging','/sales/projectz/landing','promotions','',GETDATE());
insert into [dbo].[JOB_LIST] values(6,'tims_dim','/staging','/sales/projectz/landing','times','',GETDATE());
insert into [dbo].[JOB_LIST] values(7,'sales_fact','/staging','/sales/projectz/landing','sales','',GETDATE()); 

/*
/******************************************************
File Activity Output Methods to get data from Activity
*******************************************************
dataRead, dataWritten, filesRead, filesWritten, sourcePeakConnections, sinkPeakConnections, copyDuration, throughput, errors, effectiveIntegrationRuntime, usedDataIntegrationUnits, 
billingReference, usedParallelCopies, executionDetails, dataConsistencyVerification, durationInQueue'.
*/
/*************************************************
DB Server: pysparksql
UserName: pyspark
password: Admin@123
*************************************************/

/*******************************************
** Creating ETL Load Status table in SQL DB...
********************************************/
CREATE TABLE [dbo].[JOB_LIST](
[id] [varchar](100) NULL,
[jobname] [varchar](100) NULL,
[src_folderpath] [varchar](100) NULL,
[tgt_folderpath] [varchar](100) NULL,
[directory] [varchar](200) NULL,
[status] [varchar](100) NULL,
[processedon] [datetime] NULL
)
GO

/***********************************************************
**  INSERT SCRIPT FOR LIST OF JOBS
************************************************************/

insert into [dbo].[JOB_LIST] values(1,'channels_dim','/staging','/sales/projectz/source','channels','',GETDATE());
insert into [dbo].[JOB_LIST] values(2,'countries_dim','/staging','/sales/projectz/source','countries','',GETDATE());
insert into [dbo].[JOB_LIST] values(3,'customers_dim','/staging','/sales/projectz/source','customers','',GETDATE());
insert into [dbo].[JOB_LIST] values(4,'product_dim','/staging','/sales/projectz/source','product','',GETDATE());
insert into [dbo].[JOB_LIST] values(5,'promotions_dim','/staging','/sales/projectz/source','promotions','',GETDATE());
insert into [dbo].[JOB_LIST] values(6,'tims_dim','/staging','/sales/projectz/source','times','',GETDATE());
insert into [dbo].[JOB_LIST] values(7,'sales_fact','/staging','/sales/projectz/source','sales','',GETDATE()); 



/*******************************************
** Creating ETL Load Status table in SQL DB...
********************************************/
CREATE TABLE [dbo].[ETL_LOAD_STATUS](
[DataFactoryName] [varchar](100) NULL,
[PipelineName] [varchar](100) NULL,
[RunId] [varchar](100) NULL,
[activityname] [varchar](200) NULL,
[readrows] [varchar](100) NULL,
[writerows] [varchar](100) NULL,
[activitystatus] [varchar](200) NULL,
[ErrorMessage] [varchar](1000) NULL,
[starttime] [datetime] NULL,
[endtime] [datetime] NULL,
[Createdon] [datetime] NULL
)
GO

/*************************************************************************
** Creating PROCEDURE FOR ETL Load Status updating table in SQL DB...
*************************************************************************/

CREATE PROCEDURE [dbo].[PROC_ETL_LOAD_STATUS]
(@DataFactoryName varchar(100), @PipelineName varchar(100), @runid varchar(100),@activityname varchar(100),@readrows varchar(100),@writerows varchar(100),
@activitystatus varchar(100),@starttime datetime,@endtime datetime,@ErrorMessage varchar(1000),@Createdon datetime)
AS

BEGIN

INSERT INTO [dbo].[ETL_LOAD_STATUS]
(
[DataFactoryName],
[PipelineName],
[RunId],
[activityname],
[readrows],
[writerows],
[activitystatus],
[ErrorMessage],
[starttime],
[endtime],
[Createdon]
)
VALUES
(
@DataFactoryName,
@PipelineName,
@runid,
@activityname,
@readrows,
@writerows,
@activitystatus,
@ErrorMessage,
@starttime,
@endtime,
@Createdon
)

END
GO


CREATE TABLE [dbo].[JOB_LIST_DYNAMIC](
[id] [varchar](100) NOT NULL,
[jobname] [varchar](100) NOT NULL,
[source_database_host] [varchar] (200) NULL,
[source_database_username] [varchar] (200) NULL,
[source_database_password] [varchar] (200) NULL,
[source_database_name] [varchar] (200) NULL,
[source_schema_name] [varchar] (200) NULL,
[source_table_name] [varchar] (200) NULL,
[source_file_path] [varchar] (200) NULL,
[source_file_username] [varchar] (200) NULL,
[source_file_password] [varchar] (200) NULL,
[source_folder_name] [varchar] (200) NULL,
[source_file_name] [varchar] (200) NULL,
[source_file_type] [varchar] (200) NULL,
[landing_zone_file_path] [varchar] (200) NULL,
[landing_zone_folder_name] [varchar] (200) NULL,
[landing_zone_file_name] [varchar] (200) NULL,
[landing_zone_file_type] [varchar] (200) NULL,
[staging_zone_database_name] [varchar] (200) NULL,
[staging_zone_schema_name] [varchar] (200) NULL,
[staging_zone_table_name] [varchar] (200) NULL,
[staging_zone_table_pk_name] [varchar] (200) NULL,
[curation_zone_database_name] [varchar] (200) NULL,
[curation_zone_schema_name] [varchar] (200) NULL,
[curation_zone_table_name] [varchar] (200) NULL,
[curation_zone_table_pk_name] [varchar] (200) NULL,
[dw_zone_database_name] [varchar] (200) NULL,
[dw_zone_schema_name] [varchar] (200) NULL,
[dw_zone_table_name] [varchar] (200) NULL,
[dw_zone_table_pk_column] [varchar] (200) NULL,
[raw_zone_file_path] [varchar] (200) NULL,
[raw_zone_folder_name] [varchar] (200) NULL,
[raw_zone_file_name] [varchar] (200) NULL,
[raw_zone_file_type] [varchar] (200) NULL,
[pyspark_schema] [varchar](200) NULL,
[table_type] [varchar](20) NULL,
[job_type] [varchar](20) NULL,
[job_status] [varchar](20) NULL,
[watermark] [datetime] NOT NULL,
[created_on] [datetime] NOT NULL,
[created_by] [varchar](200) NOT NULL,
[updated_on] [datetime] NULL,
[updated_by] [varchar](200) NULL
);




insert into [dbo].[JOB_LIST_DYNAMIC] values(1,'countries','localhost','sales_read','sales_read','sh','sh','countries','E:/files/SH/','raveendra','password','countries','countries','csv','/mnt/landing/sales/','countries','countries','csv','stg_sales','sales','stg_countries','COUNTRY_ID','curation_sales','sales','curation_countries','COUNTRY_ID','dw_sales','sales','dim_countries','COUNTRY_KEY','/mnt/raw/sales/','countries','countries','csv','countries_schema','dim','incremental',1,GETDATE(),GETDATE(),'ETLJOB',GETDATE(),'ETLJOB');
insert into [dbo].[JOB_LIST_DYNAMIC] values(2,'channels','localhost','sales_read','sales_read','sh','sh','channels','E:/files/SH/','raveendra','password','channels','channels','csv','/mnt/landing/sales/','channels','channels','csv','stg_sales','sales','stg_channels','CHANNEL_ID','curation_sales','sales','curation_channels','CHANNEL_ID','dw_sales','sales','dim_channels','CHANNEL_KEY','/mnt/raw/sales/','channels','channels','csv','channels_schema','dim','incremental',1,GETDATE(),GETDATE(),'ETLJOB',GETDATE(),'ETLJOB');
insert into [dbo].[JOB_LIST_DYNAMIC] values(3,'customers','localhost','sales_read','sales_read','sh','sh','customers','E:/files/SH/','raveendra','password','customers','customers','csv','/mnt/landing/sales/','customers','customers','csv','stg_sales','sales','stg_customers','CUST_ID','curation_sales','sales','curation_customers','CUST_ID','dw_sales','sales','dim_customers','CUST_KEY','/mnt/raw/sales/','customers','customers','csv','customers_schema','dim','incremental',1,GETDATE(),GETDATE(),'ETLJOB',GETDATE(),'ETLJOB');
insert into [dbo].[JOB_LIST_DYNAMIC] values(4,'product','localhost','sales_read','sales_read','sh','sh','product','E:/files/SH/','raveendra','password','product','product','csv','/mnt/landing/sales/','product','product','csv','stg_sales','sales','stg_product','PROD_ID','curation_sales','sales','curation_product','PROD_ID','dw_sales','sales','dim_product','PROD_KEY','/mnt/raw/sales/','product','product','csv','product_schema','dim','incremental',1,GETDATE(),GETDATE(),'ETLJOB',GETDATE(),'ETLJOB');
insert into [dbo].[JOB_LIST_DYNAMIC] values(5,'promotions','localhost','sales_read','sales_read','sh','sh','promotions','E:/files/SH/','raveendra','password','promotions','promotions','csv','/mnt/landing/sales/','promotions','promotions','csv','stg_sales','sales','stg_promotions','PROMO_ID','curation_sales','sales','curation_promotions','PROMO_ID','dw_sales','sales','dim_promotions','PROMO_KEY','/mnt/raw/sales/','promotions','promotions','csv','promotions_schema','dim','incremental',1,GETDATE(),GETDATE(),'ETLJOB',GETDATE(),'ETLJOB');
insert into [dbo].[JOB_LIST_DYNAMIC] values(6,'times','localhost','sales_read','sales_read','sh','sh','times','E:/files/SH/','raveendra','password','times','times','csv','/mnt/landing/sales/','times','times','csv','stg_sales','sales','stg_times','TIME_ID','curation_sales','sales','curation_times','TIME_ID','dw_sales','sales','dim_times','TIME_KEY','/mnt/raw/sales/','times','times','csv','times_schema','dim','incremental',1,GETDATE(),GETDATE(),'ETLJOB',GETDATE(),'ETLJOB');
insert into [dbo].[JOB_LIST_DYNAMIC] values(7,'sales_transaction','localhost','sales_read','sales_read','sh','sh','sales_transaction','E:/files/SH/','raveendra','password','sales_transaction','sales_transaction','csv','/mnt/landing/sales/','sales_transaction','sales_transaction','csv','stg_sales','sales','stg_sales_transaction','PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID','curation_sales','sales','curation_sales_transaction','PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID','dw_sales','sales','fact_sales_transaction','SALES_FACT_KEY','/mnt/raw/sales/','sales_transaction','sales_transaction','csv','sales_schema','dim','incremental',1,GETDATE(),GETDATE(),'ETLJOB',GETDATE(),'ETLJOB');
insert into [dbo].[JOB_LIST_DYNAMIC] values(8,'costs_transaction','localhost','sales_read','sales_read','sh','sh','costs_transaction','E:/files/SH/','raveendra','password','costs_transaction','costs_transaction','csv','/mnt/landing/sales/','costs_transaction','costs_transaction','csv','stg_sales','sales','stg_costs_transaction','PROD_ID,TIME_ID,CHANNEL_ID,PROMO_ID','curation_sales','sales','curation_costs_transaction','PROD_ID,TIME_ID,CHANNEL_ID,PROMO_ID','dw_sales','sales','fact_costs_transaction','COST_FACT_KEY','/mnt/raw/sales/','costs_transaction','costs_transaction','csv','costs_schema','dim','incremental',1,GETDATE(),GETDATE(),'ETLJOB',GETDATE(),'ETLJOB');




/***********************************************************
**  JOB Table script for Delta Lake syntax with data
************************************************************/

CREATE TABLE JOB_LIST_DYNAMIC(
id varchar(100) ,
jobname varchar(100) ,
source_database_host varchar (200) ,
source_database_username varchar (200) ,
source_database_password varchar (200) ,
source_database_name varchar (200) ,
source_schema_name varchar (200) ,
source_table_name varchar (200) ,
source_file_path varchar (200) ,
source_file_username varchar (200) ,
source_file_password varchar (200) ,
source_folder_name varchar (200) ,
source_file_name varchar (200) ,
source_file_type varchar (200) ,
landing_zone_file_path varchar (200) ,
landing_zone_folder_name varchar (200) ,
landing_zone_file_name varchar (200) ,
landing_zone_file_type varchar (200) ,
staging_zone_database_name varchar (200) ,
staging_zone_schema_name varchar (200) ,
staging_zone_table_name varchar (200) ,
staging_zone_table_pk_name varchar (200) ,
curation_zone_database_name varchar (200) ,
curation_zone_schema_name varchar (200) ,
curation_zone_table_name varchar (200) ,
curation_zone_table_pk_name varchar (200) ,
dw_zone_database_name varchar (200) ,
dw_zone_schema_name varchar (200) ,
dw_zone_table_name varchar (200) ,
dw_zone_table_pk_column varchar (200) ,
raw_zone_file_path varchar (200) ,
raw_zone_folder_name varchar (200) ,
raw_zone_file_name varchar (200) ,
raw_zone_file_type varchar (200) ,
pyspark_schema varchar(200) ,
table_type varchar(20) ,
job_type varchar(20) ,
job_status varchar(20) ,
watermark TIMESTAMP,
created_on TIMESTAMP ,
created_by varchar(200) ,
updated_on TIMESTAMP ,
updated_by varchar(200) 
) using delta;



insert into JOB_LIST_DYNAMIC values(1,'countries','localhost','sales_read','sales_read','sh','sh','countries','E:/files/SH/','username','password','countries','countries','csv','/mnt/landing/sales/','countries','countries','csv','stg_sales','sales','stg_countries','COUNTRY_ID','curation_sales','sales','curation_countries','COUNTRY_ID','dw_sales','sales','dim_countries','COUNTRY_KEY','/mnt/raw/sales/','countries','countries','csv','countries_schema','dim','incremental',1,current_timestamp(),current_timestamp(),'ETLJOB',current_timestamp(),'ETLJOB');
insert into JOB_LIST_DYNAMIC values(2,'channels','localhost','sales_read','sales_read','sh','sh','channels','E:/files/SH/','username','password','channels','channels','csv','/mnt/landing/sales/','channels','channels','csv','stg_sales','sales','stg_channels','CHANNEL_ID','curation_sales','sales','curation_channels','CHANNEL_ID','dw_sales','sales','dim_channels','CHANNEL_KEY','/mnt/raw/sales/','channels','channels','csv','channels_schema','dim','incremental',1,current_timestamp(),current_timestamp(),'ETLJOB',current_timestamp(),'ETLJOB');
insert into JOB_LIST_DYNAMIC values(3,'customers','localhost','sales_read','sales_read','sh','sh','customers','E:/files/SH/','username','password','customers','customers','csv','/mnt/landing/sales/','customers','customers','csv','stg_sales','sales','stg_customers','CUST_ID','curation_sales','sales','curation_customers','CUST_ID','dw_sales','sales','dim_customers','CUST_KEY','/mnt/raw/sales/','customers','customers','csv','customers_schema','dim','incremental',1,current_timestamp(),current_timestamp(),'ETLJOB',current_timestamp(),'ETLJOB');
insert into JOB_LIST_DYNAMIC values(4,'product','localhost','sales_read','sales_read','sh','sh','product','E:/files/SH/','username','password','product','product','csv','/mnt/landing/sales/','product','product','csv','stg_sales','sales','stg_product','PROD_ID','curation_sales','sales','curation_product','PROD_ID','dw_sales','sales','dim_product','PROD_KEY','/mnt/raw/sales/','product','product','csv','product_schema','dim','incremental',1,current_timestamp(),current_timestamp(),'ETLJOB',current_timestamp(),'ETLJOB');
insert into JOB_LIST_DYNAMIC values(5,'promotions','localhost','sales_read','sales_read','sh','sh','promotions','E:/files/SH/','username','password','promotions','promotions','csv','/mnt/landing/sales/','promotions','promotions','csv','stg_sales','sales','stg_promotions','PROMO_ID','curation_sales','sales','curation_promotions','PROMO_ID','dw_sales','sales','dim_promotions','PROMO_KEY','/mnt/raw/sales/','promotions','promotions','csv','promotions_schema','dim','incremental',1,current_timestamp(),current_timestamp(),'ETLJOB',current_timestamp(),'ETLJOB');
insert into JOB_LIST_DYNAMIC values(6,'times','localhost','sales_read','sales_read','sh','sh','times','E:/files/SH/','username','password','times','times','csv','/mnt/landing/sales/','times','times','csv','stg_sales','sales','stg_times','TIME_ID','curation_sales','sales','curation_times','TIME_ID','dw_sales','sales','dim_times','TIME_KEY','/mnt/raw/sales/','times','times','csv','times_schema','dim','incremental',1,current_timestamp(),current_timestamp(),'ETLJOB',current_timestamp(),'ETLJOB');
insert into JOB_LIST_DYNAMIC values(7,'sales_transaction','localhost','sales_read','sales_read','sh','sh','sales_transaction','E:/files/SH/','username','password','sales_transaction','sales_transaction','csv','/mnt/landing/sales/','sales_transaction','sales_transaction','csv','stg_sales','sales','stg_sales_transaction','PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID','curation_sales','sales','curation_sales_transaction','PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID','dw_sales','sales','fact_sales_transaction','SALES_FACT_KEY','/mnt/raw/sales/','sales_transaction','sales_transaction','csv','sales_schema','fact','incremental',1,current_timestamp(),current_timestamp(),'ETLJOB',current_timestamp(),'ETLJOB');
insert into JOB_LIST_DYNAMIC values(8,'costs_transaction','localhost','sales_read','sales_read','sh','sh','costs_transaction','E:/files/SH/','username','password','costs_transaction','costs_transaction','csv','/mnt/landing/sales/','costs_transaction','costs_transaction','csv','stg_sales','sales','stg_costs_transaction','PROD_ID,TIME_ID,CHANNEL_ID,PROMO_ID','curation_sales','sales','curation_costs_transaction','PROD_ID,TIME_ID,CHANNEL_ID,PROMO_ID','dw_sales','sales','fact_costs_transaction','COST_FACT_KEY','/mnt/raw/sales/','costs_transaction','costs_transaction','csv','costs_schema','fact','incremental',1,current_timestamp(),current_timestamp(),'ETLJOB',current_timestamp(),'ETLJOB');

