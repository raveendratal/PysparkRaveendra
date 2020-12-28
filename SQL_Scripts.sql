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