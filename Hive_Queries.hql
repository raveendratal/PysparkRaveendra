/********************************************
Creating Internal or Managed ORC Table in hive.
*********************************************/

CREATE TABLE `emp_orc_new`(
  `empno` string, 
  `ename` string, 
  `job` string, 
  `mgr` string, 
  `hiredate` string, 
  `sal` string, 
  `comm` string, 
  `deptno` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS ORC
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transactional'='true', 
  'transactional_properties'='default', 
  'transient_lastDdlTime'='1611888332')

 /************************************
 Hive Partitioned TABLE
 *************************************/
  
 CREATE TABLE `emp_part`(
  `empno` string, 
  `ename` string, 
  `job` string, 
  `mgr` string, 
  `hiredate` string, 
  `sal` string, 
  `comm` string 
  )
PARTITIONED BY (`deptno` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS ORC
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transactional'='true', 
  'transactional_properties'='default', 
  'transient_lastDdlTime'='1611888332')
  
  
 / ********************************************
  Partitioned with Bucketing
  **********************************************/
  CREATE TABLE `emp_partbucket`(
  `empno` string, 
  `ename` string, 
  `job` string, 
  `mgr` string, 
  `hiredate` string, 
  `sal` string, 
  `comm` string 
  )
PARTITIONED BY (`deptno` string)
CLUSTERED BY (EMPNO) INTO 5 BUCKETS
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS ORC
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transactional'='true', 
  'transactional_properties'='default', 
  'transient_lastDdlTime'='1611888332')
  
  
  
 /*************************************************
  Hive External TABLE
  *************************************************/
  
CREATE EXTERNAL TABLE `emp_external`(
  `empno` string, 
  `ename` string, 
  `job` string, 
  `mgr` string, 
  `hiredate` string, 
  `sal` string, 
  `comm` string, 
  `deptno` string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION
  'wasb://hdinsightpyspark-2021-01-29t01-35-56-782z@hdinsightpysphdistorage.blob.core.windows.net/hive/warehouse/emp/'
TBLPROPERTIES("skip.header.line.count"="1")
  
  
/************************************************
  compaction
***********************************************/
  
  ALTER TABLE EMP COMPACT 'MAJOR'
  ALTER TABLE emp_partbucket  PARTITION(DEPTNO=80) compact 'MAJOR'

  
  
