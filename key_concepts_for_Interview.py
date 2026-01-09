# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Key Concepts

# COMMAND ----------

# MAGIC %md
# MAGIC 1) **Spark Architecture**  
# MAGIC    Spark is a distributed computing framework that processes large-scale data across clusters. It consists of a driver program, cluster manager, and worker nodes.
# MAGIC
# MAGIC 2) **Master Node / Driver Node / Header Node / Name Node**  
# MAGIC    The master node (also called driver node) is responsible for orchestrating the execution of Spark applications. It maintains information about the cluster, schedules jobs, and coordinates tasks. In Hadoop, the Name Node manages metadata, but in Spark, the driver handles job scheduling and resource allocation.
# MAGIC
# MAGIC 3) **Worker Node / Data Node / Slave Node / Process Node**  
# MAGIC    Worker nodes (also called slave nodes) execute tasks assigned by the master. They store data and run computations. In Hadoop, Data Nodes store actual data blocks, while in Spark, worker nodes process data partitions.
# MAGIC
# MAGIC 4) **Executor**  
# MAGIC    An executor is a JVM process launched on each worker node. It is allocated a specific amount of RAM and CPU (vCPU/slots). Executors run tasks and keep data in memory for fast processing.
# MAGIC
# MAGIC 5) **Context (SparkContext, SQLContext)**  
# MAGIC    Context objects are entry points for Spark functionality. `SparkContext` is the main entry for core Spark features, while `SQLContext` provides access to Spark SQL capabilities. They define the working area and resources allocated for specific operations.
# MAGIC
# MAGIC 6) **SparkSession**  
# MAGIC    `SparkSession` is the unified entry point for Spark applications. It handles authentication (establishing a session with the cluster) and authorization (managing user privileges). It combines `SparkContext` and `SQLContext` into a single object.
# MAGIC
# MAGIC 7) **Spark Job Internal Process**  
# MAGIC    The execution flow in Spark:  
# MAGIC    - **Code**: User writes Spark code.  
# MAGIC    - **Job**: The code is converted into one or more jobs.  
# MAGIC    - **Stages**: Each job is split into stages based on data shuffling.  
# MAGIC    - **Tasks**: Stages are further divided into tasks, which are the smallest units of work executed by executors on partitions.
# MAGIC 8) Partitions (spark devides data into logical partitions to process data on multiple worker nodes)-- logical partitions with default 128MB Size

# COMMAND ----------

# MAGIC %md
# MAGIC #Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1) What is a DataFrame?
# MAGIC
# MAGIC A **DataFrame** in Spark is a distributed collection of data organized into named columns, similar to a table in a relational database or a data frame in Python's pandas library. DataFrames provide a higher-level abstraction for working with structured and semi-structured data, enabling efficient querying, transformation, and analysis at scale.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2) How to Create a DataFrame?
# MAGIC
# MAGIC Spark provides multiple APIs to create DataFrames:
# MAGIC
# MAGIC - **Reading from files**:  
# MAGIC   Use `spark.read` to load data from various formats:
# MAGIC   - CSV: `spark.read.csv("path")`
# MAGIC   - JSON: `spark.read.json("path")`
# MAGIC   - Parquet: `spark.read.parquet("path")`
# MAGIC   - ORC, Avro, Delta: `spark.read.format("format").load("path")`
# MAGIC - **From SQL queries**:  
# MAGIC   `spark.sql("SELECT * FROM table")`
# MAGIC - **From Python objects**:  
# MAGIC   `spark.createDataFrame(data, schema)`
# MAGIC
# MAGIC These methods allow you to ingest data from external sources or create DataFrames from in-memory data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3) Types of Spark Operations
# MAGIC
# MAGIC Spark operations on DataFrames are categorized as:
# MAGIC
# MAGIC - **Transformations**:  
# MAGIC   Operations that produce a new DataFrame from an existing one (e.g., `select`, `filter`). Transformations are *lazy*; they are not executed until an action is called.
# MAGIC - **Actions**:  
# MAGIC   Operations that trigger computation and return results (e.g., `show`, `collect`, `write`). Actions cause Spark to execute the transformations and return data to the driver or write it to storage.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4) Actions in Spark
# MAGIC
# MAGIC Actions are operations that return a value to the driver program or write data externally. Common actions include:
# MAGIC
# MAGIC - **Displaying data**:  
# MAGIC   - `df.show()`: Prints rows in tabular format.
# MAGIC   - `df.display()`: Enhanced display in Databricks notebooks.
# MAGIC - **Collecting data**:  
# MAGIC   - `df.collect()`: Returns all rows as a list to the driver.
# MAGIC - **Writing data**:  
# MAGIC   - `df.write.format("format").save("path")`: Saves DataFrame to storage.
# MAGIC   - `df.write.format("format").saveAsTable("table")`: Saves DataFrame as a managed table.
# MAGIC
# MAGIC All write operations are considered actions because they trigger Spark jobs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5) Types of Transformations
# MAGIC
# MAGIC Transformations modify DataFrames and are classified as:
# MAGIC
# MAGIC ### a) Metadata Transformations
# MAGIC
# MAGIC - **Add column**: `df.withColumn("new_col", expr)`
# MAGIC - **Change data type**: `df.withColumn("col", df.col.cast("type"))`
# MAGIC - **Rename column**: `df.withColumnRenamed("old", "new")`, `df.toDF("col1", "col2")`
# MAGIC - **Drop column(s)**: `df.drop("col")`
# MAGIC - **Select columns**: `df.select("col1", "col2")`
# MAGIC - **Select with expressions**: `df.selectExpr("col1 as new_col", "col2 * 2")`
# MAGIC
# MAGIC ### b) Data Transformations
# MAGIC
# MAGIC - **Remove duplicates**: `df.distinct()`, `df.dropDuplicates()`
# MAGIC - **Remove nulls**: `df.dropna()`, `df.na.drop()`
# MAGIC - **Fill nulls**: `df.fillna(value)`, `df.na.fill(value)`
# MAGIC - **Filter rows**: `df.filter(condition)`, `df.where(condition)`
# MAGIC - **Group and aggregate**:  
# MAGIC   - `df.groupBy("col").agg({"col2": "max"})`
# MAGIC   - Aggregations: `min`, `max`, `avg`, `count`, `sum`, `stdev`
# MAGIC - **Sort data**:  
# MAGIC   - `df.orderBy("col")`, `df.sort("col")`
# MAGIC   - Specify order: `.asc()`, `.desc()`
# MAGIC - **Set operations**:  
# MAGIC   - Merge DataFrames: `df.union(df1)`, `df.unionAll(df1)`
# MAGIC   - Subtract: `df.minus(df1)`
# MAGIC   - Intersect: `df.intersect(df1)`
# MAGIC - **Join operations**:  
# MAGIC   - `df.join(df1, condition, "type")` (types: inner, left, right, full, semi, anti)
# MAGIC   - `df.crossJoin(df2)`
# MAGIC - **Partitioning**:  
# MAGIC   - Increase/decrease: `df.repartition(n)`
# MAGIC   - Decrease only: `df.coalesce(n)`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6) Narrow vs Wide Transformations
# MAGIC
# MAGIC - **Narrow Transformations**:  
# MAGIC   Each input partition contributes to only one output partition. No data shuffling occurs. Examples:
# MAGIC   - `df.withColumn()`
# MAGIC   - `df.withColumnRenamed()`
# MAGIC   - `df.drop()`
# MAGIC   - `df.select()`
# MAGIC   - `df.selectExpr()`
# MAGIC   - `df.filter()`, `df.where()`
# MAGIC   - `df.union(df1)`
# MAGIC   - `df.coalesce(n)`
# MAGIC
# MAGIC - **Wide Transformations**:  
# MAGIC   Data from multiple input partitions may be needed for one output partition, causing a shuffle across the cluster. Examples:
# MAGIC   - `df.distinct()`, `df.dropDuplicates()`
# MAGIC   - `df.groupBy().agg()`
# MAGIC   - `df.orderBy()`, `df.sort()`, `df.sortWithinPartitions()`
# MAGIC   - `df.join(df1)`
# MAGIC   - `df.crossJoin(df1)`
# MAGIC   - `df.minus(df1)`, `df.intersect(df1)`
# MAGIC   - `df.repartition(n)`
# MAGIC
# MAGIC **Note:**  
# MAGIC - *Narrow transformations* are more efficient as they avoid shuffling.
# MAGIC - *Wide transformations* involve shuffling, which can impact performance due to network and disk I/O.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Delta Lake: Detailed Documentation
# MAGIC
# MAGIC ## 1) What is Delta Lake?
# MAGIC
# MAGIC Delta Lake is an open-source storage layer that brings reliability, scalability, and performance to data lakes. It enables ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. Delta Lake is built on top of Apache Spark and stores data in Parquet format, while maintaining a transaction log for versioning and consistency.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2) Delta Lake Architecture
# MAGIC
# MAGIC Delta Lake stores data in Parquet files within a directory. Alongside the data, it maintains a special folder called `_delta_log` that contains metadata, transaction logs, indexes, and statistics in JSON and CRC files. This architecture enables features like ACID transactions, schema enforcement, and time travel.
# MAGIC
# MAGIC - **Data Storage**: Data is stored as Parquet files for efficient columnar storage and compression.
# MAGIC - **Transaction Log**: The `_delta_log` directory contains JSON files that record every change (add, remove, update) to the table, ensuring atomicity and consistency.
# MAGIC - **Metadata & Stats**: The log files also store schema information, statistics, and indexes to optimize query performance and enable features like time travel.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3) Delta Lake Features
# MAGIC
# MAGIC ### 1. ACID Transactions
# MAGIC
# MAGIC Delta Lake supports atomic, consistent, isolated, and durable (ACID) transactions. This ensures that all writes are either fully completed or not applied at all, preventing partial or corrupt data.
# MAGIC
# MAGIC ### 2. Schema Enforcement
# MAGIC
# MAGIC Delta Lake enforces the schema of the table during write operations. If incoming data does not match the table schema, the write will fail unless schema evolution is enabled.
# MAGIC
# MAGIC - **mergeSchema=False**: Strict schema enforcement; mismatched columns cause errors.
# MAGIC
# MAGIC ### 3. Schema Evolution
# MAGIC
# MAGIC Delta Lake allows the schema of a table to evolve over time. New columns can be added, and data types can be changed as needed.
# MAGIC
# MAGIC - **mergeSchema=True**: Automatically merges new columns into the table schema.
# MAGIC - **overwriteSchema=True**: Overwrites the existing schema with the new schema.
# MAGIC
# MAGIC ### 4. Time Travel
# MAGIC
# MAGIC Delta Lake maintains a history of all changes, allowing users to query previous versions of the data. This is useful for auditing, debugging, and reproducing experiments.
# MAGIC
# MAGIC - **Syntax**: `SELECT * FROM table VERSION AS OF <version_number>`
# MAGIC
# MAGIC ### 5. Audit Logs
# MAGIC
# MAGIC All changes to Delta tables are recorded in the transaction log, providing a complete audit trail of data modifications.
# MAGIC
# MAGIC ### 6. Merge (Upsert Operations)
# MAGIC
# MAGIC Delta Lake supports the `MERGE` operation, which allows incremental data loading by upserting (inserting or updating) records based on specified conditions.
# MAGIC
# MAGIC - **Use Case**: Efficiently handle slowly changing dimensions and CDC (Change Data Capture).
# MAGIC
# MAGIC ### 7. Partitioning
# MAGIC
# MAGIC Delta tables can be partitioned by one or more columns to improve query performance and manageability.
# MAGIC
# MAGIC - **Syntax**: `PARTITIONED BY (column_name)`
# MAGIC
# MAGIC ### 8. Liquid Clustering (Cluster By)
# MAGIC
# MAGIC Delta Lake supports clustering data by columns to optimize data layout and query performance. This is more flexible than traditional partitioning.
# MAGIC
# MAGIC - **Syntax**: `CLUSTER BY (column_name)`
# MAGIC
# MAGIC ### 9. Vacuum (Purging Old Snapshots)
# MAGIC
# MAGIC The `VACUUM` command removes obsolete files and data that are no longer referenced by the transaction log, freeing up storage and maintaining table health.
# MAGIC
# MAGIC - **Syntax**: `VACUUM table_name RETAIN <hours>`
# MAGIC
# MAGIC ### 10. Optimize (Compacting Small Files)
# MAGIC
# MAGIC The `OPTIMIZE` command compacts small files into larger ones, improving read performance and reducing metadata overhead.
# MAGIC
# MAGIC - **Syntax**: `OPTIMIZE table_name`
# MAGIC
# MAGIC ### 11. Optimize with ZORDER (Data Skipping)
# MAGIC
# MAGIC `OPTIMIZE ... ZORDER BY` physically sorts data files by specified columns, enabling efficient data skipping and faster queries.
# MAGIC
# MAGIC - **Syntax**: `OPTIMIZE table_name ZORDER BY (column_name)`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Delta Lake combines the reliability of data warehouses with the scalability of data lakes, making it a powerful solution for modern data engineering and analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC #ADF

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Azure Data Factory (ADF) Overview
# MAGIC
# MAGIC Azure Data Factory (ADF) is a cloud-based data integration service that enables you to create, schedule, and orchestrate data pipelines for moving and transforming data from various sources to destinations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1) Why Use ADF?
# MAGIC
# MAGIC - **Ingestion and Orchestration:**  
# MAGIC   ADF is designed for ingesting data from diverse sources and orchestrating complex data workflows. It automates data movement and transformation across cloud and on-premises environments.
# MAGIC
# MAGIC - **External System Data Migration:**  
# MAGIC   For migrating data from external systems (e.g., on-premises databases, SaaS applications) to cloud storage or data lakes, ADF acts as a robust ingestion tool.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2) Integration Runtime (IR)
# MAGIC
# MAGIC - **Self-Hosted Integration Runtime:**  
# MAGIC   A self-hosted IR is an ADF component installed on-premises or in a private network. It enables secure data movement and transformation between on-premises sources and cloud destinations, overcoming network boundaries and firewalls.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3) Linked Service
# MAGIC
# MAGIC - **Definition:**  
# MAGIC   A linked service defines the connection information required for ADF to connect to external resources.  
# MAGIC - **Examples:**  
# MAGIC   - Databases (SQL Server, Oracle, MySQL, etc.)
# MAGIC   - File systems (Azure Data Lake, Blob Storage)
# MAGIC   - Cloud services (Databricks, Key Vault)
# MAGIC   - External APIs and other cloud systems
# MAGIC
# MAGIC Linked services act as connection managers, storing credentials and endpoints securely.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4) Pipeline
# MAGIC
# MAGIC - **Definition:**  
# MAGIC   A pipeline is a logical grouping of activities that perform data movement and transformation.  
# MAGIC - **Purpose:**  
# MAGIC   Pipelines enable workflow orchestration, allowing you to chain activities, implement control flow (loops, conditions), and manage dependencies.
# MAGIC
# MAGIC - **Typical Activities in a Pipeline:**  
# MAGIC   - Data copying
# MAGIC   - Data transformation (using Databricks, SQL, etc.)
# MAGIC   - Data validation and metadata extraction
# MAGIC   - Orchestration logic (looping, conditional branching)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5) Activities in ADF
# MAGIC
# MAGIC Activities are the building blocks of pipelines. Each activity performs a specific operation:
# MAGIC
# MAGIC - **Copy Activity:**  
# MAGIC   Moves data from a source to a destination (data migration).
# MAGIC
# MAGIC - **Get Metadata Activity:**  
# MAGIC   Retrieves metadata (e.g., schema, file size) from data sources.
# MAGIC
# MAGIC - **Lookup Activity:**  
# MAGIC   Executes queries or reads data for validation or extraction.
# MAGIC
# MAGIC - **Filter Activity:**  
# MAGIC   Filters data or pipeline items based on conditions.
# MAGIC
# MAGIC - **ForEach Activity:**  
# MAGIC   Iterates over a collection of items, executing child activities for each item.
# MAGIC
# MAGIC - **If Condition Activity:**  
# MAGIC   Implements branching logic based on evaluated conditions.
# MAGIC
# MAGIC - **Switch Activity:**  
# MAGIC   Routes execution to different branches based on matching values.
# MAGIC
# MAGIC - **Execute Pipeline Activity:**  
# MAGIC   Invokes another pipeline from within a pipeline, enabling modular workflows.
# MAGIC
# MAGIC - **Wait Activity:**  
# MAGIC   Pauses pipeline execution for a specified duration.
# MAGIC
# MAGIC - **Stored Procedure Activity:**  
# MAGIC   Executes stored procedures in supported databases.
# MAGIC
# MAGIC - **Web Activity:**  
# MAGIC   Integrates with REST APIs or web services for data retrieval or triggering external processes.
# MAGIC
# MAGIC - **Databricks Notebook Activity:**  
# MAGIC   Executes Databricks notebooks for advanced data processing and analytics.
# MAGIC
# MAGIC - **Databricks Job Activity:**  
# MAGIC   Triggers Databricks jobs or pipelines for scalable data engineering tasks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC ADF provides a scalable, flexible, and secure platform for building end-to-end data integration solutions. Its modular architecture (linked services, pipelines, activities, integration runtimes) supports a wide range of data movement, transformation, and orchestration scenarios across hybrid environments.

# COMMAND ----------

# MAGIC %md
# MAGIC #python

# COMMAND ----------

# MAGIC %md
# MAGIC 1) **Variables**  
# MAGIC    Variables are used to store data in Python. You assign a value to a variable using the `=` operator.  
# MAGIC    Example:  
# MAGIC    python
# MAGIC    x = 10
# MAGIC    name = "Alice"
# MAGIC    
# MAGIC    Variables can hold different data types such as integers, floats, strings, lists, etc.
# MAGIC
# MAGIC 2) **Print Function / Format Function**  
# MAGIC    The `print()` function outputs data to the console.  
# MAGIC    Example:  
# MAGIC    python
# MAGIC    print("Hello, World!")
# MAGIC    
# MAGIC    For formatted output, use f-strings or the `format()` method:  
# MAGIC    python
# MAGIC    print(f"Name: {name}, Age: {x}")
# MAGIC    print("Name: {}, Age: {}".format(name, x))
# MAGIC    
# MAGIC
# MAGIC 3) **Loops (for loop, while loop)**  
# MAGIC    Loops are used to repeat actions.  
# MAGIC    - **For loop**: Iterates over a sequence (list, tuple, string, etc.)  
# MAGIC      python
# MAGIC      for i in range(5):
# MAGIC          print(i)
# MAGIC      
# MAGIC    - **While loop**: Repeats as long as a condition is true  
# MAGIC      python
# MAGIC      count = 0
# MAGIC      while count < 5:
# MAGIC          print(count)
# MAGIC          count += 1
# MAGIC      
# MAGIC
# MAGIC 4) **Conditions (if, else, elif)**  
# MAGIC    Conditional statements control the flow of execution based on conditions.  
# MAGIC    Example:  
# MAGIC    python
# MAGIC    if x > 5:
# MAGIC        print("x is greater than 5")
# MAGIC    elif x == 5:
# MAGIC        print("x is equal to 5")
# MAGIC    else:
# MAGIC        print("x is less than 5")
# MAGIC    
# MAGIC
# MAGIC 5) **Collections (list, tuple, set, dict)**  
# MAGIC    Python provides several built-in collection types:  
# MAGIC    - **List**: Ordered, mutable sequence  
# MAGIC      python
# MAGIC      my_list = [1, 2, 3]
# MAGIC      
# MAGIC    - **Tuple**: Ordered, immutable sequence  
# MAGIC      python
# MAGIC      my_tuple = (1, 2, 3)
# MAGIC      
# MAGIC    - **Set**: Unordered, unique elements  
# MAGIC      python
# MAGIC      my_set = {1, 2, 3}
# MAGIC      
# MAGIC    - **Dictionary**: Key-value pairs  
# MAGIC      python
# MAGIC      my_dict = {"name": "Alice", "age": 25}
# MAGIC      
# MAGIC
# MAGIC 6) **Functions**  
# MAGIC    Functions are reusable blocks of code that perform a specific task.  
# MAGIC    Define a function using `def`:  
# MAGIC    python
# MAGIC    def greet():
# MAGIC        print("Hello!")
# MAGIC    greet()
# MAGIC    
# MAGIC
# MAGIC 7) **Functions with Parameters**  
# MAGIC    Functions can accept parameters to work with different data.  
# MAGIC    Example:  
# MAGIC    python
# MAGIC    def add(a, b):
# MAGIC        return a + b
# MAGIC    result = add(5, 3)
# MAGIC    print(result)
# MAGIC    
# MAGIC
# MAGIC 8) **Exception Handling**  
# MAGIC    Exception handling allows you to manage errors gracefully using `try`, `except`, `finally`.  
# MAGIC    Example:  
# MAGIC    python
# MAGIC    try:
# MAGIC        value = int(input("Enter a number: "))
# MAGIC    except ValueError:
# MAGIC        print("Invalid input! Please enter a number.")
# MAGIC    finally:
# MAGIC        print("Execution completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC #SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) SELECT Statement
# MAGIC
# MAGIC The `SELECT` statement is used to query data from one or more tables or views. It supports various clauses and operations:
# MAGIC - **Joins**: Combine rows from two or more tables based on related columns (e.g., `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`).
# MAGIC - **GROUP BY**: Aggregate data across rows sharing the same values in specified columns (e.g., `SUM`, `COUNT`, `AVG`).
# MAGIC - **ORDER BY**: Sort the result set by one or more columns, in ascending (`ASC`) or descending (`DESC`) order.
# MAGIC - **Set Operators**: Combine results from multiple queries (e.g., `UNION`, `INTERSECT`, `EXCEPT`).
# MAGIC - **CASE**: Conditional logic within queries to return values based on conditions.
# MAGIC - **LIMIT**: Restrict the number of rows returned by the query.
# MAGIC
# MAGIC **Example:**
# MAGIC sql
# MAGIC SELECT department, COUNT(*) AS num_employees
# MAGIC FROM employees
# MAGIC WHERE salary > 50000
# MAGIC GROUP BY department
# MAGIC ORDER BY num_employees DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2) DML Operations: INSERT, UPDATE, DELETE, MERGE
# MAGIC
# MAGIC - **INSERT**: Add new rows to a table.
# MAGIC   sql
# MAGIC   INSERT INTO employees (id, name, department) VALUES (1, 'Alice', 'HR');
# MAGIC   
# MAGIC - **UPDATE**: Modify existing rows in a table.
# MAGIC   sql
# MAGIC   UPDATE employees SET salary = salary * 1.1 WHERE department = 'Sales';
# MAGIC   
# MAGIC - **DELETE**: Remove rows from a table.
# MAGIC   sql
# MAGIC   DELETE FROM employees WHERE id = 1;
# MAGIC   
# MAGIC - **MERGE**: Perform upsert operations (insert, update, or delete) based on matching conditions.
# MAGIC   sql
# MAGIC   MERGE INTO target_table t
# MAGIC   USING source_table s
# MAGIC   ON t.id = s.id
# MAGIC   WHEN MATCHED THEN UPDATE SET t.value = s.value
# MAGIC   WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value);
# MAGIC   
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3) DDL Operations: CREATE, ALTER, DROP, TRUNCATE
# MAGIC
# MAGIC - **CREATE**: Define new tables, views, or other database objects.
# MAGIC   sql
# MAGIC   CREATE TABLE employees (id INT, name STRING, department STRING);
# MAGIC   
# MAGIC - **ALTER**: Modify the structure of existing objects (e.g., add/drop columns).
# MAGIC   sql
# MAGIC   ALTER TABLE employees ADD COLUMN hire_date DATE;
# MAGIC   
# MAGIC - **DROP**: Remove database objects permanently.
# MAGIC   sql
# MAGIC   DROP TABLE employees;
# MAGIC   
# MAGIC - **TRUNCATE**: Remove all rows from a table without deleting the table itself.
# MAGIC   sql
# MAGIC   TRUNCATE TABLE employees;
# MAGIC   
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4) Metadata and Information Commands
# MAGIC
# MAGIC - **DESCRIBE**: Show the schema of a table or view.
# MAGIC   sql
# MAGIC   DESCRIBE employees;
# MAGIC   
# MAGIC - **DESCRIBE HISTORY**: View the audit log/history of a Delta table (track changes over time).
# MAGIC   sql
# MAGIC   DESCRIBE HISTORY employees;
# MAGIC   
# MAGIC - **DESCRIBE DETAIL**: Show detailed properties and statistics of a table.
# MAGIC   sql
# MAGIC   DESCRIBE DETAIL employees;
# MAGIC   
# MAGIC - **DESCRIBE EXTENDED**: Display extended metadata, including table properties.
# MAGIC   sql
# MAGIC   DESCRIBE EXTENDED employees;
# MAGIC   
# MAGIC - **SHOW CREATE TABLE**: Output the DDL statement used to create a table.
# MAGIC   sql
# MAGIC   SHOW CREATE TABLE employees;
# MAGIC   
# MAGIC - **SHOW TABLES**: List all tables in the current or specified database.
# MAGIC   sql
# MAGIC   SHOW TABLES;
# MAGIC   
# MAGIC - **SHOW DATABASES**: List all databases in the system.
# MAGIC   sql
# MAGIC   SHOW DATABASES;
# MAGIC   
# MAGIC - **SHOW VIEWS**: List all views in the current or specified database.
# MAGIC   sql
# MAGIC   SHOW VIEWS;
# MAGIC   
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5) EXPLAIN Statement
# MAGIC
# MAGIC The `EXPLAIN` command displays the execution plan for a SQL query, showing how Spark will execute the query, including stages, partitions, and operations. This helps in understanding and optimizing query performance.
# MAGIC
# MAGIC **Example:**
# MAGIC sql
# MAGIC EXPLAIN SELECT * FROM employees WHERE department = 'IT';

# COMMAND ----------

# MAGIC %md
# MAGIC #databricks

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Databricks Key Concepts and Integration: Detailed Documentation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1) How to Call One Notebook from Another
# MAGIC
# MAGIC - **%run Command**:  
# MAGIC   Use `%run ./notebook_path` at the top of a notebook to include and execute another notebook's code inline. All variables and functions become available in the current notebook's scope.
# MAGIC   - *Example*:  
# MAGIC     
# MAGIC     %run ./SharedUtilities
# MAGIC     
# MAGIC   - *Use Case*: Sharing reusable code, functions, or setup logic.
# MAGIC
# MAGIC - **dbutils.notebook.run()**:  
# MAGIC   Programmatically runs another notebook as a separate job, optionally passing parameters and capturing its output.
# MAGIC   - *Example*:  
# MAGIC     python
# MAGIC     result = dbutils.notebook.run("notebook_path", timeout_seconds=60, arguments={"param1": "value"})
# MAGIC     
# MAGIC   - *Use Case*: Modular workflows, chaining notebooks, parameterization.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2) Difference Between %run and dbutils.notebook.run
# MAGIC
# MAGIC - **%run**:  
# MAGIC   - Executes the target notebook inline; variables/functions are imported into the current notebook.
# MAGIC   - No parameter passing or output retrieval.
# MAGIC   - Synchronous and shares the same Spark context.
# MAGIC
# MAGIC - **dbutils.notebook.run**:  
# MAGIC   - Runs the target notebook as a separate job/subprocess.
# MAGIC   - Supports parameter passing and output retrieval.
# MAGIC   - Isolated execution context; does not share variables/functions.
# MAGIC   - Returns output via `dbutils.notebook.exit()`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3) How to Pass Parameters to a Notebook
# MAGIC
# MAGIC - **Widgets**:  
# MAGIC   Use Databricks widgets to define parameters that can be set when running a notebook.
# MAGIC   - *Create a text widget*:  
# MAGIC     python
# MAGIC     dbutils.widgets.text("param_name", "default_value", "Description")
# MAGIC     
# MAGIC   - *Pass parameters via dbutils.notebook.run*:  
# MAGIC     python
# MAGIC     dbutils.notebook.run("notebook_path", 60, {"param_name": "value"})
# MAGIC     
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4) How to Retrieve Parameters in a Notebook
# MAGIC
# MAGIC - **Get Widget Value**:  
# MAGIC   Use `dbutils.widgets.get("param_name")` to access the value of a widget/parameter.
# MAGIC   - *Example*:  
# MAGIC     python
# MAGIC     param_value = dbutils.widgets.get("param_name")
# MAGIC     
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5) Cluster Types in Databricks
# MAGIC
# MAGIC - **All-Purpose Compute**:  
# MAGIC   Interactive clusters for notebooks, ad-hoc analysis, and collaborative development.
# MAGIC
# MAGIC - **Job Compute**:  
# MAGIC   Dedicated clusters for running scheduled jobs and production workloads.
# MAGIC
# MAGIC - **Serverless Compute**:  
# MAGIC   Managed, auto-scaling clusters for optimized resource usage and cost efficiency.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6) Job Types in Databricks
# MAGIC
# MAGIC - **Spark Job**:  
# MAGIC   Executes Spark code (Scala, Python, SQL) for distributed data processing.
# MAGIC
# MAGIC - **Notebook Job**:  
# MAGIC   Runs a Databricks notebook as a scheduled or triggered job.
# MAGIC
# MAGIC - **Python Job**:  
# MAGIC   Executes standalone Python scripts.
# MAGIC
# MAGIC - **SQL Job**:  
# MAGIC   Runs SQL queries or scripts.
# MAGIC
# MAGIC - **Spark Submit Job**:  
# MAGIC   Submits Spark applications using the `spark-submit` interface.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7) How to Retrieve Notebook Output
# MAGIC
# MAGIC - **dbutils.notebook.exit(msg)**:  
# MAGIC   Use this function to return a value or message from a notebook when called via `dbutils.notebook.run`.
# MAGIC   - *Example*:  
# MAGIC     python
# MAGIC     dbutils.notebook.exit("Success")
# MAGIC     
# MAGIC   - The returned value is captured in the calling notebook.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 8) Integrating Databricks Notebooks in Azure Data Factory (ADF)
# MAGIC
# MAGIC - **ADF Linked Service**:  
# MAGIC   Connects ADF to Databricks workspace using token-based authentication.
# MAGIC
# MAGIC - **Notebook Activity**:  
# MAGIC   Executes Databricks notebooks within ADF pipelines for data transformation or analytics.
# MAGIC
# MAGIC - *Steps*:  
# MAGIC   1. Create a Databricks linked service in ADF.
# MAGIC   2. Add a Notebook activity to your pipeline.
# MAGIC   3. Configure parameters and authentication (token).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 9) Storing Credentials in Azure
# MAGIC
# MAGIC - **Azure Key Vault**:  
# MAGIC   Securely stores secrets, tokens, passwords, and keys.  
# MAGIC   - *Use Case*: Centralized credential management for cloud resources.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 10) Retrieving Credentials from Azure
# MAGIC
# MAGIC - **ADF Linked Service to Key Vault**:  
# MAGIC   ADF can access secrets from Key Vault via linked service integration.
# MAGIC   - *Example*:  
# MAGIC     Configure your pipeline to reference secrets for authentication.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 11) Retrieving Credentials from Databricks
# MAGIC
# MAGIC - **Databricks Secrets**:  
# MAGIC   Store and retrieve secrets using Databricks Secret Scopes.
# MAGIC   - *Retrieve a secret*:  
# MAGIC     python
# MAGIC     token = dbutils.secrets.get(scope="scope_name", key="token_key")
# MAGIC     
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 12) Integrating Data Lake with Databricks
# MAGIC
# MAGIC - **Unity Catalog External Location**:  
# MAGIC   Register external storage (e.g., Azure Data Lake) in Unity Catalog for secure, managed access.
# MAGIC   - *Use Case*: Read/write data from/to data lakes using Databricks tables.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 13) What is a Volume?
# MAGIC
# MAGIC - **Volume**:  
# MAGIC   A storage object in Databricks for storing files and data, backed by the workspace's default storage account.
# MAGIC   - *Features*:  
# MAGIC     - Access/security controls via Unity Catalog.
# MAGIC     - Part of a schema; supports managed access.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 14) What is Unity Catalog?
# MAGIC
# MAGIC - **Unity Catalog**:  
# MAGIC   Centralized metadata management for Databricks, supporting region-level sharing across multiple workspaces.
# MAGIC   - *Hierarchy*:  
# MAGIC     - **Catalogs** → **Schemas** → **Tables/Views/Volumes**
# MAGIC   - *Benefits*:  
# MAGIC     - Fine-grained access control, data governance, and auditability.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 15) How to Retrieve Data from a Volume
# MAGIC
# MAGIC - **Path-Based Access**:  
# MAGIC   Access files in a volume using its path.
# MAGIC   - *Example*:  
# MAGIC     python
# MAGIC     df = spark.read.csv("/Volumes/catalog/schema/volume/file.csv")
# MAGIC     
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 16) Types of Views in Databricks
# MAGIC
# MAGIC - **Temporary View**:  
# MAGIC   Session-scoped; not persisted.
# MAGIC
# MAGIC - **Global Temporary View**:  
# MAGIC   Available across all sessions; stored in a reserved database (`global_temp`).
# MAGIC
# MAGIC - **Permanent View**:  
# MAGIC   Persisted in the metastore; available until explicitly dropped.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 17) Types of Tables in Databricks
# MAGIC
# MAGIC - **Managed Table**:  
# MAGIC   Databricks manages both data and metadata; data stored in workspace storage.
# MAGIC
# MAGIC - **External Table**:  
# MAGIC   Metadata managed by Databricks; data stored externally (e.g., in a data lake).
# MAGIC
# MAGIC ---

# COMMAND ----------

# Metadata Transformations
metadata_transformations = [
    "df.withColumn()",            # Add or modify column
    "df.withColumnRenamed()",     # Rename column
    "df.toDF()",                  # Rename all columns
    "df.drop()",                  # Drop column(s)
    "df.select()",                # Select columns
    "df.selectExpr()"             # Select columns with expressions
]

# Data Transformations
data_transformations = [
    "df.distinct()",              # Remove duplicates
    "df.dropDuplicates()",        # Remove duplicates
    "df.dropna()",                # Remove null rows
    "df.na.drop()",               # Remove null rows
    "df.fillna()",                # Fill nulls
    "df.na.fill()",               # Fill nulls
    "df.filter()",                # Filter rows
    "df.where()",                 # Filter rows
    "df.groupBy()",               # Group data
    "df.groupBy().agg()",         # Aggregate data
    "df.orderBy()",               # Sort data
    "df.sort()",                  # Sort data
    "df.union(df1)",              # Merge dataframes (set operator)
    "df.unionAll(df1)",           # Merge dataframes (set operator)
    "df.minus(df1)",              # Set difference
    "df.intersect(df1)",          # Set intersection
    "df.join(df1, condition, type)", # Join dataframes
    "df.crossJoin(df2)",          # Cross join
    "df.repartition(n)",          # Change partitions
    "df.coalesce(n)"              # Decrease partitions
]

# Narrow Transformations (no shuffle)
narrow_transformations = [
    "df.withColumn()",
    "df.withColumnRenamed()",
    "df.drop()",
    "df.select()",
    "df.selectExpr()",
    "df.where()",
    "df.filter()",
    "df.union(df1)",
    "df.unionAll(df1)",
    "df.coalesce(n)"
]

# Wide Transformations (with shuffle)
wide_transformations = [
    "df.distinct()",
    "df.dropDuplicates()",
    "df.groupBy().agg()",
    "df.orderBy()",
    "df.sort()",
    "df.sortWithinPartitions()",
    "df.join(df1, condition, type)",
    "df.crossJoin(df1)",
    "df.minus(df1)",
    "df.intersect(df1)",
    "df.repartition(n)"
]

display([
    {"Concept": "Metadata", "Transformations": metadata_transformations},
    {"Concept": "Data", "Transformations": data_transformations},
    {"Concept": "Narrow", "Transformations": narrow_transformations},
    {"Concept": "Wide", "Transformations": wide_transformations}
])