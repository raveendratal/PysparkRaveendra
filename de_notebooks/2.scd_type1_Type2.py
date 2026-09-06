from pyspark import pipelines as dp
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# 1) Temporary view over demo_ldp.customers_snapshot
# -----------------------------------------------------------------------------
@dp.temporary_view()
def customers_snapshot_view():
    return spark.read.table("demo_ldp.customers_snapshot")


# -----------------------------------------------------------------------------
# 2) Materialized view: staging from snapshot view
# -----------------------------------------------------------------------------
@dp.materialized_view()
def customers_staging_mv():
    df = spark.table("customers_snapshot_view")
    return df.select(
        F.col("number").cast("int").alias("customer_id"),
        F.col("state").alias("state"),
        F.col("lat").cast("double").alias("lat"),
        F.col("lon").cast("double").alias("lon"),
        F.col("unit").alias("unit"),
    )


# -----------------------------------------------------------------------------
# 3) Table: persistent table from snapshot staging
# -----------------------------------------------------------------------------
@dp.table()
def customers_table():
    stg = spark.table("customers_staging_mv")
    return stg.groupBy("state").agg(F.count("*").alias("num_customers"))


# -----------------------------------------------------------------------------
# 4) Streaming table + append flows use ONLY customer_cdc_events schema
#    Schema: (customer_id INT, name STRING, city STRING,
#             operation STRING, sequenceNum LONG)
# -----------------------------------------------------------------------------
dp.create_streaming_table("customers_stream_target")


@dp.append_flow(
    name="append_to_customers_stream_target",
    target="customers_stream_target",
)
def append_to_customers_stream_target():
    df = spark.readStream.table("demo_ldp.customer_cdc_events")
    return df.select(
        "customer_id",
        "name",
        "city",
        "operation",
        "sequenceNum",
    )


# -----------------------------------------------------------------------------
# 5) Generic Delta sink for streaming CDC events
# -----------------------------------------------------------------------------
dp.create_sink(
    "customers_delta_sink",
    "delta",
    {"path": "/Volumes/workspace/demo_ldp/files/customers_delta_sink"},
)


@dp.append_flow(name="append_customer_events", target="customers_delta_sink")
def append_customer_events_flow():
    df = spark.readStream.table("demo_ldp.customer_cdc_events")
    return (
        df.where(F.col("operation").isin("INSERT", "UPDATE"))
        .select(
            "customer_id",
            "name",
            "city",
            "operation",
            "sequenceNum",
            F.current_timestamp().alias("ingested_at"),
        )
    )


# -----------------------------------------------------------------------------
# 6) Streaming tables and AUTO CDC flows
# -----------------------------------------------------------------------------
dp.create_streaming_table("customers_scd1_target")
dp.create_streaming_table("customers_scd2_target")


dp.create_auto_cdc_flow(
    target="customers_scd1_target",
    source="demo_ldp.customer_cdc_events",
    keys=["customer_id"],
    sequence_by=F.col("sequenceNum"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequenceNum"],
    stored_as_scd_type=1,
)


dp.create_auto_cdc_flow(
    target="customers_scd2_target",
    source="demo_ldp.customer_cdc_events",
    keys=["customer_id"],
    sequence_by=F.col("sequenceNum"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequenceNum"],
    stored_as_scd_type=2,
)


# -----------------------------------------------------------------------------
# 7) QC / reporting based on snapshot schema (batch)
# -----------------------------------------------------------------------------
@dp.materialized_view()
def customers_qc_mv():
    df = spark.read.table("demo_ldp.customers_snapshot")
    return df.select(
        F.col("number").cast("int").alias("customer_id"),
        F.col("state").alias("state"),
        F.col("lat").cast("double").alias("lat"),
        F.col("lon").cast("double").alias("lon"),
    )


@dp.materialized_view()
def customers_report_mv():
    qc = spark.table("customers_qc_mv")
    return qc.groupBy("state").agg(
        F.count("*").alias("count_by_state")
    )

 