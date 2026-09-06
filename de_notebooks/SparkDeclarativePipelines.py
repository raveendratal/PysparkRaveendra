# =============================================================================
# Lakeflow Spark Declarative Pipelines (SDP) – Python (Latest 2025 Syntax)
# Pipeline Name: lakeflow_airline_pipeline_dp
# Description: Medallion Architecture for ASA Airlines Dataset (Ingestion → Bronze → Silver → Gold)
# Author: Raveendra Reddy (@TRRaveendra)
# Created: 2025-12-07 
# Version: 1.1 | Syntax: pyspark.pipelines (dp)
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# SECTION 1: Landing → Bronze (Raw Streaming Ingestion + Quarantine)
# -----------------------------------------------------------------------------

@dp.table(
    name="bronze_airlines_raw",
    comment="Raw immutable ingestion of ASA airlines CSV files with rescue handling",
    table_properties={"quality": "bronze", "pipeline.medallion": "bronze"},
    # keep spark_conf minimal here; runtime may override
    spark_conf={
        "spark.sql.adaptive.enabled": "true"
    }
)
def bronze_airlines_raw():
    """
    Auto Loader streaming ingestion.
    Guarantee _rescued_data exists so downstream expectations/filters don't fail.
    """
    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            # schemaEvolutionMode "rescue" allows rescued columns in _rescued_data
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            # optionally route malformed to a badRecordsPath
            .option("badRecordsPath", "/lakeflow/quarantine/asa_airlines/bad_records")
            .load("/databricks-datasets/asa/airlines/*.csv")
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source_file", F.col("_metadata.file_path"))
    )

    # streaming-safe check: df.columns is available on streaming DF
    if "_rescued_data" not in df.columns:
        df = df.withColumn("_rescued_data", F.lit(None).cast(StringType()))

    # add ingestion_date for partitioning (avoid high-cardinality timestamp partitioning)
    df = df.withColumn("ingestion_date", F.to_date("ingestion_timestamp"))

    return df


@dp.table(
    name="bronze_airlines",
    comment="Bronze: Clean raw data excluding corrupt records",
    table_properties={"quality": "bronze", "pipeline.medallion": "bronze"},
    partition_cols=["ingestion_date"]  # partition by date (lower cardinality)
)
@dp.expect_or_drop("no_corrupt_records", "_rescued_data IS NULL")
def bronze_airlines():
    """
    Cleaned bronze that drops rescued/corrupt rows (or routes them via quarantine table).
    dp.expect_or_drop guarantees this table only contains rows where _rescued_data IS NULL.
    """
    # Use dp.read to reference upstream SDP table (runtime will handle streaming semantics).
    df = dp.read("bronze_airlines_raw")

    # normalize column names (lower-case) to avoid case mismatch downstream
    for c in df.columns:
        if c != c.lower():
            df = df.withColumnRenamed(c, c.lower())

    # keep canonical columns and preserve raw payload if present
    selected_cols = [
        "ingestion_timestamp", "ingestion_date", "source_file",
        "year", "month", "dayofmonth", "dayofweek",
        "deptime", "crsdeptime", "arrtime", "crsarrtime",
        "uniquecarrier", "flightnum", "tailnum",
        "actualelapsedtime", "crselapsedtime", "airtime",
        "arrdelay", "depdelay",
        "origin", "dest", "distance", "taxiin", "taxiout",
        "cancelled", "cancellationcode", "diverted",
        "carrierdelay", "weatherdelay", "nasdelay", "securitydelay", "lateaircraftdelay",
        "_rescued_data"
    ]

    # select only columns that exist (avoid selecting non-existent columns)
    cols_present = [c for c in selected_cols if c in df.columns]

    return df.select(*cols_present)


@dp.table(
    name="bronze_airlines_quarantine",
    comment="Fully unparseable/rescued records for audit (quarantine pattern)",
    table_properties={"quality": "quarantine"}
)
def bronze_airlines_quarantine():
    """
    Store rescued/unparseable rows for audit/troubleshooting.
    """
    df = dp.read("bronze_airlines_raw")

    # If column naming differs use case-insensitive lookup
    rescued_col = "_rescued_data" if "_rescued_data" in df.columns else "_rescued_data"

    # some Auto Loader versions use _rescued_data as MAP or STRING; cast to string for audit
    return (
        df.filter(F.col(rescued_col).isNotNull())
          .select(
              F.current_timestamp().alias("quarantine_timestamp"),
              F.col(rescued_col).cast(StringType()).alias("raw_rescued_data"),
              F.col("source_file"),
              F.col("ingestion_date")
          )
    )

# -----------------------------------------------------------------------------
# SECTION 2: Bronze → Silver (Cleaning, Dedup, Validation with Expectations)
# -----------------------------------------------------------------------------

@dp.table(
    name="silver_airlines_clean",
    comment="Silver: Standardized, deduplicated, validated airline data",
    table_properties={
        "quality": "silver",
        "pipeline.medallion": "silver",
        "delta.enableChangeDataFeed": "true"
    },
    cluster_by=["carrier_code", "flight_number"]
)
@dp.expect("valid_flight_date", "flight_date IS NOT NULL AND flight_date BETWEEN DATE '1987-01-01' AND current_date()")
@dp.expect("valid_carrier_code", "carrier_code IS NOT NULL AND length(trim(carrier_code)) = 2")
@dp.expect_or_drop("valid_distance", "distance_miles > 0")
@dp.expect_or_drop("reasonable_arrival_delay", "arrival_delay_min IS NULL OR arrival_delay_min >= -120")
@dp.expect_or_drop("reasonable_departure_delay", "departure_delay_min IS NULL OR departure_delay_min >= -120")
@dp.expect_or_fail("no_future_flights", "flight_date <= current_date()")
def silver_airlines_clean():
    """
    Read canonical bronze table and produce a cleaned silver table.
    Assumes bronze columns are normalized to lower-case names.
    """
    df = dp.read("bronze_airlines")  # SDP runtime uses streaming/batch semantics automatically

    # defensive column name handling
    cols = set(df.columns)

    # cast & canonicalize columns (use getColumn if missing -> produce NULL)
    def safe_col(name):
        return F.col(name) if name in cols else F.lit(None)

    cleaned = (
        df
        .withColumn("flight_year", safe_col("year").cast(IntegerType()))
        .withColumn("flight_month", safe_col("month").cast(IntegerType()))
        .withColumn("flight_day_of_month", safe_col("dayofmonth").cast(IntegerType()))
        .withColumn("flight_day_of_week", safe_col("dayofweek").cast(IntegerType()))
        .withColumn("flight_date", F.when(
            (F.col("flight_year").isNotNull()) & (F.col("flight_month").isNotNull()) & (F.col("flight_day_of_month").isNotNull()),
            F.expr("make_date(flight_year, flight_month, flight_day_of_month)")
        ).otherwise(F.lit(None).cast("date")))
        .withColumn("dep_time", safe_col("deptime").cast(DoubleType()))
        .withColumn("crs_dep_time", safe_col("crsdeptime").cast(DoubleType()))
        .withColumn("arr_time", safe_col("arrtime").cast(DoubleType()))
        .withColumn("crs_arr_time", safe_col("crsarrtime").cast(DoubleType()))
        .withColumn("carrier_code", safe_col("uniquecarrier"))
        .withColumn("flight_number", safe_col("flightnum"))
        .withColumn("tail_number", safe_col("tailnum"))
        .withColumn("origin_airport", safe_col("origin"))
        .withColumn("dest_airport", safe_col("dest"))
        .withColumn("actual_elapsed_time_min", safe_col("actualelapsedtime").cast(DoubleType()))
        .withColumn("crs_elapsed_time_min", safe_col("crselapsedtime").cast(DoubleType()))
        .withColumn("air_time_min", safe_col("airtime").cast(DoubleType()))
        .withColumn("distance_miles", safe_col("distance").cast(DoubleType()))
        .withColumn("taxi_in_min", safe_col("taxiin").cast(DoubleType()))
        .withColumn("taxi_out_min", safe_col("taxiout").cast(DoubleType()))
        .withColumn("arrival_delay_min", safe_col("arrdelay").cast(DoubleType()))
        .withColumn("departure_delay_min", safe_col("depdelay").cast(DoubleType()))
        .withColumn("carrier_delay_min", safe_col("carrierdelay").cast(DoubleType()))
        .withColumn("weather_delay_min", safe_col("weatherdelay").cast(DoubleType()))
        .withColumn("nas_delay_min", safe_col("nasdelay").cast(DoubleType()))
        .withColumn("security_delay_min", safe_col("securitydelay").cast(DoubleType()))
        .withColumn("late_aircraft_delay_min", safe_col("lateaircraftdelay").cast(DoubleType()))
        .withColumn("is_cancelled", safe_col("cancelled").cast(IntegerType()))
        .withColumn("cancellation_reason", safe_col("cancellationcode"))
        .withColumn("is_diverted", safe_col("diverted").cast(IntegerType()))
    )

    # Deduplication: Latest record per unique flight key (use ingestion_timestamp if present)
    window_spec = Window.partitionBy(
        "flight_date", "carrier_code", "flight_number", "origin_airport", "dest_airport", "crs_dep_time"
    ).orderBy(F.col("ingestion_timestamp").desc_nulls_last())

    deduped = cleaned.withColumn("rn", F.row_number().over(window_spec))

    result = (
        deduped
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Drop legacy/raw columns if present to keep silver canonical
    drop_cols = [c for c in ["year", "month", "dayofmonth", "dayofweek",
                             "deptime", "crsdeptime", "arrtime", "crsarrtime",
                             "uniquecarrier", "flightnum", "tailnum", "origin", "dest"]
                 if c in result.columns]
    result = result.drop(*drop_cols)

    return result

# -----------------------------------------------------------------------------
# SECTION 3: Silver → Gold (Aggregated Materialized Views for BI)
# -----------------------------------------------------------------------------

@dp.materialized_view(
    name="gold_monthly_airline_otp",
    comment="Gold: Monthly on-time performance per carrier – BI dashboard ready",
    table_properties={"quality": "gold", "pipeline.medallion": "gold"},
    cluster_by=["flight_year", "flight_month"]
)
def gold_monthly_airline_otp():
    filtered = dp.read("silver_airlines_clean").filter(
        (F.coalesce(F.col("is_cancelled"), F.lit(0)) == 0) & (F.coalesce(F.col("is_diverted"), F.lit(0)) == 0)
    )

    agg = filtered.groupBy("flight_year", "flight_month", "carrier_code").agg(
        F.count("*").alias("total_flights"),
        F.sum(F.when(F.col("is_cancelled") == 1, 1).otherwise(0)).alias("cancelled_flights"),
        F.sum(F.when(F.col("is_diverted") == 1, 1).otherwise(0)).alias("diverted_flights"),
        F.sum(F.when(F.col("arrival_delay_min") <= 15, 1).otherwise(0)).alias("on_time_flights"),
        F.round(F.avg("arrival_delay_min"), 2).alias("avg_arrival_delay_min"),
        F.round(F.avg("departure_delay_min"), 2).alias("avg_departure_delay_min")
    )

    return (
        agg
        .withColumn("cancellation_rate_pct", F.round(100.0 * F.col("cancelled_flights") / F.col("total_flights"), 2))
        .withColumn("diversion_rate_pct", F.round(100.0 * F.col("diverted_flights") / F.col("total_flights"), 2))
        .withColumn("on_time_percentage", F.round(100.0 * F.col("on_time_flights") / F.col("total_flights"), 2))
        .orderBy(F.col("flight_year").desc(), F.col("flight_month").desc(), F.col("cancellation_rate_pct").desc())
    )


@dp.materialized_view(
    name="gold_delay_reason_analytics",
    comment="Gold: Delay causes breakdown by month/carrier",
    table_properties={"quality": "gold"}
)
def gold_delay_reason_analytics():
    delayed_flights = dp.read("silver_airlines_clean").filter(F.col("arrival_delay_min") > 15)

    return (
        delayed_flights.groupBy("flight_year", "flight_month", "carrier_code").agg(
            F.count("*").alias("flights_with_delay_data"),
            F.round(F.sum("carrier_delay_min"), 2).alias("total_carrier_delay_min"),
            F.round(F.sum("weather_delay_min"), 2).alias("total_weather_delay_min"),
            F.round(F.sum("nas_delay_min"), 2).alias("total_nas_delay_min"),
            F.round(F.sum("security_delay_min"), 2).alias("total_security_delay_min"),
            F.round(F.sum("late_aircraft_delay_min"), 2).alias("total_late_aircraft_delay_min"),
            F.round(F.avg("carrier_delay_min"), 2).alias("avg_carrier_delay_when_attributed"),
            F.round(F.avg("weather_delay_min"), 2).alias("avg_weather_delay_when_attributed")
        )
        .filter(F.col("flights_with_delay_data") > 10)
        .orderBy(F.col("total_carrier_delay_min").desc())
    )


@dp.materialized_view(
    name="gold_airport_kpis",
    comment="Gold: Airport KPIs for dashboards – major airports only",
    table_properties={"quality": "gold"},
    cluster_by=["airport"]
)
def gold_airport_kpis():
    # Departures aggregation
    departures = (
        dp.read("silver_airlines_clean")
        .groupBy("origin_airport")
        .agg(
            F.count("*").alias("total_ops"),
            F.avg("departure_delay_min").alias("avg_delay_min"),
            F.sum(F.when(F.col("departure_delay_min") > 15, 1).otherwise(0)).alias("delayed_ops"),
            F.sum(F.when(F.col("is_cancelled") == 1, 1).otherwise(0)).alias("cancelled_ops")
        )
        .withColumn("flow_type", F.lit("departure"))
        .withColumnRenamed("origin_airport", "airport")
    )

    # Arrivals aggregation
    arrivals = (
        dp.read("silver_airlines_clean")
        .groupBy("dest_airport")
        .agg(
            F.count("*").alias("total_ops"),
            F.avg("arrival_delay_min").alias("avg_delay_min"),
            F.sum(F.when(F.col("arrival_delay_min") > 15, 1).otherwise(0)).alias("delayed_ops"),
            F.sum(F.when(F.col("is_cancelled") == 1, 1).otherwise(0)).alias("cancelled_ops")
        )
        .withColumn("flow_type", F.lit("arrival"))
        .withColumnRenamed("dest_airport", "airport")
    )

    combined = departures.unionByName(arrivals, allowMissingColumns=True)

    return (
        combined
        .groupBy("airport")
        .agg(
            F.sum("total_ops").alias("total_operations"),
            F.round(F.avg("avg_delay_min"), 2).alias("avg_delay_all_flights"),
            F.sum("delayed_ops").alias("delayed_ops"),
            F.sum("cancelled_ops").alias("cancelled_ops"),
            F.sum("total_ops").alias("total_flights")
        )
        .withColumn("delay_rate_pct", F.round(100.0 * F.col("delayed_ops") / F.col("total_flights"), 2))
        .withColumn("cancellation_rate_pct", F.round(100.0 * F.col("cancelled_ops") / F.col("total_flights"), 2))
        .filter(F.col("total_flights") >= 1000)
        .orderBy(F.col("total_operations").desc())
    )

# -----------------------------------------------------------------------------
# END OF PIPELINE – Deploy as Serverless Lakeflow SDP (Triggered/Continuous Mode)
# -----------------------------------------------------------------------------
