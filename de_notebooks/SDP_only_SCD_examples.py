from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, lit, when
from pyspark.sql.types import StringType, ArrayType

catalog = "workspace"
schema = "emp_dp"
employees_cdf_table = "employees_cdf"
employees_table_current = "employees_current"
employees_table_historical = "employees_historical"

# =============================================================================
# STEP 2: One-time function to populate realistic CDC data
# Run this once manually (or via dbutils.notebook.run) to seed test data
# Includes: inserts, updates, deletes, and OUT-OF-ORDER events (real life!)
# =============================================================================
def seed_employees_cdc_data():
    """
    Creates realistic CDC events with tricky scenarios:
    - Out-of-order UPDATE (seq 5 arrives after seq 6 → should be dropped in SCD1)
    - DELETE followed by INSERT (tombstone + resurrection)
    - Normal inserts and updates
    """
    data = [
        (1, "Alex",     "Chef",         "France",     "INSERT", 1),
        (2, "Jessica",  "Owner",        "USA",        "INSERT", 2),
        (3, "Mikhail",  "Security",     "UK",         "INSERT", 3),
        (4, "Gary",     "Cleaner",      "UK",         "INSERT", 4),
        # Out-of-order update: seq=5 arrives after seq=6 → SCD1 will ignore it!
        (5, "Chris",    "Owner",        "Netherlands","INSERT", 6),
        (5, "Chris",    "Manager",      "Netherlands","UPDATE", 5),   # ← arrives late!
        (6, "Pat",      "Mechanic",     "Netherlands","DELETE", 8),
        (6, "Pat",      "Senior Mech",  "Netherlands","INSERT", 7),   # Resurrection!
        (2, "Jessica",  "CEO",          "USA",        "UPDATE", 9),
        (4, "Gary",     None,           None,         "DELETE", 10)
    ]

    columns = ["id", "name", "role", "country", "operation", "sequenceNum"]
    df = spark.createDataFrame(data, columns)

    # Write as Delta table (bronze layer)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{employees_cdf_table}")
    print(f"CDC data seeded into {catalog}.{schema}.{employees_cdf_table}")
    display(df.orderBy("sequenceNum"))

# Uncomment the line below and run once to create the test data
seed_employees_cdc_data()


# =============================================================================
# STEP 3: Define the streaming source (reads CDC events as they arrive)
# =============================================================================

@dp.temporary_view
def employees_cdf():
 return spark.readStream.format("delta").table(f"{catalog}.{schema}.{employees_cdf_table}")

dp.create_target_table(f"{catalog}.{schema}.{employees_table_current}")

dp.create_auto_cdc_flow(
 target=f"{catalog}.{schema}.{employees_table_current}",
 source=employees_cdf_table,
 keys=["id"],
 sequence_by=col("sequenceNum"),
 apply_as_deletes=expr("operation = 'DELETE'"),
 except_column_list = ["operation", "sequenceNum"],
 stored_as_scd_type = 1
)

dp.create_target_table(f"{catalog}.{schema}.{employees_table_historical}")

dp.create_auto_cdc_flow(
 target=f"{catalog}.{schema}.{employees_table_historical}",
 source=employees_cdf_table,
 keys=["id"],
 sequence_by=col("sequenceNum"),
 apply_as_deletes=expr("operation = 'DELETE'"),
 except_column_list = ["operation", "sequenceNum"],
 stored_as_scd_type = 2
)