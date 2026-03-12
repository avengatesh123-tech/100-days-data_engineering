"""
Batch job to merge late data into the main Delta table.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col
from delta.tables import DeltaTable
import os

spark = (
    SparkSession.builder
    .appName("BackfillJob")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

LATE_DATA_PATH   = "/app/late_data/"
DELTA_TABLE_PATH = "/app/delta_tables/raw_orders"

late_files = [f for f in os.listdir(LATE_DATA_PATH) if f.endswith(".json")]
if not late_files:
    print("No late data files found. Exiting.")
    spark.stop()
    exit(0)

print(f"Found {len(late_files)} file(s). Running backfill...")

late_df = (
    spark.read
    .option("multiLine", True)
    .json(LATE_DATA_PATH)
    .withColumn("event_ts",  to_timestamp("event_time"))
    .withColumn("ingest_ts", to_timestamp("ingest_time"))
)

print(f"Late records count: {late_df.count()}")
late_df.show(5, truncate=False)

if not os.path.exists(DELTA_TABLE_PATH):
    late_df.write.format("delta").save(DELTA_TABLE_PATH)
else:
    dt = DeltaTable.forPath(spark, DELTA_TABLE_PATH)

    dt = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
    (
        dt.alias("target")
        .merge(
            late_df.alias("source"),
            "target.order_id = source.order_id"
        )
        .whenMatchedUpdate(set={
            "amount"      : "source.amount",
            "status"      : "source.status",
            "ingest_time" : "source.ingest_time",
            "ingest_ts"   : "source.ingest_ts",
            "is_late"     : "source.is_late"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Merge complete.")

    dt.history(5).select("version", "timestamp", "operation").show()

spark.stop()
