"""
Spark streaming job to handle orders and watermarking.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, sum as _sum,
    count, to_timestamp, current_timestamp,
    when, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, BooleanType, TimestampType
)

spark = (
    SparkSession.builder
    .appName("LateDataETL")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

order_schema = StructType([
    StructField("order_id",    StringType(),  True),
    StructField("customer_id", StringType(),  True),
    StructField("product",     StringType(),  True),
    StructField("amount",      DoubleType(),  True),
    StructField("city",        StringType(),  True),
    StructField("status",      StringType(),  True),
    StructField("event_time",  StringType(),  True),
    StructField("ingest_time", StringType(),  True),
    StructField("is_late",     BooleanType(), True),
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .load()
)

orders = (
    raw_stream
    .select(from_json(col("value").cast("string"), order_schema).alias("d"))
    .select("d.*")
    .withColumn("event_ts",  to_timestamp("event_time"))
    .withColumn("ingest_ts", to_timestamp("ingest_time"))
    .withColumn("latency_minutes",
                (col("ingest_ts").cast("long") - col("event_ts").cast("long")) / 60)
)

orders_wm = orders.withWatermark("event_ts", "2 minutes")

windowed_agg = (
    orders_wm
    .groupBy(
        window("event_ts", "2 minutes"),
        "city",
        "product"
    )
    .agg(
        _sum("amount").alias("total_revenue"),
        count("*").alias("order_count"),
        _sum(when(col("is_late"), 1).otherwise(0)).alias("late_order_count")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "city", "product",
        "total_revenue", "order_count", "late_order_count"
    )
)

raw_query = (
    orders.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/app/checkpoints/raw_orders")
    .option("mergeSchema", "true")
    .start("/app/delta_tables/raw_orders")
)

agg_query = (
    windowed_agg.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/app/checkpoints/agg_orders")
    .option("mergeSchema", "true")
    .start("/app/delta_tables/agg_orders")
)

debug_query = (
    orders.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .trigger(processingTime="10 seconds")
    .start()
)

print("Streaming started... monitoring Kafka orders")
spark.streams.awaitAnyTermination()
