"""Pure DataFrame -> DataFrame transforms for each layer (easy to unit-test)."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Raw NYC TLC yellow-taxi columns we use, and their silver names.
RENAMES = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "PULocationID": "pickup_zone_id",
    "DOLocationID": "dropoff_zone_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "tip_amount": "tip_amount",
    "total_amount": "total_amount",
}
BRONZE_REQUIRED = list(RENAMES)
TRIP_KEY = ["vendor_id", "pickup_ts", "dropoff_ts", "pickup_zone_id", "dropoff_zone_id", "total_amount"]


def to_bronze(raw: DataFrame) -> DataFrame:
    """Keep raw data as-is; add lineage metadata."""
    return raw.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file", F.input_file_name()
    )


def to_silver(bronze: DataFrame, month: str | None = None) -> DataFrame:
    """Rename, cast, filter invalid rows, deduplicate, add trip_date.

    month ("YYYY-MM"), if given, keeps only trips picked up in that month: TLC
    monthly files contain a few rows with bad device clocks (e.g. 2002, 2009).
    """
    df = bronze.select([F.col(src).alias(dst) for src, dst in RENAMES.items()])
    df = (
        df.withColumn("pickup_ts", F.col("pickup_ts").cast("timestamp"))
        .withColumn("dropoff_ts", F.col("dropoff_ts").cast("timestamp"))
        .withColumn("vendor_id", F.col("vendor_id").cast("int"))
        .withColumn("pickup_zone_id", F.col("pickup_zone_id").cast("int"))
        .withColumn("dropoff_zone_id", F.col("dropoff_zone_id").cast("int"))
        .withColumn("payment_type", F.col("payment_type").cast("int"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("tip_amount", F.col("tip_amount").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
    )
    duration_min = (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60
    df = df.filter(
        F.col("pickup_ts").isNotNull()
        & F.col("dropoff_ts").isNotNull()
        & (F.col("fare_amount") > 0)
        & (F.col("fare_amount") <= 1000)
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") <= 200)
        & (F.col("tip_amount") >= 0)
        & (duration_min >= 1)
        & (duration_min <= 300)
    )
    if month:
        df = df.filter(F.date_format("pickup_ts", "yyyy-MM") == month)
    return (
        df.dropDuplicates(TRIP_KEY)
        .withColumn("duration_min", F.round(duration_min, 2))
        .withColumn("trip_date", F.to_date("pickup_ts"))
    )


def to_gold(silver: DataFrame) -> DataFrame:
    """Daily metrics per pickup zone."""
    is_card = F.col("payment_type") == 1
    return silver.groupBy("trip_date", "pickup_zone_id").agg(
        F.count("*").alias("trips"),
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.round(F.avg("trip_distance"), 3).alias("avg_distance"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
        # Tip % only for card trips: cash tips are usually not recorded.
        F.round(
            F.avg(F.when(is_card, F.col("tip_amount") / F.col("fare_amount"))) * 100, 2
        ).alias("avg_card_tip_pct"),
    )
