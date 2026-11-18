"""Run bronze -> silver -> gold with quality checks and per-stage row counts.

    python -m lakehouse.run_pipeline --landing data/landing --lake data/lake --month 2024-01
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

from lakehouse import quality as dq
from lakehouse.spark import get_spark
from lakehouse.transforms import BRONZE_REQUIRED, TRIP_KEY, to_bronze, to_gold, to_silver

log = logging.getLogger("lakehouse")


def run_bronze(spark, landing: str, lake: str, fmt: str) -> int:
    raw = spark.read.parquet(f"{landing}/*.parquet")
    dq.enforce("bronze", dq.check_columns(raw, BRONZE_REQUIRED))
    bronze = to_bronze(raw)
    bronze.write.format(fmt).mode("overwrite").save(f"{lake}/bronze/yellow_trips")
    return spark.read.format(fmt).load(f"{lake}/bronze/yellow_trips").count()


def run_silver(spark, lake: str, fmt: str, month: str | None = None) -> int:
    bronze = spark.read.format(fmt).load(f"{lake}/bronze/yellow_trips")
    silver = to_silver(bronze, month)
    (silver.write.format(fmt).mode("overwrite").partitionBy("trip_date")
     .save(f"{lake}/silver/yellow_trips"))
    silver = spark.read.format(fmt).load(f"{lake}/silver/yellow_trips")
    dq.enforce("silver", dq.check_not_null(silver, ["pickup_ts", "dropoff_ts", "fare_amount"])
               + dq.check_unique(silver, TRIP_KEY)
               + dq.check_range(silver, "fare_amount", 0.01, 1000))
    return silver.count()


def run_gold(spark, lake: str, fmt: str) -> int:
    silver = spark.read.format(fmt).load(f"{lake}/silver/yellow_trips")
    gold = to_gold(silver)
    gold.write.format(fmt).mode("overwrite").save(f"{lake}/gold/daily_zone_metrics")
    gold = spark.read.format(fmt).load(f"{lake}/gold/daily_zone_metrics")
    dq.enforce("gold", dq.check_unique(gold, ["trip_date", "pickup_zone_id"]))
    return gold.count()


def main(argv=None) -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument("--landing", default="data/landing")
    parser.add_argument("--lake", default="data/lake")
    parser.add_argument("--format", choices=["parquet", "delta"], default="delta")
    parser.add_argument("--month", help="YYYY-MM: keep only trips picked up in this month")
    parser.add_argument("--stage", choices=["all", "bronze", "silver", "gold"], default="all")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    spark = get_spark(fmt=args.format)
    Path(args.lake).mkdir(parents=True, exist_ok=True)
    stages = {"bronze": lambda: run_bronze(spark, args.landing, args.lake, args.format),
              "silver": lambda: run_silver(spark, args.lake, args.format, args.month),
              "gold": lambda: run_gold(spark, args.lake, args.format)}
    metrics = {}
    for name, fn in stages.items():
        if args.stage not in ("all", name):
            continue
        start = time.monotonic()
        rows = fn()
        metrics[name] = {"rows": rows, "seconds": round(time.monotonic() - start, 2)}
        log.info("stage=%s rows=%d seconds=%.2f", name, rows, metrics[name]["seconds"])
    if "bronze" in metrics and "silver" in metrics and metrics["bronze"]["rows"]:
        kept = metrics["silver"]["rows"] / metrics["bronze"]["rows"]
        log.info("silver kept %.1f%% of bronze rows", kept * 100)
    print(json.dumps(metrics))
    return metrics


if __name__ == "__main__":
    main()
