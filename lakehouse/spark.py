"""SparkSession factory. 'parquet' works offline; 'delta' downloads Delta Lake jars on first use."""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

# Run Spark's Python workers with the same interpreter as the driver, so a
# different system Python on PATH can't cause a version mismatch.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def get_spark(app_name: str = "nyc-taxi-lakehouse", fmt: str = "parquet") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")  # small local data
        .config("spark.driver.host", "127.0.0.1")  # local mode: don't depend on the LAN address
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "4g")
    )
    if fmt == "delta":
        from delta import configure_spark_with_delta_pip

        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
    return builder.getOrCreate()
