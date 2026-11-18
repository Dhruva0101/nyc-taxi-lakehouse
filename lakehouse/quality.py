"""Data quality checks. Each returns a list of failure messages (empty = pass)."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    pass


def check_columns(df: DataFrame, expected: list[str]) -> list[str]:
    missing = [c for c in expected if c not in df.columns]
    return [f"missing columns: {missing}"] if missing else []


def check_not_null(df: DataFrame, columns: list[str]) -> list[str]:
    row = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]).first()
    return [f"{c} has {row[c]} nulls" for c in columns if row[c]]


def check_unique(df: DataFrame, key: list[str]) -> list[str]:
    dupes = df.groupBy(*key).count().filter("count > 1").count()
    return [f"{dupes} duplicate keys on {key}"] if dupes else []


def check_range(df: DataFrame, column: str, low: float, high: float) -> list[str]:
    bad = df.filter((F.col(column) < low) | (F.col(column) > high)).count()
    return [f"{bad} rows with {column} outside [{low}, {high}]"] if bad else []


def enforce(stage: str, failures: list[str]) -> None:
    if failures:
        raise DataQualityError(f"[{stage}] " + "; ".join(failures))
