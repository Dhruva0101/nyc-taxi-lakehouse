from datetime import datetime

import pytest

from lakehouse import quality as dq
from lakehouse.run_pipeline import main
from lakehouse.transforms import TRIP_KEY, to_gold, to_silver

COLUMNS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
           "trip_distance", "PULocationID", "DOLocationID", "payment_type",
           "fare_amount", "tip_amount", "total_amount"]


def trip(pu, do, dist=2.0, fare=10.0, tip=2.0, total=14.0, pay=1, zone=161, vendor=1):
    return (vendor, pu, do, 1, dist, zone, 236, pay, fare, tip, total)


T = datetime
GOOD_1 = trip(T(2024, 1, 1, 8, 0), T(2024, 1, 1, 8, 20))
GOOD_2 = trip(T(2024, 1, 1, 9, 0), T(2024, 1, 1, 9, 10), fare=20.0, tip=0.0, total=24.0, pay=2)
GOOD_3 = trip(T(2024, 1, 2, 9, 0), T(2024, 1, 2, 9, 15), zone=132)
BAD_FARE = trip(T(2024, 1, 1, 10, 0), T(2024, 1, 1, 10, 20), fare=-5.0)
BAD_DIST = trip(T(2024, 1, 1, 11, 0), T(2024, 1, 1, 11, 20), dist=0.0)
BAD_TIME = trip(T(2024, 1, 1, 12, 0), T(2024, 1, 1, 11, 50))  # drop-off before pick-up


def raw_df(spark, rows):
    return spark.createDataFrame(rows, COLUMNS)


def test_silver_filters_invalid_rows(spark):
    df = to_silver(raw_df(spark, [GOOD_1, GOOD_2, GOOD_3, BAD_FARE, BAD_DIST, BAD_TIME]))
    assert df.count() == 3


def test_silver_removes_duplicates(spark):
    df = to_silver(raw_df(spark, [GOOD_1, GOOD_1, GOOD_2]))
    assert df.count() == 2
    assert dq.check_unique(df, TRIP_KEY) == []


def test_silver_adds_trip_date_and_duration(spark):
    row = to_silver(raw_df(spark, [GOOD_1])).first()
    assert str(row["trip_date"]) == "2024-01-01"
    assert row["duration_min"] == pytest.approx(20.0)


def test_silver_month_filter_drops_out_of_month_trips(spark):
    clock_error = trip(T(2009, 1, 1, 0, 5), T(2009, 1, 1, 0, 25))
    df = to_silver(raw_df(spark, [GOOD_1, GOOD_3, clock_error]), month="2024-01")
    assert df.count() == 2


def test_gold_daily_zone_metrics(spark):
    gold = to_gold(to_silver(raw_df(spark, [GOOD_1, GOOD_2, GOOD_3])))
    rows = {(str(r["trip_date"]), r["pickup_zone_id"]): r for r in gold.collect()}
    jan1 = rows[("2024-01-01", 161)]
    assert jan1["trips"] == 2
    assert jan1["revenue"] == pytest.approx(38.0)
    # only GOOD_1 is a card trip: tip 2 / fare 10 = 20%
    assert jan1["avg_card_tip_pct"] == pytest.approx(20.0)
    assert rows[("2024-01-02", 132)]["trips"] == 1


def test_quality_checks_catch_problems(spark):
    df = spark.createDataFrame([(1, None), (1, 5.0)], ["id", "fare_amount"])
    assert dq.check_not_null(df, ["fare_amount"])
    assert dq.check_unique(df, ["id"])
    assert dq.check_columns(df, ["id", "missing_col"])
    with pytest.raises(dq.DataQualityError):
        dq.enforce("test", dq.check_unique(df, ["id"]))


def test_end_to_end_pipeline(spark, tmp_path):
    landing = tmp_path / "landing"
    raw_df(spark, [GOOD_1, GOOD_1, GOOD_2, GOOD_3, BAD_FARE]).write.parquet(str(landing / "f.parquet"))
    # the reader expects *.parquet files directly in landing/
    metrics = main(["--landing", str(landing / "f.parquet"), "--lake", str(tmp_path / "lake"),
                    "--format", "parquet"])
    assert metrics["bronze"]["rows"] == 5
    assert metrics["silver"]["rows"] == 3   # 1 duplicate + 1 bad fare removed
    assert metrics["gold"]["rows"] == 2     # (Jan 1, zone 161) and (Jan 2, zone 132)
