"""Trip statistics: SQL builders (pure) and, with Java, Spark results vs the Rust definitions.

Expected values come from the unit tests of the reference Rust `compute_trip_stats`
(apache-iceberg-s3-metadata/lambda/src/stats.rs) and from `rust_stats_oracle.py`, a line-by-line
Python port of that function.
"""

from __future__ import annotations

import pytest

import iceberg_nyc_taxi_load as load
import rust_stats_oracle as oracle

# A cash trip from the 2013 data (Rust `sample_row()` in lambda/src/models.rs).
SAMPLE_ROW = {
    "medallion": "C6F86DFD1C85EEFBFC234FCB953E1D4B",
    "hack_license": "79C37DA10EA88D6B467F9FA2B29F8006",
    "vendor_id": "CMT",
    "rate_code": 1,
    "store_and_fwd_flag": "N",
    "pickup_datetime": "2013-11-17 12:26:45",
    "dropoff_datetime": "2013-11-17 12:32:02",
    "passenger_count": 1,
    "trip_time_in_secs": 316,
    "trip_distance": 0.8,
    "pickup_longitude": -73.98864,
    "pickup_latitude": 40.748764,
    "dropoff_longitude": -74.002075,
    "dropoff_latitude": 40.754913,
    "payment_type": "CSH",
    "fare_amount": 5.5,
    "surcharge": 0.0,
    "mta_tax": 0.5,
    "tolls_amount": 0.0,
    "total_amount": 6.0,
    "tip_amount": 0.0,
    "tipped": 0,
    "tip_class": 0,
}


def row(vendor_id, payment_type, trip_distance, fare_amount, tip_amount, passenger_count):
    r = dict(SAMPLE_ROW)
    r.update(
        vendor_id=vendor_id,
        payment_type=payment_type,
        trip_distance=trip_distance,
        fare_amount=fare_amount,
        tip_amount=tip_amount,
        passenger_count=passenger_count,
    )
    return r


RUST_TEST_ROWS = [
    row("CMT", "CSH", 1.0, 10.0, 2.0, 1),
    row("VTS", "CRD", 3.0, 20.0, 3.0, 2),
    row("VTS", "CRD", 2.333, 15.0, 0.0, 3),
]
RUST_TEST_EXPECTED = {
    "total_trips": 3,
    "total_passengers": 6,
    "total_distance_miles": 6.33,
    "total_fare_usd": 45.0,
    "total_tips_usd": 5.0,
    "avg_distance_miles": 2.11,
    "avg_fare_usd": 15.0,
    "avg_tip_usd": 1.67,
    "tip_rate_percent": 11.11,
    "max_distance_miles": 3.0,
    "min_distance_miles": 1.0,
    "vendor_breakdown": {"CMT": 1, "VTS": 2},
    "payment_type_breakdown": {"CRD": 2, "CSH": 1},
}


# --- pure Python ----------------------------------------------------------------------------


def test_oracle_reproduces_the_rust_unit_test():
    assert oracle.compute_trip_stats(RUST_TEST_ROWS) == RUST_TEST_EXPECTED
    assert oracle.compute_trip_stats([]) == {}


def test_round2_sql_is_the_rust_formula():
    assert load.round2_sql("x") == "(round((x) * 100D) / 100D)"


def test_trip_stats_sql_has_every_rust_key_once():
    sql = load.trip_stats_sql("t")
    for key in load.STATS_SCALAR_KEYS:
        assert sql.count(f" AS {key}") == 1, key
    assert "CASE WHEN total_fare > 0 THEN total_tips / total_fare * 100D ELSE 0D END" in sql
    assert "max_distance AS max_distance_miles" in sql  # not rounded
    assert "round(min" not in sql and "round(max" not in sql
    assert sql.rstrip().endswith("FROM t\n)")


def test_breakdown_sql_groups_by_the_column():
    sql = load.breakdown_sql("t VERSION AS OF 1", "payment_type")
    assert sql == "SELECT `payment_type` AS key, count(*) AS trips\nFROM t VERSION AS OF 1\nGROUP BY `payment_type`\nORDER BY key"


def test_assemble_trip_stats_shape_and_types():
    totals = {k: 1 for k in load.STATS_SCALAR_KEYS}
    stats = load.assemble_trip_stats(totals, [("CMT", 1)], [("CSH", 1)])
    assert list(stats) == list(load.STATS_SCALAR_KEYS) + ["vendor_breakdown", "payment_type_breakdown"]
    assert isinstance(stats["total_trips"], int) and isinstance(stats["total_passengers"], int)
    assert isinstance(stats["avg_fare_usd"], float)
    assert stats["vendor_breakdown"] == {"CMT": 1}


def test_assemble_trip_stats_empty_is_empty_object():
    assert load.assemble_trip_stats({"total_trips": 0}, [], []) == {}
    assert load.assemble_trip_stats({}, [], []) == {}


def test_format_stats_sorts_keys_like_serde_json():
    text = load.format_stats({"b": 1, "a": {"z": 1, "y": 2}})
    assert text.index('"a"') < text.index('"b"') and text.index('"y"') < text.index('"z"')


# --- Spark (needs Java) ---------------------------------------------------------------------


def _view(spark, rows, name):
    schema = load.expected_spark_schema()
    names = [f.name for f in schema.fields]
    spark.createDataFrame([tuple(r[n] for n in names) for r in rows], schema).createOrReplaceTempView(name)
    return name


@pytest.mark.spark
def test_spark_stats_match_the_rust_unit_test(spark):
    assert load.compute_trip_stats(spark, _view(spark, RUST_TEST_ROWS, "rust_rows")) == RUST_TEST_EXPECTED


@pytest.mark.spark
def test_spark_zero_fare_gives_zero_tip_rate(spark):
    stats = load.compute_trip_stats(spark, _view(spark, [row("CMT", "NOC", 0.5, 0.0, 0.0, 1)], "zero_fare"))
    assert stats["tip_rate_percent"] == 0.0


@pytest.mark.spark
def test_spark_empty_relation_gives_empty_object(spark):
    assert load.compute_trip_stats(spark, _view(spark, [], "no_rows")) == {}


@pytest.mark.spark
def test_spark_stats_match_the_oracle_on_the_sample_payload(spark, payload_rows):
    view = _view(spark, payload_rows, "payload")
    stats = load.compute_trip_stats(spark, view)
    assert stats == oracle.compute_trip_stats(payload_rows)
    assert stats["total_trips"] == 5000
    assert sum(stats["vendor_breakdown"].values()) == 5000
    assert sum(stats["payment_type_breakdown"].values()) == 5000


@pytest.mark.spark
@pytest.mark.parametrize(
    "value", [1.005, 0.125, -0.125, 2.675, 0.285, 1.115, 12345.675, 6.3330000000000002, 11.111111, -2.5e-3, 0.0]
)
def test_spark_round2_matches_rust_rounding(spark, value):
    got = spark.sql(f"SELECT {load.round2_sql(f'CAST({value!r} AS DOUBLE)')} AS v").first()["v"]
    assert got == oracle.round2(value)
