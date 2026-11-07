"""Tests of iceberg_nyc_taxi_analytics.py: arguments and the curated-table SQL builders."""

from __future__ import annotations

import datetime

import pytest

import iceberg_nyc_taxi_analytics as analytics

BASE_ARGS = ["--warehouse", "s3://bucket/warehouse"]


def test_parse_args_defaults():
    args = analytics.parse_args(BASE_ARGS)
    assert (args.catalog, args.database, args.table) == ("glue", "nyc_taxi_database", "nyc_taxi_table")
    assert args.curated_table == "nyc_taxi_trips_curated"
    assert args.retain_last == 5
    assert args.warehouse == "s3://bucket/warehouse"


@pytest.mark.parametrize(
    "extra",
    [[], ["--retain-last", "0"], ["--retain-last", "x"], ["--curated-table", "nyc_taxi_table"], ["--curated-table", "Bad"]],
)
def test_parse_args_rejects_invalid_input(extra):
    argv = extra if not extra else BASE_ARGS + extra
    with pytest.raises(SystemExit):
        analytics.parse_args(argv)


def test_curated_columns_replace_datetime_strings_with_timestamps_in_place():
    columns = analytics.curated_base_columns()
    assert columns[0] == ("trip_id", "STRING")
    assert len(columns) == 24
    names = [n for n, _ in columns]
    assert names[6:8] == ["pickup_ts", "dropoff_ts"]
    assert dict(columns)["pickup_ts"] == "TIMESTAMP_NTZ"
    assert "pickup_datetime" not in names and analytics.EVOLVED_COLUMN not in names


def test_create_curated_table_sql():
    sql = analytics.create_curated_table_sql("`glue`.`db`.`c`", "s3://b/warehouse/db/c")
    assert sql.startswith("CREATE TABLE IF NOT EXISTS `glue`.`db`.`c` (")
    assert "PARTITIONED BY (days(`pickup_ts`))" in sql
    assert "LOCATION 's3://b/warehouse/db/c'" in sql
    assert "'format-version'='2'" in sql
    for mode in ("delete", "update", "merge"):
        assert f"'write.{mode}.mode'='merge-on-read'" in sql
    assert "`pickup_ts` TIMESTAMP_NTZ NOT NULL" in sql


def test_merge_sql_has_insert_update_and_delete_clauses():
    sql = analytics.merge_sql("`glue`.`db`.`c`", "src", ["fare_per_mile"])
    assert "ON t.`trip_id` = s.`trip_id`" in sql
    assert "WHEN MATCHED AND NOT (t.`medallion` <=> s.`medallion` AND" in sql
    assert "t.`fare_per_mile` = NULL" in sql
    assert "WHEN NOT MATCHED THEN\n  INSERT (`trip_id`, `medallion`" in sql
    assert sql.endswith("WHEN NOT MATCHED BY SOURCE THEN\n  DELETE")
    assert "fare_per_mile" not in analytics.merge_sql("c", "src")


def test_row_level_and_evolution_sql():
    assert analytics.add_column_sql("c").startswith("ALTER TABLE c ADD COLUMN `fare_per_mile` DOUBLE COMMENT")
    assert analytics.backfill_sql("c") == (
        "UPDATE c\nSET `fare_per_mile` = round(try_divide(fare_amount, trip_distance), 2)\n"
        "WHERE `fare_per_mile` IS NULL AND trip_distance > 0"
    )
    assert analytics.delete_sql("c") == "DELETE FROM c\nWHERE trip_distance < 0.2"


def test_maintenance_procedure_sql():
    rewrite = analytics.rewrite_data_files_sql("glue", "db", "c")
    assert rewrite.startswith("CALL `glue`.system.rewrite_data_files(\n  table => 'db.c',")
    assert "'delete-file-threshold', '1'" in rewrite
    assert "rewrite_position_delete_files" in analytics.rewrite_position_delete_files_sql("glue", "db", "c")
    expire = analytics.expire_snapshots_sql("glue", "db", "c", "2026-11-06 00:00:00.000", 5)
    assert "older_than => TIMESTAMP '2026-11-06 00:00:00.000'" in expire and "retain_last => 5" in expire
    orphans = analytics.remove_orphan_files_sql("glue", "db", "c")
    assert "dry_run => true" in orphans
    # Iceberg rejects older_than < 24 hours, so the default (3 days) is used.
    assert "older_than" not in orphans


def test_tag_sql():
    assert analytics.create_tag_sql("t", "first_snapshot", 42, 30, replace=False) == (
        "ALTER TABLE t CREATE TAG IF NOT EXISTS `first_snapshot` AS OF VERSION 42 RETAIN 30 DAYS"
    )
    assert "CREATE OR REPLACE TAG `x`" in analytics.create_tag_sql("t", "x", 1, 7, replace=True)


def test_curated_schema_and_spec_checks():
    good = {n: {"STRING": "string", "INT": "int", "BIGINT": "bigint", "DOUBLE": "double",
                "TIMESTAMP_NTZ": "timestamp_ntz"}[t] for n, t in analytics.curated_base_columns()}
    assert analytics.curated_schema_mismatches({**good, "fare_per_mile": "double"}) == []
    bad = dict(good, pickup_ts="string")
    del bad["trip_id"]
    assert len(analytics.curated_schema_mismatches(bad)) == 2
    spec = {"spec-id": 0, "fields": [{"name": "pickup_ts_day", "transform": "day", "source-id": 7, "field-id": 1000}]}
    assert analytics.curated_spec_mismatches(spec, 7) == []
    assert analytics.curated_spec_mismatches({"fields": []}, 7)
    assert analytics.curated_spec_mismatches(spec, 8)


@pytest.mark.spark
def test_curated_source_dedupes_and_parses_timestamps(spark):
    import iceberg_nyc_taxi_load as load
    from test_trip_stats import SAMPLE_ROW

    a = dict(SAMPLE_ROW)
    b = dict(SAMPLE_ROW, medallion="B", pickup_datetime="2013-13-45 99:00:00")  # invalid datetime
    c = dict(SAMPLE_ROW, medallion="C", pickup_datetime="2013-11-30 23:59:59", dropoff_datetime="2013-12-01 00:10:00")
    schema = load.expected_spark_schema()
    names = [f.name for f in schema.fields]
    rows = [tuple(r[n] for n in names) for r in (a, a, b, c)]  # `a` loaded twice
    spark.createDataFrame(rows, schema).createOrReplaceTempView("shared_rows")

    result = spark.sql(analytics.curated_source_sql("shared_rows"))
    assert result.columns == analytics.curated_column_names()
    got = {r["medallion"]: r for r in result.collect()}
    assert sorted(got) == ["C", SAMPLE_ROW["medallion"]]
    assert got[SAMPLE_ROW["medallion"]]["pickup_ts"] == datetime.datetime(2013, 11, 17, 12, 26, 45)
    assert got["C"]["dropoff_ts"] == datetime.datetime(2013, 12, 1, 0, 10, 0)
    expected_id = spark.sql(
        f"SELECT sha2(concat_ws('|', '{a['medallion']}', '{a['hack_license']}', '{a['pickup_datetime']}'), 256)"
    ).first()[0]
    assert got[SAMPLE_ROW["medallion"]]["trip_id"] == expected_id
    assert dict(result.dtypes)["pickup_ts"] == "timestamp_ntz"
