"""Pure-Python tests of iceberg_nyc_taxi_load.py: arguments, contract schema, DDL, checks."""

from __future__ import annotations

import pytest
from pyspark.sql import types as T

import iceberg_nyc_taxi_load as load

# The shared table contract (apache-iceberg-s3-metadata/create_glue_database.sh): field id, name,
# Iceberg type; every field is required.
CONTRACT_FIELDS = [
    (1, "medallion", "string"),
    (2, "hack_license", "string"),
    (3, "vendor_id", "string"),
    (4, "rate_code", "int"),
    (5, "store_and_fwd_flag", "string"),
    (6, "pickup_datetime", "string"),
    (7, "dropoff_datetime", "string"),
    (8, "passenger_count", "int"),
    (9, "trip_time_in_secs", "long"),
    (10, "trip_distance", "double"),
    (11, "pickup_longitude", "double"),
    (12, "pickup_latitude", "double"),
    (13, "dropoff_longitude", "double"),
    (14, "dropoff_latitude", "double"),
    (15, "payment_type", "string"),
    (16, "fare_amount", "double"),
    (17, "surcharge", "double"),
    (18, "mta_tax", "double"),
    (19, "tolls_amount", "double"),
    (20, "total_amount", "double"),
    (21, "tip_amount", "double"),
    (22, "tipped", "int"),
    (23, "tip_class", "int"),
]

BASE_ARGS = ["--warehouse", "s3://bucket/warehouse", "--input", "s3a://bucket/lab/input/p.json"]


def contract_schema_json():
    return {
        "type": "struct",
        "schema-id": 0,
        "fields": [{"id": i, "name": n, "required": True, "type": t} for i, n, t in CONTRACT_FIELDS],
    }


# --- arguments ------------------------------------------------------------------------------


def test_parse_args_defaults():
    args = load.parse_args(BASE_ARGS)
    assert args.catalog == "glue"
    assert args.database == "nyc_taxi_database"
    assert args.table == "nyc_taxi_table"
    assert args.warehouse == "s3://bucket/warehouse"
    assert args.input == "s3a://bucket/lab/input/p.json"
    assert args.mode == "append"


def test_parse_args_all_options_and_trailing_slash():
    args = load.parse_args(
        ["--catalog", "local", "--database", "db1", "--table", "t_1", "--warehouse", "file:/tmp/wh//",
         "--input", "/tmp/p.json", "--mode", "overwrite"]
    )
    assert (args.catalog, args.database, args.table, args.mode) == ("local", "db1", "t_1", "overwrite")
    assert args.warehouse == "file:/tmp/wh"


@pytest.mark.parametrize("missing", ["--warehouse", "--input"])
def test_parse_args_requires_warehouse_and_input(missing):
    argv = list(BASE_ARGS)
    i = argv.index(missing)
    del argv[i : i + 2]
    with pytest.raises(SystemExit):
        load.parse_args(argv)


@pytest.mark.parametrize(
    "option,value",
    [
        ("--database", "NYC"),
        ("--database", "nyc-taxi"),
        ("--table", "t;drop"),
        ("--table", "1table"),
        ("--catalog", "glue.x"),
        ("--warehouse", "bucket/warehouse"),
        ("--warehouse", "s3://bucket/it's"),
        ("--mode", "merge"),
    ],
)
def test_parse_args_rejects_invalid_values(option, value):
    with pytest.raises(SystemExit):
        load.parse_args(BASE_ARGS + [option, value])


# --- contract schema ------------------------------------------------------------------------


def test_table_columns_match_contract_order_and_types():
    assert [(i + 1, n, t) for i, (n, t) in enumerate(load.TABLE_COLUMNS)] == CONTRACT_FIELDS
    assert load.TABLE_FORMAT_VERSION == 2


def test_expected_spark_schema_is_23_required_columns():
    schema = load.expected_spark_schema()
    assert len(schema.fields) == 23
    assert all(not f.nullable for f in schema.fields)
    spark_types = {"string": T.StringType(), "int": T.IntegerType(), "long": T.LongType(), "double": T.DoubleType()}
    assert [(f.name, f.dataType) for f in schema.fields] == [(n, spark_types[t]) for _, n, t in CONTRACT_FIELDS]


def test_create_table_sql_has_exact_columns_location_and_format():
    sql = load.create_table_sql("`glue`.`db`.`t`", "s3://b/warehouse/db/t")
    assert sql.startswith("CREATE TABLE IF NOT EXISTS `glue`.`db`.`t` (")
    column_lines = [line.strip().rstrip(",") for line in sql.splitlines() if line.startswith("  `")]
    ddl_types = {"string": "STRING", "int": "INT", "long": "BIGINT", "double": "DOUBLE"}
    assert column_lines == [f"`{n}` {ddl_types[t]} NOT NULL" for _, n, t in CONTRACT_FIELDS]
    assert "USING iceberg" in sql
    assert "LOCATION 's3://b/warehouse/db/t'" in sql
    assert "TBLPROPERTIES ('format-version'='2')" in sql
    assert "PARTITIONED BY" not in sql


def test_locations_follow_the_contract_layout():
    assert load.namespace_location("s3://b/warehouse", "db") == "s3://b/warehouse/db"
    assert load.table_location("s3://b/warehouse", "db", "t") == "s3://b/warehouse/db/t"
    assert load.table_ident("glue", "db", "t") == "`glue`.`db`.`t`"
    assert load.metadata_table("`glue`.`db`.`t`", "snapshots") == "`glue`.`db`.`t`.`snapshots`"


def test_create_namespace_sql_with_and_without_metadata():
    sql = load.create_namespace_sql("glue", "db", "s3://b/warehouse/db", "NYC Taxi")
    assert sql == "CREATE NAMESPACE IF NOT EXISTS `glue`.`db`\nCOMMENT 'NYC Taxi'\nLOCATION 's3://b/warehouse/db'"
    assert load.create_namespace_sql("local", "db", None, None) == "CREATE NAMESPACE IF NOT EXISTS `local`.`db`"


def test_sql_string_escapes_quotes():
    assert load.sql_string("it's") == "'it\\'s'"


def test_payload_read_schema_and_casts():
    schema = load.payload_read_schema()
    assert [f.name for f in schema.fields] == ["rows"]
    row_type = schema.fields[0].dataType.elementType
    assert [f.name for f in row_type.fields] == [n for _, n, _ in CONTRACT_FIELDS]
    exprs = load.payload_select_exprs("r")
    assert exprs[0] == "CAST(`r`.`medallion` AS STRING) AS `medallion`"
    assert exprs[8] == "CAST(`r`.`trip_time_in_secs` AS BIGINT) AS `trip_time_in_secs`"
    assert len(exprs) == 23


# --- verification helpers -------------------------------------------------------------------


def test_spark_schema_mismatches_accepts_the_contract():
    assert load.spark_schema_mismatches(load.expected_spark_schema()) == []


def test_spark_schema_mismatches_reports_type_nullability_and_order():
    fields = list(load.expected_spark_schema().fields)
    fields[3] = T.StructField("rate_code", T.LongType(), nullable=False)
    fields[0] = T.StructField("medallion", T.StringType(), nullable=True)
    fields[1], fields[2] = fields[2], fields[1]
    problems = load.spark_schema_mismatches(T.StructType(fields))
    assert any("columns differ" in p for p in problems)
    assert any("rate_code: type bigint, expected int" in p for p in problems)
    assert any("medallion: nullable" in p for p in problems)


def test_spark_schema_mismatches_reports_missing_and_extra_columns():
    fields = list(load.expected_spark_schema().fields)[:-1] + [T.StructField("extra", T.StringType(), False)]
    (problem,) = load.spark_schema_mismatches(T.StructType(fields))
    assert "missing=['tip_class']" in problem and "extra=['extra']" in problem


def test_iceberg_schema_mismatches_checks_field_ids():
    assert load.iceberg_schema_mismatches(contract_schema_json()) == []
    bad = contract_schema_json()
    bad["fields"][8]["type"] = "int"
    bad["fields"][22]["id"] = 24
    bad["fields"][0]["required"] = False
    problems = load.iceberg_schema_mismatches(bad)
    assert len(problems) == 3
    assert any("#9" in p and "'type': 'int'" in p for p in problems)
    short = contract_schema_json()
    short["fields"] = short["fields"][:22]
    assert any("has 22 fields" in p for p in load.iceberg_schema_mismatches(short))


def test_table_properties_mismatches():
    assert load.table_properties_mismatches(2, True) == []
    assert len(load.table_properties_mismatches(1, False)) == 2
    assert load.table_properties_mismatches(3, True) == ["format version 3, expected 2"]


def test_null_count_sql_covers_every_column():
    sql = load.null_count_sql("payload_rows", [n for _, n, _ in CONTRACT_FIELDS])
    assert sql.count("count_if(") == 23
    assert "count_if(`tip_class` IS NULL) AS `tip_class`" in sql
    assert sql.endswith("FROM payload_rows")
