#!/usr/bin/env python3
"""Load the NYC Taxi sample payload into the shared Iceberg table.

Spark port of the reference Lambda `write` action (apache-iceberg-s3-metadata): it reads the
`{"rows": [...]}` payload JSON, casts every column to the exact type of the shared table and
commits the rows with the DataFrameWriterV2 API.

The first run also creates the Glue database and the Iceberg table (the IDE role has no Glue
permissions, so the objects are created from inside the cluster with the job's IRSA role):

- `CREATE NAMESPACE IF NOT EXISTS` with an explicit location `<warehouse>/<database>`.
- `CREATE TABLE IF NOT EXISTS` with the 23 NOT NULL columns of the contract, in order, so the
  Iceberg field ids are 1..23 (the Rust writer relies on them), unpartitioned,
  format version 2, explicit `LOCATION '<warehouse>/<database>/<table>'`.
- If the table already exists, its schema, partition spec and format version are verified and
  the job fails before writing anything if they differ.

The Iceberg catalog is configured only through Spark configuration (`spark.sql.catalog.<name>`,
set in the SparkApplication); the script receives just the catalog name. The same code therefore
runs against the AWS Glue catalog on EKS and against a local `type=hadoop` catalog for local end-to-end runs.

The script is self-contained on purpose (no shared module shipped with `--py-files`): the Spark
Operator runs `spark-submit` in its own pod, which resolves remote `--py-files` URIs and has no
S3 access. `tests/test_consistency.py` checks that the definitions duplicated in
`iceberg_nyc_taxi_analytics.py` stay identical.

Usage:
    spark-submit iceberg_nyc_taxi_load.py --catalog glue --database nyc_taxi_database \
        --table nyc_taxi_table --warehouse s3://<bucket>/warehouse \
        --input s3a://<bucket>/lab/input/nyctaxi_payload.json [--mode append|overwrite]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from pyspark.sql import types as T

APP_NAME = "iceberg-nyc-taxi-load"
LOG_PREFIX = "[iceberg-load]"

# ---------------------------------------------------------------------------------------------
# Shared table contract (also duplicated in iceberg_nyc_taxi_analytics.py; keep both identical).
# ---------------------------------------------------------------------------------------------

# (column name, Iceberg type) in field-id order: field id = position + 1. Every column is
# REQUIRED (NOT NULL). Source: apache-iceberg-s3-metadata/create_glue_database.sh.
TABLE_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("medallion", "string"),
    ("hack_license", "string"),
    ("vendor_id", "string"),
    ("rate_code", "int"),
    ("store_and_fwd_flag", "string"),
    ("pickup_datetime", "string"),
    ("dropoff_datetime", "string"),
    ("passenger_count", "int"),
    ("trip_time_in_secs", "long"),
    ("trip_distance", "double"),
    ("pickup_longitude", "double"),
    ("pickup_latitude", "double"),
    ("dropoff_longitude", "double"),
    ("dropoff_latitude", "double"),
    ("payment_type", "string"),
    ("fare_amount", "double"),
    ("surcharge", "double"),
    ("mta_tax", "double"),
    ("tolls_amount", "double"),
    ("total_amount", "double"),
    ("tip_amount", "double"),
    ("tipped", "int"),
    ("tip_class", "int"),
)
TABLE_FORMAT_VERSION = 2

# Iceberg primitive type -> Spark SQL DDL type / Spark DataType.
ICEBERG_TO_SPARK_SQL: Dict[str, str] = {
    "string": "STRING",
    "int": "INT",
    "long": "BIGINT",
    "double": "DOUBLE",
}
ICEBERG_TO_SPARK_TYPE: Dict[str, T.DataType] = {
    "string": T.StringType(),
    "int": T.IntegerType(),
    "long": T.LongType(),
    "double": T.DoubleType(),
}

# Glue (and Athena) names: lowercase letters, digits and underscores.
GLUE_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
# Spark catalog names (the `<name>` of `spark.sql.catalog.<name>`).
CATALOG_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Any URI with a scheme: s3://, s3a://, file:/, file:///, hdfs://, ...
URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:/")


class ValidationError(Exception):
    """A check failed; the job exits with status 1."""


# ---------------------------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------------------------


def glue_name(value: str) -> str:
    """argparse type: a Glue database or table name."""
    if not GLUE_NAME_RE.match(value):
        raise argparse.ArgumentTypeError(
            f"invalid name {value!r}: use lowercase letters, digits and underscores"
        )
    return value


def catalog_name(value: str) -> str:
    """argparse type: a Spark catalog name."""
    if not CATALOG_NAME_RE.match(value):
        raise argparse.ArgumentTypeError(
            f"invalid catalog name {value!r}: use letters, digits and underscores"
        )
    return value


def location_uri(value: str) -> str:
    """argparse type: an absolute URI with a scheme, returned without trailing slashes."""
    stripped = value.rstrip("/")
    if not URI_RE.match(stripped) or stripped.endswith(":"):
        raise argparse.ArgumentTypeError(
            f"invalid location {value!r}: expected a URI such as s3://<bucket>/warehouse"
        )
    if any(c in stripped for c in "'`\n\r"):
        raise argparse.ArgumentTypeError(f"invalid location {value!r}: quotes are not allowed")
    return stripped


def input_uri(value: str) -> str:
    """argparse type: the payload JSON path (a URI or a local path)."""
    if not value or any(c in value for c in "'`\n\r"):
        raise argparse.ArgumentTypeError(f"invalid input {value!r}")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="iceberg_nyc_taxi_load.py",
        description="Load the NYC Taxi payload JSON into the shared Iceberg table.",
    )
    parser.add_argument(
        "--catalog",
        type=catalog_name,
        default="glue",
        help="Spark catalog name, configured with spark.sql.catalog.<name>.* (default: glue)",
    )
    parser.add_argument(
        "--database",
        type=glue_name,
        default="nyc_taxi_database",
        help="Glue database / Iceberg namespace (default: nyc_taxi_database)",
    )
    parser.add_argument(
        "--table",
        type=glue_name,
        default="nyc_taxi_table",
        help="Iceberg table name (default: nyc_taxi_table)",
    )
    parser.add_argument(
        "--warehouse",
        type=location_uri,
        required=True,
        help="Iceberg warehouse root, e.g. s3://<bucket>/warehouse",
    )
    parser.add_argument(
        "--input",
        type=input_uri,
        required=True,
        help='Payload JSON ({"rows": [...]}), e.g. s3a://<bucket>/lab/input/nyctaxi_payload.json',
    )
    parser.add_argument(
        "--mode",
        choices=("append", "overwrite"),
        default="append",
        help="append the rows (default) or overwrite all rows of the table",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


# ---------------------------------------------------------------------------------------------
# Names, locations and DDL
# ---------------------------------------------------------------------------------------------


def quote(name: str) -> str:
    """Backtick-quote one identifier part (names are validated, so no escaping is needed)."""
    return f"`{name}`"


def table_ident(catalog: str, database: str, table: str) -> str:
    return ".".join(quote(part) for part in (catalog, database, table))


def metadata_table(ident: str, name: str) -> str:
    """Iceberg metadata table of a table identifier, e.g. `c`.`db`.`t`.`snapshots`."""
    return f"{ident}.{quote(name)}"


def namespace_location(warehouse: str, database: str) -> str:
    return f"{warehouse}/{database}"


def table_location(warehouse: str, database: str, table: str) -> str:
    return f"{warehouse}/{database}/{table}"


def sql_string(value: str) -> str:
    """A single-quoted SQL string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def create_namespace_sql(
    catalog: str, database: str, location: Optional[str], comment: Optional[str]
) -> str:
    """CREATE NAMESPACE IF NOT EXISTS; location/comment are omitted when None.

    The Glue catalog stores the location as the database LocationUri and the comment as its
    description. Hadoop catalogs do not support namespace metadata, so both are left out there.
    """
    sql = f"CREATE NAMESPACE IF NOT EXISTS {quote(catalog)}.{quote(database)}"
    if comment is not None:
        sql += f"\nCOMMENT {sql_string(comment)}"
    if location is not None:
        sql += f"\nLOCATION {sql_string(location)}"
    return sql


def column_ddl() -> List[str]:
    return [f"{quote(name)} {ICEBERG_TO_SPARK_SQL[itype]} NOT NULL" for name, itype in TABLE_COLUMNS]


def create_table_sql(ident: str, location: str) -> str:
    """DDL of the shared table: 23 NOT NULL columns, unpartitioned, v2, explicit LOCATION."""
    columns = ",\n".join(f"  {c}" for c in column_ddl())
    return (
        f"CREATE TABLE IF NOT EXISTS {ident} (\n{columns}\n)\n"
        "USING iceberg\n"
        f"LOCATION {sql_string(location)}\n"
        f"TBLPROPERTIES ('format-version'='{TABLE_FORMAT_VERSION}')"
    )


def expected_spark_schema() -> T.StructType:
    """Spark schema of the shared table (NOT NULL columns are non-nullable)."""
    return T.StructType(
        [T.StructField(name, ICEBERG_TO_SPARK_TYPE[itype], nullable=False) for name, itype in TABLE_COLUMNS]
    )


def payload_read_schema() -> T.StructType:
    """Explicit schema for reading the payload: only `rows` is read; values may be null here."""
    row = T.StructType(
        [T.StructField(name, ICEBERG_TO_SPARK_TYPE[itype], nullable=True) for name, itype in TABLE_COLUMNS]
    )
    return T.StructType([T.StructField("rows", T.ArrayType(row, containsNull=True), nullable=True)])


def payload_select_exprs(struct_column: str = "r") -> List[str]:
    """Exploded payload struct -> the 23 columns in table order, cast to the exact types."""
    return [
        f"CAST({quote(struct_column)}.{quote(name)} AS {ICEBERG_TO_SPARK_SQL[itype]}) AS {quote(name)}"
        for name, itype in TABLE_COLUMNS
    ]


# ---------------------------------------------------------------------------------------------
# Schema verification (pure functions)
# ---------------------------------------------------------------------------------------------


def spark_schema_mismatches(actual: T.StructType) -> List[str]:
    """Differences between a table's Spark schema and the contract (empty list = match)."""
    problems: List[str] = []
    expected = expected_spark_schema()
    actual_names = [f.name for f in actual.fields]
    expected_names = [f.name for f in expected.fields]
    if actual_names != expected_names:
        missing = [n for n in expected_names if n not in actual_names]
        extra = [n for n in actual_names if n not in expected_names]
        problems.append(
            f"columns differ: expected {len(expected_names)} columns in contract order, got "
            f"{len(actual_names)} (missing={missing}, extra={extra}, order={actual_names})"
        )
    for exp in expected.fields:
        act = next((f for f in actual.fields if f.name == exp.name), None)
        if act is None:
            continue
        if act.dataType != exp.dataType:
            problems.append(
                f"column {exp.name}: type {act.dataType.simpleString()}, expected {exp.dataType.simpleString()}"
            )
        if act.nullable:
            problems.append(f"column {exp.name}: nullable, expected NOT NULL (required)")
    return problems


def iceberg_schema_mismatches(schema_json: Mapping[str, Any]) -> List[str]:
    """Differences between an Iceberg schema (SchemaParser JSON) and the contract field ids."""
    problems: List[str] = []
    fields = list(schema_json.get("fields", []))
    if len(fields) != len(TABLE_COLUMNS):
        problems.append(f"Iceberg schema has {len(fields)} fields, expected {len(TABLE_COLUMNS)}")
    for position, (name, itype) in enumerate(TABLE_COLUMNS):
        expected = {"id": position + 1, "name": name, "type": itype, "required": True}
        if position >= len(fields):
            problems.append(f"Iceberg field {expected} is missing")
            continue
        field = fields[position]
        actual = {k: field.get(k) for k in expected}
        if actual != expected:
            problems.append(f"Iceberg field #{position + 1}: got {actual}, expected {expected}")
    return problems


def table_properties_mismatches(format_version: int, unpartitioned: bool) -> List[str]:
    problems: List[str] = []
    if format_version != TABLE_FORMAT_VERSION:
        problems.append(f"format version {format_version}, expected {TABLE_FORMAT_VERSION}")
    if not unpartitioned:
        problems.append("table is partitioned, expected an unpartitioned table")
    return problems


# ---------------------------------------------------------------------------------------------
# Validation and trip statistics SQL (statistics also duplicated in the analytics job)
# ---------------------------------------------------------------------------------------------


def null_count_sql(relation: str, columns: Iterable[str]) -> str:
    """One row with the number of NULL values of every column."""
    exprs = ",\n  ".join(f"count_if({quote(c)} IS NULL) AS {quote(c)}" for c in columns)
    return f"SELECT\n  {exprs}\nFROM {relation}"


def round2_sql(expr: str) -> str:
    """Rust `(x * 100.0).round() / 100.0` in Spark SQL.

    Spark `round(double)` rounds half up (away from zero) on the shortest decimal
    representation of the double, which gives the same result as Rust's `f64::round` on the
    product (an exact .5 tie can only be shown as `N.5` when the double is exactly N.5).
    `round(x, 2)` is NOT used: it rounds the decimal form of x, not the f64 product x * 100.
    """
    return f"(round(({expr}) * 100D) / 100D)"


def trip_stats_sql(relation: str) -> str:
    """Scalar trip statistics with the definitions of Rust `compute_trip_stats` (stats.rs).

    Sums, averages and the tip rate are computed in double precision from the raw values and
    rounded to 2 decimals at the end; min/max distance are not rounded.
    """
    return f"""SELECT
  total_trips,
  total_passengers,
  {round2_sql('total_distance')} AS total_distance_miles,
  {round2_sql('total_fare')} AS total_fare_usd,
  {round2_sql('total_tips')} AS total_tips_usd,
  {round2_sql('CASE WHEN total_trips > 0 THEN total_distance / total_trips END')} AS avg_distance_miles,
  {round2_sql('CASE WHEN total_trips > 0 THEN total_fare / total_trips END')} AS avg_fare_usd,
  {round2_sql('CASE WHEN total_trips > 0 THEN total_tips / total_trips END')} AS avg_tip_usd,
  {round2_sql('CASE WHEN total_fare > 0 THEN total_tips / total_fare * 100D ELSE 0D END')} AS tip_rate_percent,
  max_distance AS max_distance_miles,
  min_distance AS min_distance_miles
FROM (
  SELECT
    count(*) AS total_trips,
    sum(passenger_count) AS total_passengers,
    sum(CAST(trip_distance AS DOUBLE)) AS total_distance,
    sum(CAST(fare_amount AS DOUBLE)) AS total_fare,
    sum(CAST(tip_amount AS DOUBLE)) AS total_tips,
    max(trip_distance) AS max_distance,
    min(trip_distance) AS min_distance
  FROM {relation}
)"""


def breakdown_sql(relation: str, column: str) -> str:
    """Trip count per value of a column (vendor_id / payment_type breakdowns)."""
    return (
        f"SELECT {quote(column)} AS key, count(*) AS trips\n"
        f"FROM {relation}\nGROUP BY {quote(column)}\nORDER BY key"
    )


STATS_SCALAR_KEYS: Tuple[str, ...] = (
    "total_trips",
    "total_passengers",
    "total_distance_miles",
    "total_fare_usd",
    "total_tips_usd",
    "avg_distance_miles",
    "avg_fare_usd",
    "avg_tip_usd",
    "tip_rate_percent",
    "max_distance_miles",
    "min_distance_miles",
)


def assemble_trip_stats(
    totals: Mapping[str, Any],
    vendor_rows: Iterable[Tuple[str, int]],
    payment_rows: Iterable[Tuple[str, int]],
) -> Dict[str, Any]:
    """The Rust JSON shape: scalar keys plus the two breakdowns; `{}` for an empty input."""
    if not totals or not totals.get("total_trips"):
        return {}
    stats: Dict[str, Any] = {}
    for key in STATS_SCALAR_KEYS:
        value = totals.get(key)
        if key in ("total_trips", "total_passengers"):
            stats[key] = int(value)
        else:
            stats[key] = float(value)
    stats["vendor_breakdown"] = {str(k): int(n) for k, n in vendor_rows}
    stats["payment_type_breakdown"] = {str(k): int(n) for k, n in payment_rows}
    return stats


def compute_trip_stats(spark: Any, relation: str) -> Dict[str, Any]:
    """Run the trip statistics SQL over a table, view or `<table> VERSION AS OF <id>`."""
    totals = spark.sql(trip_stats_sql(relation)).first().asDict()
    if not totals.get("total_trips"):
        return {}
    vendors = [(r["key"], r["trips"]) for r in spark.sql(breakdown_sql(relation, "vendor_id")).collect()]
    payments = [(r["key"], r["trips"]) for r in spark.sql(breakdown_sql(relation, "payment_type")).collect()]
    return assemble_trip_stats(totals, vendors, payments)


def format_stats(stats: Mapping[str, Any]) -> str:
    """Sorted keys, like serde_json's default map, so the output diffs cleanly with the Rust CLI."""
    return json.dumps(stats, indent=2, sort_keys=True)


# ---------------------------------------------------------------------------------------------
# Spark side
# ---------------------------------------------------------------------------------------------


def log(message: str = "") -> None:
    print(f"{LOG_PREFIX} {message}" if message else "", flush=True)


def section(title: str) -> None:
    log()
    log("=" * 78)
    log(title)
    log("=" * 78)


def run_sql(spark: Any, sql: str) -> Any:
    for line in sql.splitlines():
        log(f"  SQL> {line}")
    return spark.sql(sql)


def show(df: Any, rows: int = 20) -> None:
    # DataFrame.show prints to stdout; flush around it to keep the log ordered.
    sys.stdout.flush()
    df.show(rows, truncate=False)
    sys.stdout.flush()


def load_iceberg_table(spark: Any, name: str) -> Any:
    """The Java org.apache.iceberg.Table behind a Spark identifier (catalog.db.table)."""
    return spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)


def verify_shared_table(spark: Any, ident: str, name: str, expected_location: str) -> Dict[str, Any]:
    """Fail with ValidationError unless the table matches the contract exactly."""
    problems = spark_schema_mismatches(spark.table(ident).schema)
    jtable = load_iceberg_table(spark, name)
    schema_json = json.loads(spark._jvm.org.apache.iceberg.SchemaParser.toJson(jtable.schema()))
    problems += iceberg_schema_mismatches(schema_json)
    format_version = int(jtable.operations().current().formatVersion())
    unpartitioned = bool(jtable.spec().isUnpartitioned())
    problems += table_properties_mismatches(format_version, unpartitioned)
    location = str(jtable.location())
    if problems:
        raise ValidationError(
            f"table {name} does not match the shared table contract:\n  - " + "\n  - ".join(problems)
        )
    if location.rstrip("/") != expected_location:
        # Not fatal: another tool may have created the table with an equivalent URI (s3a://).
        log(f"WARNING: table location is {location}, expected {expected_location}")
    log(
        f"Schema OK: {len(TABLE_COLUMNS)} required columns, field ids 1..{len(TABLE_COLUMNS)}, "
        f"format-version {format_version}, unpartitioned, location {location}"
    )
    return {"format_version": format_version, "location": location, "schema_id": schema_json.get("schema-id")}


def find_job_snapshot(spark: Any, ident: str, app_id: str) -> Optional[Dict[str, Any]]:
    """Latest snapshot committed by this Spark application (Iceberg records spark.app.id)."""
    rows = spark.sql(
        f"SELECT snapshot_id, parent_id, committed_at, operation, summary "
        f"FROM {metadata_table(ident, 'snapshots')} "
        f"WHERE summary['spark.app.id'] = {sql_string(app_id)} "
        f"ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return rows[0].asDict() if rows else None


def run(spark: Any, args: argparse.Namespace) -> int:
    from pyspark.sql import functions as F

    name = f"{args.catalog}.{args.database}.{args.table}"
    ident = table_ident(args.catalog, args.database, args.table)
    ns_location = namespace_location(args.warehouse, args.database)
    tbl_location = table_location(args.warehouse, args.database, args.table)
    app_id = spark.sparkContext.applicationId

    section("Configuration")
    catalog_conf = f"spark.sql.catalog.{args.catalog}"
    catalog_class = spark.conf.get(catalog_conf, None)
    if not catalog_class:
        raise ValidationError(f"Spark catalog '{args.catalog}' is not configured ({catalog_conf} is not set)")
    catalog_type = spark.conf.get(f"{catalog_conf}.type", None)
    catalog_impl = spark.conf.get(f"{catalog_conf}.catalog-impl", None)
    log(f"Spark {spark.version}, application id {app_id}")
    log(f"catalog {args.catalog}: {catalog_class} type={catalog_type} catalog-impl={catalog_impl}")
    log(f"catalog warehouse (Spark conf): {spark.conf.get(f'{catalog_conf}.warehouse', None)}")
    log(f"table {name}, mode {args.mode}, input {args.input}")
    log(f"namespace location {ns_location}, table location {tbl_location}")

    section("1. Namespace (Glue database)")
    # Hadoop catalogs reject namespace properties; the Glue catalog stores them on the database.
    supports_ns_metadata = (catalog_type or "").lower() != "hadoop"
    if not supports_ns_metadata:
        log("Hadoop catalog: the namespace location is implied by the warehouse (no LOCATION/COMMENT)")
    run_sql(
        spark,
        create_namespace_sql(
            args.catalog,
            args.database,
            ns_location if supports_ns_metadata else None,
            "NYC Taxi Iceberg tables (iceberg-on-eks)" if supports_ns_metadata else None,
        ),
    )
    show(spark.sql(f"DESCRIBE NAMESPACE EXTENDED {quote(args.catalog)}.{quote(args.database)}"))

    section("2. Shared Iceberg table")
    existed = bool(spark.catalog.tableExists(name))
    if existed:
        log(f"Table {name} already exists: verifying its schema against the contract")
    else:
        log(f"Table {name} does not exist: creating it")
        run_sql(spark, create_table_sql(ident, tbl_location))
    table_info = verify_shared_table(spark, ident, name, tbl_location)

    section("3. Read the payload")
    raw = (
        spark.read.schema(payload_read_schema())
        .option("multiLine", "true")
        .option("mode", "FAILFAST")
        .json(args.input)
    )
    rows = raw.select(F.explode("rows").alias("r")).selectExpr(*payload_select_exprs("r")).cache()
    payload_count = rows.count()
    log(f"Payload rows: {payload_count}")
    if payload_count == 0:
        raise ValidationError(f"the payload {args.input} has no rows")
    rows.createOrReplaceTempView("payload_rows")
    payload_nulls = spark.sql(null_count_sql("payload_rows", [c for c, _ in TABLE_COLUMNS])).first().asDict()
    bad = {c: n for c, n in payload_nulls.items() if n}
    if bad:
        raise ValidationError(f"the payload has NULL or missing values (all columns are required): {bad}")
    log("Payload null check OK: no NULL values in the 23 columns")

    section(f"4. Write ({args.mode})")
    count_before = spark.table(ident).count()
    log(f"Rows in the table before the write: {count_before}")
    writer = (
        rows.writeTo(ident)
        .option("snapshot-property.lab.job", APP_NAME)
        .option("snapshot-property.lab.mode", args.mode)
        .option("snapshot-property.lab.input", args.input)
    )
    if args.mode == "append":
        log(f"DataFrameWriterV2: writeTo({name}).append()")
        writer.append()
    else:
        log(f"DataFrameWriterV2: writeTo({name}).overwrite(lit(True))  -- replaces every row")
        writer.overwrite(F.lit(True))
    count_after = spark.table(ident).count()
    log(f"Rows in the table after the write: {count_after}")

    section("5. Validate")
    snapshot = find_job_snapshot(spark, ident, app_id)
    if snapshot is None:
        raise ValidationError(f"no snapshot with summary spark.app.id={app_id} found after the write")
    summary = snapshot["summary"] or {}
    rows_written = int(summary.get("added-records", "0"))
    log(
        f"New snapshot {snapshot['snapshot_id']} (parent {snapshot['parent_id']}), "
        f"operation {snapshot['operation']}, committed at {snapshot['committed_at']}"
    )
    for key in ("added-records", "deleted-records", "added-data-files", "deleted-data-files",
                "total-records", "total-data-files", "added-files-size"):
        if key in summary:
            log(f"  summary {key} = {summary[key]}")
    log(f"Rows written: {rows_written} (payload rows: {payload_count})")
    if rows_written != payload_count:
        raise ValidationError(f"the snapshot added {rows_written} rows, expected {payload_count}")
    expected_after = count_before + payload_count if args.mode == "append" else payload_count
    if count_after != expected_after:
        log(
            f"WARNING: {count_after} rows after the write, expected {expected_after}; "
            "another writer probably committed at the same time"
        )

    snapshot_relation = f"{ident} VERSION AS OF {snapshot['snapshot_id']}"
    table_nulls = spark.sql(null_count_sql(snapshot_relation, [c for c, _ in TABLE_COLUMNS])).first().asDict()
    bad = {c: n for c, n in table_nulls.items() if n}
    if bad:
        raise ValidationError(f"NULL values found in the table: {bad}")
    log("Table null check OK: 0 NULL values in all 23 columns")

    log("Sample rows (5) from the new snapshot:")
    show(
        spark.sql(
            f"SELECT medallion, vendor_id, pickup_datetime, dropoff_datetime, passenger_count, "
            f"trip_distance, payment_type, fare_amount, tip_amount, total_amount, tip_class "
            f"FROM {snapshot_relation} LIMIT 5"
        ),
        5,
    )

    section("6. Trip statistics (definitions of the Rust compute_trip_stats)")
    written_stats = compute_trip_stats(spark, "payload_rows")
    log("Rows written by this run (what the Rust write_and_annotate action annotates):")
    for line in format_stats(written_stats).splitlines():
        log(f"  {line}")
    table_stats = compute_trip_stats(spark, ident)
    log(f"Whole table {name}:")
    for line in format_stats(table_stats).splitlines():
        log(f"  {line}")

    rows.unpersist()
    result = {
        "table": name,
        "mode": args.mode,
        "created_table": not existed,
        "rows_before": count_before,
        "rows_after": count_after,
        "rows_written": rows_written,
        "snapshot_id": snapshot["snapshot_id"],
        "format_version": table_info["format_version"],
        "location": table_info["location"],
        "trip_stats_written": written_stats,
        "trip_stats_table": table_stats,
    }
    section("Result")
    log("RESULT " + json.dumps(result, sort_keys=True))
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        return run(spark, args)
    except ValidationError as exc:
        log(f"ERROR: {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
