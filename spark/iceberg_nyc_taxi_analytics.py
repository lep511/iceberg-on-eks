#!/usr/bin/env python3
"""Iceberg features on the NYC Taxi tables with Spark SQL.

Read-only on the structure of the shared table (`nyc_taxi_table`, also written by the Rust CLI):

1. Trip statistics with the same definitions as the reference Rust `compute_trip_stats`
   (apache-iceberg-s3-metadata/lambda/src/stats.rs), so the numbers can be compared.
2. Metadata tables: snapshots, history, files, manifests, refs.
3. Time travel: `VERSION AS OF` (first snapshot vs current) and `TIMESTAMP AS OF`.
4. Tags on the shared table (a ref only: schema, partition spec and format stay unchanged).

Everything that changes a table's structure runs on a separate derived table
(`--curated-table`, default `nyc_taxi_trips_curated`) with real timestamp columns, partitioned by
`days(pickup_ts)`, format version 2, explicit LOCATION, merge-on-read row-level operations:

5. MERGE INTO (sync from the shared table), schema evolution (ADD COLUMN), UPDATE, DELETE.
6. Maintenance procedures: rewrite_data_files, rewrite_position_delete_files, expire_snapshots
   (retain_last), and remove_orphan_files with dry_run => true (reports only, deletes nothing).

The job is idempotent: each run converges to the same curated table contents.

The Iceberg catalog is configured only through Spark configuration (`spark.sql.catalog.<name>`);
the script receives just the catalog name. It is self-contained on purpose (see the load job's
docstring); `tests/test_consistency.py` checks the definitions duplicated from
`iceberg_nyc_taxi_load.py`.

Usage:
    spark-submit iceberg_nyc_taxi_analytics.py --catalog glue --database nyc_taxi_database \
        --table nyc_taxi_table --warehouse s3://<bucket>/warehouse \
        [--curated-table nyc_taxi_trips_curated] [--retain-last 5]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

APP_NAME = "iceberg-nyc-taxi-analytics"
LOG_PREFIX = "[iceberg-analytics]"

# ---------------------------------------------------------------------------------------------
# Shared table contract (duplicated from iceberg_nyc_taxi_load.py; keep both identical).
# ---------------------------------------------------------------------------------------------

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

ICEBERG_TO_SPARK_SQL: Dict[str, str] = {
    "string": "STRING",
    "int": "INT",
    "long": "BIGINT",
    "double": "DOUBLE",
}

GLUE_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
CATALOG_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:/")

# ---------------------------------------------------------------------------------------------
# Curated (derived) table: the only table whose structure this job changes.
# ---------------------------------------------------------------------------------------------

DEFAULT_CURATED_TABLE = "nyc_taxi_trips_curated"
SOURCE_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
# The string datetime columns of the shared table and their timestamp columns in the curated one.
# TIMESTAMP_NTZ maps to the Iceberg `timestamp` type (no time zone): the source values are local
# New York wall-clock times without an offset.
TIMESTAMP_COLUMNS: Dict[str, str] = {
    "pickup_datetime": "pickup_ts",
    "dropoff_datetime": "dropoff_ts",
}
# A taxi cannot start two trips with the same driver at the same second.
NATURAL_KEY: Tuple[str, ...] = ("medallion", "hack_license", "pickup_datetime")
KEY_COLUMN = "trip_id"
PARTITION_SOURCE_COLUMN = "pickup_ts"
CURATED_PROPERTIES: Tuple[Tuple[str, str], ...] = (
    ("format-version", "2"),
    # Row-level operations write position delete files instead of rewriting data files.
    ("write.delete.mode", "merge-on-read"),
    ("write.update.mode", "merge-on-read"),
    ("write.merge.mode", "merge-on-read"),
)
# Schema evolution: a column added after the table was created, then backfilled with UPDATE.
EVOLVED_COLUMN = "fare_per_mile"
EVOLVED_COLUMN_TYPE = "DOUBLE"
EVOLVED_COLUMN_COMMENT = "fare_amount / trip_distance in USD per mile (added by schema evolution)"
EVOLVED_COLUMN_EXPR = "round(try_divide(fare_amount, trip_distance), 2)"
# Row-level DELETE: a data quality rule (meter or GPS glitches).
DELETE_RULE = "trip_distance < 0.2"
DELETE_RULE_DESCRIPTION = "meter or GPS glitches: trips shorter than 0.2 miles"
SOURCE_VIEW = "curated_source"


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


def positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid integer {value!r}") from None
    if number < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {number}")
    return number


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="iceberg_nyc_taxi_analytics.py",
        description="Iceberg analytics, time travel, row-level operations and maintenance.",
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
        help="shared Iceberg table, read only (default: nyc_taxi_table)",
    )
    parser.add_argument(
        "--warehouse",
        type=location_uri,
        required=True,
        help="Iceberg warehouse root, e.g. s3://<bucket>/warehouse",
    )
    parser.add_argument(
        "--curated-table",
        type=glue_name,
        default=DEFAULT_CURATED_TABLE,
        help=f"derived table for row-level, evolution and maintenance demos (default: {DEFAULT_CURATED_TABLE})",
    )
    parser.add_argument(
        "--retain-last",
        type=positive_int,
        default=5,
        help="expire_snapshots keeps at least this many snapshots of the curated table (default: 5)",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    args = build_parser().parse_args(argv)
    if args.curated_table == args.table:
        build_parser().error("--curated-table must differ from --table (the shared table is never altered)")
    return args


# ---------------------------------------------------------------------------------------------
# Names and locations (duplicated from the load job)
# ---------------------------------------------------------------------------------------------


def quote(name: str) -> str:
    """Backtick-quote one identifier part (names are validated, so no escaping is needed)."""
    return f"`{name}`"


def table_ident(catalog: str, database: str, table: str) -> str:
    return ".".join(quote(part) for part in (catalog, database, table))


def metadata_table(ident: str, name: str) -> str:
    """Iceberg metadata table of a table identifier, e.g. `c`.`db`.`t`.`snapshots`."""
    return f"{ident}.{quote(name)}"


def table_location(warehouse: str, database: str, table: str) -> str:
    return f"{warehouse}/{database}/{table}"


def sql_string(value: str) -> str:
    """A single-quoted SQL string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


# ---------------------------------------------------------------------------------------------
# Trip statistics SQL (duplicated from the load job; keep both identical)
# ---------------------------------------------------------------------------------------------


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
# Curated table SQL builders (pure functions)
# ---------------------------------------------------------------------------------------------


def curated_base_columns() -> List[Tuple[str, str]]:
    """(name, Spark SQL type) of the curated table before schema evolution, in order."""
    columns = [(KEY_COLUMN, "STRING")]
    for name, itype in TABLE_COLUMNS:
        if name in TIMESTAMP_COLUMNS:
            columns.append((TIMESTAMP_COLUMNS[name], "TIMESTAMP_NTZ"))
        else:
            columns.append((name, ICEBERG_TO_SPARK_SQL[itype]))
    return columns


def curated_column_names() -> List[str]:
    return [name for name, _ in curated_base_columns()]


def create_curated_table_sql(ident: str, location: str) -> str:
    columns = ",\n".join(f"  {quote(name)} {sql_type} NOT NULL" for name, sql_type in curated_base_columns())
    properties = ", ".join(f"{sql_string(k)}={sql_string(v)}" for k, v in CURATED_PROPERTIES)
    return (
        f"CREATE TABLE IF NOT EXISTS {ident} (\n{columns}\n)\n"
        "USING iceberg\n"
        f"PARTITIONED BY (days({quote(PARTITION_SOURCE_COLUMN)}))\n"
        f"LOCATION {sql_string(location)}\n"
        f"TBLPROPERTIES ({properties})"
    )


def trip_id_expr() -> str:
    """Deterministic surrogate key from the natural key (same trip -> same id in every run)."""
    parts = ", ".join(quote(c) for c in NATURAL_KEY)
    return f"sha2(concat_ws('|', {parts}), 256)"


def parse_timestamp_expr(column: str) -> str:
    """String -> TIMESTAMP_NTZ, NULL when the string does not match the source format.

    try_to_timestamp parses in the session time zone, and the cast to TIMESTAMP_NTZ uses the
    same zone, so the wall-clock value is kept (the job sets the session time zone to UTC).
    """
    return f"CAST(try_to_timestamp({quote(column)}, {sql_string(SOURCE_TIMESTAMP_FORMAT)}) AS TIMESTAMP_NTZ)"


def curated_source_sql(shared_ident: str) -> str:
    """Typed, de-duplicated rows of the shared table (the load job may append the same rows twice).

    Duplicates of a natural key are reduced to one row; the window is ordered by every column, so
    the surviving row is deterministic. Rows whose datetimes cannot be parsed are dropped.
    """
    select_list = []
    for name, _ in TABLE_COLUMNS:
        if name in TIMESTAMP_COLUMNS:
            select_list.append(f"{parse_timestamp_expr(name)} AS {quote(TIMESTAMP_COLUMNS[name])}")
        else:
            select_list.append(quote(name))
    partition_by = ", ".join(quote(c) for c in NATURAL_KEY)
    order_by = ", ".join(quote(name) for name, _ in TABLE_COLUMNS if name not in NATURAL_KEY)
    projection = ",\n    ".join([f"{trip_id_expr()} AS {quote(KEY_COLUMN)}"] + select_list)
    return (
        "SELECT * FROM (\n"
        f"  SELECT\n    {projection}\n"
        "  FROM (\n"
        f"    SELECT *, row_number() OVER (PARTITION BY {partition_by} ORDER BY {order_by}) AS rn\n"
        f"    FROM {shared_ident}\n"
        "  )\n"
        "  WHERE rn = 1\n"
        ")\n"
        + "WHERE "
        + " AND ".join(f"{quote(c)} IS NOT NULL" for c in TIMESTAMP_COLUMNS.values())
    )


def merge_sql(curated_ident: str, source: str, reset_columns: Sequence[str] = ()) -> str:
    """MERGE INTO that makes the curated table equal to the source (insert, update, delete).

    `reset_columns` (derived columns such as fare_per_mile) are set to NULL when a row changes,
    so the UPDATE backfill recomputes them.
    """
    data_columns = [c for c in curated_column_names() if c != KEY_COLUMN]
    changed = " AND ".join(f"t.{quote(c)} <=> s.{quote(c)}" for c in data_columns)
    assignments = [f"t.{quote(c)} = s.{quote(c)}" for c in data_columns]
    assignments += [f"t.{quote(c)} = NULL" for c in reset_columns]
    all_columns = curated_column_names()
    insert_columns = ", ".join(quote(c) for c in all_columns)
    insert_values = ", ".join(f"s.{quote(c)}" for c in all_columns)
    return (
        f"MERGE INTO {curated_ident} t\n"
        f"USING {source} s\n"
        f"ON t.{quote(KEY_COLUMN)} = s.{quote(KEY_COLUMN)}\n"
        f"WHEN MATCHED AND NOT ({changed}) THEN\n"
        f"  UPDATE SET {', '.join(assignments)}\n"
        "WHEN NOT MATCHED THEN\n"
        f"  INSERT ({insert_columns}) VALUES ({insert_values})\n"
        "WHEN NOT MATCHED BY SOURCE THEN\n"
        "  DELETE"
    )


def add_column_sql(ident: str) -> str:
    return (
        f"ALTER TABLE {ident} ADD COLUMN {quote(EVOLVED_COLUMN)} {EVOLVED_COLUMN_TYPE} "
        f"COMMENT {sql_string(EVOLVED_COLUMN_COMMENT)}"
    )


def backfill_predicate() -> str:
    return f"{quote(EVOLVED_COLUMN)} IS NULL AND trip_distance > 0"


def backfill_sql(ident: str) -> str:
    return f"UPDATE {ident}\nSET {quote(EVOLVED_COLUMN)} = {EVOLVED_COLUMN_EXPR}\nWHERE {backfill_predicate()}"


def delete_sql(ident: str) -> str:
    return f"DELETE FROM {ident}\nWHERE {DELETE_RULE}"


def procedure_table_arg(database: str, table: str) -> str:
    """The `table => '<db>.<table>'` argument of an Iceberg procedure (resolved in its catalog)."""
    return sql_string(f"{database}.{table}")


def rewrite_data_files_sql(catalog: str, database: str, table: str) -> str:
    """Compact small files and apply position deletes (files with >= 1 delete are rewritten)."""
    return (
        f"CALL {quote(catalog)}.system.rewrite_data_files(\n"
        f"  table => {procedure_table_arg(database, table)},\n"
        "  options => map('min-input-files', '2', 'delete-file-threshold', '1', "
        "'remove-dangling-deletes', 'true')\n"
        ")"
    )


def expire_snapshots_sql(catalog: str, database: str, table: str, older_than: str, retain_last: int) -> str:
    return (
        f"CALL {quote(catalog)}.system.expire_snapshots(\n"
        f"  table => {procedure_table_arg(database, table)},\n"
        f"  older_than => TIMESTAMP {sql_string(older_than)},\n"
        f"  retain_last => {int(retain_last)}\n"
        ")"
    )


def rewrite_position_delete_files_sql(catalog: str, database: str, table: str) -> str:
    """Rewrite position delete files, dropping records that point at data files already removed.

    rewrite_data_files gives the compacted files the starting sequence number, so the delete files
    of the latest DELETE are not "dangling" by sequence number even though every record they hold
    references a removed file; this procedure removes them.
    """
    return (
        f"CALL {quote(catalog)}.system.rewrite_position_delete_files(\n"
        f"  table => {procedure_table_arg(database, table)},\n"
        "  options => map('rewrite-all', 'true')\n"
        ")"
    )


def remove_orphan_files_sql(catalog: str, database: str, table: str) -> str:
    """Dry run only: lists the files that would be deleted, deletes nothing.

    older_than is left at the procedure default (3 days ago); Iceberg rejects intervals shorter
    than 24 hours because files of in-flight commits would look orphaned.
    """
    return (
        f"CALL {quote(catalog)}.system.remove_orphan_files(\n"
        f"  table => {procedure_table_arg(database, table)},\n"
        "  dry_run => true\n"
        ")"
    )


def create_tag_sql(ident: str, tag: str, snapshot_id: int, retain_days: int, replace: bool) -> str:
    verb = "CREATE OR REPLACE TAG" if replace else "CREATE TAG IF NOT EXISTS"
    return f"ALTER TABLE {ident} {verb} {quote(tag)} AS OF VERSION {int(snapshot_id)} RETAIN {int(retain_days)} DAYS"


def curated_schema_mismatches(actual_columns: Mapping[str, str]) -> List[str]:
    """Base columns missing from an existing curated table, or with another type.

    `actual_columns` maps column name -> Spark simpleString type (e.g. timestamp_ntz, bigint).
    """
    expected_types = {
        "STRING": "string",
        "INT": "int",
        "BIGINT": "bigint",
        "DOUBLE": "double",
        "TIMESTAMP_NTZ": "timestamp_ntz",
    }
    problems = []
    for name, sql_type in curated_base_columns():
        if name not in actual_columns:
            problems.append(f"column {name} is missing")
        elif actual_columns[name] != expected_types[sql_type]:
            problems.append(f"column {name}: type {actual_columns[name]}, expected {expected_types[sql_type]}")
    return problems


def curated_spec_mismatches(spec_json: Mapping[str, Any], source_field_id: Optional[int]) -> List[str]:
    """The curated table must be partitioned by exactly days(pickup_ts) (PartitionSpecParser JSON)."""
    fields = list(spec_json.get("fields", []))
    if (
        len(fields) == 1
        and fields[0].get("transform") == "day"
        and source_field_id is not None
        and fields[0].get("source-id") == source_field_id
    ):
        return []
    return [f"partition spec is {fields}, expected days({PARTITION_SOURCE_COLUMN})"]


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
    sys.stdout.flush()
    df.show(rows, truncate=False)
    sys.stdout.flush()


def load_iceberg_table(spark: Any, name: str) -> Any:
    """The Java org.apache.iceberg.Table behind a Spark identifier (catalog.db.table)."""
    return spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)


def scalar(spark: Any, sql: str) -> Any:
    return spark.sql(sql).first()[0]


def latest_snapshot(spark: Any, ident: str) -> Optional[Dict[str, Any]]:
    rows = spark.sql(
        f"SELECT snapshot_id, operation, summary FROM {metadata_table(ident, 'snapshots')} "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return rows[0].asDict() if rows else None


SUMMARY_KEYS = (
    "added-records",
    "deleted-records",
    "added-data-files",
    "deleted-data-files",
    "added-delete-files",
    "removed-delete-files",
    "added-position-deletes",
    "removed-position-deletes",
    "total-records",
    "total-position-deletes",
    "total-data-files",
    "total-delete-files",
)


def report_snapshot(spark: Any, ident: str, previous_id: Optional[int]) -> Optional[int]:
    """Print the latest snapshot's operation and summary; return its id."""
    snap = latest_snapshot(spark, ident)
    if snap is None or snap["snapshot_id"] == previous_id:
        log("No new snapshot (nothing changed)")
        return previous_id
    summary = snap["summary"] or {}
    details = ", ".join(f"{k}={summary[k]}" for k in SUMMARY_KEYS if k in summary)
    log(f"Snapshot {snap['snapshot_id']} ({snap['operation']}): {details}")
    return snap["snapshot_id"]


def file_summary(spark: Any, ident: str) -> Dict[str, Dict[str, int]]:
    """Live data and delete files of a table (content 0 = data, 1 = position deletes)."""
    rows = spark.sql(
        f"SELECT content, count(*) AS files, sum(record_count) AS records, sum(file_size_in_bytes) AS bytes "
        f"FROM {metadata_table(ident, 'files')} GROUP BY content ORDER BY content"
    ).collect()
    names = {0: "data", 1: "position_deletes", 2: "equality_deletes"}
    return {
        names.get(r["content"], str(r["content"])): {"files": r["files"], "records": r["records"], "bytes": r["bytes"]}
        for r in rows
    }


def print_stats(title: str, stats: Mapping[str, Any]) -> None:
    log(title)
    for line in format_stats(stats).splitlines():
        log(f"  {line}")


def shared_table_section(spark: Any, args: argparse.Namespace, shared: str, shared_name: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {}

    section(f"1. Trip statistics of {shared_name} (Rust compute_trip_stats definitions)")
    stats = compute_trip_stats(spark, shared)
    print_stats("Current snapshot:", stats)
    result["trip_stats"] = stats

    section("2. Metadata tables of the shared table")
    log("snapshots (latest 10):")
    show(
        spark.sql(
            "SELECT committed_at, snapshot_id, parent_id, operation, summary['added-records'] AS added_records, "
            "summary['total-records'] AS total_records, summary['spark.app.id'] AS spark_app_id "
            f"FROM {metadata_table(shared, 'snapshots')} ORDER BY committed_at DESC LIMIT 10"
        )
    )
    log("history (latest 10):")
    show(
        spark.sql(
            "SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor "
            f"FROM {metadata_table(shared, 'history')} ORDER BY made_current_at DESC LIMIT 10"
        )
    )
    log("files (live data files, grouped by content; 5 sample paths):")
    show(
        spark.sql(
            "SELECT content, file_format, count(*) AS files, sum(record_count) AS records, "
            f"sum(file_size_in_bytes) AS bytes FROM {metadata_table(shared, 'files')} GROUP BY content, file_format"
        )
    )
    show(
        spark.sql(
            f"SELECT file_path, record_count, file_size_in_bytes FROM {metadata_table(shared, 'files')} "
            "ORDER BY file_path LIMIT 5"
        ),
        5,
    )
    log("manifests:")
    show(
        spark.sql(
            "SELECT added_snapshot_id, added_data_files_count, existing_data_files_count, "
            f"deleted_data_files_count, length, path FROM {metadata_table(shared, 'manifests')}"
        )
    )

    section("3. Time travel on the shared table")
    snapshots = spark.sql(
        "SELECT snapshot_id, date_format(committed_at, 'yyyy-MM-dd HH:mm:ss.SSS') AS committed_at "
        f"FROM {metadata_table(shared, 'snapshots')} ORDER BY committed_at, snapshot_id"
    ).collect()
    current = spark.sql(f"SELECT snapshot_id FROM {metadata_table(shared, 'refs')} WHERE name = 'main'").collect()
    if not snapshots or not current:
        log("The table has no snapshots yet: skipping time travel and tags")
        return result
    first_id, first_ts = snapshots[0]["snapshot_id"], snapshots[0]["committed_at"]
    current_id = current[0]["snapshot_id"]
    log(f"{len(snapshots)} snapshots; first {first_id} at {first_ts} (UTC); current {current_id}")
    count_first = scalar(spark, f"SELECT count(*) FROM {shared} VERSION AS OF {first_id}")
    count_ts = scalar(spark, f"SELECT count(*) FROM {shared} TIMESTAMP AS OF {sql_string(first_ts)}")
    count_now = scalar(spark, f"SELECT count(*) FROM {shared}")
    log(f"  SQL> SELECT count(*) FROM {shared} VERSION AS OF {first_id}            -> {count_first}")
    log(f"  SQL> SELECT count(*) FROM {shared} TIMESTAMP AS OF '{first_ts}' -> {count_ts}")
    log(f"  SQL> SELECT count(*) FROM {shared}                                   -> {count_now}")
    if count_first != count_ts:
        raise ValidationError(f"VERSION AS OF gave {count_first} rows but TIMESTAMP AS OF gave {count_ts}")
    first_stats = compute_trip_stats(spark, f"{shared} VERSION AS OF {first_id}")
    log(
        f"First snapshot: {first_stats.get('total_trips', 0)} trips, "
        f"tip rate {first_stats.get('tip_rate_percent')}% (stats as of the first snapshot)"
    )
    result["time_travel"] = {
        "snapshots": len(snapshots),
        "first_snapshot_id": first_id,
        "first_snapshot_rows": count_first,
        "timestamp_as_of_rows": count_ts,
        "current_snapshot_id": current_id,
        "current_rows": count_now,
    }

    section("4. Tags on the shared table (refs only; schema and spec unchanged)")
    run_sql(spark, create_tag_sql(shared, "first_snapshot", first_id, retain_days=30, replace=False))
    run_sql(spark, create_tag_sql(shared, "analytics_latest", current_id, retain_days=7, replace=True))
    count_tag = scalar(spark, f"SELECT count(*) FROM {shared} VERSION AS OF 'analytics_latest'")
    log(f"  SQL> SELECT count(*) FROM {shared} VERSION AS OF 'analytics_latest' -> {count_tag}")
    log("refs:")
    show(
        spark.sql(
            "SELECT name, type, snapshot_id, max_reference_age_in_ms, min_snapshots_to_keep, max_snapshot_age_in_ms "
            f"FROM {metadata_table(shared, 'refs')} ORDER BY name"
        )
    )
    result["tags"] = {"first_snapshot": first_id, "analytics_latest": current_id}
    return result


def verify_curated_table(spark: Any, ident: str, name: str) -> None:
    columns = {f.name: f.dataType.simpleString() for f in spark.table(ident).schema.fields}
    problems = curated_schema_mismatches(columns)
    jtable = load_iceberg_table(spark, name)
    spec = json.loads(spark._jvm.org.apache.iceberg.PartitionSpecParser.toJson(jtable.spec()))
    field = jtable.schema().findField(PARTITION_SOURCE_COLUMN)
    problems += curated_spec_mismatches(spec, int(field.fieldId()) if field is not None else None)
    if problems:
        raise ValidationError(
            f"the existing table {name} is not the curated table this job manages:\n  - "
            + "\n  - ".join(problems)
            + f"\nDrop it first (DROP TABLE {name} PURGE) or pick another --curated-table."
        )


def curated_table_section(spark: Any, args: argparse.Namespace, shared: str) -> Dict[str, Any]:
    name = f"{args.catalog}.{args.database}.{args.curated_table}"
    curated = table_ident(args.catalog, args.database, args.curated_table)
    location = table_location(args.warehouse, args.database, args.curated_table)
    result: Dict[str, Any] = {"table": name}

    section(f"5. Curated table {name}")
    run_sql(spark, create_curated_table_sql(curated, location))
    verify_curated_table(spark, curated, name)
    log(f"Location {load_iceberg_table(spark, name).location()}, partitioned by days({PARTITION_SOURCE_COLUMN})")
    last_id = report_snapshot(spark, curated, None) if latest_snapshot(spark, curated) else None

    # Pin the source to one snapshot of the shared table: the view is evaluated several times
    # (MERGE, final checks) and the Rust CLI may append rows meanwhile.
    main_ref = spark.sql(f"SELECT snapshot_id FROM {metadata_table(shared, 'refs')} WHERE name = 'main'").collect()
    source_snapshot = main_ref[0]["snapshot_id"] if main_ref else None
    shared_relation = f"{shared} VERSION AS OF {source_snapshot}" if source_snapshot is not None else shared
    source = spark.sql(curated_source_sql(shared_relation))
    source.createOrReplaceTempView(SOURCE_VIEW)
    shared_rows = scalar(spark, f"SELECT count(*) FROM {shared_relation}")
    source_rows = scalar(spark, f"SELECT count(*) FROM {SOURCE_VIEW}")
    log(
        f"Source: shared table snapshot {source_snapshot}: {shared_rows} rows -> "
        f"{source_rows} distinct trips with valid timestamps"
    )
    result["source_rows"] = source_rows
    result["source_snapshot_id"] = source_snapshot

    section("5a. MERGE INTO (insert new trips, update changed ones, delete trips gone from the source)")
    has_evolved = EVOLVED_COLUMN in spark.table(curated).columns
    before = scalar(spark, f"SELECT count(*) FROM {curated}")
    run_sql(spark, merge_sql(curated, SOURCE_VIEW, [EVOLVED_COLUMN] if has_evolved else []))
    after = scalar(spark, f"SELECT count(*) FROM {curated}")
    log(f"Rows: {before} -> {after}")
    last_id = report_snapshot(spark, curated, last_id)
    result["merge"] = {"rows_before": before, "rows_after": after}

    section(f"5b. Schema evolution: ADD COLUMN {EVOLVED_COLUMN}")
    if has_evolved:
        log(f"Column {EVOLVED_COLUMN} already exists (added by a previous run)")
    else:
        pre_evolution_id = last_id
        run_sql(spark, add_column_sql(curated))
        log("Schema change only: no data files are rewritten; existing rows read the new column as NULL")
        if pre_evolution_id is not None:
            old_columns = spark.sql(f"SELECT * FROM {curated} VERSION AS OF {pre_evolution_id} LIMIT 0").columns
            log(f"Columns as of snapshot {pre_evolution_id} (before): {len(old_columns)}; "
                f"current: {len(spark.table(curated).columns)}")
    show(spark.sql(f"DESCRIBE TABLE {curated}"), 40)
    result["evolved_column_added"] = not has_evolved

    section(f"5c. UPDATE: backfill {EVOLVED_COLUMN}")
    to_backfill = scalar(spark, f"SELECT count(*) FROM {curated} WHERE {backfill_predicate()}")
    log(f"Rows to backfill: {to_backfill}")
    run_sql(spark, backfill_sql(curated))
    last_id = report_snapshot(spark, curated, last_id)
    result["update_rows"] = to_backfill

    section(f"5d. DELETE: {DELETE_RULE_DESCRIPTION}")
    to_delete = scalar(spark, f"SELECT count(*) FROM {curated} WHERE {DELETE_RULE}")
    log(f"Rows matching the rule: {to_delete}")
    run_sql(spark, delete_sql(curated))
    last_id = report_snapshot(spark, curated, last_id)
    result["delete_rows"] = to_delete
    files_before = file_summary(spark, curated)
    log(f"Live files after the row-level operations: {files_before}")

    section("6. Table maintenance on the curated table")
    log("rewrite_data_files: compact small files and apply the position deletes")
    rewrite = run_sql(spark, rewrite_data_files_sql(args.catalog, args.database, args.curated_table)).first().asDict()
    log(f"  -> {rewrite}")
    last_id = report_snapshot(spark, curated, last_id)
    log("rewrite_position_delete_files: drop delete records that point at removed data files")
    rewrite_deletes = run_sql(
        spark, rewrite_position_delete_files_sql(args.catalog, args.database, args.curated_table)
    ).first().asDict()
    log(f"  -> {rewrite_deletes}")
    last_id = report_snapshot(spark, curated, last_id)
    files_after = file_summary(spark, curated)
    log(f"Live files after compaction: {files_after}")

    now = scalar(spark, "SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')")
    snapshots_before = scalar(spark, f"SELECT count(*) FROM {metadata_table(curated, 'snapshots')}")
    log(f"expire_snapshots: snapshots older than now ({now} UTC), always keeping the last {args.retain_last}")
    expire = run_sql(
        spark, expire_snapshots_sql(args.catalog, args.database, args.curated_table, now, args.retain_last)
    ).first().asDict()
    snapshots_after = scalar(spark, f"SELECT count(*) FROM {metadata_table(curated, 'snapshots')}")
    log(f"  -> {expire}")
    log(f"Snapshots: {snapshots_before} -> {snapshots_after}")

    log("remove_orphan_files (dry run, files older than 3 days that no snapshot references; deletes nothing)")
    orphans = [r[0] for r in run_sql(
        spark, remove_orphan_files_sql(args.catalog, args.database, args.curated_table)
    ).collect()]
    log(f"  -> {len(orphans)} orphan file(s) would be deleted")
    for path in orphans[:10]:
        log(f"     {path}")

    section("7. Final state of the curated table")
    final_rows = scalar(spark, f"SELECT count(*) FROM {curated}")
    remaining_rule = scalar(spark, f"SELECT count(*) FROM {curated} WHERE {DELETE_RULE}")
    remaining_backfill = scalar(spark, f"SELECT count(*) FROM {curated} WHERE {backfill_predicate()}")
    partitions = scalar(spark, f"SELECT count(*) FROM {metadata_table(curated, 'partitions')}")
    log(f"Rows {final_rows}, partitions {partitions}, rows breaking the DELETE rule {remaining_rule}, "
        f"rows left to backfill {remaining_backfill}")
    expected_rows = scalar(spark, f"SELECT count(*) FROM {SOURCE_VIEW} WHERE NOT ({DELETE_RULE})")
    if final_rows != expected_rows or remaining_rule or remaining_backfill:
        raise ValidationError(
            f"curated table check failed: rows {final_rows} (expected {expected_rows}), "
            f"rule rows {remaining_rule}, rows to backfill {remaining_backfill}"
        )
    log("partitions (first 5 days):")
    show(
        spark.sql(
            "SELECT partition, record_count, file_count, position_delete_record_count "
            f"FROM {metadata_table(curated, 'partitions')} ORDER BY partition LIMIT 5"
        ),
        5,
    )
    first_day = scalar(spark, f"SELECT CAST(min({quote(PARTITION_SOURCE_COLUMN)}) AS DATE) FROM {curated}")
    if first_day is not None:
        log("Partition-pruned query (first 3 days, real timestamps instead of strings):")
        show(
            run_sql(
                spark,
                f"SELECT CAST(pickup_ts AS DATE) AS day, count(*) AS trips,\n"
                f"       round(avg({quote(EVOLVED_COLUMN)}), 2) AS avg_fare_per_mile,\n"
                f"       round(avg(timestampdiff(SECOND, pickup_ts, dropoff_ts)) / 60, 1) AS avg_minutes\n"
                f"FROM {curated}\n"
                f"WHERE pickup_ts >= TIMESTAMP_NTZ '{first_day} 00:00:00'\n"
                f"  AND pickup_ts < TIMESTAMP_NTZ '{first_day} 00:00:00' + INTERVAL 3 DAYS\n"
                "GROUP BY 1 ORDER BY 1",
            )
        )
    result.update(
        {
            "rows": final_rows,
            "partitions": partitions,
            "files_before_compaction": files_before,
            "files_after_compaction": files_after,
            "rewrite_data_files": rewrite,
            "rewrite_position_delete_files": rewrite_deletes,
            "expire_snapshots": expire,
            "snapshots_before_expire": snapshots_before,
            "snapshots_after_expire": snapshots_after,
            "orphan_files_dry_run": len(orphans),
        }
    )
    return result


def run(spark: Any, args: argparse.Namespace) -> int:
    # Timestamps in the log and in TIMESTAMP literals are UTC, and string -> TIMESTAMP_NTZ parsing
    # keeps the wall-clock value (no daylight saving gaps in UTC).
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    shared_name = f"{args.catalog}.{args.database}.{args.table}"
    shared = table_ident(args.catalog, args.database, args.table)

    section("Configuration")
    catalog_conf = f"spark.sql.catalog.{args.catalog}"
    catalog_class = spark.conf.get(catalog_conf, None)
    if not catalog_class:
        raise ValidationError(f"Spark catalog '{args.catalog}' is not configured ({catalog_conf} is not set)")
    log(f"Spark {spark.version}, application id {spark.sparkContext.applicationId}")
    log(
        f"catalog {args.catalog}: {catalog_class} type={spark.conf.get(f'{catalog_conf}.type', None)} "
        f"catalog-impl={spark.conf.get(f'{catalog_conf}.catalog-impl', None)}"
    )
    log(f"shared table {shared_name}, curated table {args.curated_table}, warehouse {args.warehouse}")
    if not spark.catalog.tableExists(shared_name):
        raise ValidationError(f"table {shared_name} does not exist: run the load job (iceberg_nyc_taxi_load.py) first")

    result: Dict[str, Any] = {"shared_table": shared_name}
    result.update(shared_table_section(spark, args, shared, shared_name))
    result["curated"] = curated_table_section(spark, args, shared)

    section("Result")
    log("RESULT " + json.dumps(result, sort_keys=True, default=str))
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
