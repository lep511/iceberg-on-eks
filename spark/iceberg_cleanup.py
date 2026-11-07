#!/usr/bin/env python3
"""Drop the project's Iceberg tables and the Glue database (run BEFORE `terraform destroy`).

The IDE role has no Glue permissions, so the Glue objects created by the Spark jobs are removed
from inside the cluster with the jobs' IRSA role. Run it before `terraform destroy` deletes that
role. Steps, in order, each one safe when the object is already gone:

1. DROP TABLE IF EXISTS <catalog>.<db>.<curated-table> PURGE
2. DROP TABLE IF EXISTS <catalog>.<db>.<table> PURGE
3. DROP NAMESPACE IF EXISTS <catalog>.<db>
   (never CASCADE: if other tables are left in the namespace, the job lists them and fails)

PURGE deletes every file the table metadata references (data, delete files, manifests, manifest
lists, metadata files). Files no snapshot references (orphans, e.g. from failed writes) are not
deleted; they are reported, and `terraform destroy` removes them with the bucket (force_destroy).

Nothing is dropped unless `--confirm` is passed.

A table whose metadata file is gone (NoSuchKey / NoSuchBucket) cannot be loaded, so it cannot be
purged: only its catalog entry is dropped. Any other load error (for example AccessDenied) stops
the job, because it usually means a permission problem on a table whose data still exists. Pass
`--drop-unreadable-tables` to drop the catalog entry of such tables too; use it to recover after
`terraform destroy` ran before this job (the re-created IRSA role cannot read the old, deleted
bucket, so S3 answers AccessDenied instead of NoSuchBucket).

Usage:
    spark-submit iceberg_cleanup.py --catalog glue --database nyc_taxi_database \
        --table nyc_taxi_table --curated-table nyc_taxi_trips_curated \
        [--warehouse s3://<bucket>/warehouse] [--drop-unreadable-tables] --confirm
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Any, Dict, List, Optional, Sequence

APP_NAME = "iceberg-cleanup"
LOG_PREFIX = "[iceberg-cleanup]"
DEFAULT_CURATED_TABLE = "nyc_taxi_trips_curated"

# Duplicated from iceberg_nyc_taxi_load.py (see tests/test_consistency.py).
GLUE_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
CATALOG_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:/")


class ValidationError(Exception):
    """A check failed; the job exits with status 1."""


# ---------------------------------------------------------------------------------------------
# Argument parsing (validators duplicated from the load job)
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="iceberg_cleanup.py",
        description="Drop the curated and shared Iceberg tables (PURGE) and their namespace.",
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
        help="Glue database / Iceberg namespace to drop (default: nyc_taxi_database)",
    )
    parser.add_argument(
        "--table",
        type=glue_name,
        default="nyc_taxi_table",
        help="shared Iceberg table to drop (default: nyc_taxi_table)",
    )
    parser.add_argument(
        "--curated-table",
        type=glue_name,
        default=DEFAULT_CURATED_TABLE,
        help=f"derived table of the analytics job to drop (default: {DEFAULT_CURATED_TABLE})",
    )
    parser.add_argument(
        "--warehouse",
        type=location_uri,
        default=None,
        help="Iceberg warehouse root; when set, files left under <warehouse>/<database> are reported",
    )
    parser.add_argument(
        "--drop-unreadable-tables",
        action="store_true",
        help=(
            "drop only the catalog entry of a table whose metadata cannot be read for any reason "
            "(e.g. AccessDenied on a bucket deleted by an earlier terraform destroy); its files are not purged"
        ),
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="required: confirm that the tables (and all their data) and the namespace are deleted",
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.confirm:
        parser.error(
            "refusing to drop anything without --confirm "
            f"(would drop {args.catalog}.{args.database}.{args.curated_table}, "
            f"{args.catalog}.{args.database}.{args.table} and namespace {args.catalog}.{args.database})"
        )
    if args.curated_table == args.table:
        parser.error("--curated-table must differ from --table")
    return args


# ---------------------------------------------------------------------------------------------
# SQL builders (pure functions)
# ---------------------------------------------------------------------------------------------


def quote(name: str) -> str:
    """Backtick-quote one identifier part (names are validated, so no escaping is needed)."""
    return f"`{name}`"


def table_ident(catalog: str, database: str, table: str) -> str:
    return ".".join(quote(part) for part in (catalog, database, table))


def metadata_table(ident: str, name: str) -> str:
    return f"{ident}.{quote(name)}"


def namespace_ident(catalog: str, database: str) -> str:
    return f"{quote(catalog)}.{quote(database)}"


def drop_table_sql(catalog: str, database: str, table: str) -> str:
    return f"DROP TABLE IF EXISTS {table_ident(catalog, database, table)} PURGE"


def drop_namespace_sql(catalog: str, database: str) -> str:
    # No CASCADE on purpose: a namespace that still has tables is reported, not emptied.
    return f"DROP NAMESPACE IF EXISTS {namespace_ident(catalog, database)}"


def tables_to_drop(args: argparse.Namespace) -> List[str]:
    """Drop order: the derived table first, then the shared table."""
    return [args.curated_table, args.table]


def remaining_tables_message(catalog: str, database: str, tables: Sequence[str]) -> str:
    listing = "\n".join(f"  - {catalog}.{database}.{t}" for t in tables)
    return (
        f"namespace {catalog}.{database} still contains {len(tables)} other table(s):\n{listing}\n"
        "They were not created by this project's Spark jobs, so they are not dropped (no CASCADE). "
        "Drop or move them, then run this job again."
    )


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
    log(f"  SQL> {sql}")
    return spark.sql(sql)


def load_iceberg_table(spark: Any, name: str) -> Any:
    return spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)


def list_files(spark: Any, location: str, limit: int = 10) -> Dict[str, Any]:
    """Files under a location through the Hadoop FileSystem (S3A on EKS). Read only."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(location)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return {"files": 0, "sample": []}
    iterator = fs.listFiles(path, True)
    count, sample = 0, []
    while iterator.hasNext():
        status = iterator.next()
        count += 1
        if len(sample) < limit:
            sample.append(str(status.getPath().toString()))
    return {"files": count, "sample": sample}


def describe_table(spark: Any, catalog: str, database: str, table: str) -> Dict[str, Any]:
    """What is about to be dropped (from metadata only, no data scan)."""
    name = f"{catalog}.{database}.{table}"
    ident = table_ident(catalog, database, table)
    jtable = load_iceberg_table(spark, name)
    current = jtable.currentSnapshot()
    # all_files can list a file once per manifest that references it: count distinct paths.
    live = spark.sql(
        "SELECT count(*) AS files, coalesce(sum(file_size_in_bytes), 0) AS bytes FROM ("
        f"SELECT DISTINCT file_path, file_size_in_bytes FROM {metadata_table(ident, 'all_files')})"
    ).first()
    data = spark.sql(
        "SELECT count(*) AS files, coalesce(sum(record_count), 0) AS records "
        f"FROM {metadata_table(ident, 'data_files')}"
    ).first()
    return {
        "table": name,
        "location": str(jtable.location()),
        "current_snapshot_id": int(current.snapshotId()) if current is not None else None,
        "snapshots": int(spark.sql(f"SELECT count(*) FROM {metadata_table(ident, 'snapshots')}").first()[0]),
        "current_data_files": int(data["files"]),
        "current_records": int(data["records"]),
        "files_in_all_snapshots": int(live["files"]),
        "bytes_in_all_snapshots": int(live["bytes"]),
    }


MISSING_METADATA_MARKERS = ("NotFoundException", "NoSuchKey", "NoSuchBucket", "FileNotFoundException")


def is_missing_metadata_error(exc: BaseException) -> bool:
    """True when the table's metadata file (or its whole bucket) no longer exists."""
    text = str(exc)
    return any(marker in text for marker in MISSING_METADATA_MARKERS)


def drop_from_catalog_only(spark: Any, catalog: str, database: str, table: str) -> bool:
    """Remove a catalog entry whose metadata file is gone (Spark cannot load it to drop it)."""
    jvm = spark._jvm
    plugin = spark._jsparkSession.sessionState().catalogManager().catalog(catalog)
    # TableIdentifier.of(String...) is varargs, which py4j does not map; parse() takes one string.
    identifier = jvm.org.apache.iceberg.catalog.TableIdentifier.parse(f"{database}.{table}")
    return bool(plugin.icebergCatalog().dropTable(identifier, False))


def drop_table(spark: Any, args: argparse.Namespace, table: str) -> Dict[str, Any]:
    name = f"{args.catalog}.{args.database}.{table}"
    try:
        exists = bool(spark.catalog.tableExists(name))
    except Exception as exc:  # noqa: BLE001 - the catalog entry exists but cannot be loaded
        if not (is_missing_metadata_error(exc) or args.drop_unreadable_tables):
            raise
        log(f"Table {name} is in the catalog but its metadata cannot be read: {str(exc).splitlines()[0]}")
        dropped = drop_from_catalog_only(spark, args.catalog, args.database, table)
        log(f"Dropped the catalog entry only (no purge possible): {dropped}")
        return {"table": name, "status": "dropped_catalog_entry_only" if dropped else "not_found"}

    if not exists:
        log(f"Table {name} does not exist (already dropped): nothing to do")
        run_sql(spark, drop_table_sql(args.catalog, args.database, table))
        return {"table": name, "status": "not_found"}

    info = describe_table(spark, args.catalog, args.database, table)
    log(
        f"Dropping {name}: location {info['location']}, {info['snapshots']} snapshot(s), "
        f"{info['current_records']} records in {info['current_data_files']} current data file(s), "
        f"{info['files_in_all_snapshots']} data/delete file(s) and {info['bytes_in_all_snapshots']} bytes "
        "across all snapshots"
    )
    run_sql(spark, drop_table_sql(args.catalog, args.database, table))
    if spark.catalog.tableExists(name):
        raise ValidationError(f"table {name} still exists after DROP TABLE ... PURGE")
    leftover = list_files(spark, info["location"])
    if leftover["files"]:
        log(
            f"{leftover['files']} file(s) not referenced by the table metadata are left under "
            f"{info['location']} (orphans; removed with the bucket by terraform destroy), e.g.:"
        )
        for path in leftover["sample"]:
            log(f"    {path}")
    else:
        log(f"No files left under {info['location']}")
    info.update({"status": "dropped", "files_left_under_location": leftover["files"]})
    return info


def drop_namespace(spark: Any, args: argparse.Namespace) -> Dict[str, Any]:
    ns = f"{args.catalog}.{args.database}"
    if not spark.catalog.databaseExists(ns):
        log(f"Namespace {ns} does not exist (already dropped): nothing to do")
        run_sql(spark, drop_namespace_sql(args.catalog, args.database))
        return {"namespace": ns, "status": "not_found"}

    location = None
    for row in spark.sql(f"DESCRIBE NAMESPACE EXTENDED {namespace_ident(args.catalog, args.database)}").collect():
        values = row.asDict()
        key = values.get("info_name") or values.get("database_description_item")
        if key == "Location":
            location = values.get("info_value") or values.get("database_description_value")
    remaining = sorted(r["tableName"] for r in spark.sql(f"SHOW TABLES IN {namespace_ident(args.catalog, args.database)}").collect())
    if remaining:
        raise ValidationError(remaining_tables_message(args.catalog, args.database, remaining))
    try:
        run_sql(spark, drop_namespace_sql(args.catalog, args.database))
    except Exception as exc:  # noqa: BLE001 - e.g. Glue tables that are not Iceberg tables
        if "NamespaceNotEmpty" in str(exc) or "not empty" in str(exc).lower():
            raise ValidationError(
                f"namespace {ns} is not empty: it still contains tables Spark does not list "
                f"(for example non-Iceberg Glue tables). Original error: {str(exc).splitlines()[0]}"
            ) from exc
        raise
    if spark.catalog.databaseExists(ns):
        raise ValidationError(f"namespace {ns} still exists after DROP NAMESPACE")
    log(f"Dropped namespace {ns} (location {location})")
    result: Dict[str, Any] = {"namespace": ns, "status": "dropped", "location": location}
    if args.warehouse:
        ns_location = f"{args.warehouse}/{args.database}"
        leftover = list_files(spark, ns_location)
        result["files_left_under_namespace_location"] = leftover["files"]
        log(f"Files left under {ns_location}: {leftover['files']}")
        for path in leftover["sample"]:
            log(f"    {path}")
    return result


def run(spark: Any, args: argparse.Namespace) -> int:
    section("Configuration")
    catalog_conf = f"spark.sql.catalog.{args.catalog}"
    catalog_class = spark.conf.get(catalog_conf, None)
    if not catalog_class:
        raise ValidationError(f"Spark catalog '{args.catalog}' is not configured ({catalog_conf} is not set)")
    log(f"Spark {spark.version}, application id {spark.sparkContext.applicationId}")
    log(f"catalog {args.catalog}: {catalog_class} type={spark.conf.get(f'{catalog_conf}.type', None)} "
        f"catalog-impl={spark.conf.get(f'{catalog_conf}.catalog-impl', None)}")

    result: Dict[str, Any] = {"tables": []}
    for step, table in enumerate(tables_to_drop(args), start=1):
        section(f"{step}. DROP TABLE {args.catalog}.{args.database}.{table} PURGE")
        result["tables"].append(drop_table(spark, args, table))

    section(f"3. DROP NAMESPACE {args.catalog}.{args.database}")
    result["namespace"] = drop_namespace(spark, args)

    section("Result")
    for entry in result["tables"]:
        log(f"table {entry['table']}: {entry['status']}")
    log(f"namespace {result['namespace']['namespace']}: {result['namespace']['status']}")
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
