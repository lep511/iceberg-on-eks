"""Tests of iceberg_cleanup.py: the --confirm guard and the DROP statements."""

from __future__ import annotations

import pytest

import iceberg_cleanup as cleanup


def test_confirm_is_required(capsys):
    with pytest.raises(SystemExit) as exc:
        cleanup.parse_args([])
    assert exc.value.code == 2
    assert "refusing to drop anything without --confirm" in capsys.readouterr().err


def test_parse_args_defaults_with_confirm():
    args = cleanup.parse_args(["--confirm"])
    assert (args.catalog, args.database, args.table, args.curated_table) == (
        "glue", "nyc_taxi_database", "nyc_taxi_table", "nyc_taxi_trips_curated"
    )
    assert args.warehouse is None


@pytest.mark.parametrize("extra", [["--database", "Bad-Name"], ["--curated-table", "nyc_taxi_table"], ["--warehouse", "nope"]])
def test_parse_args_rejects_invalid_input(extra):
    with pytest.raises(SystemExit):
        cleanup.parse_args(["--confirm"] + extra)


def test_drop_order_is_curated_then_shared():
    args = cleanup.parse_args(["--confirm", "--table", "t", "--curated-table", "c"])
    assert cleanup.tables_to_drop(args) == ["c", "t"]


def test_drop_statements_are_idempotent_and_never_cascade():
    assert cleanup.drop_table_sql("glue", "db", "t") == "DROP TABLE IF EXISTS `glue`.`db`.`t` PURGE"
    sql = cleanup.drop_namespace_sql("glue", "db")
    assert sql == "DROP NAMESPACE IF EXISTS `glue`.`db`"
    assert "CASCADE" not in sql.upper()


def test_remaining_tables_message_lists_every_table():
    message = cleanup.remaining_tables_message("glue", "db", ["other_a", "other_b"])
    assert "glue.db.other_a" in message and "glue.db.other_b" in message
    assert "2 other table(s)" in message and "no CASCADE" in message


def test_missing_metadata_error_detection():
    assert cleanup.is_missing_metadata_error(RuntimeError("org.apache.iceberg.exceptions.NotFoundException: x"))
    assert cleanup.is_missing_metadata_error(
        RuntimeError("software.amazon.awssdk.services.s3.model.NoSuchBucketException: The specified bucket does not exist")
    )
    assert not cleanup.is_missing_metadata_error(RuntimeError("AccessDeniedException"))


def test_drop_unreadable_tables_flag_defaults_off():
    assert cleanup.parse_args(["--database", "db", "--table", "t", "--confirm"]).drop_unreadable_tables is False
    args = cleanup.parse_args(["--database", "db", "--table", "t", "--drop-unreadable-tables", "--confirm"])
    assert args.drop_unreadable_tables is True
