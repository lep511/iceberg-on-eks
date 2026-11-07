"""The jobs are self-contained (no shared module), so the duplicated definitions must not drift."""

from __future__ import annotations

import inspect

import pytest

import iceberg_cleanup as cleanup
import iceberg_nyc_taxi_analytics as analytics
import iceberg_nyc_taxi_load as load

SHARED_WITH_ANALYTICS = [
    "glue_name", "catalog_name", "location_uri", "quote", "table_ident", "metadata_table",
    "table_location", "sql_string", "round2_sql", "trip_stats_sql", "breakdown_sql",
    "assemble_trip_stats", "compute_trip_stats", "format_stats", "log", "section",
    "load_iceberg_table",
]
SHARED_WITH_CLEANUP = ["glue_name", "catalog_name", "location_uri", "quote", "table_ident", "log", "section"]
CONSTANTS = ["TABLE_COLUMNS", "ICEBERG_TO_SPARK_SQL", "GLUE_NAME_RE", "CATALOG_NAME_RE", "URI_RE", "STATS_SCALAR_KEYS"]


@pytest.mark.parametrize("name", SHARED_WITH_ANALYTICS)
def test_functions_identical_in_load_and_analytics(name):
    assert inspect.getsource(getattr(load, name)) == inspect.getsource(getattr(analytics, name))


@pytest.mark.parametrize("name", SHARED_WITH_CLEANUP)
def test_functions_identical_in_load_and_cleanup(name):
    assert inspect.getsource(getattr(load, name)) == inspect.getsource(getattr(cleanup, name))


@pytest.mark.parametrize("name", CONSTANTS)
def test_constants_identical_in_load_and_analytics(name):
    assert getattr(load, name) == getattr(analytics, name)


@pytest.mark.parametrize("name", ["GLUE_NAME_RE", "CATALOG_NAME_RE", "URI_RE"])
def test_constants_identical_in_load_and_cleanup(name):
    assert getattr(load, name) == getattr(cleanup, name)


def test_curated_table_default_is_the_same():
    assert analytics.DEFAULT_CURATED_TABLE == cleanup.DEFAULT_CURATED_TABLE == "nyc_taxi_trips_curated"
