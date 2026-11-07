"""Shared fixtures for the Spark job tests.

Most tests are pure Python (no JVM). Tests marked `spark` start a local SparkSession and are
skipped when no Java runtime is found (set JAVA_HOME or put `java` on PATH to run them).
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest

SPARK_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = SPARK_DIR.parent
TESTS_DIR = Path(__file__).resolve().parent
for path in (SPARK_DIR, TESTS_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "spark: needs a Java runtime to start a local SparkSession")


def _java_available() -> bool:
    java_home = os.environ.get("JAVA_HOME")
    if java_home and (Path(java_home) / "bin" / "java").exists():
        return True
    return shutil.which("java") is not None


@pytest.fixture(scope="session")
def spark():
    if not _java_available():
        pytest.skip("no Java runtime (set JAVA_HOME) - Spark tests skipped")
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("iceberg-on-eks-spark-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def payload_rows():
    import json

    with open(PROJECT_DIR / "data" / "nyctaxi_payload.json", encoding="utf-8") as handle:
        return json.load(handle)["rows"]
