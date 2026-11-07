"""Text checks of the SparkApplication templates (no YAML parser needed)."""

from __future__ import annotations

import re

import pytest

import iceberg_nyc_taxi_analytics as analytics
from conftest import SPARK_DIR

MANIFESTS = sorted((SPARK_DIR / "manifests").glob("*.yaml"))
# Exactly the placeholders that scripts/render-manifests.sh fills from Terraform outputs.
CONTRACT_PLACEHOLDERS = {
    "REGION", "CLUSTER_NAME", "WAREHOUSE_BUCKET", "ICEBERG_WAREHOUSE", "LAB_URI", "GLUE_DATABASE",
    "ICEBERG_TABLE", "SPARK_IRSA_ROLE_ARN", "JOBS_IRSA_ROLE_ARN", "SPARK_NAMESPACE", "SPARK_SERVICE_ACCOUNT", "JOBS_NAMESPACE",
    "JOBS_SERVICE_ACCOUNT", "SPARK_EVENT_LOG_DIR",
}
IMAGE = "public.ecr.aws/data-on-eks/spark:4.0.1-scala2.13-java21-python3-r-ubuntu"


def test_three_manifests():
    assert [m.name for m in MANIFESTS] == [
        "spark-iceberg-analytics.yaml", "spark-iceberg-cleanup.yaml", "spark-iceberg-load.yaml"
    ]


@pytest.mark.parametrize("manifest", MANIFESTS, ids=lambda p: p.name)
def test_manifest_conventions(manifest):
    text = manifest.read_text()
    assert set(re.findall(r"<([A-Z_]+)>", text)) <= CONTRACT_PLACEHOLDERS
    assert "apiVersion: sparkoperator.k8s.io/v1beta2" in text
    assert "namespace: <SPARK_NAMESPACE>" in text
    assert text.count("serviceAccount: <SPARK_SERVICE_ACCOUNT>") == 2
    assert f'image: "{IMAGE}"' in text
    script = re.search(r'mainApplicationFile: "s3a://<WAREHOUSE_BUCKET>/lab/scripts/spark/([a-z_]+\.py)"', text)
    assert script and (SPARK_DIR / script.group(1)).is_file()
    assert '"spark.sql.catalog.glue.type": "glue"' in text
    assert '"spark.sql.catalog.glue.warehouse": "<ICEBERG_WAREHOUSE>"' in text
    assert '"spark.eventLog.dir": "<SPARK_EVENT_LOG_DIR>"' in text
    assert 'karpenter.sh/do-not-disrupt: "true"' in text
    assert text.count('NodeGroupType: "SparkComputeOptimized"') == 2
    assert 'karpenter.sh/capacity-type: "on-demand"' in text
    assert "timeToLiveSeconds:" in text
    assert '- "--catalog"\n    - "glue"' in text
    # scripts/cleanup.sh and the other components select lab workloads by this label.
    assert text.count("app.kubernetes.io/part-of: spark-on-eks-iceberg") == 3  # app, driver, executor
    assert "iceberg-on-eks" not in text


def test_load_job_is_never_retried_after_it_started():
    text = (SPARK_DIR / "manifests" / "spark-iceberg-load.yaml").read_text()
    assert "onFailureRetries: 0" in text


def test_cleanup_manifest_confirms_and_names_the_curated_table():
    text = (SPARK_DIR / "manifests" / "spark-iceberg-cleanup.yaml").read_text()
    assert '- "--confirm"' in text
    assert f'- "--curated-table"\n    - "{analytics.DEFAULT_CURATED_TABLE}"' in text
    analytics_text = (SPARK_DIR / "manifests" / "spark-iceberg-analytics.yaml").read_text()
    assert f'- "--curated-table"\n    - "{analytics.DEFAULT_CURATED_TABLE}"' in analytics_text
