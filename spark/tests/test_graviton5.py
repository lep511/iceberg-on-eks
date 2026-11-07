"""Project contract: every workload runs on AWS Graviton5 (M9g/M9gd, Neoverse V3), see GRAVITON5.md.

Text checks only (no YAML or HCL parser needed): the Spark and Rust manifests, the Terraform
NodePool, and the Rust build configuration.
"""

from __future__ import annotations

import re

import pytest

from conftest import PROJECT_DIR

SPARK_MANIFESTS = sorted((PROJECT_DIR / "spark" / "manifests").glob("*.yaml"))
RUST_MANIFESTS = sorted((PROJECT_DIR / "rust" / "manifests").glob("*.yaml"))
GRAVITON5_FAMILIES = '["m9g", "m9gd"]'
GRAVITON5_RUSTFLAGS = 'rustflags = ["-Ctarget-cpu=neoverse-v3", "-Ctarget-feature=+lse"]'
X86_MARKERS = re.compile(r"amd64|x86[-_]64|x86\b|NodeGroupType", re.IGNORECASE)


def test_manifests_found():
    assert len(SPARK_MANIFESTS) == 3
    assert len(RUST_MANIFESTS) == 10


@pytest.mark.parametrize("manifest", RUST_MANIFESTS, ids=lambda p: p.name)
def test_rust_jobs_run_only_on_graviton5(manifest):
    text = manifest.read_text()
    selector = re.search(r"\n      nodeSelector:\n((?:        \S.*\n)+)", text)
    assert selector, "the pod template must have a nodeSelector"
    assert selector.group(1).splitlines() == [
        "        karpenter.sh/nodepool: <GRAVITON5_NODE_POOL>",
        "        kubernetes.io/arch: arm64",
        "        karpenter.sh/capacity-type: on-demand",
    ]
    assert not X86_MARKERS.search(text)


@pytest.mark.parametrize("manifest", SPARK_MANIFESTS, ids=lambda p: p.name)
def test_spark_pods_run_only_on_graviton5(manifest):
    text = manifest.read_text()
    selectors = re.findall(r"\n    nodeSelector:\n((?:      \S.*\n)+)", text)
    assert len(selectors) == 2  # driver, executor
    driver, executor = (s.splitlines() for s in selectors)
    assert driver == [
        '      karpenter.sh/nodepool: "<GRAVITON5_NODE_POOL>"',
        '      kubernetes.io/arch: "arm64"',
        '      karpenter.sh/capacity-type: "on-demand"',
    ]
    assert executor == [
        '      karpenter.sh/nodepool: "<GRAVITON5_NODE_POOL>"',
        '      kubernetes.io/arch: "arm64"',
    ]
    assert not X86_MARKERS.search(text)


def test_terraform_node_pool_is_graviton5_only():
    main = (PROJECT_DIR / "terraform" / "main.tf").read_text()
    assert f"graviton5_instance_families = {GRAVITON5_FAMILIES}" in main
    assert 'graviton5_node_pool         = "${var.name}-graviton5"' in main
    pool = (PROJECT_DIR / "terraform" / "nodepool.tf").read_text()
    assert 'kind       = "NodePool"' in pool
    assert "values   = local.graviton5_instance_families" in pool
    assert 'values   = ["arm64"]' in pool
    # Auto Mode needs more than 1 vCPU: no medium (or smaller) sizes.
    sizes = re.search(r'key      = "eks.amazonaws.com/instance-size"\n.*\n\s+values   = (\[.*\])', pool)
    assert sizes and sizes.group(1) == '["large", "xlarge", "2xlarge", "4xlarge", "8xlarge"]'
    outputs = (PROJECT_DIR / "terraform" / "outputs.tf").read_text()
    assert 'output "graviton5_node_pool"' in outputs


def test_rust_cli_is_compiled_for_neoverse_v3():
    config = (PROJECT_DIR / "rust" / ".cargo" / "config.toml").read_text()
    assert "[target.aarch64-unknown-linux-gnu]\n" + GRAVITON5_RUSTFLAGS in config
    build = (PROJECT_DIR / "rust" / "build.sh").read_text()
    assert 'TARGET="aarch64-unknown-linux-gnu"' in build
    assert "unset \"${var}\"" in build  # RUSTFLAGS & co. cannot replace the Graviton5 flags
    assert 'cd "${CRATE_DIR}"' in build  # so Cargo finds rust/.cargo/config.toml
    dockerfile = (PROJECT_DIR / "rust" / "Dockerfile").read_text()
    assert dockerfile.count("FROM --platform=linux/arm64 ") == 2
    assert "COPY .cargo/config.toml ./.cargo/config.toml" in dockerfile
    assert "--target aarch64-unknown-linux-gnu" in dockerfile
    assert "amd64" not in dockerfile


def test_upload_expects_an_aarch64_binary():
    upload = (PROJECT_DIR / "scripts" / "upload-artifacts.sh").read_text()
    assert "ELF 64-bit.*ARM aarch64" in upload
    assert "x86-64" not in upload
