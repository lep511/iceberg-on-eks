#!/usr/bin/env bash
# Builds the iceberg-k8s CLI in release mode for AWS Graviton5 (linux aarch64, Neoverse V3) and
# copies it to <project root>/build/bin/iceberg-k8s, from where scripts/upload-artifacts.sh
# uploads it to s3://<warehouse bucket>/lab/bin/iceberg-k8s for the Kubernetes Jobs in
# rust/manifests/. Those Jobs run only on the Graviton5 NodePool (M9g/M9gd).
#
# The compiler flags come from rust/.cargo/config.toml (see GRAVITON5.md):
#   -Ctarget-cpu=neoverse-v3 -Ctarget-feature=+lse
#
# Run it on an arm64 Amazon Linux 2023 host, such as the workshop IDE (a Graviton5 host): the
# binary links the build host's glibc (2.34), which matches the runtime image used by the Jobs
# (amazonlinux:2023 minimal, arm64 variant).
#
# Usage: rust/build.sh            (env: CARGO_TARGET_DIR, default rust/iceberg-k8s/target)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CRATE_DIR="${SCRIPT_DIR}/iceberg-k8s"
CARGO_CONFIG="${SCRIPT_DIR}/.cargo/config.toml"
TARGET="aarch64-unknown-linux-gnu"
# MIDR_EL1 part number of Arm Neoverse V3, the Graviton5 core.
NEOVERSE_V3_PART="0xd84"
OUT_DIR="${PROJECT_ROOT}/build/bin"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${CRATE_DIR}/target}"

command -v cargo >/dev/null || { echo "error: cargo not found (install Rust with rustup)" >&2; exit 1; }

HOST="$(rustc -vV | sed -n 's/^host: //p')"
if [[ "${HOST}" != "${TARGET}" ]]; then
  echo "error: this script builds natively for ${TARGET} (Graviton5 nodes), but the host is ${HOST}." >&2
  echo "       Build on an arm64 Amazon Linux 2023 host (the IDE), or use rust/Dockerfile (docker build --platform linux/arm64)." >&2
  exit 1
fi

# The Graviton5 flags must come from rust/.cargo/config.toml.
if ! grep -q -- '-Ctarget-cpu=neoverse-v3' "${CARGO_CONFIG}" 2>/dev/null; then
  echo "error: ${CARGO_CONFIG} does not set -Ctarget-cpu=neoverse-v3 for ${TARGET} (see GRAVITON5.md)." >&2
  exit 1
fi
# These variables would replace the flags of the config file.
for var in RUSTFLAGS CARGO_ENCODED_RUSTFLAGS CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS; do
  if [[ -n "${!var:-}" ]]; then
    echo "warning: ignoring ${var}=${!var}: it would replace the Graviton5 flags of rust/.cargo/config.toml" >&2
  fi
  unset "${var}"
done

CPU_PART="$(awk -F': *' '/^CPU part/ {print $2; exit}' /proc/cpuinfo 2>/dev/null || true)"
if [[ "${CPU_PART}" == "${NEOVERSE_V3_PART}" ]]; then
  echo "==> build host CPU: Neoverse V3 (Graviton5, CPU part ${CPU_PART})"
else
  echo "note: the build host CPU (part ${CPU_PART:-unknown}) is not Neoverse V3. The binary still targets" >&2
  echo "      Graviton5, but it may not run on this host (and neither may cargo test)." >&2
fi
echo "==> rustflags for ${TARGET}: $(sed -n 's/^rustflags *= *//p' "${CARGO_CONFIG}")"

# Cargo looks for .cargo/config.toml from the current directory upwards: run it from the crate.
cd "${CRATE_DIR}"
echo "==> cargo build --release --locked --target ${TARGET}"
cargo build --release --locked --target "${TARGET}"

BIN="${CARGO_TARGET_DIR}/${TARGET}/release/iceberg-k8s"
mkdir -p "${OUT_DIR}"
install -m 0755 "${BIN}" "${OUT_DIR}/iceberg-k8s"

echo
echo "==> ${OUT_DIR}/iceberg-k8s"
ls -l "${OUT_DIR}/iceberg-k8s"
command -v file >/dev/null && file "${OUT_DIR}/iceberg-k8s"
echo
echo "==> shared libraries (ldd)"
ldd "${OUT_DIR}/iceberg-k8s"
if command -v objdump >/dev/null; then
  GLIBC_MAX="$(objdump -T "${OUT_DIR}/iceberg-k8s" | grep -o 'GLIBC_[0-9.]*' | sort -Vu | tail -n 1)"
  echo "==> highest glibc symbol version required: ${GLIBC_MAX}"
fi
echo
echo "Next: scripts/upload-artifacts.sh uploads it to s3://<warehouse bucket>/lab/bin/iceberg-k8s"
