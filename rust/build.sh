#!/usr/bin/env bash
# Builds the iceberg-k8s CLI in release mode for linux x86_64 and copies it to
# <project root>/build/bin/iceberg-k8s, from where scripts/upload-artifacts.sh uploads it to
# s3://<warehouse bucket>/lab/bin/iceberg-k8s for the Kubernetes Jobs in rust/manifests/.
#
# Run it on the x86_64 Amazon Linux 2023 IDE: the binary links the build host's glibc (2.34),
# which matches the runtime image used by the Jobs (amazonlinux:2023 minimal).
#
# Usage: rust/build.sh            (env: CARGO_TARGET_DIR, default rust/iceberg-k8s/target)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CRATE_DIR="${SCRIPT_DIR}/iceberg-k8s"
TARGET="x86_64-unknown-linux-gnu"
OUT_DIR="${PROJECT_ROOT}/build/bin"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${CRATE_DIR}/target}"

command -v cargo >/dev/null || { echo "error: cargo not found (install Rust with rustup)" >&2; exit 1; }

HOST="$(rustc -vV | sed -n 's/^host: //p')"
if [[ "${HOST}" != "${TARGET}" ]]; then
  echo "error: this script builds natively for ${TARGET}, but the host is ${HOST}." >&2
  echo "       Build on the x86_64 IDE, or use rust/Dockerfile (docker build --platform linux/amd64)." >&2
  exit 1
fi

echo "==> cargo build --release --locked --target ${TARGET}"
cargo build --manifest-path "${CRATE_DIR}/Cargo.toml" --release --locked --target "${TARGET}"

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
