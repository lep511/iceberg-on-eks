#!/usr/bin/env bash
# Uploads the lab artifacts to the warehouse bucket (bucket and region from the Terraform outputs):
#
#   data/nyctaxi_payload.json  -> s3://<bucket>/lab/input/nyctaxi_payload.json
#   spark/*.py                 -> s3://<bucket>/lab/scripts/spark/
#   build/bin/iceberg-k8s      -> s3://<bucket>/lab/bin/iceberg-k8s   (skipped if not built yet)
#
# Every request uses --expected-bucket-owner with the caller's account id. Objects whose ETag
# already equals the local MD5 are skipped, so re-running only uploads what changed.

set -euo pipefail

# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

usage() {
  cat <<EOF
Usage: scripts/upload-artifacts.sh [--dry-run] [--prune] [--outputs-file FILE]

Uploads the sample data, the PySpark scripts and the Rust CLI binary to the lab/ prefix of the
warehouse bucket (Terraform outputs warehouse_bucket, region and lab_uri).

Options:
  --dry-run            only show what would be uploaded (no writes)
  --prune              also delete objects under lab/scripts/spark/ that have no local file
  --outputs-file FILE  read the Terraform outputs from FILE instead of running terraform
                       (same as TF_OUTPUT_JSON=FILE)
  -h, --help           show this help

Build the Rust CLI first with rust/build.sh; if build/bin/iceberg-k8s is missing it is skipped.
EOF
}

DRY_RUN=false
PRUNE=false
while (($#)); do
  case "$1" in
    --dry-run) DRY_RUN=true ;;
    --prune) PRUNE=true ;;
    --outputs-file)
      [[ $# -ge 2 ]] || die "--outputs-file needs a value"
      export TF_OUTPUT_JSON="$2"
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      die "unknown argument: $1"
      ;;
  esac
  shift
done

require_cmd aws jq md5sum
load_tf_outputs
BUCKET="$(tf_output warehouse_bucket)"
REGION="$(tf_output region)"
LAB_URI="$(tf_output lab_uri "s3://$BUCKET/lab")"
[[ -n "$BUCKET" && -n "$REGION" ]] ||
  die "Terraform outputs warehouse_bucket and region are required. Run 'terraform -chdir=terraform apply' first."

# Key prefix of lab_uri (s3://<bucket>/lab -> lab); it must be in the warehouse bucket.
[[ "$LAB_URI" =~ ^s3://([^/]+)/(.+)$ ]] || die "lab_uri is not an s3://<bucket>/<prefix> URI: $LAB_URI"
[[ "${BASH_REMATCH[1]}" == "$BUCKET" ]] || die "lab_uri ($LAB_URI) is not in the warehouse bucket $BUCKET"
LAB_PREFIX="${BASH_REMATCH[2]%/}"

if [[ "$BUCKET" != spark-on-eks* ]]; then
  warn "bucket $BUCKET does not start with spark-on-eks; the workshop IDE role may only write to spark-on-eks* buckets"
fi

# REMOTE=true once the bucket is known to exist and belong to the caller's account; a dry run
# without it still lists the local files.
REMOTE=false
if ! ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)"; then
  ACCOUNT_ID=""
  $DRY_RUN || die "could not get the AWS account id. Are AWS credentials configured?"
  warn "could not get the AWS account id (no credentials?); the dry run only lists local files"
elif aws s3api head-bucket --bucket "$BUCKET" --expected-bucket-owner "$ACCOUNT_ID" --region "$REGION" >/dev/null 2>&1; then
  REMOTE=true
else
  $DRY_RUN || die "bucket $BUCKET is missing or not owned by account $ACCOUNT_ID (did 'terraform apply' finish?)"
  warn "bucket $BUCKET is missing or not owned by account $ACCOUNT_ID; the dry run only lists local files"
fi

MODE=""
$DRY_RUN && MODE=" | DRY RUN"
info "Bucket: s3://$BUCKET/$LAB_PREFIX/ | Region: $REGION | Account: ${ACCOUNT_ID:-unknown}$MODE"

# Local file -> key and content type.
SOURCES=()
KEYS=()
TYPES=()
add() {
  SOURCES+=("$1")
  KEYS+=("$LAB_PREFIX/$2")
  TYPES+=("$3")
}

PAYLOAD="$REPO_ROOT/data/nyctaxi_payload.json"
[[ -f "$PAYLOAD" ]] || die "missing $PAYLOAD"
add "$PAYLOAD" "input/nyctaxi_payload.json" "application/json"

shopt -s nullglob
spark_scripts=("$REPO_ROOT"/spark/*.py)
shopt -u nullglob
if ((${#spark_scripts[@]} == 0)); then
  warn "no PySpark scripts in spark/*.py"
fi
for f in "${spark_scripts[@]}"; do
  add "$f" "scripts/spark/$(basename "$f")" "text/x-python"
done

BINARY="$REPO_ROOT/build/bin/iceberg-k8s"
if [[ -f "$BINARY" ]]; then
  if command -v file >/dev/null 2>&1 && ! file -b "$BINARY" | grep -q 'ELF 64-bit.*ARM aarch64'; then
    warn "$BINARY is not a linux aarch64 ELF binary ($(file -b "$BINARY" | cut -c1-60)); the Jobs run on Graviton5 (arm64) nodes. Rebuild it with rust/build.sh"
  fi
  add "$BINARY" "bin/iceberg-k8s" "application/octet-stream"
else
  warn "build/bin/iceberg-k8s not built yet: skipping it. Run rust/build.sh, then run this script again."
fi

# ETag of an object without quotes, empty if it does not exist.
remote_etag() {
  aws s3api head-object --bucket "$BUCKET" --key "$1" --expected-bucket-owner "$ACCOUNT_ID" --region "$REGION" \
    --query ETag --output text 2>/dev/null | tr -d '"' || true
}

uploaded=0
unchanged=0
for i in "${!SOURCES[@]}"; do
  src="${SOURCES[$i]}" key="${KEYS[$i]}"
  rel="${src#"$REPO_ROOT"/}"
  local_md5="$(md5sum -- "$src" | cut -d' ' -f1)"
  etag=""
  if $REMOTE; then etag="$(remote_etag "$key")"; fi
  if [[ "$etag" == "$local_md5" ]]; then
    printf '  unchanged  %s -> s3://%s/%s\n' "$rel" "$BUCKET" "$key"
    unchanged=$((unchanged + 1))
    continue
  fi
  if $DRY_RUN; then
    printf '  would put  %s -> s3://%s/%s\n' "$rel" "$BUCKET" "$key"
  else
    aws s3api put-object --bucket "$BUCKET" --key "$key" --body "$src" --content-type "${TYPES[$i]}" \
      --expected-bucket-owner "$ACCOUNT_ID" --region "$REGION" --output text --query ETag >/dev/null
    printf '  uploaded   %s -> s3://%s/%s\n' "$rel" "$BUCKET" "$key"
  fi
  uploaded=$((uploaded + 1))
done

if $PRUNE && $REMOTE; then
  prefix="$LAB_PREFIX/scripts/spark/"
  remote_keys="$(aws s3api list-objects-v2 --bucket "$BUCKET" --prefix "$prefix" --expected-bucket-owner "$ACCOUNT_ID" \
    --region "$REGION" --query 'Contents[].Key' --output json | jq -r '.[]?')"
  while IFS= read -r key; do
    [[ -z "$key" || "$key" == "None" ]] && continue
    keep=false
    for k in "${KEYS[@]}"; do [[ "$k" == "$key" ]] && keep=true && break; done
    $keep && continue
    if $DRY_RUN; then
      printf '  would delete stale s3://%s/%s\n' "$BUCKET" "$key"
    else
      aws s3api delete-object --bucket "$BUCKET" --key "$key" --expected-bucket-owner "$ACCOUNT_ID" --region "$REGION" >/dev/null
      printf '  deleted stale s3://%s/%s\n' "$BUCKET" "$key"
    fi
  done <<<"$remote_keys"
elif $PRUNE; then
  warn "--prune skipped: the bucket could not be read"
fi

if $DRY_RUN; then
  ok "Dry run: $uploaded file(s) would be uploaded, $unchanged unchanged. Nothing was written."
else
  ok "Done: $uploaded uploaded, $unchanged unchanged."
fi
