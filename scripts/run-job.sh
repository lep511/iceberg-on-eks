#!/usr/bin/env bash
# Runs one rendered Rust CLI Job (build/manifests/rust/*.yaml) and prints its JSON response.
#
# 1. Creates the Job with `kubectl create` (the templates use generateName).
#    NAME=VALUE arguments are set as env vars of the iceberg-k8s container first
#    (e.g. DATA_FILE_KEY for the annotation Jobs).
# 2. Waits until the Job is Complete OR Failed (plain `kubectl wait --for=condition=complete`
#    would block until the timeout when the Job fails).
# 3. Prints the response, which is the last log line of the iceberg-k8s container, through jq.
#
# Exit code: 0 if the Job completed and the response has "ok": true, 1 otherwise.
set -euo pipefail

# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

usage() {
  cat <<'EOF'
Usage: scripts/run-job.sh [--timeout SECONDS] [--raw] <rendered-job.yaml> [NAME=VALUE ...]

Creates a Rust CLI Job from a rendered manifest, waits until it completes or fails, and prints
the JSON response (last log line of the iceberg-k8s container).

Options:
  --timeout SECONDS  give up after SECONDS (default 900)
  --raw              print the response line as is (no jq formatting)
  -h, --help         show this help

Examples:
  scripts/run-job.sh build/manifests/rust/job-read.yaml
  KEY=$(scripts/run-job.sh --raw build/manifests/rust/job-write-and-annotate.yaml | jq -r '.data.annotated_keys[0]')
  scripts/run-job.sh build/manifests/rust/job-get-annotation.yaml DATA_FILE_KEY="$KEY"
EOF
}

TIMEOUT=900
RAW=false
MANIFEST=""
ENV_OVERRIDES=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --timeout)
      [[ $# -ge 2 && "$2" =~ ^[0-9]+$ ]] || die "--timeout needs a number of seconds"
      TIMEOUT="$2"
      shift 2
      ;;
    --raw)
      RAW=true
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    -*)
      usage >&2
      die "unknown option: $1"
      ;;
    *=*)
      [[ -n "$MANIFEST" ]] || die "give the manifest before NAME=VALUE arguments"
      [[ "${1%%=*}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || die "invalid env var name in: $1"
      ENV_OVERRIDES+=("$1")
      shift
      ;;
    *)
      [[ -z "$MANIFEST" ]] || die "only one manifest per run (got '$MANIFEST' and '$1')"
      MANIFEST="$1"
      shift
      ;;
  esac
done

[[ -n "$MANIFEST" ]] || {
  usage >&2
  exit 2
}
[[ -f "$MANIFEST" ]] || die "manifest not found: $MANIFEST (run scripts/render-manifests.sh first)"
grep -q '^kind: Job$' "$MANIFEST" || die "$MANIFEST is not a Job manifest"
require_cmd kubectl jq

CONTAINER=iceberg-k8s

if [[ ${#ENV_OVERRIDES[@]} -gt 0 ]]; then
  JOB="$(kubectl set env --local -f "$MANIFEST" -c "$CONTAINER" "${ENV_OVERRIDES[@]}" -o yaml |
    kubectl create -f - -o name)"
else
  JOB="$(kubectl create -f "$MANIFEST" -o name)"
fi
NAMESPACE="$(kubectl create --dry-run=client -f "$MANIFEST" -o jsonpath='{.metadata.namespace}')"
info "Created $JOB in namespace $NAMESPACE; waiting up to ${TIMEOUT}s for it to complete or fail..."

state="$(wait_for_job "$NAMESPACE" "$JOB" "$TIMEOUT")"

if [[ -z "$state" ]]; then
  err "$JOB did not finish within ${TIMEOUT}s. Inspect it with:"
  err "  kubectl -n $NAMESPACE describe $JOB; kubectl -n $NAMESPACE logs $JOB --all-containers"
  exit 1
fi

# The response is the last line the CLI prints (kubectl logs merges stdout and stderr).
response="$(kubectl -n "$NAMESPACE" logs "$JOB" -c "$CONTAINER" 2>/dev/null | tail -n 1 || true)"

if [[ "$state" == "Failed" ]]; then
  err "$JOB failed."
  [[ -n "$response" ]] && printf '%s\n' "$response"
  err "Logs: kubectl -n $NAMESPACE logs $JOB --all-containers"
  exit 1
fi

if $RAW; then
  printf '%s\n' "$response"
else
  printf '%s\n' "$response" | jq . 2>/dev/null || printf '%s\n' "$response"
fi

if [[ "$(printf '%s' "$response" | jq -r '.ok' 2>/dev/null || true)" == "true" ]]; then
  ok "$JOB completed: ok=true"
  exit 0
fi
err "$JOB completed but the response is not ok=true"
exit 1
