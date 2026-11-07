#!/usr/bin/env bash
# Computes the trip statistics of the shared table with a scan split across N pods, in three
# steps, and prints the merged result:
#   1. plan   (job-trip-stats-plan.yaml):  pins the current snapshot and assigns the data files
#                                          to N shards, balanced by size
#   2. shards (job-trip-stats-shard.yaml): Indexed Job, one pod per shard; each one reads only
#                                          its data files and writes partial statistics
#   3. merge  (job-trip-stats-merge.yaml): checks the partial results against the plan and
#                                          merges them
# The run's documents stay in s3://<warehouse bucket>/lab/runs/<run id>/: plan.json,
# shards/shard-<index>.json and result.json.
#
# Exit code: 0 if every step succeeded and the merge response has "ok": true, 1 otherwise.
set -euo pipefail

# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
SCRIPT_DIR="$REPO_ROOT/scripts"

usage() {
  cat <<'EOF'
Usage: scripts/run-distributed-stats.sh [--shards N] [--parallelism N] [--run-id ID]
                                        [--manifest-dir DIR] [--timeout SECONDS] [--raw]

Runs plan, shards (Indexed Job) and merge for the trip statistics of the shared table and prints
the merge response. Render the manifests first (scripts/render-manifests.sh).

Options:
  --shards N          number of shards, one pod each (default 4, at most 1000). Shards beyond
                      the number of data files stay empty.
  --parallelism N     shards running at the same time (default: all of them)
  --run-id ID         run id: lowercase letters, digits and '-', at most 63 characters
                      (default: stats-<UTC time>-<random>). A run id can be used only once.
  --manifest-dir DIR  rendered Job manifests (default build/manifests/rust)
  --timeout SECONDS   time limit of each step (default 1800)
  --raw               print the merge response as is (no jq formatting)
  -h, --help          show this help

Example:
  scripts/run-distributed-stats.sh --shards 3 | jq '.data.trip_stats'
EOF
}

SHARDS=4
PARALLELISM=""
RUN_ID=""
TIMEOUT=1800
RAW=false
MANIFEST_DIR="$REPO_ROOT/build/manifests/rust"

while (($#)); do
  case "$1" in
    --shards | --parallelism | --run-id | --manifest-dir | --timeout)
      [[ $# -ge 2 ]] || die "$1 needs a value"
      case "$1" in
        --shards) SHARDS="$2" ;;
        --parallelism) PARALLELISM="$2" ;;
        --run-id) RUN_ID="$2" ;;
        --manifest-dir) MANIFEST_DIR="$2" ;;
        --timeout) TIMEOUT="$2" ;;
      esac
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
    *)
      usage >&2
      die "unknown argument: $1"
      ;;
  esac
done

# in_range VALUE MIN MAX: VALUE is a decimal integer between MIN and MAX.
# Base 10 is explicit so that a leading zero is not read as octal.
in_range() { [[ "$1" =~ ^[0-9]{1,9}$ ]] && ((10#$1 >= $2 && 10#$1 <= $3)); }
in_range "$SHARDS" 1 1000 || die "--shards must be between 1 and 1000, got '$SHARDS'"
SHARDS=$((10#$SHARDS))
PARALLELISM="${PARALLELISM:-$SHARDS}"
in_range "$PARALLELISM" 1 "$SHARDS" ||
  die "--parallelism must be between 1 and --shards ($SHARDS), got '$PARALLELISM'"
PARALLELISM=$((10#$PARALLELISM))
in_range "$TIMEOUT" 1 999999999 || die "--timeout needs a number of seconds, got '$TIMEOUT'"
TIMEOUT=$((10#$TIMEOUT))
RUN_ID="${RUN_ID:-stats-$(date -u +%Y%m%d-%H%M%S)-$(printf '%04x' "$RANDOM")}"
[[ "$RUN_ID" =~ ^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$ ]] ||
  die "--run-id must be lowercase letters, digits and '-' (at most 63, no '-' at the ends), got '$RUN_ID'"

require_cmd kubectl jq
PLAN_MANIFEST="$MANIFEST_DIR/job-trip-stats-plan.yaml"
SHARD_MANIFEST="$MANIFEST_DIR/job-trip-stats-shard.yaml"
MERGE_MANIFEST="$MANIFEST_DIR/job-trip-stats-merge.yaml"
for manifest in "$PLAN_MANIFEST" "$SHARD_MANIFEST" "$MERGE_MANIFEST"; do
  [[ -f "$manifest" ]] || die "manifest not found: $manifest (run scripts/render-manifests.sh first)"
done
CONTAINER=iceberg-k8s
STARTED=$SECONDS

info "Run $RUN_ID: $SHARDS shard(s), $PARALLELISM at a time"

# --- 1. Plan -------------------------------------------------------------------------------
info "==> 1/3 plan"
if ! plan="$("$SCRIPT_DIR/run-job.sh" --raw --timeout "$TIMEOUT" "$PLAN_MANIFEST" \
  RUN_ID="$RUN_ID" SHARD_COUNT="$SHARDS")"; then
  [[ -n "${plan:-}" ]] && printf '%s\n' "$plan" >&2
  die "plan step failed"
fi
jq -r '.data
  | "  snapshot \(.snapshot_id // "none"): \(.total_files) data files, \(.total_bytes) bytes, "
    + "expected rows \(.expected_rows // "unknown")",
    (.shards[] | "  shard \(.index): \(.files) files, \(.bytes) bytes")' <<<"$plan" >&2

# --- 2. Shards (Indexed Job) ---------------------------------------------------------------
info "==> 2/3 shards: Indexed Job with $SHARDS completions, parallelism $PARALLELISM"
NAMESPACE="$(kubectl create --dry-run=client -f "$SHARD_MANIFEST" -o jsonpath='{.metadata.namespace}')"
JOB="$(kubectl set env --local -f "$SHARD_MANIFEST" -c "$CONTAINER" RUN_ID="$RUN_ID" -o json |
  jq --argjson n "$SHARDS" --argjson p "$PARALLELISM" \
    '.spec.completions = $n | .spec.parallelism = $p' |
  kubectl create -f - -o name)"
info "Created $JOB in namespace $NAMESPACE; waiting up to ${TIMEOUT}s for it to complete or fail..."
state="$(wait_for_job "$NAMESPACE" "$JOB" "$TIMEOUT")"

# One line per pod (retried shards have several), from the response each pod printed last.
kubectl -n "$NAMESPACE" get pods -l "batch.kubernetes.io/job-name=${JOB#*/}" -o json |
  jq -r '.items[]
    | [(.metadata.annotations["batch.kubernetes.io/job-completion-index"] // "?"),
       .metadata.name, .status.phase, (.spec.nodeName // "-")] | @tsv' |
  sort -n |
  while IFS=$'\t' read -r index pod phase node; do
    response="$(kubectl -n "$NAMESPACE" logs "$pod" -c "$CONTAINER" 2>/dev/null | tail -n 1 || true)"
    summary="$(jq -r 'if .ok then "\(.data.files_read) files, \(.data.rows_read) rows, \(.data.elapsed_ms) ms"
                      else "error: \(.message)" end' <<<"$response" 2>/dev/null || echo "no response")"
    printf '  shard %s  %-10s %-20s %s\n' "$index" "$phase" "$node" "$summary" >&2
  done

if [[ "$state" != "Complete" ]]; then
  if [[ -z "$state" ]]; then
    err "$JOB did not finish within ${TIMEOUT}s."
  else
    failed="$(kubectl -n "$NAMESPACE" get "$JOB" -o jsonpath='{.status.failedIndexes}' 2>/dev/null || true)"
    err "$JOB failed${failed:+ (failed shards: $failed)}."
  fi
  err "Inspect it with: kubectl -n $NAMESPACE describe $JOB; kubectl -n $NAMESPACE logs $JOB --all-containers"
  exit 1
fi
ok "$JOB completed: $SHARDS shard(s)"

# --- 3. Merge ------------------------------------------------------------------------------
info "==> 3/3 merge"
merge_args=(--timeout "$TIMEOUT")
$RAW && merge_args+=(--raw)
status=0
"$SCRIPT_DIR/run-job.sh" "${merge_args[@]}" "$MERGE_MANIFEST" RUN_ID="$RUN_ID" || status=$?

run_uri="$(jq -r '.data.run_uri // empty' <<<"$plan")"
info "Run $RUN_ID finished in $((SECONDS - STARTED))s; documents in ${run_uri:-lab/runs/$RUN_ID}/"
exit "$status"
