# shellcheck shell=bash
# Shared helpers for the scripts in this directory. Source it, do not execute it:
#   # shellcheck source=common.sh
#   source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
#
# Provides:
#   REPO_ROOT, TERRAFORM_DIR      project root and Terraform root module
#   info/ok/warn/err/die          messages on stderr
#   require_cmd <cmd>...          fail if a command is missing
#   load_tf_outputs               reads the Terraform outputs into TF_OUTPUTS (flat JSON object of
#                                 the string outputs). Source: the file in $TF_OUTPUT_JSON if set
#                                 (testing, or a saved `terraform output -json`), otherwise
#                                 `terraform -chdir=$TERRAFORM_DIR output -json`.
#   tf_output <name> [default]    value of a string output (default if missing)
#   wait_for_job <ns> <job> <sec> waits until the Job is Complete or Failed; prints the condition
#                                 (empty on timeout)

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "common.sh is a library: source it from another script." >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TERRAFORM_DIR="${TERRAFORM_DIR:-$REPO_ROOT/terraform}"
TF_OUTPUTS='{}'

if [[ -t 2 && -z "${NO_COLOR:-}" ]]; then
  C_RED=$'\033[0;31m' C_GREEN=$'\033[0;32m' C_YELLOW=$'\033[1;33m' C_CYAN=$'\033[0;36m' C_RESET=$'\033[0m'
else
  C_RED='' C_GREEN='' C_YELLOW='' C_CYAN='' C_RESET=''
fi

info() { printf '%s\n' "${C_CYAN}$*${C_RESET}" >&2; }
ok() { printf '%s\n' "${C_GREEN}$*${C_RESET}" >&2; }
warn() { printf '%s\n' "${C_YELLOW}WARNING: $*${C_RESET}" >&2; }
err() { printf '%s\n' "${C_RED}ERROR: $*${C_RESET}" >&2; }
die() {
  err "$@"
  exit 1
}

require_cmd() {
  local cmd missing=()
  for cmd in "$@"; do
    command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
  done
  if ((${#missing[@]})); then
    die "missing required command(s): ${missing[*]} (run scripts/check-prereqs.sh)"
  fi
}

# Reads the Terraform outputs. Keeps only string outputs and flattens {name: {value: ...}}
# (the `terraform output -json` format) to {name: value}.
load_tf_outputs() {
  local raw
  if [[ -n "${TF_OUTPUT_JSON:-}" ]]; then
    [[ -r "$TF_OUTPUT_JSON" ]] || die "TF_OUTPUT_JSON file not readable: $TF_OUTPUT_JSON"
    raw="$(cat -- "$TF_OUTPUT_JSON")"
  else
    require_cmd terraform
    if ! raw="$(terraform -chdir="$TERRAFORM_DIR" output -json 2>/dev/null)"; then
      die "'terraform -chdir=$TERRAFORM_DIR output -json' failed. Run 'terraform -chdir=$TERRAFORM_DIR init' and apply first."
    fi
  fi
  TF_OUTPUTS="$(jq -c '
      if type != "object" then error("Terraform outputs must be a JSON object") else . end
      | with_entries(.value |= (if type == "object" and has("value") then .value else . end))
      | with_entries(select(.value | type == "string"))
    ' <<<"$raw")" || die "could not parse the Terraform outputs as JSON"
}

tf_output() {
  local value
  value="$(jq -r --arg n "$1" '.[$n] // empty' <<<"$TF_OUTPUTS")"
  if [[ -n "$value" ]]; then
    printf '%s\n' "$value"
  else
    printf '%s\n' "${2:-}"
  fi
}

tf_outputs_count() {
  jq 'length' <<<"$TF_OUTPUTS"
}

# Waits until Job <job> (e.g. job.batch/name) in <namespace> has the condition Complete or Failed,
# for at most <timeout> seconds. Prints "Complete", "Failed", or nothing on timeout. A plain
# `kubectl wait --for=condition=complete` would block until the timeout when the Job fails.
wait_for_job() {
  local namespace="$1" job="$2" timeout="$3" state="" deadline
  deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    state="$(kubectl -n "$namespace" get "$job" \
      -o jsonpath='{range .status.conditions[?(@.status=="True")]}{.type}{"\n"}{end}' |
      grep -E '^(Complete|Failed)$' | head -n 1 || true)"
    [[ -n "$state" ]] && break
    sleep 5
  done
  printf '%s\n' "$state"
}
