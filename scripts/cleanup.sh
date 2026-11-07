#!/usr/bin/env bash
# Deletes the lab's Kubernetes workloads, then prints the remaining cleanup steps in order.
#
# Deleted (and nothing else):
#   - SparkApplications / ScheduledSparkApplications defined in spark/manifests, in <spark_namespace>
#   - Jobs / CronJobs defined in rust/manifests (or spark/manifests), in <jobs_namespace>
#   - objects of those kinds, in those two namespaces, that match the lab label selector
#     (default: app.kubernetes.io/part-of in (spark-on-eks-iceberg, iceberg-on-eks))
# Never touched: namespaces, service accounts, RBAC, other SparkApplications, the Spark Operator,
# or any AWS resource. Glue, S3 Metadata and Terraform cleanup are printed as next steps.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source-path=SCRIPTDIR source=common.sh
source "$SCRIPT_DIR/common.sh"

usage() {
  cat <<EOF
Usage: scripts/cleanup.sh [--dry-run] [--yes] [--outputs-file FILE]
                          [--spark-namespace NS] [--jobs-namespace NS] [--selector SELECTOR]

Deletes the lab's SparkApplications (in spark_namespace) and Jobs (in jobs_namespace), then
prints the next steps: the in-cluster Glue cleanup Spark job, the optional S3 Metadata cleanup
(Rust Job), and 'terraform -chdir=terraform destroy'.

Options:
  --dry-run              show what would be deleted (kubectl --dry-run=server), delete nothing
  -y, --yes              do not ask for confirmation
  --outputs-file FILE    read the Terraform outputs from FILE (same as TF_OUTPUT_JSON=FILE)
  --spark-namespace NS   override the Terraform output spark_namespace (default spark-team-a)
  --jobs-namespace NS    override the Terraform output jobs_namespace (default iceberg)
  --selector SELECTOR    label selector that also finds lab workloads (only SparkApplication,
                         ScheduledSparkApplication, Job and CronJob objects in the two
                         namespaces above); default:
                         'app.kubernetes.io/part-of in (spark-on-eks-iceberg,iceberg-on-eks)'
  -h, --help             show this help
EOF
}

DRY_RUN=false
ASSUME_YES=false
SPARK_NS_ARG=""
JOBS_NS_ARG=""
LAB_SELECTOR="app.kubernetes.io/part-of in (spark-on-eks-iceberg,iceberg-on-eks)"
while (($#)); do
  case "$1" in
    --dry-run) DRY_RUN=true ;;
    -y | --yes) ASSUME_YES=true ;;
    --outputs-file | --spark-namespace | --jobs-namespace | --selector)
      [[ $# -ge 2 ]] || die "$1 needs a value"
      case "$1" in
        --outputs-file) export TF_OUTPUT_JSON="$2" ;;
        --spark-namespace) SPARK_NS_ARG="$2" ;;
        --jobs-namespace) JOBS_NS_ARG="$2" ;;
        --selector) LAB_SELECTOR="$2" ;;
      esac
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

[[ "$LAB_SELECTOR" == *[![:space:]]* ]] || die "--selector must not be empty"
require_cmd kubectl jq

load_tf_outputs
HAVE_OUTPUTS=true
if [[ "$(tf_outputs_count)" -eq 0 ]]; then
  HAVE_OUTPUTS=false
  warn "no Terraform outputs (not applied, or already destroyed): using defaults and build/manifests"
fi
SPARK_NS="${SPARK_NS_ARG:-$(tf_output spark_namespace spark-team-a)}"
JOBS_NS="${JOBS_NS_ARG:-$(tf_output jobs_namespace iceberg)}"
[[ "$SPARK_NS" != "$JOBS_NS" ]] || die "spark_namespace and jobs_namespace must differ ($SPARK_NS)"

kubectl get --raw /readyz >/dev/null 2>&1 ||
  die "the Kubernetes API is not reachable with the current kubeconfig ($(kubectl config current-context 2>/dev/null || echo 'no context'))"

# Rendered manifests: render fresh copies into a temp dir when the outputs exist,
# otherwise use the last render in build/manifests.
WORK="$(mktemp -d)"
trap 'rm -rf -- "$WORK"' EXIT
MANIFESTS="$REPO_ROOT/build/manifests"
if $HAVE_OUTPUTS; then
  if "$SCRIPT_DIR/render-manifests.sh" --out-dir "$WORK/manifests" >/dev/null 2>"$WORK/render.log"; then
    MANIFESTS="$WORK/manifests"
  else
    warn "rendering the manifests failed, using build/manifests instead:"
    sed 's/^/    /' "$WORK/render.log" >&2
  fi
fi

info "Context: $(kubectl config current-context) | spark_namespace: $SPARK_NS | jobs_namespace: $JOBS_NS | selector: $LAB_SELECTOR"

# kind -> kubectl resource (kind.group) and the only namespace where it may be deleted.
resource_of() {
  case "$1" in
    SparkApplication) echo "sparkapplication.sparkoperator.k8s.io" ;;
    ScheduledSparkApplication) echo "scheduledsparkapplication.sparkoperator.k8s.io" ;;
    Job) echo "job.batch" ;;
    CronJob) echo "cronjob.batch" ;;
    *) return 1 ;;
  esac
}
namespace_for() {
  case "$1" in
    SparkApplication | ScheduledSparkApplication) echo "$SPARK_NS" ;;
    *) echo "$JOBS_NS" ;;
  esac
}

TARGETS="$WORK/targets.tsv" # namespace <TAB> resource/name
: >"$TARGETS"
# Prefixes each line of stdin with "<namespace><TAB>".
with_ns() { awk -v ns="$1" 'NF { print ns "\t" $0 }'; }

# 1) Workloads defined in the manifests.
for component in spark rust; do
  shopt -s nullglob
  files=("$MANIFESTS/$component"/*.yaml)
  shopt -u nullglob
  for f in "${files[@]}"; do
    if ! objects="$(kubectl create --dry-run=client -o json -f "$f" 2>"$WORK/parse.log")"; then
      warn "could not parse $f: $(head -c 300 "$WORK/parse.log")"
      continue
    fi
    # Fields are separated by \x1f, not tabs: with a whitespace IFS, empty fields would collapse.
    while IFS=$'\x1f' read -r kind ns name generate_name selector; do
      resource="$(resource_of "$kind")" || continue
      expected_ns="$(namespace_for "$kind")"
      ns="${ns:-$expected_ns}"
      if [[ "$ns" != "$expected_ns" ]]; then
        warn "skipping $kind ${name:-$generate_name} from $(basename "$f"): namespace $ns is not $expected_ns"
        continue
      fi
      if [[ -n "$name" ]]; then
        printf '%s\t%s/%s\n' "$ns" "$resource" "$name" >>"$TARGETS"
      elif [[ -n "$generate_name" && -n "$selector" ]]; then
        # generateName objects: live objects with that name prefix AND all the template's labels.
        kubectl get "$resource" -n "$ns" -l "$selector" -o name 2>/dev/null |
          awk -v p="$resource/$generate_name" 'index($0, p) == 1' | with_ns "$ns" >>"$TARGETS" || true
      elif [[ -n "$generate_name" ]]; then
        warn "skipping $kind $generate_name* from $(basename "$f"): generateName without labels cannot be matched safely"
      fi
    done < <(jq -r '
        (if .kind == "List" then .items[] else . end)
        | [.kind, (.metadata.namespace // ""), (.metadata.name // ""), (.metadata.generateName // ""),
           ((.metadata.labels // {}) | to_entries | map("\(.key)=\(.value)") | join(","))]
        | join("\u001f")' <<<"$objects")
  done
done

# 2) Workloads that carry the lab label (e.g. created by hand from a manifest copy).
kubectl get sparkapplications.sparkoperator.k8s.io,scheduledsparkapplications.sparkoperator.k8s.io \
  -n "$SPARK_NS" -l "$LAB_SELECTOR" -o name 2>/dev/null | with_ns "$SPARK_NS" >>"$TARGETS" || true
kubectl get jobs.batch,cronjobs.batch -n "$JOBS_NS" -l "$LAB_SELECTOR" -o name 2>/dev/null |
  with_ns "$JOBS_NS" >>"$TARGETS" || true

# Keep only what exists now.
LIVE=()
while IFS=$'\t' read -r ns obj; do
  [[ -n "$obj" ]] || continue
  if [[ -n "$(kubectl get "$obj" -n "$ns" --ignore-not-found -o name 2>/dev/null)" ]]; then
    LIVE+=("$ns"$'\t'"$obj")
  fi
done < <(sort -u "$TARGETS")

echo ""
echo "=== Lab workloads ==="
if ((${#LIVE[@]} == 0)); then
  ok "No lab SparkApplications or Jobs found."
else
  for entry in "${LIVE[@]}"; do
    printf '  [x] %s (namespace %s)\n' "${entry#*$'\t'}" "${entry%%$'\t'*}"
  done
  if $DRY_RUN; then
    for entry in "${LIVE[@]}"; do
      kubectl delete "${entry#*$'\t'}" -n "${entry%%$'\t'*}" --dry-run=server 2>&1 | sed 's/^/      /'
    done
    info "Dry run: nothing was deleted."
  else
    if ! $ASSUME_YES; then
      [[ -t 0 ]] || die "not running interactively: pass --yes to delete the ${#LIVE[@]} object(s) above"
      read -r -p "Delete the ${#LIVE[@]} object(s) above? [y/N] " answer
      [[ "$answer" =~ ^[Yy]$ ]] || die "aborted, nothing was deleted"
    fi
    for entry in "${LIVE[@]}"; do
      kubectl delete "${entry#*$'\t'}" -n "${entry%%$'\t'*}" --ignore-not-found --wait=true --timeout=120s
    done
    ok "Deleted ${#LIVE[@]} lab workload(s)."
  fi
fi

# --- Next steps ---------------------------------------------------------------------------------

# Prints the commands that run one rendered manifest and wait for it:
#   run_steps <rendered file or ""> <path shown to the user> <kind> <namespace> <wait condition>
# With metadata.generateName the object must be created with 'kubectl create' (apply cannot).
run_steps() {
  local file=$1 shown=$2 kind=$3 ns=$4 condition=$5 meta="" name="" generate_name=""
  if [[ -n "$file" && -f "$file" ]]; then
    meta="$(kubectl create --dry-run=client -o json -f "$file" 2>/dev/null |
      jq -r --arg k "$kind" '(if .kind == "List" then .items[] else . end) | select(.kind == $k)
        | [(.metadata.name // ""), (.metadata.generateName // "")] | join("\u001f")' | head -n1)" || true
    IFS=$'\x1f' read -r name generate_name <<<"$meta" || true
  fi
  if [[ -n "$name" ]]; then
    printf '     kubectl apply -f %s\n' "$shown"
    printf '     kubectl -n %s wait %s/%s %s\n' "$ns" "${kind,,}" "$name" "$condition"
  elif [[ "$kind" == "Job" ]]; then
    # run-job.sh creates the Job, waits until it completes OR fails, and prints the response.
    printf '     scripts/run-job.sh %s\n' "$shown"
  else
    # shellcheck disable=SC2016 # $OBJ is meant literally: the user runs these lines.
    printf '     OBJ=$(kubectl create -f %s -o name)\n' "$shown"
    # shellcheck disable=SC2016
    printf '     kubectl -n %s wait "$OBJ" %s\n' "$ns" "$condition"
  fi
}

# Object names come from the rendered manifests, else from the templates (names have no placeholders).
CLEANUP_APP=""
for f in "$MANIFESTS/spark/spark-iceberg-cleanup.yaml" "$REPO_ROOT/spark/manifests/spark-iceberg-cleanup.yaml"; do
  if [[ -f "$f" ]]; then
    CLEANUP_APP="$f"
    break
  fi
done
METADATA_JOB=""
for dir in "$MANIFESTS/rust" "$REPO_ROOT/rust/manifests"; do
  METADATA_JOB="$(grep -lE 'delete[_-]metadata[_-]config' "$dir"/*.yaml 2>/dev/null | head -n1 || true)"
  [[ -z "$METADATA_JOB" ]] || break
done
METADATA_JOB_SHOWN="build/manifests/rust/<the delete_metadata_config Job manifest>"
[[ -n "$METADATA_JOB" ]] && METADATA_JOB_SHOWN="build/manifests/rust/$(basename "$METADATA_JOB")"
BUCKET="$(tf_output warehouse_bucket '<warehouse_bucket>')"
REGION="$(tf_output region us-west-2)"
TABLE_BUCKET_ARN="arn:aws:s3tables:$REGION:<account id>:bucket/aws-s3"

cat <<EOF

=== Next steps (in this order) ===

The Glue database and tables were created in the cluster with the Spark IRSA role, and the workshop
IDE role has no Glue permissions: clean them up IN THE CLUSTER, before 'terraform destroy'
removes the IRSA roles and the service accounts.

1. Drop the Iceberg tables (PURGE) and the Glue database with the Spark cleanup job
   (spark/iceberg_cleanup.py, uploaded by scripts/upload-artifacts.sh):

     scripts/render-manifests.sh
EOF
run_steps "$CLEANUP_APP" "build/manifests/spark/spark-iceberg-cleanup.yaml" SparkApplication "$SPARK_NS" \
  "--for=jsonpath='{.status.applicationState.state}'=COMPLETED --timeout=30m"
cat <<EOF

   Check the driver log, then run scripts/cleanup.sh again to delete the finished application.

2. Optional: if you enabled S3 Metadata on the warehouse bucket, delete its configuration with
   the Rust CLI Job that runs delete_metadata_config:

EOF
run_steps "$METADATA_JOB" "$METADATA_JOB_SHOWN" Job "$JOBS_NS" "--for=condition=complete --timeout=15m"
cat <<EOF

   Deleting the configuration does NOT delete the metadata tables: journal, inventory and
   annotation stay in the AWS managed table bucket aws-s3, namespace b_$BUCKET, and keep
   their storage costs. Deleting them needs s3tables:DeleteTable and s3tables:DeleteNamespace,
   which the IRSA roles do not have (and the IDE role probably not either); with an identity
   that has them:

     for t in journal inventory annotation; do
       aws s3tables delete-table --region $REGION --namespace "b_$BUCKET" --name "\$t" \\
         --table-bucket-arn "$TABLE_BUCKET_ARN"
     done
     aws s3tables delete-namespace --region $REGION --namespace "b_$BUCKET" \\
       --table-bucket-arn "$TABLE_BUCKET_ARN"

   (Right after the configuration is deleted, delete-table can fail for a short while: retry.)

3. Delete the AWS and Kubernetes resources of this project (warehouse bucket and all its
   objects, IRSA roles, jobs namespace with its Jobs, service accounts):

     terraform -chdir=terraform destroy

scripts/cleanup.sh can be run again at any time; it only deletes lab workloads that still exist.
EOF
if [[ -z "$CLEANUP_APP" ]]; then
  warn "spark/manifests/spark-iceberg-cleanup.yaml not found: step 1 needs it (it is part of the Spark component)"
fi
if [[ -z "$METADATA_JOB" ]]; then
  warn "no rust/manifests/*.yaml runs delete_metadata_config yet: step 2 needs that Job manifest"
fi
