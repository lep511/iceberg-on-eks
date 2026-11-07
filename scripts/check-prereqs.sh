#!/usr/bin/env bash
# Read-only checks of everything this project expects before 'terraform apply' and the lab:
# local tools, the Rust build host (arm64, Graviton5), AWS identity, Graviton5 (M9g/M9gd)
# capacity in the region, the EKS cluster and kubectl access, the Spark Operator and the
# namespaces it watches, the Auto Mode NodeClass and NodePool for Graviton5, and the cluster's
# IAM OIDC provider (IRSA).
#
# It only calls read APIs (sts get-caller-identity, eks describe-cluster,
# ec2 describe-instance-type-offerings, iam list-open-id-connect-providers, kubectl get).
# Exit code: 0 if no check failed, else 1.

set -euo pipefail

# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

usage() {
  cat <<EOF
Usage: scripts/check-prereqs.sh [--region R] [--cluster-name C] [--spark-namespace NS]
                                [--jobs-namespace NS] [--operator-namespace NS]
                                [--node-class NC] [--outputs-file FILE]

Values come from the flags, else from the Terraform outputs (if applied), else from the
defaults: us-west-2, spark-on-eks, spark-team-a, iceberg, spark-operator, ebs-gp3-100gi.

Options:
  --region R              AWS region of the cluster
  --cluster-name C        EKS cluster name
  --spark-namespace NS    namespace for SparkApplications (must be watched by the Spark Operator)
  --jobs-namespace NS     namespace for the Rust CLI Jobs (created by terraform apply)
  --operator-namespace NS namespace of the Spark Operator (default spark-operator)
  --node-class NC         Auto Mode NodeClass of the Graviton5 NodePool (Terraform graviton5_node_class)
  --outputs-file FILE     read the Terraform outputs from FILE (same as TF_OUTPUT_JSON=FILE)
  -h, --help              show this help
EOF
}

REGION_ARG="" CLUSTER_ARG="" SPARK_NS_ARG="" JOBS_NS_ARG="" OPERATOR_NS="spark-operator" NODE_CLASS_ARG=""
while (($#)); do
  case "$1" in
    --region | --cluster-name | --spark-namespace | --jobs-namespace | --operator-namespace | --node-class | --outputs-file)
      [[ $# -ge 2 ]] || die "$1 needs a value"
      case "$1" in
        --region) REGION_ARG="$2" ;;
        --cluster-name) CLUSTER_ARG="$2" ;;
        --spark-namespace) SPARK_NS_ARG="$2" ;;
        --jobs-namespace) JOBS_NS_ARG="$2" ;;
        --operator-namespace) OPERATOR_NS="$2" ;;
        --node-class) NODE_CLASS_ARG="$2" ;;
        --outputs-file) export TF_OUTPUT_JSON="$2" ;;
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

FAILS=0
WARNS=0
pass() { printf '  %sPASS%s  %s\n' "$C_GREEN" "$C_RESET" "$*"; }
fail() {
  printf '  %sFAIL%s  %s\n' "$C_RED" "$C_RESET" "$*"
  FAILS=$((FAILS + 1))
}
warning() {
  printf '  %sWARN%s  %s\n' "$C_YELLOW" "$C_RESET" "$*"
  WARNS=$((WARNS + 1))
}
note() { printf '  INFO  %s\n' "$*"; }
first_line() { head -n1 | cut -c1-100; }

echo "=== Tools ==="
for cmd in aws kubectl terraform jq; do
  if command -v "$cmd" >/dev/null 2>&1; then
    case "$cmd" in
      aws) version="$(aws --version 2>&1 | first_line)" ;;
      kubectl) version="$(kubectl version --client 2>/dev/null | first_line)" ;;
      terraform) version="$(terraform version 2>/dev/null | first_line)" ;;
      jq) version="$(jq --version 2>&1 | first_line)" ;;
    esac
    [[ "$version" =~ [0-9] ]] || version="installed (version not reported)"
    pass "$cmd: $version"
  else
    fail "$cmd is not installed (required)"
  fi
done
if command -v cargo >/dev/null 2>&1; then
  pass "cargo: $(cargo --version 2>/dev/null | first_line) (optional, builds the Rust CLI)"
  # rust/build.sh builds natively for aarch64-unknown-linux-gnu with -Ctarget-cpu=neoverse-v3.
  rust_host="$(rustc -vV 2>/dev/null | sed -n 's/^host: //p')"
  cpu_part="$(awk -F': *' '/^CPU part/ {print $2; exit}' /proc/cpuinfo 2>/dev/null || true)"
  if [[ "$rust_host" != "aarch64-unknown-linux-gnu" ]]; then
    warning "Rust host is ${rust_host:-unknown}: rust/build.sh needs an aarch64 Linux host (the Jobs run on Graviton5)"
  elif [[ "$cpu_part" == "0xd84" ]]; then
    pass "Rust build host: aarch64, Neoverse V3 (Graviton5)"
  else
    note "Rust build host: aarch64, CPU part ${cpu_part:-unknown} (not Neoverse V3): rust/build.sh works, but the Graviton5 binary and cargo test may not run here"
  fi
else
  note "cargo not found (optional: only needed to build the Rust CLI with rust/build.sh)"
fi
for cmd in aws kubectl jq; do
  command -v "$cmd" >/dev/null 2>&1 || {
    echo ""
    die "$cmd is required for the remaining checks"
  }
done

# Values: flags > Terraform outputs > defaults.
TF_STATE="not applied"
if [[ -n "${TF_OUTPUT_JSON:-}" ]] || command -v terraform >/dev/null 2>&1; then
  if (load_tf_outputs) >/dev/null 2>&1; then
    load_tf_outputs
    [[ "$(tf_outputs_count)" -gt 0 ]] && TF_STATE="applied (warehouse bucket $(tf_output warehouse_bucket '?'))"
  else
    TF_STATE="unknown (terraform output failed; run 'terraform -chdir=terraform init')"
  fi
fi
REGION="${REGION_ARG:-$(tf_output region "${AWS_REGION:-${AWS_DEFAULT_REGION:-us-west-2}}")}"
CLUSTER="${CLUSTER_ARG:-$(tf_output cluster_name spark-on-eks)}"
SPARK_NS="${SPARK_NS_ARG:-$(tf_output spark_namespace spark-team-a)}"
JOBS_NS="${JOBS_NS_ARG:-$(tf_output jobs_namespace iceberg)}"
NODE_CLASS="${NODE_CLASS_ARG:-$(tf_output graviton5_node_class ebs-gp3-100gi)}"
NODE_POOL="$(tf_output graviton5_node_pool)"
note "region $REGION | cluster $CLUSTER | spark_namespace $SPARK_NS | jobs_namespace $JOBS_NS"
note "Terraform: $TF_STATE"

echo ""
echo "=== AWS ==="
if identity="$(aws sts get-caller-identity --output json 2>&1)"; then
  pass "identity: $(jq -r '.Arn' <<<"$identity") (account $(jq -r '.Account' <<<"$identity"))"
else
  fail "no AWS identity: $(first_line <<<"$identity")"
fi

# Graviton5 (GRAVITON5.md): every pod of this project runs on M9g/M9gd nodes.
if offerings="$(aws ec2 describe-instance-type-offerings --region "$REGION" --location-type availability-zone \
  --filters Name=instance-type,Values='m9g.*,m9gd.*' --output json 2>&1)"; then
  zones="$(jq -r '[.InstanceTypeOfferings[].Location] | unique | join(" ")' <<<"$offerings")"
  if [[ -n "$zones" ]]; then
    pass "Graviton5 (M9g/M9gd) offered in $REGION: $zones"
  else
    fail "no Graviton5 (M9g/M9gd) instance types are offered in $REGION"
  fi
else
  warning "cannot list EC2 instance type offerings (not verifiable with this identity): $(first_line <<<"$offerings")"
fi

ISSUER=""
CLUSTER_ENDPOINT=""
if cluster="$(aws eks describe-cluster --name "$CLUSTER" --region "$REGION" --output json 2>&1)"; then
  status="$(jq -r '.cluster.status' <<<"$cluster")"
  version="$(jq -r '.cluster.version' <<<"$cluster")"
  ISSUER="$(jq -r '.cluster.identity.oidc.issuer // empty' <<<"$cluster")"
  CLUSTER_ENDPOINT="$(jq -r '.cluster.endpoint // empty' <<<"$cluster")"
  if [[ "$status" == "ACTIVE" ]]; then
    pass "EKS cluster $CLUSTER is ACTIVE (Kubernetes $version)"
  else
    fail "EKS cluster $CLUSTER status is $status"
  fi
else
  fail "cannot describe EKS cluster $CLUSTER in $REGION: $(first_line <<<"$cluster")"
fi

if [[ -n "$ISSUER" ]]; then
  provider_path="${ISSUER#https://}"
  if providers="$(aws iam list-open-id-connect-providers --output json 2>&1)"; then
    arn="$(jq -r --arg p "oidc-provider/$provider_path" \
      '.OpenIDConnectProviderList[].Arn | select(endswith($p))' <<<"$providers" | head -n1)"
    if [[ -n "$arn" ]]; then
      pass "IAM OIDC provider for IRSA: $arn"
    else
      fail "no IAM OIDC provider for $ISSUER (IRSA cannot work; the platform should have created it)"
    fi
  else
    warning "cannot list IAM OIDC providers (not verifiable with this identity): $(first_line <<<"$providers")"
  fi
else
  warning "cluster OIDC issuer unknown: IRSA provider not checked"
fi

echo ""
echo "=== Kubernetes ==="
context="$(kubectl config current-context 2>/dev/null || true)"
if kubectl get --raw /readyz >/dev/null 2>&1; then
  pass "API server reachable (context ${context:-none})"
else
  fail "API server not reachable with context '${context:-none}'. Run: aws eks update-kubeconfig --region $REGION --name $CLUSTER"
  echo ""
  echo "Summary: $FAILS failed, $WARNS warning(s) (Kubernetes checks skipped)"
  exit 1
fi
server="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || true)"
if [[ -n "$CLUSTER_ENDPOINT" && -n "$server" && "${server%/}" != "${CLUSTER_ENDPOINT%/}" ]]; then
  fail "kubectl points to $server, not to cluster $CLUSTER ($CLUSTER_ENDPOINT). Run: aws eks update-kubeconfig --region $REGION --name $CLUSTER"
elif [[ -n "$CLUSTER_ENDPOINT" ]]; then
  pass "kubectl context targets cluster $CLUSTER"
fi

if kubectl get crd sparkapplications.sparkoperator.k8s.io >/dev/null 2>&1; then
  pass "CRD sparkapplications.sparkoperator.k8s.io installed"
else
  fail "CRD sparkapplications.sparkoperator.k8s.io not found (Spark Operator not installed?)"
fi

controller="$(kubectl get deployments -n "$OPERATOR_NS" \
  -l app.kubernetes.io/name=spark-operator,app.kubernetes.io/component=controller -o json 2>/dev/null || echo '{}')"
if [[ "$(jq '.items | length' <<<"$controller" 2>/dev/null || echo 0)" -eq 0 ]]; then
  fail "Spark Operator controller deployment not found in namespace $OPERATOR_NS"
else
  deploy="$(jq -r '.items[0].metadata.name' <<<"$controller")"
  ready="$(jq -r '.items[0].status.readyReplicas // 0' <<<"$controller")"
  operator_version="$(jq -r '.items[0].metadata.labels["app.kubernetes.io/version"] // "unknown"' <<<"$controller")"
  if [[ "$ready" -ge 1 ]]; then
    pass "Spark Operator $operator_version running ($OPERATOR_NS/$deploy, $ready ready)"
  else
    fail "Spark Operator deployment $OPERATOR_NS/$deploy has no ready replica"
  fi
  # --namespaces=a,b,c (or "--namespaces" "a,b,c"); empty or absent means all namespaces.
  watched="$(jq -r '
      .items[0].spec.template.spec.containers[0] | ((.command // []) + (.args // [])) as $a
      | [range(0; $a | length) as $i
         | if ($a[$i] | startswith("--namespaces=")) then ($a[$i] | sub("^--namespaces="; ""))
           elif $a[$i] == "--namespaces" then ($a[$i + 1] // "")
           else empty end] | last // "*"' <<<"$controller")"
  if [[ "$watched" == "*" || -z "$watched" ]]; then
    pass "Spark Operator watches all namespaces (includes $SPARK_NS)"
  elif [[ ",$watched," == *",$SPARK_NS,"* ]]; then
    pass "Spark Operator watches $SPARK_NS (watched: $watched)"
  else
    fail "Spark Operator does not watch $SPARK_NS (watched: $watched). Set spark_namespace to one of them."
  fi
fi

if kubectl get namespace "$SPARK_NS" >/dev/null 2>&1; then
  pass "namespace $SPARK_NS exists"
else
  fail "namespace $SPARK_NS does not exist (spark_namespace must be an EXISTING platform namespace)"
fi

if kubectl get namespace "$JOBS_NS" >/dev/null 2>&1; then
  pass "namespace $JOBS_NS exists"
elif [[ "$TF_STATE" == applied* ]]; then
  fail "namespace $JOBS_NS does not exist although Terraform is applied"
else
  note "namespace $JOBS_NS not created yet (terraform apply creates it)"
fi
if [[ "$JOBS_NS" == "$SPARK_NS" ]]; then
  fail "jobs_namespace and spark_namespace must differ"
fi

# Graviton5 NodePool (terraform/nodepool.tf) and the existing NodeClass it uses.
if kubectl get nodeclasses.eks.amazonaws.com "$NODE_CLASS" >/dev/null 2>&1; then
  pass "Auto Mode NodeClass $NODE_CLASS exists (used by the Graviton5 NodePool)"
else
  fail "Auto Mode NodeClass $NODE_CLASS not found (set graviton5_node_class to one of: $(kubectl get nodeclasses.eks.amazonaws.com -o name 2>/dev/null | sed 's|.*/||' | paste -sd, -))"
fi
if [[ -n "$NODE_POOL" ]]; then
  if families="$(kubectl get nodepools.karpenter.sh "$NODE_POOL" -o json 2>/dev/null | jq -r '
      .spec.template.spec.requirements[] | select(.key == "eks.amazonaws.com/instance-family") | .values | join(",")')"; then
    if [[ "$families" == "m9g,m9gd" ]]; then
      pass "Graviton5 NodePool $NODE_POOL exists (instance families $families)"
    else
      fail "NodePool $NODE_POOL allows instance families '${families:-any}', not only Graviton5 (m9g,m9gd)"
    fi
  else
    fail "Graviton5 NodePool $NODE_POOL not found although Terraform is applied"
  fi
else
  note "Graviton5 NodePool not created yet (terraform apply creates it)"
fi

echo ""
if ((FAILS > 0)); then
  err "Summary: $FAILS check(s) failed, $WARNS warning(s)."
  exit 1
fi
ok "Summary: all checks passed ($WARNS warning(s))."
