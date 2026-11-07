#!/usr/bin/env bash
# Renders the Kubernetes manifest templates with the Terraform outputs.
#
# For every string output foo_bar, the placeholder <FOO_BAR> is replaced in each *.yaml of
# spark/manifests and rust/manifests. Results go to build/manifests/<component>/. The
# replacement is literal (jq split/join): values are never interpreted as regex or sed syntax.
# Fails, without touching build/manifests, if any <UPPER_CASE> placeholder remains (on a
# full-line YAML comment it is only a warning).

set -euo pipefail

# shellcheck source-path=SCRIPTDIR source=common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

COMPONENTS=(spark rust)

usage() {
  cat <<EOF
Usage: scripts/render-manifests.sh [--outputs-file FILE] [--out-dir DIR] [--src-dir DIR]

Renders spark/manifests/*.yaml and rust/manifests/*.yaml into <out-dir>/<component>/,
replacing <FOO_BAR> with the Terraform string output foo_bar.

Options:
  --outputs-file FILE  read the outputs from FILE (format of 'terraform output -json')
                       instead of running terraform; same as TF_OUTPUT_JSON=FILE
  --out-dir DIR        output root (default: build/manifests)
  --src-dir DIR        directory that contains <component>/manifests (default: the project
                       root); lets you test templates kept outside the project
  -h, --help           show this help

Environment:
  TF_OUTPUT_JSON       same as --outputs-file
  MANIFESTS_SRC_DIR    same as --src-dir
  TERRAFORM_DIR        Terraform root module (default: terraform/)

The script is idempotent: each run replaces the rendered *.yaml files of a component.
EOF
}

OUT_DIR="$REPO_ROOT/build/manifests"
SRC_DIR="${MANIFESTS_SRC_DIR:-$REPO_ROOT}"
while (($#)); do
  case "$1" in
    --outputs-file)
      [[ $# -ge 2 ]] || die "--outputs-file needs a value"
      export TF_OUTPUT_JSON="$2"
      shift
      ;;
    --out-dir)
      [[ $# -ge 2 ]] || die "--out-dir needs a value"
      OUT_DIR="$2"
      shift
      ;;
    --src-dir)
      [[ $# -ge 2 ]] || die "--src-dir needs a value"
      SRC_DIR="$2"
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

require_cmd jq
load_tf_outputs
if [[ "$(tf_outputs_count)" -eq 0 ]]; then
  die "no Terraform string outputs found. Run 'terraform -chdir=terraform apply' first (or pass --outputs-file)."
fi

# Placeholder -> value map, e.g. {"<WAREHOUSE_BUCKET>": "spark-on-eks-iceberg-..."}.
PLACEHOLDERS="$(jq -c 'with_entries(.key |= "<" + ascii_upcase + ">")' <<<"$TF_OUTPUTS")"
info "Placeholders from Terraform outputs: $(jq -r 'keys | join(" ")' <<<"$PLACEHOLDERS")"

STAGING="$(mktemp -d)"
trap 'rm -rf -- "$STAGING"' EXIT

rendered=0
unresolved=0
for component in "${COMPONENTS[@]}"; do
  src="$SRC_DIR/$component/manifests"
  mkdir -p "$STAGING/$component"
  shopt -s nullglob
  templates=("$src"/*.yaml)
  shopt -u nullglob
  if ((${#templates[@]} == 0)); then
    warn "no templates in $src/ (nothing to render for $component)"
    continue
  fi
  for template in "${templates[@]}"; do
    target="$STAGING/$component/$(basename "$template")"
    # -R -s: whole file as one string; -j: no extra newline. split/join is a literal replace.
    jq -R -s -j --argjson map "$PLACEHOLDERS" \
      'reduce ($map | to_entries[]) as $p (.; split($p.key) | join($p.value))' \
      "$template" >"$target"
    # Any placeholder left means a missing Terraform output or a typo in the template. On a
    # full-line comment it cannot break the manifest, so it is only a warning there.
    while IFS=: read -r line placeholder; do
      if sed -n "${line}p" "$target" | grep -qE '^[[:space:]]*#'; then
        warn "${template#"$REPO_ROOT"/}:$line: unresolved $placeholder in a comment (left as is)"
      else
        err "${template#"$REPO_ROOT"/}:$line: unresolved placeholder $placeholder"
        unresolved=$((unresolved + 1))
      fi
    done < <(grep -noE '<[A-Z][A-Z0-9_]*>' "$target" || true)
    rendered=$((rendered + 1))
  done
done

if ((unresolved > 0)); then
  die "$unresolved unresolved placeholder(s); nothing was written to $OUT_DIR. Valid placeholders: $(jq -r 'keys | join(" ")' <<<"$PLACEHOLDERS")"
fi

for component in "${COMPONENTS[@]}"; do
  dest="$OUT_DIR/$component"
  mkdir -p "$dest"
  # Replace the previous render of this component (only *.yaml files, nothing else).
  find "$dest" -maxdepth 1 -type f -name '*.yaml' -delete
  shopt -s nullglob
  files=("$STAGING/$component"/*.yaml)
  shopt -u nullglob
  for f in "${files[@]}"; do
    mv -- "$f" "$dest/"
    printf '%s\n' "$dest/$(basename "$f")"
  done
done

ok "Rendered $rendered manifest(s) into $OUT_DIR"
