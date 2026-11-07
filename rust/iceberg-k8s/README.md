# iceberg-k8s

Rust CLI that reads and writes Apache Iceberg tables through the AWS Glue Data Catalog, manages
S3 Metadata configurations and S3 Object Annotations. It is a port of the reference
`iceberg-lambda-s3` AWS Lambda (`apache-iceberg-s3-metadata/lambda/`) to a binary that runs as a
Kubernetes Job on EKS with IRSA credentials. It keeps the Lambda's 13 actions and its
request/response JSON, so the reference README's payloads work unchanged. Three more actions
compute the trip statistics of a table with the scan split across the pods of an Indexed Job
(see [Distributed trip stats](#distributed-trip-stats)).

The crate started from the reference `Cargo.lock` (iceberg 0.10.1); `lambda_runtime` was dropped
and `clap` added. It now uses iceberg, iceberg-catalog-glue and iceberg-storage-opendal 0.11.0,
arrow/parquet 58.4 and Rust edition 2024 (Rust 1.95 or later). Keep `arrow-*`/`parquet` on the
version that `iceberg` uses internally (58.4 for 0.11.0).

## Build and test

```bash
cargo test                       # 84 offline tests (73 unit + 11 CLI); no AWS credentials needed
cargo clippy --all-targets -- -D warnings
# Optional, read-only AWS check of the FileIO credential path (reads one object):
ICEBERG_K8S_TEST_S3_URI=s3://<bucket>/<key> AWS_REGION=us-west-2 cargo test -- --ignored
../build.sh                      # release build for x86_64-unknown-linux-gnu -> ../../build/bin/iceberg-k8s
```

`build.sh` must run on the x86_64 Amazon Linux 2023 IDE: the binary links the host glibc (2.34).
It needs no OpenSSL (all TLS is rustls) but reads the system CA bundle at run time.

## Usage

```
iceberg-k8s [--request <file>|-] [--request-json <json>]
            [--action A] [--namespace N] [--table T] [--bucket B] [--key K]
            [--run-uri URI] [--shard-count N] [--shard-index N]
```

- The request is a JSON object (default `{}`), read from a file, stdin (`--request -`) or
  `--request-json`. Flags override the matching request fields, so the 5,000-row payload (whose
  action is `write`) can be sent as another action:

  ```bash
  ICEBERG_WAREHOUSE=s3://<bucket>/warehouse iceberg-k8s --request nyctaxi_payload.json \
    --action write_and_annotate --bucket <bucket> --namespace nyc_taxi_database --table nyc_taxi_table
  ```

- Output: the response JSON on one line on stdout; logs go to stderr (`RUST_LOG`, default `warn`).
- Exit code: `0` when `ok` is true, `1` when it is false, `2` for usage errors (invalid flags, or a
  request that cannot be read or parsed, e.g. no `action`). Usage errors about the request also
  print a `{"ok":false,"message":...}` response.
- Environment: `ICEBERG_WAREHOUSE` (required for every action, `s3://<bucket>/warehouse`; `s3a://`
  also works), `AWS_REGION`, `RUST_LOG`, and `JOB_COMPLETION_INDEX` (the shard of
  `trip_stats_shard` when `--shard-index` is not given; Kubernetes sets it in Indexed Jobs).

```console
$ iceberg-k8s --request-json '{}'; echo "exit=$?"
{"ok":false,"message":"missing field `action`: set it in the request JSON or with --action"}
exit=2
```

## Actions

| Action | Required fields | Notes |
|---|---|---|
| `read` | `namespace`, `table` | Full scan; row count in `rows_read` |
| `write` | `namespace`, `table`, `rows` | One Parquet file (zstd unless the table's `write.parquet.*` properties say otherwise), fast-append commit |
| `write_and_annotate` | `namespace`, `table`, `bucket`, `rows` | `write`, then a `trip_stats` annotation on each new data file (`data.annotated_keys`) |
| `create_metadata_config` | `bucket` | Optional `metadata_config` (journal expiration, inventory, annotation); SSE-S3 |
| `get_metadata_config` | `bucket` | `data.configuration` is SDK debug text |
| `delete_metadata_config` | `bucket` | The tables stay in the `aws-s3` table bucket |
| `query_metadata_table` | `bucket`, `table` (`journal`, `inventory`, `annotation`) | Row count in `rows_read` |
| `put_annotation` | `bucket`, `key`, `annotation_name` | Optional `annotation_value` |
| `get_annotation` | `bucket`, `key`, `annotation_name` | |
| `delete_annotation` | `bucket`, `key`, `annotation_name` | |
| `list_annotations` | `bucket`, `key` | |
| `bulk_annotate` | `bucket`, `annotations` | Up to 10 in parallel |
| `query_annotation_table` | `bucket` | Optional `filter_name`, `key_prefix`, `limit` (100) |
| `plan_trip_stats` | `namespace`, `table`, `run_uri`, `shard_count` | Step 1 of [Distributed trip stats](#distributed-trip-stats): writes `<run_uri>/plan.json` |
| `trip_stats_shard` | `run_uri` | Step 2, one run per shard: `shard_index` or `JOB_COMPLETION_INDEX` |
| `merge_trip_stats` | `run_uri` | Step 3: merged statistics in `data.trip_stats`; writes `<run_uri>/result.json` |

The Iceberg table must already exist (the Spark load job creates it). The `rows` schema is the
23-column NYC Taxi table (all fields required).

Changes from the Lambda (same JSON contract):

- `write_and_annotate` derives each data file's object key from its full path for `s3://`,
  `s3a://` and `s3n://` table locations. A file outside `bucket` is skipped with a warning instead
  of being sent to S3 with the whole URI as the key.
- `query_metadata_table` loads the table through the S3 Tables API (like
  `query_annotation_table`) instead of the Glue default catalog, which cannot see
  `s3tablescatalog`.
- `ICEBERG_WAREHOUSE` must be an S3 URI.
- Data files follow the table's `write.parquet.*` properties (iceberg's
  `ParquetWriterBuilder::from_table_properties`). Compression is zstd level 3 by default, like
  Spark, and `write.parquet.compression-codec` / `write.parquet.compression-level` override it.
  The Lambda used the parquet-rs defaults, which write uncompressed files: 807,046 bytes against
  408,283 with zstd for the 5,000-row payload. The `info` log shows the codec
  (`writing Parquet data files compression=Zstd(level=3)`).

## Distributed trip stats

One process can only read as fast as one pod. These three actions split the scan of a table
across pods, map-reduce style, and compute the same statistics as `write_and_annotate`'s
`trip_stats` (`compute_trip_stats`) for the whole table. They need no shuffle, because every
statistic can be merged from partial results.

1. **`plan_trip_stats`** pins the current snapshot and plans the scan once. It assigns the data
   files to `shard_count` shards, largest first to the shard with the fewest bytes, and writes
   `<run_uri>/plan.json` with the snapshot id and each shard's file paths. The write is
   conditional (`If-None-Match: *`), so a run URI cannot be reused.
2. **`trip_stats_shard`** runs once per shard. It plans the scan of the plan's snapshot again,
   keeps the tasks of its files (and fails if one is missing or duplicated), reads only the six
   columns the statistics use, and writes `<run_uri>/shards/shard-<index>.json`. A retried shard
   overwrites its own result.
3. **`merge_trip_stats`** reads every shard result and checks it against the plan: same snapshot,
   same shard count, and exactly the planned number of files. When the table has no delete files,
   it also checks that the rows read equal the record counts in the manifests. It then merges the
   results and writes `<run_uri>/result.json`.

Design notes:

- **Why the plan holds paths and not `FileScanTask`s.** In iceberg 0.11 every planned task
  carries its partition data, and serializing that field is not implemented. So each shard plans
  the pinned snapshot again, and the library still builds its tasks, delete files included.
- **Exact merges.** Sums are integers in micro-units (1e-6), not floats. The merged result is
  therefore the same for any number of shards and any merge order, and it equals
  `compute_trip_stats` on the same rows (a test checks this on the 5,000-row payload). Values
  that are not finite, or larger than 1e12, are rejected.
- **Limits.**
  - The smallest unit of work is one data file: at most one shard per file does useful work.
  - Statistics that cannot be merged from partial results (joins, exact distinct counts,
    medians) are not supported.
  - Every shard reads the manifests again during planning, and every shard downloads the whole
    `plan.json`.

## Credentials on EKS (IRSA)

The EKS Pod Identity webhook injects `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_REGION`
and `AWS_STS_REGIONAL_ENDPOINTS=regional` into pods whose service account has the
`eks.amazonaws.com/role-arn` annotation. No static keys are used.

- S3 and S3 Tables clients: `aws-config` default chain (env, profile, web identity, ECS/Pod
  Identity, IMDS); the SDK identity cache refreshes the credentials before they expire.
- Glue client: `iceberg-catalog-glue` loads its own `aws-config` default chain, so IRSA works.
- Iceberg `FileIO` (opendal S3): the CLI passes a credential loader backed by the same
  `aws-config` chain, cached per process and refreshed 5 minutes before expiry (`src/aws.rs`).
  opendal's own chain (reqsign) also supports web identity, but `iceberg-storage-opendal` creates
  a new operator, with an empty credential cache, for every file operation, which would call STS
  once per metadata, manifest or data file access.

The projected token is rotated by the kubelet and re-read on every refresh, so jobs that run
longer than the STS session (1 hour) keep working. Do not pass static `s3.access-key-id` /
`s3.secret-access-key` / `s3.session-token` properties instead: those never refresh, and every
S3 request fails once the session expires.

When `AWS_ROLE_ARN` is missing (service account not annotated), the chain falls back to the node
role through IMDS; the `info` log line at startup says which source is expected.

## Kubernetes Jobs

Templates in `../manifests/` (rendered by `scripts/render-manifests.sh` into
`build/manifests/rust/`). Each Job uses the IRSA service account, an `aws-cli` initContainer that
copies `s3://<bucket>/lab/bin/iceberg-k8s` (and the payload when needed) into an `emptyDir`
mounted at `/work`, and runs the binary on `amazonlinux:2023-minimal` as a non-root user with a
read-only root filesystem on an on-demand amd64 node, with `karpenter.sh/do-not-disrupt`. Jobs use
`generateName`, so create them with `kubectl create` (not `kubectl apply`); finished Jobs are
deleted after one hour. `job-write-and-annotate.yaml` and `job-s3-metadata-enable.yaml` never
retry (`backoffLimit: 0`): a retried append would write the rows twice.

| File | Action |
|---|---|
| `job-read.yaml` | `read` (row count of the shared table) |
| `job-write-and-annotate.yaml` | `write_and_annotate` with `lab/input/nyctaxi_payload.json` |
| `job-list-annotations.yaml` | `list_annotations` of `DATA_FILE_KEY` |
| `job-get-annotation.yaml` | `get_annotation` `trip_stats` of `DATA_FILE_KEY` |
| `job-s3-metadata-enable.yaml` | `create_metadata_config`: journal (7 days) + inventory, annotation disabled |
| `job-s3-metadata-status.yaml` | `get_metadata_config` |
| `job-s3-metadata-disable.yaml` | `delete_metadata_config` (optional cleanup; the metadata tables stay in `aws-s3`) |
| `job-trip-stats-plan.yaml` | `plan_trip_stats` with `RUN_ID` and `SHARD_COUNT` (never retried: the plan is write-once) |
| `job-trip-stats-shard.yaml` | `trip_stats_shard`: Indexed Job, one pod per shard (1 CPU request, 2 CPU limit), each shard retried once (`backoffLimitPerIndex: 1`) |
| `job-trip-stats-merge.yaml` | `merge_trip_stats` with `RUN_ID` |

`scripts/run-job.sh` (project root) creates a Job, waits until it completes or fails, and prints
the response:

```bash
KEY=$(scripts/run-job.sh --raw build/manifests/rust/job-write-and-annotate.yaml | jq -r '.data.annotated_keys[0]')
# Annotation Jobs take the object key from the DATA_FILE_KEY env var:
scripts/run-job.sh build/manifests/rust/job-list-annotations.yaml DATA_FILE_KEY="$KEY"
```

`kubectl logs` merges stdout and stderr; the response is always the last line, which is what
`run-job.sh` prints.

`scripts/run-distributed-stats.sh` runs the three distributed steps. It sets the run id, and it
sets the Indexed Job's `completions` and `parallelism` to the shard count:

```bash
scripts/run-distributed-stats.sh --shards 4 | jq '.data.trip_stats'
```

Run documents go to `s3://<bucket>/lab/runs/<run id>/`.

## IAM (IRSA role)

The Jobs run with the IRSA role `spark-on-eks-iceberg-jobs` (`terraform/iam.tf`): S3 read and
write on the warehouse bucket (no delete; this also covers the `lab/runs/` documents of the
distributed trip stats), Glue `GetDatabase`/`GetTable`/`UpdateTable` on the
database and table, S3 Object Annotations, and for the S3
Metadata actions `s3:CreateBucketMetadataTableConfiguration` (the IAM action for
`CreateBucketMetadataConfiguration`), `s3:GetBucketMetadataTableConfiguration`,
`s3:DeleteBucketMetadataTableConfiguration` and the S3 Tables permissions on the `aws-s3` table
bucket. AWS documents `kms:DescribeKey` for `CreateBucketMetadataConfiguration` as well; it should
only matter for SSE-KMS (this CLI uses SSE-S3).

## Container image (optional)

`../Dockerfile` builds the same binary into an `amazonlinux:2023-minimal` image for users with
Docker. The ECR repository name must start with `spark-on-eks` (IDE role restriction).
