# Apache Iceberg on Amazon EKS

Run Apache Iceberg workloads on an existing Amazon EKS cluster, using the AWS Glue Data Catalog as
the Iceberg catalog and Amazon S3 as the warehouse. There are two engines, and both share one
Iceberg table:

- **Apache Spark 4.0.1 + Iceberg 1.10.0.** SparkApplications run through the Kubeflow Spark
  Operator.
- **Rust (`iceberg-rust` 0.11.0).** A CLI runs as a Kubernetes Job. It is a port of the
  `iceberg-lambda-s3` AWS Lambda. It can also split a scan of the table across the pods of an
  Indexed Job (distributed trip statistics).

Everything runs on **AWS Graviton5** (M9g/M9gd, Neoverse V3; see [GRAVITON5.md](GRAVITON5.md)).
Every Spark driver and executor and every Rust Job is scheduled on the project's own Graviton5
NodePool. The Rust CLI is compiled for `aarch64` with
`-Ctarget-cpu=neoverse-v3 -Ctarget-feature=+lse`.

This project is a port of the Lambda-based reference project `apache-iceberg-s3-metadata` to
Kubernetes. That project is a Rust Lambda that reads and writes the NYC Taxi Iceberg table and
manages S3 Metadata and S3 Object Annotations. Here it runs on the `spark-on-eks` cluster built by
the [data-on-eks](https://github.com/awslabs/data-on-eks) Spark Operator blueprint
(`analytics/terraform/spark-k8s-operator`, `spark-workshop` branch).

This is a standalone project. It reads the platform (cluster, OIDC provider, Spark Operator) and
adds its own resources next to it. It does not modify the data-on-eks repository or anything that
its Terraform state manages.

## Table of contents

- [What was ported](#what-was-ported)
- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [Permission model](#permission-model)
- [Quick start](#quick-start)
- [Components](#components)
- [S3 Metadata and S3 Object Annotations](#s3-metadata-and-s3-object-annotations)
- [Testing](#testing)
- [Cleanup](#cleanup)
- [Troubleshooting](#troubleshooting)
- [Known limitations](#known-limitations)

## What was ported

| Reference project (Lambda) | This project (EKS) |
|---|---|
| `lambda/` Rust Lambda `iceberg-lambda-s3`, 13 actions | `rust/iceberg-k8s/` CLI with the same 13 actions and request/response JSON, run as Kubernetes Jobs (`rust/manifests/`) |
| Lambda execution role (`deploy.sh`) | Two IRSA roles created by Terraform, one per Kubernetes service account: `spark-on-eks-iceberg-spark` (Spark jobs) and `spark-on-eks-iceberg-jobs` (Rust CLI Jobs) |
| `create_glue_database.sh` (Glue DB + table bootstrap) | `spark/iceberg_nyc_taxi_load.py` creates the Glue database and the Iceberg table (same 23-column schema, field ids 1..23, format v2) from inside the cluster |
| `populate.sh` (`write_and_annotate` via `aws lambda invoke`) | Spark load job (bulk append) and the Rust `job-write-and-annotate.yaml` (append + `trip_stats` annotations) |
| `iceberg-lambda-warehouse-<account>` bucket | `spark-on-eks-iceberg-<suffix>` bucket created by Terraform |
| `database/nyctaxi_payload.json` (5,000 trips, Nov 2013) | `data/nyctaxi_payload.json` (same file) |
| `cleanup.sh` | `scripts/cleanup.sh` + the in-cluster Spark cleanup job + `terraform destroy` |
| (none) | Spark analytics job: time travel, metadata tables, tags, MERGE/UPDATE/DELETE, schema evolution, and maintenance procedures on a derived table |
| (none) | Distributed trip statistics in Rust: plan, an Indexed Job with one pod per shard, merge (`scripts/run-distributed-stats.sh`) |

The Rust CLI and the Spark jobs compute trip statistics with the same definitions, taken from the
reference `compute_trip_stats`, so their numbers can be compared. On the sample payload: 5,000
trips, tip rate 11.67%, vendors CMT 2,432 / VTS 2,568.

## Architecture

```
                         existing platform (data-on-eks, not modified)
  +---------------------------------------------------------------------------------+
  |  EKS cluster spark-on-eks (Auto Mode)                                           |
  |                                                                                 |
  |   NodePool spark-on-eks-iceberg-graviton5 (this project): M9g/M9gd, arm64       |
  |   -> every pod below runs on Graviton5 nodes                                    |
  |                                                                                 |
  |   namespace spark-team-a (watched by the Spark Operator)                        |
  |   +--------------------------------------+                                      |
  |   | SparkApplications (this project)     |   SA spark-iceberg                   |
  |   |  spark-iceberg-load                  |     -> IRSA role                     |
  |   |  spark-iceberg-analytics             |        spark-on-eks-iceberg-spark    |
  |   |  spark-iceberg-cleanup               |        (Glue DB/tables, bucket)      |
  |   +--------------------------------------+                                      |
  |                                                                                 |
  |   namespace iceberg (created by this project)                                   |
  |   +--------------------------------------+                                      |
  |   | Jobs: iceberg-k8s (Rust CLI)         |   SA iceberg-jobs                    |
  |   |  read, write_and_annotate,           |     -> IRSA role                     |
  |   |  annotations, S3 Metadata,           |        spark-on-eks-iceberg-jobs     |
  |   |  distributed trip stats (Indexed Job)|                                      |
  |   +--------------------------------------+        (append, annotations, S3 Meta)|
  +---------------------------------------------------------------------------------+
            |                         |                              |
            v                         v                              v
   +------------------+    +----------------------------+   +--------------------------+
   | AWS Glue Data    |    | S3 bucket                  |   | S3 Metadata (optional)   |
   | Catalog          |    | spark-on-eks-iceberg-*     |   | journal + inventory      |
   | nyc_taxi_database|    |  warehouse/  Iceberg data  |   | tables in the AWS managed|
   |  nyc_taxi_table  |    |  lab/        lab artifacts |   | table bucket aws-s3      |
   |  nyc_taxi_trips_ |    +----------------------------+   +--------------------------+
   |  curated         |
   +------------------+
```

- `nyc_taxi_table` is the **shared table**. It has the exact reference schema (23 required
  columns), is unpartitioned and uses format v2. Spark creates and appends to it; the Rust CLI
  reads and appends to it. Never change its schema, partition spec or format version, because the
  Rust writer depends on them. Both writers follow its `write.parquet.*` properties, so data files
  are zstd-compressed unless the table sets another codec.
- `nyc_taxi_trips_curated` is a **derived table** that the Spark analytics job owns. It has typed
  timestamps and is partitioned by `days(pickup_ts)`. All row-level operations, schema evolution
  and maintenance run on it.
- **Graviton5 only.** `terraform/nodepool.tf` creates an EKS Auto Mode NodePool limited to the
  Graviton5 families `m9g` and `m9gd` (arm64, `large` to `8xlarge`, on-demand or spot). It uses
  an existing platform NodeClass (`graviton5_node_class`, default `ebs-gp3-100gi`). The pods
  select it with `karpenter.sh/nodepool` and `kubernetes.io/arch: arm64`. None of the platform's
  NodePools is limited to Graviton5, so they are not used. All images are multi-arch and have a
  `linux/arm64` variant: the Spark image, `aws-cli` and `amazonlinux:2023-minimal`.
- Artifacts reach the cluster through S3, so Docker is not needed. Spark reads its scripts from
  `s3a://<bucket>/lab/scripts/spark/`. Each Rust Job downloads the binary from `lab/bin/` with an
  `aws-cli` initContainer and runs it on `amazonlinux:2023-minimal`. A `rust/Dockerfile` is
  provided for environments that can build and push images.

S3 layout of the warehouse bucket:

| Prefix | Content |
|---|---|
| `warehouse/<db>/<table>/{data,metadata}/` | Iceberg tables (`ICEBERG_WAREHOUSE = s3://<bucket>/warehouse`) |
| `lab/input/nyctaxi_payload.json` | Sample data |
| `lab/scripts/spark/*.py` | PySpark jobs |
| `lab/bin/iceberg-k8s` | Rust CLI (linux aarch64, built for Graviton5 / Neoverse V3) |
| `lab/runs/<run id>/` | Distributed trip stats: `plan.json`, `shards/shard-<index>.json`, `result.json` |
| `lab/spark-event-logs/` | Spark event logs (default; see `spark_event_logs_bucket`) |

## Repository layout

```
.
├── GRAVITON5.md                # Graviton5 reference: instances, compiler flags, performance tips
├── terraform/                  # Standalone root module: bucket, IRSA roles, namespace, SAs, RBAC,
│                               #   Graviton5 NodePool (nodepool.tf)
├── spark/
│   ├── iceberg_nyc_taxi_load.py        # Create DB + table, load the payload, validate
│   ├── iceberg_nyc_taxi_analytics.py   # Trip stats, time travel, tags, curated table, maintenance
│   ├── iceberg_cleanup.py              # DROP TABLE ... PURGE + DROP NAMESPACE (needs --confirm)
│   ├── manifests/                      # SparkApplication templates
│   └── tests/                          # pytest suite (helpers, manifests, Graviton5 contract, trip-stats SQL)
├── rust/
│   ├── iceberg-k8s/                    # Rust CLI (port of the Lambda), README with all actions
│   ├── manifests/                      # Kubernetes Job templates
│   ├── .cargo/config.toml              # Graviton5 rustflags (neoverse-v3, +lse) for aarch64
│   ├── build.sh                        # Graviton5 release build -> build/bin/iceberg-k8s
│   └── Dockerfile                      # Optional container image (linux/arm64)
├── scripts/
│   ├── check-prereqs.sh                # Read-only checks of tools, AWS, cluster, Spark Operator
│   ├── upload-artifacts.sh             # data + Spark scripts + binary -> s3://<bucket>/lab/
│   ├── render-manifests.sh             # Fill <PLACEHOLDERS> from Terraform outputs -> build/manifests/
│   ├── run-job.sh                      # Create a Rust CLI Job, wait for complete/failed, print the response
│   ├── run-distributed-stats.sh        # Distributed trip stats: plan, Indexed Job (one pod per shard), merge
│   ├── cleanup.sh                      # Delete lab workloads, print the remaining cleanup steps
│   └── common.sh                       # Shared helpers (sourced)
├── data/nyctaxi_payload.json           # 5,000 NYC Taxi trips (from the reference project)
└── build/                              # Generated: binary and rendered manifests (git-ignored)
```

## Prerequisites

- **The platform.** The data-on-eks Spark Operator blueprint must already be deployed: EKS cluster
  `spark-on-eks` in `us-west-2`, the Kubeflow Spark Operator watching `spark-team-a`, and the IAM
  OIDC provider of the cluster. Every name can be changed with Terraform variables.
- **Tools:** AWS CLI v2, `kubectl`, Terraform >= 1.5.7, `jq`, and `cargo` (Rust 1.95+: iceberg
  0.11, edition 2024) to build the CLI. `uv` is optional and only needed to run the Python tests.
- **Graviton5 capacity.** M9g/M9gd must be offered in the region (they are in all four
  `us-west-2` zones), and the cluster must use EKS Auto Mode, which supports both families. The
  platform NodeClass `ebs-gp3-100gi` must exist; it is used by the Graviton5 NodePool.
- **Rust build host.** The Rust CLI is built natively on an arm64 Amazon Linux 2023 host. The
  workshop IDE is a Graviton5 host (Neoverse V3). The binary links glibc 2.34, which matches the
  arm64 `amazonlinux:2023` runtime image.
- **kubectl** must point at the cluster:
  `aws eks update-kubeconfig --region us-west-2 --name spark-on-eks`.

Check everything, read-only:

```bash
scripts/check-prereqs.sh
```

## Permission model

The workshop IDE runs Terraform and the scripts as the instance role
`spark-on-eks-SparkonEKSIdeRole-*`. That role was checked against its IAM policies and has these
limits, which shape the design:

| Limit | Consequence in this project |
|---|---|
| No AWS Glue or Lake Formation permissions | Terraform cannot create the Glue database. The **Spark load job creates it in-cluster** through IRSA, and the **Spark cleanup job drops it** before `terraform destroy`. |
| IAM roles and policies can only be named `spark-on-eks-*`, and only inline role policies are unrestricted | The resource prefix is `spark-on-eks-iceberg` (validated in `variables.tf`). Each IRSA role has one inline policy. |
| S3 buckets can only be named `spark-on-eks*`; no `PutBucketPolicy`, `PutLifecycleConfiguration` or `CreateBucketMetadataConfiguration` | Bucket `spark-on-eks-iceberg-*`, with SSE-S3 and public access block only. Versioning is off (without a lifecycle rule, noncurrent versions would pile up). **S3 Metadata is enabled by an in-cluster Rust Job.** |
| Docker daemon not usable | The Rust binary is delivered through S3 instead of a container image |
| No `iam:UpdateAssumeRolePolicy` | Changing a namespace or service account name replaces the IRSA role instead of updating it |
| No `iam:UntagRole` | Removing a key from `var.tags` after the apply fails on the roles; add tags, do not remove them |

`terraform/iam.tf` creates one IRSA role per workload. Each role is trusted only by its own
service account and scoped to the project's resources:

| | `spark-on-eks-iceberg-spark` (SA `spark-team-a/spark-iceberg`) | `spark-on-eks-iceberg-jobs` (SA `iceberg/iceberg-jobs`) |
|---|---|---|
| S3 warehouse bucket | read, write, delete (PURGE, compaction, snapshot expiry) | read and write (append only, no delete), including the `lab/runs/` documents of the distributed trip stats |
| S3 event logs | optional write on `<spark_event_logs_bucket>/spark-event-logs/` | - |
| Glue | create, update and delete the database and its tables (`catalog`, `database/<db>`, `table/<db>/*`, `userDefinedFunction/<db>/*`) | read the database and tables, `UpdateTable` to commit appends |
| Lake Formation | `lakeformation:GetDataAccess` | `lakeformation:GetDataAccess` |
| S3 Object Annotations | - | put, get, delete and list on the warehouse objects |
| S3 Metadata | - | `s3:*BucketMetadataTableConfiguration` on the bucket; S3 Tables read and create on the AWS managed table bucket `aws-s3` |

## Quick start

All commands run from the project root.

### 1. Create the AWS and Kubernetes resources

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars   # optional, see below
terraform -chdir=terraform init
terraform -chdir=terraform apply
```

The apply creates 16 resources:
- the Graviton5 NodePool `spark-on-eks-iceberg-graviton5` (M9g/M9gd), where every pod of the
  project runs;
- the warehouse bucket, its public access block, its encryption configuration, and the
  `lab/spark-event-logs/` marker object;
- the two IRSA roles, with one inline policy and one trust-policy tracker each;
- the `iceberg` namespace;
- the service accounts `spark-team-a/spark-iceberg` and `iceberg/iceberg-jobs`;
- the Spark driver Role and RoleBinding in `spark-team-a`.

To see these Spark jobs in the platform's Spark History Server, set
`spark_event_logs_bucket = "<the platform's spark-on-eks-spark-logs-* bucket>"` in
`terraform.tfvars`. The platform's `terraform output s3_bucket_id_spark_history_server` prints
it. Without it, event logs go to `s3a://<warehouse bucket>/lab/spark-event-logs`.

### 2. Build the Rust CLI and upload the artifacts

```bash
rust/build.sh                     # ~15 min the first time; writes build/bin/iceberg-k8s (aarch64, Graviton5)
scripts/upload-artifacts.sh       # data, Spark scripts and binary -> s3://<bucket>/lab/
scripts/render-manifests.sh       # build/manifests/{spark,rust}/*.yaml
```

### 3. Create the Glue database and the Iceberg table, and load the sample (Spark)

```bash
kubectl apply -f build/manifests/spark/spark-iceberg-load.yaml
kubectl -n spark-team-a get sparkapplication spark-iceberg-load -w     # until COMPLETED
kubectl -n spark-team-a logs spark-iceberg-load-driver | grep -A40 "6. Trip statistics"
```

The first run creates `nyc_taxi_database` and `nyc_taxi_table`. Every run **appends** the 5,000
rows; to replace them instead, change `--mode` to `overwrite` in the manifest. The job never
retries after it starts, so a retry cannot load the rows twice. Before running a
SparkApplication again, delete the finished one with
`kubectl -n spark-team-a delete sparkapplication spark-iceberg-load`. Finished applications are
also removed automatically after one hour.

### 4. Read and write the same table from Rust

```bash
scripts/run-job.sh build/manifests/rust/job-read.yaml          # {"ok": true, "rows_read": 5000, ...}

# Append the 5,000 rows again from Rust and annotate the new data file with trip_stats
scripts/run-job.sh --raw build/manifests/rust/job-write-and-annotate.yaml | tee /tmp/write.json | jq .
KEY=$(jq -r '.data.annotated_keys[0]' /tmp/write.json)

# Read the annotation back
scripts/run-job.sh --raw build/manifests/rust/job-get-annotation.yaml DATA_FILE_KEY="$KEY" \
  | jq -r '.data.annotation_value' | jq .
scripts/run-job.sh build/manifests/rust/job-read.yaml          # now 10,000 rows
```

`scripts/run-job.sh` does the following:
- It creates the Job with `kubectl create`. The templates use `generateName`, which
  `kubectl apply` does not support.
- It sets any `NAME=VALUE` arguments as environment variables of the CLI container.
- It waits until the Job completes **or fails**.
- It prints the JSON response, which is the last log line. Its exit code is 0 only when the
  response has `"ok": true`.

The write Job never retries (`backoffLimit: 0`), so a retry cannot append the rows twice. See
[rust/iceberg-k8s/README.md](rust/iceberg-k8s/README.md#kubernetes-jobs) for every Job.

### 5. Explore Iceberg features (Spark)

```bash
kubectl apply -f build/manifests/spark/spark-iceberg-analytics.yaml
kubectl -n spark-team-a get sparkapplication spark-iceberg-analytics -w
kubectl -n spark-team-a logs spark-iceberg-analytics-driver
```

The job does the following:
1. Prints the trip stats of the shared table. They now include the Spark and Rust appends.
2. Shows the metadata tables: `snapshots`, `history`, `files`, `manifests`, `refs`.
3. Runs time travel: `VERSION AS OF` the first snapshot and `TIMESTAMP AS OF`.
4. Creates the tags `first_snapshot` and `analytics_latest`. A tag is only a reference, so the
   table's structure does not change.
5. On the curated table, runs MERGE INTO, ADD COLUMN, UPDATE and DELETE (merge-on-read).
6. Runs `rewrite_data_files`, `rewrite_position_delete_files` and `expire_snapshots`, and
   `remove_orphan_files` with `dry_run => true`.

It is idempotent: every run converges to the same curated table.

### 6. Split a scan across pods (Rust)

```bash
scripts/run-distributed-stats.sh --shards 3 | jq '.data | {rows_read, shards, trip_stats}'
```

The script runs three Jobs:
1. **Plan.** Pins the table's current snapshot and assigns its data files to 3 shards, balanced
   by size.
2. **Shards.** An Indexed Job with one pod per shard. Each pod reads only its own data files and
   only the six columns the statistics need.
3. **Merge.** Checks the partial results against the plan and merges them.

The result is the same `trip_stats` as a single-process scan, for any number of shards. See
[Distributed trip stats](#distributed-trip-stats-rust).

### 7. (Optional) S3 Metadata

See [S3 Metadata and S3 Object Annotations](#s3-metadata-and-s3-object-annotations).

## Components

### Terraform (`terraform/`)

Providers are pinned exactly: `hashicorp/aws` 6.72.0 and `hashicorp/kubernetes` 3.5.0. The
backend is local; an S3 backend example is commented out in `versions.tf`. The module uses data
sources for the platform. It checks that the cluster is `ACTIVE`, that the OIDC provider exists,
and that `spark_namespace` and `graviton5_node_class` exist. It also warns if the Spark Operator
does not watch that namespace.

| Variable | Default | Notes |
|---|---|---|
| `region` | `us-west-2` | |
| `cluster_name` | `spark-on-eks` | Existing cluster |
| `name` | `spark-on-eks-iceberg` | Resource prefix; must start with `spark-on-eks`, at most 36 characters |
| `glue_database` | `nyc_taxi_database` | Created by the Spark load job, not by Terraform |
| `iceberg_table` | `nyc_taxi_table` | Created by the Spark load job |
| `spark_namespace` | `spark-team-a` | Existing namespace watched by the Spark Operator |
| `spark_service_account` | `spark-iceberg` | |
| `jobs_namespace` | `iceberg` | Created by this module (Pod Security `baseline`) |
| `jobs_service_account` | `iceberg-jobs` | |
| `graviton5_node_class` | `ebs-gp3-100gi` | Existing Auto Mode NodeClass of the Graviton5 NodePool (subnets, security groups, node role, disk) |
| `spark_event_logs_bucket` | `""` | Platform logs bucket for the Spark History Server (optional) |
| `tags` | `{}` | Extra tags; `Project = <name>` is always added |

Outputs: `region`, `cluster_name`, `warehouse_bucket`, `iceberg_warehouse`, `lab_uri`,
`glue_database`, `iceberg_table`, `spark_irsa_role_arn`, `jobs_irsa_role_arn`, `spark_namespace`,
`spark_service_account`, `jobs_namespace`, `jobs_service_account`, `graviton5_node_pool`,
`graviton5_node_class`, `spark_event_log_dir`, `configure_kubectl`.

`scripts/render-manifests.sh` replaces `<FOO_BAR>` in every template with the string output
`foo_bar`, and fails if any placeholder is left.

### Spark jobs (`spark/`)

- **Image:** `public.ecr.aws/data-on-eks/spark:4.0.1-scala2.13-java21-python3-r-ubuntu`, the
  platform's image. Its layers were inspected: it contains
  `iceberg-spark-runtime-4.0_2.13-1.10.0`, the Iceberg Spark extensions, the AWS SDK v2 bundle
  2.38.0 (Glue, S3, STS) and hadoop-aws 3.4.2. No `spark.jars.packages` is needed.
- **Catalog:** `spark.sql.catalog.glue` is `SparkCatalog` with `type=glue`, `S3FileIO`,
  `warehouse=s3://<bucket>/warehouse` and `http-client.type=apache`. The SDK bundle has no
  URLConnection client, so the Apache client is set explicitly. S3A uses
  `WebIdentityTokenFileCredentialsProvider`, the IRSA provider.
- **Scheduling:** the driver and the executors run only on the Graviton5 NodePool
  (`karpenter.sh/nodepool: <GRAVITON5_NODE_POOL>`, `kubernetes.io/arch: arm64`). The driver is
  on-demand with `karpenter.sh/do-not-disrupt`; executors may run on spot. The JVM uses the
  `linux/arm64` variant of the image. Pods run as non-root uid 185.
- **Configuration:** the catalog is set only through Spark configuration. The scripts receive just
  the catalog name, so the same code also runs against a local Iceberg Hadoop catalog (see
  [Testing](#testing)).

| Job | What it does | Retries |
|---|---|---|
| `spark-iceberg-load` | Creates the namespace and table if they are missing, with explicit LOCATIONs. If the table exists, verifies its schema, partition spec and format version. Loads the payload (append or overwrite), then prints counts, null checks, sample rows, the new snapshot and trip stats. | none after start |
| `spark-iceberg-analytics` | See [step 5](#5-explore-iceberg-features-spark) | 2 |
| `spark-iceberg-cleanup` | `DROP TABLE ... PURGE` (curated, then shared), then `DROP NAMESPACE`. If other tables are in the database, it lists them instead of using CASCADE. A table whose metadata file or bucket is gone loses only its catalog entry. `--drop-unreadable-tables` extends that to any table that cannot be loaded. | 2 |

### Rust CLI (`rust/`)

- **Graviton5 build:** `rust/build.sh` builds natively for `aarch64-unknown-linux-gnu`. The flags
  come from `rust/.cargo/config.toml`: `-Ctarget-cpu=neoverse-v3 -Ctarget-feature=+lse`, the
  general best configuration of [GRAVITON5.md](GRAVITON5.md). The binary needs a Graviton5 CPU,
  and every Job pins on-demand nodes of the Graviton5 NodePool.
- **Port:** `iceberg-k8s` is the reference Lambda without `lambda_runtime`. The 13 actions and the
  JSON contract are unchanged. Dependencies: iceberg 0.11.0 (with iceberg-catalog-glue and
  iceberg-storage-opendal 0.11.0), arrow/parquet 58.4, Rust edition 2024.
- **Distributed trip stats:** three more actions (`plan_trip_stats`, `trip_stats_shard`,
  `merge_trip_stats`) split a scan across pods. See
  [Distributed trip stats](#distributed-trip-stats-rust).
- **Writer:** the Parquet settings come from the table's `write.parquet.*` properties, through
  iceberg's `ParquetWriterBuilder::from_table_properties`. Compression is zstd level 3 unless the
  table sets `write.parquet.compression-codec` or `write.parquet.compression-level`, the same
  default as Spark (Iceberg Java). The job log shows the codec, for example
  `writing Parquet data files compression=Zstd(level=3)`.
- **Credentials:** IRSA is resolved through the `aws-config` default chain for S3, S3 Tables and
  Glue. Iceberg's opendal FileIO gets a credential loader backed by the same chain, cached per
  process and refreshed before expiry. Jobs longer than the 1-hour STS session therefore keep
  working, and STS is not called for every file.
- **Fixes over the Lambda:**
  - The S3 key of each new data file is derived correctly for `s3://`, `s3a://` and `s3n://`
    table locations.
  - `query_metadata_table` loads the table through the S3 Tables API.
  - Data files are compressed. The Lambda used the parquet-rs defaults and wrote uncompressed
    files, about twice as large: 807,046 bytes against 408,283 with zstd for the 5,000-row
    payload.

The full CLI and Job reference is in [rust/iceberg-k8s/README.md](rust/iceberg-k8s/README.md).

| Job template | Action |
|---|---|
| `job-read.yaml` | `read` (row count) |
| `job-write-and-annotate.yaml` | `write_and_annotate` with the 5,000-row payload |
| `job-list-annotations.yaml` / `job-get-annotation.yaml` | `list_annotations` / `get_annotation trip_stats` of `DATA_FILE_KEY` |
| `job-s3-metadata-enable.yaml` | `create_metadata_config`: journal (7-day expiration) + inventory, SSE-S3 |
| `job-s3-metadata-status.yaml` | `get_metadata_config` |
| `job-s3-metadata-disable.yaml` | `delete_metadata_config` |
| `job-trip-stats-plan.yaml` / `job-trip-stats-shard.yaml` / `job-trip-stats-merge.yaml` | `plan_trip_stats` / `trip_stats_shard` (Indexed Job) / `merge_trip_stats`, run by `scripts/run-distributed-stats.sh` |

### Distributed trip stats (Rust)

A single Rust Job is one process, so a large scan is limited to what one pod can read. For
statistics that can be merged from partial results, the CLI splits the scan across the pods of
a Kubernetes Indexed Job, map-reduce style. There is no shuffle.

| Step | Job | What it does |
|---|---|---|
| 1. Plan | `job-trip-stats-plan.yaml` | Pins the current snapshot and plans the scan once. Assigns the data files to N shards, largest first to the shard with the fewest bytes. Writes `lab/runs/<run id>/plan.json`, only if it does not exist yet. |
| 2. Shards | `job-trip-stats-shard.yaml` (Indexed Job) | Pod `i` (`JOB_COMPLETION_INDEX`) plans the same snapshot again and keeps the tasks of its files. It fails if one is missing. It reads only `vendor_id`, `passenger_count`, `trip_distance`, `payment_type`, `fare_amount` and `tip_amount`, then writes `shards/shard-<i>.json`. Each shard is retried once. |
| 3. Merge | `job-trip-stats-merge.yaml` | Checks every shard result against the plan: snapshot, shard count and number of files. Without delete files, it also checks the row count against the manifests. Merges the results and writes `result.json`. |

- **Exact results.** Partial sums are integers in micro-units, not floats. The result therefore
  does not depend on the number of shards or on the merge order. It equals the single-process
  `compute_trip_stats` (unit-tested on the 5,000-row payload).
- **Why paths and not scan tasks.** In iceberg-rust 0.11 a planned `FileScanTask` always
  carries partition data that cannot be serialized. The plan therefore lists file paths, and
  each shard plans the pinned snapshot again, so the library still builds complete tasks, delete
  files included.
- **Scaling.**
  - Up: each shard pod reads as many files at once as its CPU limit allows (2 CPUs in the
    template).
  - Out: `--shards` adds pods.
  - The smallest unit of work is one data file, so shards beyond the number of files stay empty.
- **Not covered.** Joins, high-cardinality GROUP BY, global sorts and window functions need a
  shuffle. Run those in Spark.

## S3 Metadata and S3 Object Annotations

**S3 Object Annotations** are named payloads attached to an S3 object. The
`write_and_annotate` Job puts a `trip_stats` annotation on every Parquet data file it writes,
holding statistics of the rows in that file. Read them back with `job-list-annotations.yaml` or
`job-get-annotation.yaml`.

**S3 Metadata** keeps read-only, AWS managed Iceberg tables about the bucket's objects: journal,
inventory and annotation. The IDE role cannot create the configuration, so the IRSA role does it
from a Job:

```bash
scripts/run-job.sh build/manifests/rust/job-s3-metadata-enable.yaml   # once; it never retries
scripts/run-job.sh build/manifests/rust/job-s3-metadata-status.yaml   # repeat until ACTIVE
```

- **Cost and time.** The tables are billed. The inventory table first *backfills* the existing
  objects, which takes minutes to hours.
- **Annotation table.** It needs an IAM role that S3 assumes to read the annotations, and the CLI
  does not send one (the same limit as the reference Lambda). Without the annotation table,
  `query_annotation_table` cannot work. `query_metadata_table` works on `journal` and `inventory`
  once they are `ACTIVE`.

## Testing

Offline suites, with no AWS credentials needed:

| Suite | Command | Result |
|---|---|---|
| Spark helpers and Graviton5 contract | `uv run --with pyspark==4.0.1 --with pytest pytest spark/tests` | 112 passed, 16 skipped without Java; the 16 run with `JAVA_HOME` set (local Spark, trip-stats SQL on temp views; no Iceberg catalog). `test_graviton5.py` checks that every manifest selects the Graviton5 NodePool and arm64, that the NodePool allows only `m9g`/`m9gd`, and the Rust Graviton5 build settings |
| Rust CLI | `cd rust/iceberg-k8s && cargo test` | 73 unit + 11 CLI tests passed (1 ignored AWS test); they include a write to an in-memory table that checks the zstd codec |
| Rust lint | `cargo fmt --check && cargo clippy --all-targets -- -D warnings` | clean |
| Terraform | `terraform -chdir=terraform fmt -check && terraform -chdir=terraform validate` | valid |
| Manifests | `scripts/render-manifests.sh --outputs-file <stub.json> --out-dir /tmp/m`, then `kubectl apply --dry-run=server` (Spark) or `kubectl create --dry-run=client` (Rust Jobs). The three trip-stats Jobs were also checked with `kubectl create --dry-run=server` | all pass |
| Shell scripts | `uvx --from shellcheck-py shellcheck -x scripts/*.sh` | clean |

`spark/tests/rust_stats_oracle.py` is a line-by-line Python port of the Rust
`compute_trip_stats`. The tests check that the Spark SQL statistics, rounding included, match it
exactly on the full payload.

The suites above do not touch Iceberg. Beyond them, these end-to-end runs were done by hand
during development, with Spark 4.0.1, the Iceberg 1.10.0 runtime jar and Java 21 in a scratch
directory:
- **Spark only.** The three Spark jobs ran against a local Iceberg Hadoop catalog: load twice,
  analytics twice (it converges to the same curated table), cleanup twice, then load again.
- **Rust and Spark together.** A scratch harness that reuses the CLI's `writer.rs` read the
  Spark-written table (5,000 rows) and fast-appended to it (10,000). It kept the tags, properties,
  schema and partition spec. The Spark load job's schema check and the analytics job then passed
  on the result.

The [Quick start](#quick-start) steps 1 to 5 were then run once on the `spark-on-eks` cluster
(2026-11-06, Kubernetes 1.36, Spark Operator 2.3.0), with the Glue catalog and IRSA:

| Step | Result |
|---|---|
| `terraform apply` | 15 resources added |
| `spark-iceberg-load` | Created the database and the table, appended 5,000 rows; schema check and trip stats as expected (tip rate 11.67%, CMT 2,432 / VTS 2,568) |
| Rust `job-read.yaml` | `rows_read: 5000` |
| Rust `job-write-and-annotate.yaml` | 5,000 rows appended, 1 data file annotated; its `trip_stats` are identical to the Spark ones |
| Rust `job-get-annotation.yaml` / `job-list-annotations.yaml` | `trip_stats` annotation read back and listed |
| Rust `job-read.yaml` | `rows_read: 10000` |
| `spark-iceberg-analytics` | Completed on the first attempt: 2 snapshots on the shared table, MERGE (5,000 rows), UPDATE (5,000), DELETE (18), compaction 60 -> 30 data files, 4,982 rows in 30 partitions |

Auto Mode scheduled the Spark driver and executors on `compute-optimized-x86` and the Rust Jobs
on `general-purpose`. All images were pulled from public ECR. These runs, and the ones below,
predate the move to Graviton5 (x86_64 binary and nodes).

After the move to iceberg-rust 0.11.0 (edition 2024), these steps ran on the same table (still
2026-11-06):

| Step | Result |
|---|---|
| Rust `job-read.yaml` | `rows_read: 10000` (the table written by Spark and by the 0.10.1 CLI) |
| Rust `job-write-and-annotate.yaml` | 5,000 rows appended with the 0.11.0 writer and annotated; the table now has 15,000 rows in 3 data files |
| `run-distributed-stats.sh --shards 3` | One file per shard pod (5,000 rows each), 57 s for the three steps. Merged `trip_stats` are exactly 3 times those of the payload: 15,000 trips, CMT 7,296 / VTS 7,704, tip rate 11.67% |
| `--shards 1` and `--shards 5` | Same `trip_stats` as with 3 shards. With 5 shards, shards 3 and 4 had no files and stayed empty |
| Error paths | Reusing a run id fails on the conditional `plan.json` write. Merging before the shards ran lists the missing shards. An empty `RUN_ID` is rejected |
| Rust `job-write-and-annotate.yaml` with zstd | The log shows `compression=Zstd(level=3)`. The new data file has 408,283 bytes, against 807,046 for the earlier uncompressed Rust files and 361,760 for the Spark file |
| `run-distributed-stats.sh --shards 4` | One file per shard, zstd file included. 20,000 rows, `trip_stats` exactly 4 times those of the payload (CMT 9,728 / VTS 10,272) |

The table now has 20,000 rows in 4 data files: one from Spark, one from the 0.10.1 CLI, and two
from the 0.11.0 CLI (uncompressed, then zstd).

Running the Spark jobs against a local catalog takes three things:
- a JDK 21;
- `--packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0`;
- the catalog set up as `spark.sql.catalog.<name>.type=hadoop` with a `file:/...` warehouse.

## Cleanup

Glue objects can only be deleted from inside the cluster, so the order matters:

```bash
scripts/cleanup.sh --dry-run            # see what would be deleted
scripts/cleanup.sh                      # delete the lab SparkApplications and Jobs, print next steps

# 1. Drop the tables (PURGE) and the Glue database, in-cluster, while the IRSA roles still exist
kubectl apply -f build/manifests/spark/spark-iceberg-cleanup.yaml
kubectl -n spark-team-a get sparkapplication spark-iceberg-cleanup -w

# 2. Only if you enabled S3 Metadata
scripts/run-job.sh build/manifests/rust/job-s3-metadata-disable.yaml

# 3. Everything else (bucket with all objects, IRSA roles, namespace, service accounts, RBAC)
terraform -chdir=terraform destroy
```

- `scripts/cleanup.sh` only deletes SparkApplications and Jobs that belong to this lab, matched
  by name and by the `app.kubernetes.io/part-of: spark-on-eks-iceberg` label. It never touches
  the platform's workloads or namespaces.
- After `delete_metadata_config`, the journal and inventory tables stay in the AWS managed table
  bucket `aws-s3`, namespace `b_<bucket name>`. Deleting them needs `s3tables:DeleteTable` and
  `s3tables:DeleteNamespace`, which neither the IDE role nor the IRSA roles have.
- **If you ran `terraform destroy` before step 1**, the Glue database and tables are left behind,
  and no identity in the workshop can delete them. To recover:
  1. Run `terraform apply` again. It creates a new bucket and new roles, with the same Glue
     permissions on `<glue_database>`.
  2. Run `scripts/render-manifests.sh`.
  3. Add `"--drop-unreadable-tables"` to the `arguments` of
     `build/manifests/spark/spark-iceberg-cleanup.yaml` and run step 1.

  The tables point to metadata in the deleted bucket. The new role gets AccessDenied there instead
  of NoSuchBucket, so without the flag the job stops on purpose. With it, the job drops only the
  catalog entries (the old files are already gone).

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| SparkApplication gets no status and no driver pod | The namespace is not watched by the Spark Operator | Use a namespace from the operator's `--namespaces` (`scripts/check-prereqs.sh` shows them) |
| `AccessDenied` on `glue:*` from the IDE | Expected: the IDE role has no Glue permissions | Run Glue operations in-cluster (Spark or Rust Jobs) |
| Rust `read` fails with table not found | The table is created by the Spark load job | Run step 3 first |
| Load job fails with a schema mismatch | The table exists with another schema | The job prints every difference; drop the table with the cleanup job or fix the schema |
| `cannot use generate name with apply` | Rust Jobs use `generateName` | Use `scripts/run-job.sh` (or `kubectl create -f`) |
| Cleanup job fails with `AccessDenied` while loading a table | The table's metadata is in a bucket the role cannot read (usually one deleted by an earlier `terraform destroy`) | See the recovery steps in [Cleanup](#cleanup) |
| `render-manifests.sh` reports an unresolved placeholder | Terraform not applied, or a template uses a name that is not an output | Run `terraform apply`; use only output names in templates |
| `upload-artifacts.sh` skips the binary | `build/bin/iceberg-k8s` missing | Run `rust/build.sh` |
| `cargo` or `rust/build.sh` fails with `ld terminated with signal 7 [Bus error]` or `No space left on device` | The IDE's root disk is full. Debug builds in `rust/iceberg-k8s/target/debug` grow to more than 10 GB | `cd rust/iceberg-k8s && cargo clean --profile dev` (the release build in `target/aarch64-unknown-linux-gnu` is kept) |
| Pods stay `Pending` with no node launched | The Graviton5 NodePool is missing (Terraform not applied) or cannot launch M9g/M9gd (capacity, limits, NodeClass) | `kubectl get nodepool spark-on-eks-iceberg-graviton5`, `kubectl get nodeclaims`, and the pod events; `scripts/check-prereqs.sh` checks the NodeClass, the NodePool and the M9g/M9gd offerings |
| CLI container fails with `exec format error` | An x86_64 binary was uploaded | Rebuild with `rust/build.sh` (aarch64) and run `scripts/upload-artifacts.sh` again |
| CLI or `cargo test` stops with `Illegal instruction` (`SIGILL`) | Code built with `-Ctarget-cpu=neoverse-v3` ran on a CPU older than Graviton5 | Run it on Graviton5: the Jobs only use the Graviton5 NodePool; build and test on a Graviton5 host such as the IDE |
| `create_metadata_config` fails with `AccessDenied` mentioning KMS | Only relevant for SSE-KMS metadata tables | Add `kms:DescribeKey` to the Jobs role policy (this project uses SSE-S3) |
| Distributed plan fails with `plan.json already exists` | The run id was used before (plans are write-once) | Use a new `--run-id`, or leave it out to get a generated one |
| A shard fails with `the planned snapshot does not match the plan` | The plan's snapshot no longer has those files (for example, snapshots were expired between the plan and the shards) | Run `scripts/run-distributed-stats.sh` again |
| Merge fails with `no result yet for ... shards` | A shard pod failed or the Indexed Job was not run with that run id | Check the shard pods (`kubectl -n iceberg get pods -l app.kubernetes.io/component=trip-stats-shard`) and run again |

## Known limitations

- **Partly tested in the cluster.** Quick start steps 1 to 6 passed on the cluster (see
  [Testing](#testing)). With iceberg-rust 0.11.0, read, write-and-annotate (uncompressed and
  zstd) and step 6 were run again. Not run in the cluster yet:
  - the S3 Metadata Jobs (enable, status, disable);
  - the `spark-iceberg-cleanup` job and `terraform destroy`;
  - a second run of the load job (only tested against a local catalog).
- **Lake Formation.** The Lake Formation mode of the account could not be read with the IDE role.
  In this account the in-cluster run created and wrote the tables with IAM permissions alone, so
  no Lake Formation grants were needed. In an account where Lake Formation governs the catalog,
  Glue access would also need Lake Formation grants.
- **Shared table.** Keep `nyc_taxi_table` unpartitioned, at format v2, with the reference schema.
  The Rust writer (iceberg-rust 0.11.0) appends unpartitioned data files.
- **Distributed trip stats.** They cover only statistics that can be merged from partial
  results; there is no shuffle. The smallest unit of work is one data file. Every shard
  plans the snapshot again and downloads the whole `plan.json`, which grows with the number of
  data files.
- **Graviton5 partly run in the cluster.** The Graviton5 NodePool is applied. On 2026-11-07
  these ran on Graviton5. The Rust Jobs below ran on an on-demand `m9g.large` node:
  - `job-read.yaml`: `rows_read: 20000`, the same as before the move.
  - `job-write-and-annotate.yaml`: 5,000 rows appended and annotated, with
    `compression=Zstd(level=3)`. Its `trip_stats` are identical to the payload's, and the data
    file has 408,283 bytes, the same size as the one the x86_64 build wrote. The table now has
    25,000 rows in 5 data files.
  - `run-distributed-stats.sh --shards 5`: one data file per shard, 69 s for the three steps.
    The merged `trip_stats` are exactly 5 times those of the payload (25,000 trips, CMT 12,160 /
    VTS 12,840, tip rate 11.67%).
  - `spark-iceberg-analytics` (second run in the cluster): the driver and both executors ran on
    an on-demand `m9g.2xlarge`, and the job completed on the first attempt in 75 s. Spark read
    all 5 data files, including those written by the 0.11.0 CLI (uncompressed, and zstd on
    x86_64 and on Graviton5). Its `trip_stats` are identical, field by field, to the Rust
    distributed merge. The curated table converged to the same 4,982 rows in 30 partitions.

  The S3 Metadata Jobs and the `spark-iceberg-load` and `spark-iceberg-cleanup` jobs have only
  passed the offline tests and server-side dry runs on Graviton5 so far.
- **Rust build host.** The binary targets Graviton5 nodes only (`kubernetes.io/arch: arm64`,
  Neoverse V3). It must be built on an arm64 host with glibc 2.34 or older, such as Amazon
  Linux 2023 on the IDE. It may not run on Graviton2/3/4: for a fleet that mixes generations,
  GRAVITON5.md recommends `-Ctarget-cpu=neoverse-512tvb` instead.
