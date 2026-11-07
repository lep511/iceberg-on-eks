use aws_sdk_s3::Client as S3Client;

use crate::annotations::{delete_annotation, get_annotation, list_annotations, put_annotation};
use crate::aws;
use crate::catalog::{build_catalog, load_table, read_table};
use crate::distributed::{
    merge_shard_results, plan_trip_stats, read_plan, read_shard_results, run_trip_stats_shard,
    shard_index_from_env, validate_run_uri, validate_shard_count, write_plan, write_result,
    write_shard_result,
};
use crate::error::AppError;
use crate::metadata::{
    create_metadata_config, delete_metadata_config, get_metadata_config, load_metadata_table,
};
use crate::models::{Request, Response};
use crate::s3_uri::parse_s3_uri;
use crate::stats::{bulk_annotate, query_annotation_table, write_rows_with_annotation};
use crate::writer::write_rows;

/// The supported actions: the 13 of the reference Lambda (same names and payloads), then the
/// three steps of the distributed trip statistics.
pub const ACTIONS: [&str; 16] = [
    "read",
    "write",
    "create_metadata_config",
    "get_metadata_config",
    "delete_metadata_config",
    "query_metadata_table",
    "put_annotation",
    "get_annotation",
    "delete_annotation",
    "list_annotations",
    "write_and_annotate",
    "bulk_annotate",
    "query_annotation_table",
    "plan_trip_stats",
    "trip_stats_shard",
    "merge_trip_stats",
];

fn require_field(value: &str, name: &'static str) -> Result<(), AppError> {
    if value.is_empty() {
        return Err(AppError::MissingField(name));
    }
    Ok(())
}

async fn build_s3_client() -> S3Client {
    aws::context().await.s3_client()
}

/// Validates the `ICEBERG_WAREHOUSE` value (`s3://<bucket>/<prefix>`; `s3a://` also works).
fn validate_warehouse(warehouse: Option<String>) -> Result<String, AppError> {
    let warehouse = warehouse.ok_or_else(|| {
        AppError::Config("ICEBERG_WAREHOUSE environment variable not set".to_string())
    })?;
    if parse_s3_uri(&warehouse).is_none() {
        return Err(AppError::Config(format!(
            "ICEBERG_WAREHOUSE must be an S3 URI like s3://<bucket>/warehouse, got '{warehouse}'"
        )));
    }
    Ok(warehouse)
}

/// Routes the request to the appropriate action handler.
/// `warehouse` is the value of `ICEBERG_WAREHOUSE` (a parameter, so tests do not have to change
/// the process environment).
async fn handle_action(req: Request, warehouse: Option<String>) -> Result<Response, AppError> {
    let warehouse = validate_warehouse(warehouse)?;
    tracing::info!(
        action = %req.action,
        namespace = %req.namespace,
        table = %req.table,
        bucket = %req.bucket,
        rows = req.rows.len(),
        "running action"
    );

    match req.action.as_str() {
        "read" => {
            require_field(&req.namespace, "namespace")?;
            require_field(&req.table, "table")?;
            let catalog = build_catalog(&warehouse).await?;
            let table = load_table(&catalog, &req.namespace, &req.table).await?;
            let rows_read = read_table(&table).await?;
            Ok(Response {
                ok: true,
                message: "read completed".to_string(),
                rows_read: Some(rows_read),
                data: None,
            })
        }
        "write" => {
            require_field(&req.namespace, "namespace")?;
            require_field(&req.table, "table")?;
            if req.rows.is_empty() {
                return Err(AppError::MissingField("rows"));
            }
            let catalog = build_catalog(&warehouse).await?;
            let table = load_table(&catalog, &req.namespace, &req.table).await?;
            write_rows(&catalog, &table, &req.rows).await?;
            Ok(Response {
                ok: true,
                message: format!("{} rows written", req.rows.len()),
                rows_read: None,
                data: None,
            })
        }

        "create_metadata_config" => {
            require_field(&req.bucket, "bucket")?;
            let config = req.metadata_config.unwrap_or_default();
            let s3_client = build_s3_client().await;
            let result = create_metadata_config(&s3_client, &req.bucket, &config).await?;
            Ok(Response {
                ok: true,
                message: format!(
                    "metadata table configuration created for bucket '{}'",
                    req.bucket
                ),
                rows_read: None,
                data: Some(result),
            })
        }
        "get_metadata_config" => {
            require_field(&req.bucket, "bucket")?;
            let s3_client = build_s3_client().await;
            let result = get_metadata_config(&s3_client, &req.bucket).await?;
            Ok(Response {
                ok: true,
                message: format!("metadata table configuration for bucket '{}'", req.bucket),
                rows_read: None,
                data: Some(result),
            })
        }
        "delete_metadata_config" => {
            require_field(&req.bucket, "bucket")?;
            let s3_client = build_s3_client().await;
            delete_metadata_config(&s3_client, &req.bucket).await?;
            Ok(Response {
                ok: true,
                message: format!(
                    "metadata table configuration deleted for bucket '{}'",
                    req.bucket
                ),
                rows_read: None,
                data: None,
            })
        }

        "query_metadata_table" => {
            require_field(&req.bucket, "bucket")?;
            require_field(&req.table, "table")?;
            // Resolved through the S3 Tables API like `query_annotation_table`: the Glue
            // default catalog that the reference Lambda used cannot see `s3tablescatalog`.
            let s3_client = build_s3_client().await;
            let table = load_metadata_table(&s3_client, &req.bucket, &req.table).await?;
            let rows_read = read_table(&table).await?;
            Ok(Response {
                ok: true,
                message: format!(
                    "metadata table '{}' from bucket '{}': {} rows",
                    req.table, req.bucket, rows_read
                ),
                rows_read: Some(rows_read),
                data: None,
            })
        }

        "put_annotation" => {
            require_field(&req.bucket, "bucket")?;
            require_field(&req.key, "key")?;
            require_field(&req.annotation_name, "annotation_name")?;
            let s3_client = build_s3_client().await;
            put_annotation(
                &s3_client,
                &req.bucket,
                &req.key,
                &req.annotation_name,
                &req.annotation_value,
            )
            .await?;
            Ok(Response {
                ok: true,
                message: format!(
                    "annotation '{}' added to object '{}/{}'",
                    req.annotation_name, req.bucket, req.key
                ),
                rows_read: None,
                data: None,
            })
        }
        "get_annotation" => {
            require_field(&req.bucket, "bucket")?;
            require_field(&req.key, "key")?;
            require_field(&req.annotation_name, "annotation_name")?;
            let s3_client = build_s3_client().await;
            let result =
                get_annotation(&s3_client, &req.bucket, &req.key, &req.annotation_name).await?;
            Ok(Response {
                ok: true,
                message: format!(
                    "annotation '{}' from object '{}/{}'",
                    req.annotation_name, req.bucket, req.key
                ),
                rows_read: None,
                data: Some(result),
            })
        }
        "delete_annotation" => {
            require_field(&req.bucket, "bucket")?;
            require_field(&req.key, "key")?;
            require_field(&req.annotation_name, "annotation_name")?;
            let s3_client = build_s3_client().await;
            delete_annotation(&s3_client, &req.bucket, &req.key, &req.annotation_name).await?;
            Ok(Response {
                ok: true,
                message: format!(
                    "annotation '{}' deleted from object '{}/{}'",
                    req.annotation_name, req.bucket, req.key
                ),
                rows_read: None,
                data: None,
            })
        }
        "list_annotations" => {
            require_field(&req.bucket, "bucket")?;
            require_field(&req.key, "key")?;
            let s3_client = build_s3_client().await;
            let result = list_annotations(&s3_client, &req.bucket, &req.key).await?;
            Ok(Response {
                ok: true,
                message: format!("annotations for object '{}/{}'", req.bucket, req.key),
                rows_read: None,
                data: Some(result),
            })
        }

        "write_and_annotate" => {
            require_field(&req.namespace, "namespace")?;
            require_field(&req.table, "table")?;
            require_field(&req.bucket, "bucket")?;
            if req.rows.is_empty() {
                return Err(AppError::MissingField("rows"));
            }
            let catalog = build_catalog(&warehouse).await?;
            let table = load_table(&catalog, &req.namespace, &req.table).await?;
            let s3_client = build_s3_client().await;
            let result =
                write_rows_with_annotation(&catalog, &table, &req.rows, &s3_client, &req.bucket)
                    .await?;
            Ok(Response {
                ok: true,
                message: format!(
                    "{} rows written and data files annotated with trip_stats",
                    req.rows.len()
                ),
                rows_read: None,
                data: Some(result),
            })
        }

        "bulk_annotate" => {
            require_field(&req.bucket, "bucket")?;
            if req.annotations.is_empty() {
                return Err(AppError::MissingField("annotations"));
            }
            let s3_client = build_s3_client().await;
            let result = bulk_annotate(&s3_client, &req.bucket, &req.annotations).await?;
            Ok(Response {
                ok: true,
                message: format!(
                    "bulk annotation completed for {} objects",
                    req.annotations.len()
                ),
                rows_read: None,
                data: Some(result),
            })
        }

        "query_annotation_table" => {
            require_field(&req.bucket, "bucket")?;
            let s3_client = build_s3_client().await;
            let result = query_annotation_table(
                &req.bucket,
                &s3_client,
                &req.filter_name,
                &req.key_prefix,
                req.limit,
            )
            .await?;
            let rows_returned = result["results_returned"].as_u64().unwrap_or(0) as usize;
            Ok(Response {
                ok: true,
                message: format!("annotation table query returned {} results", rows_returned),
                rows_read: Some(rows_returned),
                data: Some(result),
            })
        }

        "plan_trip_stats" => {
            require_field(&req.namespace, "namespace")?;
            require_field(&req.table, "table")?;
            require_field(&req.run_uri, "run_uri")?;
            let shard_count = req
                .shard_count
                .ok_or(AppError::MissingField("shard_count"))?;
            validate_run_uri(&req.run_uri)?;
            validate_shard_count(shard_count)?;
            let catalog = build_catalog(&warehouse).await?;
            let table = load_table(&catalog, &req.namespace, &req.table).await?;
            let plan = plan_trip_stats(
                &table,
                &req.namespace,
                &req.table,
                &req.run_uri,
                shard_count,
            )
            .await?;
            let s3_client = build_s3_client().await;
            write_plan(&s3_client, &plan).await?;
            Ok(Response {
                ok: true,
                message: format!(
                    "planned {} data files ({} bytes) of snapshot {} into {} shards",
                    plan.total_files,
                    plan.total_bytes,
                    plan.snapshot_id
                        .map_or_else(|| "none".to_string(), |id| id.to_string()),
                    plan.shard_count
                ),
                rows_read: None,
                data: Some(plan.summary()),
            })
        }
        "trip_stats_shard" => {
            require_field(&req.run_uri, "run_uri")?;
            validate_run_uri(&req.run_uri)?;
            let index = match req.shard_index {
                Some(index) => index,
                None => shard_index_from_env(std::env::var("JOB_COMPLETION_INDEX").ok())?,
            };
            let s3_client = build_s3_client().await;
            let plan = read_plan(&s3_client, &req.run_uri).await?;
            let catalog = build_catalog(&warehouse).await?;
            let table = load_table(&catalog, &plan.namespace, &plan.table).await?;
            let result = run_trip_stats_shard(&table, &plan, index).await?;
            write_shard_result(&s3_client, &result).await?;
            Ok(Response {
                ok: true,
                message: format!(
                    "shard {} of {}: {} rows from {} data files",
                    index, plan.shard_count, result.rows_read, result.files_read
                ),
                rows_read: Some(result.rows_read as usize),
                data: Some(result.summary()),
            })
        }
        "merge_trip_stats" => {
            require_field(&req.run_uri, "run_uri")?;
            validate_run_uri(&req.run_uri)?;
            let s3_client = build_s3_client().await;
            let plan = read_plan(&s3_client, &req.run_uri).await?;
            let results = read_shard_results(&s3_client, &plan).await?;
            let merged = merge_shard_results(&plan, &results)?;
            write_result(&s3_client, &req.run_uri, &merged).await?;
            let rows = merged["rows_read"].as_u64().unwrap_or(0) as usize;
            Ok(Response {
                ok: true,
                message: format!(
                    "trip stats of {} rows from {} data files, merged from {} shards",
                    rows, plan.total_files, plan.shard_count
                ),
                rows_read: Some(rows),
                data: Some(merged),
            })
        }

        other => Err(AppError::UnknownAction(other.to_string())),
    }
}

/// Runs one request with the `ICEBERG_WAREHOUSE` of the environment. Errors become structured
/// responses with `ok: false`, like the Lambda did.
pub async fn run(req: Request) -> Response {
    run_with_warehouse(req, std::env::var("ICEBERG_WAREHOUSE").ok()).await
}

async fn run_with_warehouse(req: Request, warehouse: Option<String>) -> Response {
    match handle_action(req, warehouse).await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::error!("{e}");
            if matches!(e, AppError::UnknownAction(_)) {
                tracing::warn!("supported actions: {}", ACTIONS.join(", "));
            }
            Response {
                ok: false,
                message: e.to_string(),
                rows_read: None,
                data: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_require_field_rejects_empty_value() {
        let err = require_field("", "bucket").unwrap_err();
        assert!(matches!(err, AppError::MissingField("bucket")));
    }

    #[test]
    fn test_require_field_accepts_non_empty_value() {
        assert!(require_field("my-bucket", "bucket").is_ok());
    }

    #[test]
    fn test_validate_warehouse_accepts_s3_and_s3a() {
        for w in ["s3://my-bucket/warehouse", "s3a://my-bucket/warehouse"] {
            assert_eq!(validate_warehouse(Some(w.to_string())).unwrap(), w);
        }
    }

    #[test]
    fn test_validate_warehouse_rejects_missing_or_invalid_values() {
        assert_eq!(
            validate_warehouse(None).unwrap_err().to_string(),
            "configuration error: ICEBERG_WAREHOUSE environment variable not set"
        );
        for w in ["", "my-bucket/warehouse", "gs://my-bucket/warehouse"] {
            let err = validate_warehouse(Some(w.to_string())).unwrap_err();
            assert!(matches!(err, AppError::Config(_)), "{w}: {err}");
        }
    }

    fn warehouse() -> Option<String> {
        Some("s3://my-bucket/warehouse".to_string())
    }

    fn request(action: &str) -> Request {
        serde_json::from_value(serde_json::json!({ "action": action })).unwrap()
    }

    // The checks below fail before any AWS call, so they run offline.
    #[tokio::test]
    async fn test_handle_action_rejects_unknown_action() {
        // FIXME: Audit that the environment access only happens in single-threaded code.
        let err = handle_action(request("drop"), warehouse())
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "unknown action: drop");
    }

    #[tokio::test]
    async fn test_handle_action_checks_required_fields_first() {
        // FIXME: Audit that the environment access only happens in single-threaded code.
        let cases = [
            ("read", "namespace"),
            ("write", "namespace"),
            ("write_and_annotate", "namespace"),
            ("create_metadata_config", "bucket"),
            ("get_metadata_config", "bucket"),
            ("delete_metadata_config", "bucket"),
            ("query_metadata_table", "bucket"),
            ("put_annotation", "bucket"),
            ("get_annotation", "bucket"),
            ("delete_annotation", "bucket"),
            ("list_annotations", "bucket"),
            ("bulk_annotate", "bucket"),
            ("query_annotation_table", "bucket"),
            ("plan_trip_stats", "namespace"),
            ("trip_stats_shard", "run_uri"),
            ("merge_trip_stats", "run_uri"),
        ];
        assert_eq!(cases.len(), ACTIONS.len());
        for (action, field) in cases {
            assert!(ACTIONS.contains(&action));
            let err = handle_action(request(action), warehouse())
                .await
                .unwrap_err();
            assert_eq!(
                err.to_string(),
                format!("missing required field: {field}"),
                "action {action}"
            );
        }
    }

    #[tokio::test]
    async fn test_distributed_actions_validate_before_any_aws_call() {
        // FIXME: Audit that the environment access only happens in single-threaded code.
        let req = |json: serde_json::Value| -> Request { serde_json::from_value(json).unwrap() };
        let cases = [
            (
                serde_json::json!({"action": "plan_trip_stats", "namespace": "db", "table": "t",
                                   "run_uri": "s3://b/lab/runs/r1"}),
                "missing required field: shard_count",
            ),
            (
                serde_json::json!({"action": "plan_trip_stats", "namespace": "db", "table": "t",
                                   "run_uri": "s3://b/lab/runs/r1", "shard_count": 0}),
                "configuration error: shard_count must be between 1 and 1000, got 0",
            ),
            (
                serde_json::json!({"action": "plan_trip_stats", "namespace": "db", "table": "t",
                                   "run_uri": "s3://b/lab/runs/", "shard_count": 2}),
                "configuration error: run_uri must be s3://<bucket>/<prefix>/<run id> without a trailing slash, got 's3://b/lab/runs/'",
            ),
            (
                serde_json::json!({"action": "merge_trip_stats", "run_uri": "s3://b"}),
                "configuration error: run_uri must be s3://<bucket>/<prefix>/<run id> without a trailing slash, got 's3://b'",
            ),
        ];
        for (json, expected) in cases {
            let err = handle_action(req(json.clone()), warehouse())
                .await
                .unwrap_err();
            assert_eq!(err.to_string(), expected, "{json}");
        }
    }

    #[tokio::test]
    async fn test_run_wraps_errors_in_response() {
        // FIXME: Audit that the environment access only happens in single-threaded code.
        let resp = run_with_warehouse(request("drop"), warehouse()).await;
        assert!(!resp.ok);
        assert_eq!(resp.message, "unknown action: drop");
        assert!(resp.rows_read.is_none() && resp.data.is_none());
    }
}
