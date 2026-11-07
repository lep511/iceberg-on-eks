//! Command-line interface: builds a [`Request`] from a JSON document plus flag overrides.

use std::io::Read;
use std::path::PathBuf;

use clap::Parser;
use serde_json::{Map, Value};

use crate::models::{Request, Response};

/// Exit code when the action succeeded (`ok: true`).
pub const EXIT_OK: u8 = 0;
/// Exit code when the action ran and failed (`ok: false`).
pub const EXIT_FAILED: u8 = 1;
/// Exit code for usage errors: bad flags, or a request that cannot be read or parsed.
pub const EXIT_USAGE: u8 = 2;

const AFTER_HELP: &str = "\
Actions:
  read, write, write_and_annotate,
  create_metadata_config, get_metadata_config, delete_metadata_config, query_metadata_table,
  put_annotation, get_annotation, delete_annotation, list_annotations,
  bulk_annotate, query_annotation_table
Distributed trip stats (plan, one run per shard, merge; see the README):
  plan_trip_stats, trip_stats_shard, merge_trip_stats

The request is a JSON object with the same fields as the reference Lambda event (default: {}),
plus run_uri, shard_count and shard_index for the distributed actions.
Flags override the matching request fields.

Environment:
  ICEBERG_WAREHOUSE     Iceberg warehouse URI, e.g. s3://<bucket>/warehouse (required)
  AWS_REGION            AWS region (IRSA on EKS also injects it)
  RUST_LOG              Log filter for stderr, e.g. info or iceberg=debug (default: warn)
  JOB_COMPLETION_INDEX  Shard of trip_stats_shard when --shard-index is not given (set by
                        Kubernetes in Indexed Jobs)

Output: the response JSON on one line on stdout. Exit code 0 if \"ok\" is true, 1 if it is
false, 2 for usage errors (invalid flags or request).

Example:
  iceberg-k8s --request nyctaxi_payload.json --action write_and_annotate \\
    --bucket <bucket> --namespace nyc_taxi_database --table nyc_taxi_table";

/// Reads and writes Apache Iceberg tables (Glue catalog), manages S3 Metadata configurations and
/// S3 Object Annotations. Port of the reference iceberg-lambda-s3 Lambda as a CLI for Kubernetes Jobs.
#[derive(Debug, Parser)]
#[command(name = "iceberg-k8s", version, after_help = AFTER_HELP)]
pub struct Cli {
    /// Read the request JSON from FILE ("-" reads stdin)
    #[arg(long, value_name = "FILE", conflicts_with = "request_json")]
    pub request: Option<PathBuf>,

    /// Request JSON given inline
    #[arg(long, value_name = "JSON")]
    pub request_json: Option<String>,

    /// Override the request's "action"
    #[arg(long)]
    pub action: Option<String>,

    /// Override the request's "namespace" (Glue database)
    #[arg(long)]
    pub namespace: Option<String>,

    /// Override the request's "table"
    #[arg(long)]
    pub table: Option<String>,

    /// Override the request's "bucket"
    #[arg(long)]
    pub bucket: Option<String>,

    /// Override the request's "key" (S3 object key, for the annotation actions)
    #[arg(long)]
    pub key: Option<String>,

    /// Override the request's "run_uri" (s3://<bucket>/<prefix>/<run id>, distributed actions)
    #[arg(long, value_name = "URI")]
    pub run_uri: Option<String>,

    /// Override the request's "shard_count" (plan_trip_stats)
    #[arg(long, value_name = "N")]
    pub shard_count: Option<String>,

    /// Override the request's "shard_index" (trip_stats_shard; default JOB_COMPLETION_INDEX)
    #[arg(long, value_name = "N")]
    pub shard_index: Option<String>,
}

impl Cli {
    /// Request fields set by flags, as `(field, value)` pairs.
    pub fn overrides(&self) -> Vec<(&'static str, &str)> {
        [
            ("action", &self.action),
            ("namespace", &self.namespace),
            ("table", &self.table),
            ("bucket", &self.bucket),
            ("key", &self.key),
            ("run_uri", &self.run_uri),
            ("shard_count", &self.shard_count),
            ("shard_index", &self.shard_index),
        ]
        .into_iter()
        .filter_map(|(field, value)| value.as_deref().map(|v| (field, v)))
        .collect()
    }
}

/// Why no request could be built. Reported with exit code [`EXIT_USAGE`].
#[derive(Debug, thiserror::Error)]
pub enum UsageError {
    #[error("cannot read request file {path}: {source}")]
    ReadFile {
        path: String,
        source: std::io::Error,
    },

    #[error("cannot read request from stdin: {0}")]
    ReadStdin(std::io::Error),

    #[error("request is not valid JSON: {0}")]
    InvalidJson(serde_json::Error),

    #[error("request must be a JSON object")]
    NotAnObject,

    #[error("missing field `action`: set it in the request JSON or with --action")]
    MissingAction,

    #[error("invalid request: {0}")]
    InvalidRequest(serde_json::Error),
}

/// Reads the request JSON from `--request` (file or stdin), `--request-json`, or defaults to `{}`.
pub fn read_request_text(cli: &Cli, mut stdin: impl Read) -> Result<String, UsageError> {
    match (&cli.request, &cli.request_json) {
        (Some(path), _) if path.as_os_str() == "-" => {
            let mut text = String::new();
            stdin
                .read_to_string(&mut text)
                .map_err(UsageError::ReadStdin)?;
            Ok(text)
        }
        (Some(path), _) => std::fs::read_to_string(path).map_err(|source| UsageError::ReadFile {
            path: path.display().to_string(),
            source,
        }),
        (None, Some(json)) => Ok(json.clone()),
        (None, None) => Ok("{}".to_string()),
    }
}

/// Parses the request JSON, applies the flag overrides and deserializes it into a [`Request`].
pub fn parse_request(text: &str, overrides: &[(&str, &str)]) -> Result<Request, UsageError> {
    let mut value: Value = serde_json::from_str(text).map_err(UsageError::InvalidJson)?;
    let object: &mut Map<String, Value> = value.as_object_mut().ok_or(UsageError::NotAnObject)?;
    for (field, v) in overrides {
        object.insert((*field).to_string(), Value::String((*v).to_string()));
    }
    if !object.contains_key("action") {
        return Err(UsageError::MissingAction);
    }
    serde_json::from_value(value).map_err(UsageError::InvalidRequest)
}

/// Builds the request for `cli`, reading stdin only for `--request -`.
pub fn build_request(cli: &Cli, stdin: impl Read) -> Result<Request, UsageError> {
    let text = read_request_text(cli, stdin)?;
    parse_request(&text, &cli.overrides())
}

/// Response printed for a usage error.
pub fn usage_error_response(err: &UsageError) -> Response {
    Response {
        ok: false,
        message: err.to_string(),
        rows_read: None,
        data: None,
    }
}

/// Exit code for a response of an action that ran.
pub fn exit_code(response: &Response) -> u8 {
    if response.ok { EXIT_OK } else { EXIT_FAILED }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::SAMPLE_PAYLOAD_PATH;

    fn cli(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("iceberg-k8s").chain(args.iter().copied())).unwrap()
    }

    #[test]
    fn test_cli_parses_all_flags() {
        let c = cli(&[
            "--request-json",
            "{}",
            "--action",
            "read",
            "--namespace",
            "db",
            "--table",
            "t",
            "--bucket",
            "b",
            "--key",
            "k",
        ]);
        assert_eq!(
            c.overrides(),
            [
                ("action", "read"),
                ("namespace", "db"),
                ("table", "t"),
                ("bucket", "b"),
                ("key", "k")
            ]
        );
    }

    #[test]
    fn test_shard_flags_override_request_fields() {
        let c = cli(&[
            "--action",
            "plan_trip_stats",
            "--run-uri",
            "s3://b/lab/runs/r1",
            "--shard-count",
            "4",
            "--shard-index",
            "3",
        ]);
        let req = build_request(&c, std::io::empty()).unwrap();
        assert_eq!(req.run_uri, "s3://b/lab/runs/r1");
        assert_eq!((req.shard_count, req.shard_index), (Some(4), Some(3)));

        let bad = cli(&["--action", "plan_trip_stats", "--shard-count", "many"]);
        assert!(matches!(
            build_request(&bad, std::io::empty()),
            Err(UsageError::InvalidRequest(_))
        ));
    }

    #[test]
    fn test_cli_rejects_both_request_sources_and_unknown_flags() {
        let both =
            Cli::try_parse_from(["iceberg-k8s", "--request", "f.json", "--request-json", "{}"]);
        assert_eq!(
            both.unwrap_err().kind(),
            clap::error::ErrorKind::ArgumentConflict
        );
        let unknown = Cli::try_parse_from(["iceberg-k8s", "--bogus"]);
        assert_eq!(
            unknown.unwrap_err().kind(),
            clap::error::ErrorKind::UnknownArgument
        );
    }

    #[test]
    fn test_default_request_is_empty_object() {
        let text = read_request_text(&cli(&[]), std::io::empty()).unwrap();
        assert_eq!(text, "{}");
        // An empty request without --action is a usage error.
        assert!(matches!(
            parse_request(&text, &[]),
            Err(UsageError::MissingAction)
        ));
    }

    #[test]
    fn test_flags_alone_build_a_request() {
        let c = cli(&["--action", "read", "--namespace", "db", "--table", "t"]);
        let req = build_request(&c, std::io::empty()).unwrap();
        assert_eq!(req.action, "read");
        assert_eq!(req.namespace, "db");
        assert_eq!(req.table, "t");
        assert!(req.bucket.is_empty());
        assert_eq!(req.limit, 100);
    }

    #[test]
    fn test_flags_override_request_fields() {
        let text = r#"{"action":"write","namespace":"a","table":"b","bucket":"c","limit":5}"#;
        let req = parse_request(
            text,
            &[
                ("action", "write_and_annotate"),
                ("namespace", "db"),
                ("bucket", "my-bucket"),
            ],
        )
        .unwrap();
        assert_eq!(req.action, "write_and_annotate");
        assert_eq!(req.namespace, "db");
        assert_eq!(req.table, "b", "fields without a flag are kept");
        assert_eq!(req.bucket, "my-bucket");
        assert_eq!(req.limit, 5);
    }

    #[test]
    fn test_sample_payload_can_be_rerouted_to_write_and_annotate() {
        let c = cli(&[
            "--request",
            SAMPLE_PAYLOAD_PATH,
            "--action",
            "write_and_annotate",
            "--bucket",
            "my-bucket",
            "--namespace",
            "nyc_taxi_database",
            "--table",
            "nyc_taxi_table",
        ]);
        let req = build_request(&c, std::io::empty()).unwrap();
        assert_eq!(req.action, "write_and_annotate");
        assert_eq!(req.bucket, "my-bucket");
        assert_eq!(req.namespace, "nyc_taxi_database");
        assert_eq!(req.table, "nyc_taxi_table");
        assert_eq!(req.rows.len(), 5000);
    }

    #[test]
    fn test_request_from_stdin() {
        let c = cli(&["--request", "-", "--key", "data/f.parquet"]);
        let stdin = r#"{"action":"list_annotations","bucket":"b"}"#.as_bytes();
        let req = build_request(&c, stdin).unwrap();
        assert_eq!(req.action, "list_annotations");
        assert_eq!(req.bucket, "b");
        assert_eq!(req.key, "data/f.parquet");
    }

    #[test]
    fn test_usage_errors() {
        let missing = cli(&["--request", "/nonexistent/request.json"]);
        assert!(matches!(
            build_request(&missing, std::io::empty()),
            Err(UsageError::ReadFile { .. })
        ));
        assert!(matches!(
            parse_request("not json", &[]),
            Err(UsageError::InvalidJson(_))
        ));
        assert!(matches!(
            parse_request("[1, 2]", &[("action", "read")]),
            Err(UsageError::NotAnObject)
        ));
        // Wrong field types are reported by serde with the field name.
        let err = parse_request(r#"{"action":"read","limit":"ten"}"#, &[]).unwrap_err();
        assert!(matches!(err, UsageError::InvalidRequest(_)));
        assert!(err.to_string().starts_with("invalid request: "), "{err}");
    }

    #[test]
    fn test_usage_error_response_and_exit_codes() {
        let resp = usage_error_response(&UsageError::MissingAction);
        assert!(!resp.ok);
        assert_eq!(
            serde_json::to_value(&resp).unwrap(),
            serde_json::json!({
                "ok": false,
                "message": "missing field `action`: set it in the request JSON or with --action"
            })
        );
        assert_eq!(exit_code(&resp), EXIT_FAILED);
        let ok = Response {
            ok: true,
            message: "read completed".to_string(),
            rows_read: Some(1),
            data: None,
        };
        assert_eq!(exit_code(&ok), EXIT_OK);
        assert_eq!((EXIT_OK, EXIT_FAILED, EXIT_USAGE), (0, 1, 2));
    }
}
