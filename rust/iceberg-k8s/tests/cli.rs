//! End-to-end checks of the `iceberg-k8s` binary: output JSON and exit codes.
//!
//! Every case fails before the first AWS call, so the suite runs offline.

use std::io::Write;
use std::process::{Command, Output, Stdio};

use serde_json::Value;

const SAMPLE_PAYLOAD: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../data/nyctaxi_payload.json"
);

fn run(args: &[&str], warehouse: Option<&str>, stdin: Option<&str>) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_iceberg-k8s"));
    cmd.args(args)
        .env_remove("RUST_LOG")
        .env_remove("ICEBERG_WAREHOUSE")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(w) = warehouse {
        cmd.env("ICEBERG_WAREHOUSE", w);
    }
    let mut child = cmd.spawn().expect("binary should start");
    {
        let mut pipe = child.stdin.take().unwrap();
        if let Some(input) = stdin {
            pipe.write_all(input.as_bytes()).unwrap();
        }
    }
    child.wait_with_output().unwrap()
}

/// The single line of JSON printed on stdout.
fn response(out: &Output) -> Value {
    let stdout = String::from_utf8(out.stdout.clone()).unwrap();
    assert_eq!(stdout.lines().count(), 1, "stdout: {stdout}");
    serde_json::from_str(&stdout).expect("stdout should be JSON")
}

const WAREHOUSE: Option<&str> = Some("s3://my-bucket/warehouse");

#[test]
fn empty_request_is_a_usage_error() {
    let out = run(&["--request-json", "{}"], WAREHOUSE, None);
    assert_eq!(out.status.code(), Some(2));
    let resp = response(&out);
    assert_eq!(resp["ok"], false);
    assert_eq!(
        resp["message"],
        "missing field `action`: set it in the request JSON or with --action"
    );
}

#[test]
fn no_arguments_default_to_an_empty_request() {
    let out = run(&[], WAREHOUSE, None);
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(response(&out)["ok"], false);
}

#[test]
fn invalid_json_and_missing_file_are_usage_errors() {
    let out = run(&["--request-json", "not json"], WAREHOUSE, None);
    assert_eq!(out.status.code(), Some(2));
    assert!(
        response(&out)["message"]
            .as_str()
            .unwrap()
            .starts_with("request is not valid JSON")
    );

    let out = run(&["--request", "/nonexistent/request.json"], WAREHOUSE, None);
    assert_eq!(out.status.code(), Some(2));
    assert!(
        response(&out)["message"]
            .as_str()
            .unwrap()
            .starts_with("cannot read request file /nonexistent/request.json")
    );
}

#[test]
fn invalid_flags_exit_with_2_and_print_usage() {
    let out = run(&["--bogus"], WAREHOUSE, None);
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("Usage:"));

    let out = run(
        &["--request", "f.json", "--request-json", "{}"],
        WAREHOUSE,
        None,
    );
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn help_lists_the_actions() {
    let out = run(&["--help"], None, None);
    assert_eq!(out.status.code(), Some(0));
    let help = String::from_utf8_lossy(&out.stdout);
    for action in [
        "write_and_annotate",
        "create_metadata_config",
        "list_annotations",
    ] {
        assert!(help.contains(action), "{action} missing from --help");
    }
}

#[test]
fn unknown_action_exits_with_1() {
    let out = run(&["--request-json", r#"{"action":"drop"}"#], WAREHOUSE, None);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        response(&out),
        serde_json::json!({"ok": false, "message": "unknown action: drop"})
    );
}

#[test]
fn missing_warehouse_exits_with_1() {
    let out = run(&["--action", "read"], None, None);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        response(&out)["message"],
        "configuration error: ICEBERG_WAREHOUSE environment variable not set"
    );

    let out = run(&["--action", "read"], Some("my-bucket/warehouse"), None);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        response(&out)["message"]
            .as_str()
            .unwrap()
            .contains("ICEBERG_WAREHOUSE must be an S3 URI")
    );
}

#[test]
fn flags_fill_in_the_request() {
    let out = run(&["--action", "read", "--namespace", "db"], WAREHOUSE, None);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(response(&out)["message"], "missing required field: table");
}

#[test]
fn request_from_stdin() {
    let out = run(
        &["--request", "-"],
        WAREHOUSE,
        Some(r#"{"action":"list_annotations","bucket":"my-bucket"}"#),
    );
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(response(&out)["message"], "missing required field: key");
}

#[test]
fn sample_payload_action_can_be_overridden() {
    // The payload's action is "write"; write_and_annotate also needs a bucket.
    let out = run(
        &[
            "--request",
            SAMPLE_PAYLOAD,
            "--action",
            "write_and_annotate",
            "--namespace",
            "nyc_taxi_database",
            "--table",
            "nyc_taxi_table",
        ],
        WAREHOUSE,
        None,
    );
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(response(&out)["message"], "missing required field: bucket");
}

#[test]
fn logs_go_to_stderr_only() {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_iceberg-k8s"));
    let out = cmd
        .args(["--request-json", r#"{"action":"drop"}"#])
        .env("ICEBERG_WAREHOUSE", "s3://my-bucket/warehouse")
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    // stdout stays a single JSON line even with info logs enabled.
    assert_eq!(response(&out)["message"], "unknown action: drop");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("running action"), "stderr: {stderr}");
    assert!(stderr.contains("supported actions:"), "stderr: {stderr}");
}
