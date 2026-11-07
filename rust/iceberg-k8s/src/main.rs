//! `iceberg-k8s`: CLI that reads/writes Apache Iceberg tables via the Glue Catalog, manages
//! S3 Metadata Table configurations and S3 Object Annotations. It is a port of the reference
//! `iceberg-lambda-s3` AWS Lambda, meant to run as a Kubernetes Job on EKS with IRSA credentials.
//!
//! The request JSON is read from `--request <file>|-` or `--request-json`, flags override its
//! fields, the response JSON is printed on stdout and logs go to stderr.

mod annotations;
mod aws;
mod catalog;
mod cli;
mod distributed;
mod error;
mod handler;
mod metadata;
mod models;
mod s3_uri;
mod stats;
mod writer;

use std::io::{IsTerminal, Write};
use std::process::ExitCode;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::cli::Cli;
use crate::models::Response;

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .with_target(false)
        .init();
}

/// Prints the response as one line of JSON on stdout.
fn print_response(response: &Response) {
    let json = serde_json::to_string(response).unwrap_or_else(|e| {
        format!(r#"{{"ok":false,"message":"cannot serialize response: {e}"}}"#)
    });
    let mut stdout = std::io::stdout().lock();
    if let Err(e) = writeln!(stdout, "{json}").and_then(|()| stdout.flush()) {
        tracing::error!("cannot write the response to stdout: {e}");
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    // Invalid flags print clap's usage message on stderr and exit with code 2.
    let cli = Cli::parse();
    init_tracing();

    let request = match cli::build_request(&cli, std::io::stdin().lock()) {
        Ok(request) => request,
        Err(e) => {
            tracing::error!("{e}");
            print_response(&cli::usage_error_response(&e));
            return ExitCode::from(cli::EXIT_USAGE);
        }
    };

    let response = handler::run(request).await;
    print_response(&response);
    ExitCode::from(cli::exit_code(&response))
}
