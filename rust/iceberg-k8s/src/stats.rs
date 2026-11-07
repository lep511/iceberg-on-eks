use std::collections::HashMap;

use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch, StringArray};
use aws_sdk_s3::Client as S3Client;
use futures::{StreamExt, TryStreamExt, stream};
use iceberg::Catalog;
use serde_json::Value;

use crate::annotations::put_annotation;
use crate::error::AppError;
use crate::metadata::load_metadata_table;
use crate::models::{AnnotationInput, RowInput};
use crate::s3_uri::object_key_from_path;
use crate::writer::{build_record_batch, commit_batch};

/// Computes trip statistics from a set of rows.
pub fn compute_trip_stats(rows: &[RowInput]) -> Value {
    if rows.is_empty() {
        return serde_json::json!({});
    }

    let total_trips = rows.len();
    let total_distance: f64 = rows.iter().map(|r| r.trip_distance).sum();
    let total_fare: f64 = rows.iter().map(|r| r.fare_amount).sum();
    let total_tips: f64 = rows.iter().map(|r| r.tip_amount).sum();
    let total_passengers: i32 = rows.iter().map(|r| r.passenger_count).sum();
    let avg_distance = total_distance / total_trips as f64;
    let avg_fare = total_fare / total_trips as f64;
    let avg_tip = total_tips / total_trips as f64;
    let tip_rate = if total_fare > 0.0 {
        total_tips / total_fare * 100.0
    } else {
        0.0
    };

    let vendors: Vec<&str> = rows.iter().map(|r| r.vendor_id.as_str()).collect();
    let mut vendor_counts: HashMap<&str, usize> = HashMap::new();
    for v in &vendors {
        *vendor_counts.entry(v).or_insert(0) += 1;
    }

    let payment_types: Vec<&str> = rows.iter().map(|r| r.payment_type.as_str()).collect();
    let mut payment_counts: HashMap<&str, usize> = HashMap::new();
    for p in &payment_types {
        *payment_counts.entry(p).or_insert(0) += 1;
    }

    let max_distance = rows
        .iter()
        .map(|r| r.trip_distance)
        .fold(f64::NEG_INFINITY, f64::max);
    let min_distance = rows
        .iter()
        .map(|r| r.trip_distance)
        .fold(f64::INFINITY, f64::min);

    serde_json::json!({
        "total_trips": total_trips,
        "total_passengers": total_passengers,
        "total_distance_miles": (total_distance * 100.0).round() / 100.0,
        "total_fare_usd": (total_fare * 100.0).round() / 100.0,
        "total_tips_usd": (total_tips * 100.0).round() / 100.0,
        "avg_distance_miles": (avg_distance * 100.0).round() / 100.0,
        "avg_fare_usd": (avg_fare * 100.0).round() / 100.0,
        "avg_tip_usd": (avg_tip * 100.0).round() / 100.0,
        "tip_rate_percent": (tip_rate * 100.0).round() / 100.0,
        "max_distance_miles": max_distance,
        "min_distance_miles": min_distance,
        "vendor_breakdown": vendor_counts,
        "payment_type_breakdown": payment_counts,
    })
}

/// Writes rows to Iceberg AND annotates the data file with computed trip statistics.
pub async fn write_rows_with_annotation(
    catalog: &impl Catalog,
    table: &iceberg::table::Table,
    rows: &[RowInput],
    s3_client: &S3Client,
    bucket: &str,
) -> Result<Value, AppError> {
    let stats = compute_trip_stats(rows);
    let batch = build_record_batch(rows)?;
    let file_paths = commit_batch(catalog, table, batch).await?;

    // Annotate each written data file with trip_stats
    let stats_json = serde_json::to_string(&stats).unwrap_or_default();
    let mut annotated_files = Vec::new();

    for file_path in &file_paths {
        // Data file paths are full URIs (`s3://` or `s3a://`, from the table location).
        let Some(object_key) = object_key_from_path(file_path, bucket) else {
            tracing::warn!(
                "Skipping annotation of {}: not an object in bucket '{}'",
                file_path,
                bucket
            );
            continue;
        };

        match put_annotation(s3_client, bucket, object_key, "trip_stats", &stats_json).await {
            Ok(()) => annotated_files.push(object_key.to_string()),
            Err(e) => {
                tracing::warn!("Failed to annotate {}: {}", object_key, e);
            }
        }
    }
    tracing::info!(
        files_created = file_paths.len(),
        files_annotated = annotated_files.len(),
        "committed data files and annotated them with trip_stats"
    );

    Ok(serde_json::json!({
        "rows_written": rows.len(),
        "files_created": file_paths.len(),
        "files_annotated": annotated_files.len(),
        "trip_stats": stats,
        "annotated_keys": annotated_files,
    }))
}

/// Applies annotations to multiple S3 objects concurrently (up to 10 in parallel).
pub async fn bulk_annotate(
    s3_client: &S3Client,
    bucket: &str,
    annotations: &[AnnotationInput],
) -> Result<Value, AppError> {
    const CONCURRENCY: usize = 10;

    let results: Vec<Result<Value, Value>> = stream::iter(annotations)
        .map(|ann| async move {
            match put_annotation(
                s3_client,
                bucket,
                &ann.key,
                &ann.annotation_name,
                &ann.annotation_value,
            )
            .await
            {
                Ok(()) => Ok(serde_json::json!({
                    "key": ann.key,
                    "annotation_name": ann.annotation_name,
                })),
                Err(e) => Err(serde_json::json!({
                    "key": ann.key,
                    "annotation_name": ann.annotation_name,
                    "error": e.to_string(),
                })),
            }
        })
        .buffer_unordered(CONCURRENCY)
        .collect()
        .await;

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();
    for r in results {
        match r {
            Ok(v) => succeeded.push(v),
            Err(v) => failed.push(v),
        }
    }

    Ok(serde_json::json!({
        "total": annotations.len(),
        "succeeded": succeeded.len(),
        "failed": failed.len(),
        "succeeded_details": succeeded,
        "failed_details": failed,
    }))
}

/// Queries the S3 Metadata annotation table of `bucket`, loaded with [`load_metadata_table`]
/// (S3 Tables API + `StaticTable`, bypassing the Glue catalog).
pub async fn query_annotation_table(
    bucket: &str,
    s3_client: &S3Client,
    filter_name: &str,
    key_prefix: &str,
    limit: usize,
) -> Result<Value, AppError> {
    let namespace = format!("b_{}", bucket);
    let table = load_metadata_table(s3_client, bucket, "annotation").await?;

    let scan = table.scan().build()?;
    let mut stream = scan.to_arrow().await?;

    let mut results: Vec<Value> = Vec::new();
    let mut total_scanned: usize = 0;

    while let Some(batch) = stream.try_next().await? {
        total_scanned += batch.num_rows();
        collect_annotation_rows(&batch, filter_name, key_prefix, limit, &mut results);

        if results.len() >= limit {
            break;
        }
    }

    Ok(serde_json::json!({
        "table": "annotation",
        "namespace": namespace,
        "filter_name": filter_name,
        "key_prefix": key_prefix,
        "total_rows_scanned": total_scanned,
        "results_returned": results.len(),
        "results": results,
    }))
}

/// Appends the rows of an annotation-table batch that match `filter_name` / `key_prefix`
/// (empty = no filter) to `results`, stopping once it holds `limit` rows.
fn collect_annotation_rows(
    batch: &RecordBatch,
    filter_name: &str,
    key_prefix: &str,
    limit: usize,
    results: &mut Vec<Value>,
) {
    let schema = batch.schema();
    let col_idx =
        |name: &str| -> Option<usize> { schema.fields().iter().position(|f| f.name() == name) };

    let object_key_idx = col_idx("object_key");
    let name_idx = col_idx("name");
    let text_value_idx = col_idx("text_value");
    let size_idx = col_idx("size");
    let sequence_number_idx = col_idx("sequence_number");
    let bucket_idx = col_idx("bucket");

    for row in 0..batch.num_rows() {
        if results.len() >= limit {
            break;
        }

        let get_str = |idx: Option<usize>| -> String {
            match idx {
                Some(i) => {
                    let col = batch.column(i);
                    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                        if a.is_null(row) {
                            String::new()
                        } else {
                            a.value(row).to_string()
                        }
                    } else if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
                        if a.is_null(row) {
                            String::new()
                        } else {
                            a.value(row).to_string()
                        }
                    } else {
                        tracing::warn!("column {} has unexpected type: {:?}", i, col.data_type());
                        String::new()
                    }
                }
                None => String::new(),
            }
        };

        let obj_key = get_str(object_key_idx);
        let ann_name = get_str(name_idx);

        if !filter_name.is_empty() && ann_name != filter_name {
            continue;
        }
        if !key_prefix.is_empty() && !obj_key.starts_with(key_prefix) {
            continue;
        }

        let text_val = get_str(text_value_idx);
        let bucket_val = get_str(bucket_idx);
        let seq_num = get_str(sequence_number_idx);

        let ann_size: Value = match size_idx {
            Some(i) => batch
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| {
                    if a.is_null(row) {
                        Value::Null
                    } else {
                        Value::from(a.value(row))
                    }
                })
                .unwrap_or(Value::Null),
            None => Value::Null,
        };

        let text_value_parsed: Value =
            serde_json::from_str(&text_val).unwrap_or(Value::String(text_val));

        results.push(serde_json::json!({
            "bucket": bucket_val,
            "object_key": obj_key,
            "name": ann_name,
            "text_value": text_value_parsed,
            "size": ann_size,
            "sequence_number": seq_num,
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::ArrayRef;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    use crate::models::{sample_payload, sample_row};

    fn row(
        vendor_id: &str,
        payment_type: &str,
        trip_distance: f64,
        fare_amount: f64,
        tip_amount: f64,
        passenger_count: i32,
    ) -> RowInput {
        let mut r = sample_row();
        r.vendor_id = vendor_id.to_string();
        r.payment_type = payment_type.to_string();
        r.trip_distance = trip_distance;
        r.fare_amount = fare_amount;
        r.tip_amount = tip_amount;
        r.passenger_count = passenger_count;
        r
    }

    /// Three annotation rows; string columns as Utf8 or LargeUtf8.
    fn annotation_batch(large_strings: bool) -> RecordBatch {
        let string_type = if large_strings {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        };
        let strings = |values: Vec<Option<&str>>| -> ArrayRef {
            if large_strings {
                Arc::new(LargeStringArray::from(values))
            } else {
                Arc::new(StringArray::from(values))
            }
        };
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("bucket", string_type.clone(), true),
            Field::new("object_key", string_type.clone(), true),
            Field::new("name", string_type.clone(), true),
            Field::new("text_value", string_type.clone(), true),
            Field::new("size", DataType::Int64, true),
            Field::new("sequence_number", string_type, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                strings(vec![Some("b"), Some("b"), Some("b")]),
                strings(vec![
                    Some("data/a.parquet"),
                    Some("data/b.parquet"),
                    Some("other/c.parquet"),
                ]),
                strings(vec![
                    Some("trip_stats"),
                    Some("quality"),
                    Some("trip_stats"),
                ]),
                strings(vec![Some(r#"{"total_trips":5}"#), Some("not json"), None]),
                Arc::new(Int64Array::from(vec![Some(17), None, Some(3)])),
                strings(vec![Some("1"), Some("2"), Some("3")]),
            ],
        )
        .unwrap()
    }

    fn object_keys(results: &[Value]) -> Vec<&str> {
        results
            .iter()
            .map(|r| r["object_key"].as_str().unwrap())
            .collect()
    }

    #[test]
    fn test_compute_trip_stats_empty_rows() {
        assert_eq!(compute_trip_stats(&[]), serde_json::json!({}));
    }

    #[test]
    fn test_compute_trip_stats_totals_averages_and_breakdowns() {
        let rows = vec![
            row("CMT", "CSH", 1.0, 10.0, 2.0, 1),
            row("VTS", "CRD", 3.0, 20.0, 3.0, 2),
            row("VTS", "CRD", 2.333, 15.0, 0.0, 3),
        ];

        let stats = compute_trip_stats(&rows);

        assert_eq!(stats["total_trips"], 3);
        assert_eq!(stats["total_passengers"], 6);
        assert_eq!(stats["total_distance_miles"], 6.33);
        assert_eq!(stats["total_fare_usd"], 45.0);
        assert_eq!(stats["total_tips_usd"], 5.0);
        assert_eq!(stats["avg_distance_miles"], 2.11);
        assert_eq!(stats["avg_fare_usd"], 15.0);
        assert_eq!(stats["avg_tip_usd"], 1.67);
        assert_eq!(stats["tip_rate_percent"], 11.11);
        assert_eq!(stats["max_distance_miles"], 3.0);
        assert_eq!(stats["min_distance_miles"], 1.0);
        assert_eq!(
            stats["vendor_breakdown"],
            serde_json::json!({"CMT": 1, "VTS": 2})
        );
        assert_eq!(
            stats["payment_type_breakdown"],
            serde_json::json!({"CSH": 1, "CRD": 2})
        );
    }

    #[test]
    fn test_compute_trip_stats_zero_fare_gives_zero_tip_rate() {
        let stats = compute_trip_stats(&[row("CMT", "NOC", 0.5, 0.0, 0.0, 1)]);
        assert_eq!(stats["tip_rate_percent"], 0.0);
    }

    #[test]
    fn test_compute_trip_stats_sample_payload_breakdowns_cover_all_trips() {
        let stats = compute_trip_stats(&sample_payload().rows);
        let breakdown_total = |key: &str| -> u64 {
            stats[key]
                .as_object()
                .unwrap()
                .values()
                .map(|v| v.as_u64().unwrap())
                .sum()
        };

        assert_eq!(stats["total_trips"], 5000);
        assert_eq!(breakdown_total("vendor_breakdown"), 5000);
        assert_eq!(breakdown_total("payment_type_breakdown"), 5000);
        assert!(stats["min_distance_miles"].as_f64() <= stats["max_distance_miles"].as_f64());
    }

    #[test]
    fn test_compute_trip_stats_sample_payload_is_a_mixed_sample() {
        // The old payload was 5,000 cash trips of one vendor, so populate.sh always reported 0% tips.
        let stats = compute_trip_stats(&sample_payload().rows);
        let tip_rate = stats["tip_rate_percent"].as_f64().unwrap();

        assert!(tip_rate > 5.0, "tip_rate_percent = {tip_rate}");
        for key in ["CRD", "CSH"] {
            assert!(stats["payment_type_breakdown"][key].as_u64().unwrap() > 500);
        }
        for key in ["CMT", "VTS"] {
            assert!(stats["vendor_breakdown"][key].as_u64().unwrap() > 500);
        }
    }

    #[test]
    fn test_collect_annotation_rows_without_filters_returns_all_rows() {
        let mut results = Vec::new();
        collect_annotation_rows(&annotation_batch(false), "", "", 100, &mut results);

        assert_eq!(
            object_keys(&results),
            ["data/a.parquet", "data/b.parquet", "other/c.parquet"]
        );
        assert_eq!(results[0]["bucket"], "b");
        assert_eq!(results[0]["name"], "trip_stats");
        assert_eq!(results[0]["sequence_number"], "1");
        assert_eq!(results[0]["size"], 17);
        assert_eq!(results[1]["size"], Value::Null);
    }

    #[test]
    fn test_collect_annotation_rows_parses_json_text_values() {
        let mut results = Vec::new();
        collect_annotation_rows(&annotation_batch(false), "", "", 100, &mut results);

        assert_eq!(
            results[0]["text_value"],
            serde_json::json!({"total_trips": 5})
        );
        assert_eq!(results[1]["text_value"], "not json");
        assert_eq!(results[2]["text_value"], "");
    }

    #[test]
    fn test_collect_annotation_rows_filters_by_name_and_key_prefix() {
        let batch = annotation_batch(false);

        let mut by_name = Vec::new();
        collect_annotation_rows(&batch, "trip_stats", "", 100, &mut by_name);
        assert_eq!(object_keys(&by_name), ["data/a.parquet", "other/c.parquet"]);

        let mut by_prefix = Vec::new();
        collect_annotation_rows(&batch, "", "data/", 100, &mut by_prefix);
        assert_eq!(
            object_keys(&by_prefix),
            ["data/a.parquet", "data/b.parquet"]
        );

        let mut by_both = Vec::new();
        collect_annotation_rows(&batch, "trip_stats", "data/", 100, &mut by_both);
        assert_eq!(object_keys(&by_both), ["data/a.parquet"]);
    }

    #[test]
    fn test_collect_annotation_rows_respects_limit_across_batches() {
        let batch = annotation_batch(false);
        let mut results = Vec::new();

        collect_annotation_rows(&batch, "", "", 2, &mut results);
        assert_eq!(results.len(), 2);

        // A later batch adds nothing once the limit is reached.
        collect_annotation_rows(&batch, "", "", 2, &mut results);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_collect_annotation_rows_reads_large_utf8_columns() {
        let mut utf8 = Vec::new();
        let mut large_utf8 = Vec::new();
        collect_annotation_rows(&annotation_batch(false), "", "", 100, &mut utf8);
        collect_annotation_rows(&annotation_batch(true), "", "", 100, &mut large_utf8);

        assert_eq!(large_utf8, utf8);
    }

    #[test]
    fn test_collect_annotation_rows_missing_columns_yield_empty_values() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "name",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some("trip_stats")]))],
        )
        .unwrap();
        let mut results = Vec::new();

        collect_annotation_rows(&batch, "", "", 100, &mut results);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["name"], "trip_stats");
        assert_eq!(results[0]["object_key"], "");
        assert_eq!(results[0]["bucket"], "");
        assert_eq!(results[0]["size"], Value::Null);
    }
}
