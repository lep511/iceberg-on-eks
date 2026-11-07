use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Request payload. Same JSON contract as the reference Lambda event: `action` is required,
/// every other field is optional and defaults to empty (`limit` defaults to 100).
#[derive(Debug, Deserialize)]
pub struct Request {
    pub action: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub table: String,
    #[serde(default)]
    pub rows: Vec<RowInput>,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub metadata_config: Option<MetadataConfigInput>,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub annotation_name: String,
    #[serde(default)]
    pub annotation_value: String,
    #[serde(default)]
    pub annotations: Vec<AnnotationInput>,
    #[serde(default)]
    pub filter_name: String,
    #[serde(default)]
    pub key_prefix: String,
    #[serde(default = "default_query_limit")]
    pub limit: usize,

    // Distributed trip stats (plan_trip_stats, trip_stats_shard, merge_trip_stats); not part of
    // the reference Lambda contract.
    /// S3 prefix of the run's documents: `s3://<bucket>/<prefix>/<run id>`.
    #[serde(default)]
    pub run_uri: String,
    /// Number of shards to plan (`plan_trip_stats`). A number or a numeric string.
    #[serde(default, deserialize_with = "deserialize_opt_usize")]
    pub shard_count: Option<usize>,
    /// Shard to run (`trip_stats_shard`); defaults to `JOB_COMPLETION_INDEX`.
    #[serde(default, deserialize_with = "deserialize_opt_usize")]
    pub shard_index: Option<usize>,
}

fn default_query_limit() -> usize {
    100
}

/// Accepts a non-negative integer as a JSON number or as a string (flags override request fields
/// with strings), or null.
fn deserialize_opt_usize<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumberOrString {
        Number(usize),
        String(String),
    }
    match Option::<NumberOrString>::deserialize(deserializer)? {
        None => Ok(None),
        Some(NumberOrString::Number(n)) => Ok(Some(n)),
        Some(NumberOrString::String(s)) => s.trim().parse().map(Some).map_err(|_| {
            serde::de::Error::custom(format!("expected a non-negative integer, got \"{s}\""))
        }),
    }
}

#[derive(Debug, Deserialize)]
pub struct AnnotationInput {
    pub key: String,
    pub annotation_name: String,
    #[serde(default)]
    pub annotation_value: String,
}

/// NYC Taxi dataset row matching the 23-column Iceberg schema.
#[derive(Debug, Deserialize)]
pub struct RowInput {
    pub medallion: String,
    pub hack_license: String,
    pub vendor_id: String,
    pub rate_code: i32,
    pub store_and_fwd_flag: String,
    pub pickup_datetime: String,
    pub dropoff_datetime: String,
    pub passenger_count: i32,
    pub trip_time_in_secs: i64,
    pub trip_distance: f64,
    pub pickup_longitude: f64,
    pub pickup_latitude: f64,
    pub dropoff_longitude: f64,
    pub dropoff_latitude: f64,
    pub payment_type: String,
    pub fare_amount: f64,
    pub surcharge: f64,
    pub mta_tax: f64,
    pub tolls_amount: f64,
    pub total_amount: f64,
    pub tip_amount: f64,
    pub tipped: i32,
    pub tip_class: i32,
}

#[derive(Debug, Deserialize)]
pub struct MetadataConfigInput {
    #[serde(default = "default_expiration_enabled")]
    pub journal_expiration: String,
    #[serde(default = "default_expiration_days")]
    pub journal_expiration_days: i32,
    #[serde(default = "default_disabled")]
    pub inventory_enabled: String,
    #[serde(default = "default_disabled")]
    pub annotation_enabled: String,
}

pub fn default_expiration_enabled() -> String {
    "ENABLED".to_string()
}

pub fn default_expiration_days() -> i32 {
    30
}

pub fn default_disabled() -> String {
    "DISABLED".to_string()
}

impl Default for MetadataConfigInput {
    fn default() -> Self {
        Self {
            journal_expiration: default_expiration_enabled(),
            journal_expiration_days: default_expiration_days(),
            inventory_enabled: default_disabled(),
            annotation_enabled: default_disabled(),
        }
    }
}

/// Response printed on stdout. `rows_read` and `data` are omitted when an action does not set them.
#[derive(Debug, Serialize)]
pub struct Response {
    pub ok: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_read: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// A cash trip from the 2013 NYC taxi data; tests override the fields they care about.
#[cfg(test)]
pub(crate) fn sample_row() -> RowInput {
    RowInput {
        medallion: "C6F86DFD1C85EEFBFC234FCB953E1D4B".to_string(),
        hack_license: "79C37DA10EA88D6B467F9FA2B29F8006".to_string(),
        vendor_id: "CMT".to_string(),
        rate_code: 1,
        store_and_fwd_flag: "N".to_string(),
        pickup_datetime: "2013-11-17 12:26:45".to_string(),
        dropoff_datetime: "2013-11-17 12:32:02".to_string(),
        passenger_count: 1,
        trip_time_in_secs: 316,
        trip_distance: 0.8,
        pickup_longitude: -73.98864,
        pickup_latitude: 40.748764,
        dropoff_longitude: -74.002075,
        dropoff_latitude: 40.754913,
        payment_type: "CSH".to_string(),
        fare_amount: 5.5,
        surcharge: 0.0,
        mta_tax: 0.5,
        tolls_amount: 0.0,
        total_amount: 6.0,
        tip_amount: 0.0,
        tipped: 0,
        tip_class: 0,
    }
}

/// `data/nyctaxi_payload.json` at the project root (a copy of the reference repo's
/// `database/nyctaxi_payload.json`, uploaded to `s3://<bucket>/lab/input/nyctaxi_payload.json`).
#[cfg(test)]
pub(crate) const SAMPLE_PAYLOAD_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../data/nyctaxi_payload.json"
);

/// The 5,000-row `write` request in `data/nyctaxi_payload.json`.
#[cfg(test)]
pub(crate) fn sample_payload() -> Request {
    let payload =
        std::fs::read_to_string(SAMPLE_PAYLOAD_PATH).expect("sample payload should be readable");
    serde_json::from_str(&payload).expect("sample payload should deserialize into Request")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_defaults_for_optional_fields() {
        let req: Request = serde_json::from_str(r#"{"action":"read"}"#).unwrap();

        assert_eq!(req.action, "read");
        assert!(req.namespace.is_empty() && req.table.is_empty() && req.bucket.is_empty());
        assert!(req.key.is_empty() && req.filter_name.is_empty() && req.key_prefix.is_empty());
        assert!(req.rows.is_empty() && req.annotations.is_empty());
        assert!(req.metadata_config.is_none());
        assert_eq!(req.limit, 100);
    }

    #[test]
    fn test_shard_fields_accept_numbers_and_numeric_strings() {
        let req: Request = serde_json::from_str(
            r#"{"action":"plan_trip_stats","run_uri":"s3://b/r","shard_count":4,"shard_index":"2"}"#,
        )
        .unwrap();
        assert_eq!(req.run_uri, "s3://b/r");
        assert_eq!((req.shard_count, req.shard_index), (Some(4), Some(2)));

        let req: Request =
            serde_json::from_str(r#"{"action":"merge_trip_stats","shard_count":null}"#).unwrap();
        assert!(req.run_uri.is_empty());
        assert_eq!((req.shard_count, req.shard_index), (None, None));

        for bad in [r#""""#, r#""four""#, "-1", "1.5"] {
            let json = format!(r#"{{"action":"plan_trip_stats","shard_count":{bad}}}"#);
            assert!(serde_json::from_str::<Request>(&json).is_err(), "{bad}");
        }
    }

    #[test]
    fn test_request_requires_action() {
        assert!(serde_json::from_str::<Request>(r#"{"namespace":"db"}"#).is_err());
    }

    #[test]
    fn test_metadata_config_defaults_match_default_impl() {
        let parsed: MetadataConfigInput = serde_json::from_str("{}").unwrap();
        let default = MetadataConfigInput::default();

        assert_eq!(parsed.journal_expiration, "ENABLED");
        assert_eq!(parsed.journal_expiration_days, 30);
        assert_eq!(parsed.inventory_enabled, "DISABLED");
        assert_eq!(parsed.annotation_enabled, "DISABLED");
        assert_eq!(parsed.journal_expiration, default.journal_expiration);
        assert_eq!(
            parsed.journal_expiration_days,
            default.journal_expiration_days
        );
        assert_eq!(parsed.inventory_enabled, default.inventory_enabled);
        assert_eq!(parsed.annotation_enabled, default.annotation_enabled);
    }

    #[test]
    fn test_annotation_input_value_defaults_to_empty() {
        let req: Request = serde_json::from_str(
            r#"{"action":"bulk_annotate","annotations":[{"key":"k","annotation_name":"n"}]}"#,
        )
        .unwrap();

        assert_eq!(req.annotations.len(), 1);
        assert_eq!(req.annotations[0].key, "k");
        assert_eq!(req.annotations[0].annotation_name, "n");
        assert!(req.annotations[0].annotation_value.is_empty());
    }

    #[test]
    fn test_sample_payload_deserializes() {
        let req = sample_payload();
        // Compared with the raw JSON, so regenerating the payload does not break the test.
        let raw: Value =
            serde_json::from_str(&std::fs::read_to_string(SAMPLE_PAYLOAD_PATH).unwrap()).unwrap();
        let first = &raw["rows"][0];

        assert_eq!(req.action, "write");
        assert_eq!(req.namespace, "nyc_taxi_database");
        assert_eq!(req.table, "nyc_taxi_table");
        assert_eq!(req.rows.len(), 5000);
        assert_eq!(req.rows[0].medallion, first["medallion"].as_str().unwrap());
        assert_eq!(
            req.rows[0].trip_time_in_secs,
            first["trip_time_in_secs"].as_i64().unwrap()
        );
        assert_eq!(
            req.rows[0].total_amount,
            first["total_amount"].as_f64().unwrap()
        );
        assert_eq!(
            req.rows[0].tip_class,
            first["tip_class"].as_i64().unwrap() as i32
        );
    }

    #[test]
    fn test_response_omits_unset_optional_fields() {
        let resp = Response {
            ok: true,
            message: "done".to_string(),
            rows_read: None,
            data: None,
        };
        assert_eq!(
            serde_json::to_value(&resp).unwrap(),
            serde_json::json!({"ok": true, "message": "done"})
        );

        let resp = Response {
            ok: true,
            message: "read completed".to_string(),
            rows_read: Some(3),
            data: Some(serde_json::json!({"a": 1})),
        };
        assert_eq!(
            serde_json::to_value(&resp).unwrap(),
            serde_json::json!({"ok": true, "message": "read completed", "rows_read": 3, "data": {"a": 1}})
        );
    }
}
