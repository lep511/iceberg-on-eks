//! Parsing of the S3 URIs that Iceberg uses for table, metadata and data file locations.

/// URI schemes that address S3 objects. Tables created through Iceberg's `S3FileIO` (Spark with
/// the Glue catalog, PyIceberg, iceberg-rust) use `s3://`; tables created through Hadoop's
/// `S3AFileSystem` use `s3a://` (or the legacy `s3n://`). All three address the same object.
pub const S3_SCHEMES: [&str; 3] = ["s3", "s3a", "s3n"];

/// Splits `<scheme>://<bucket>/<key>` into `(bucket, key)`.
///
/// Returns `None` when the scheme is not one of [`S3_SCHEMES`] or the bucket is missing. The key
/// is empty for `s3://bucket` and `s3://bucket/`.
pub fn parse_s3_uri(uri: &str) -> Option<(&str, &str)> {
    let (scheme, rest) = uri.split_once("://")?;
    if !S3_SCHEMES.iter().any(|s| s.eq_ignore_ascii_case(scheme)) {
        return None;
    }
    let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return None;
    }
    Some((bucket, key))
}

/// S3 object key of a data file path in `bucket`, i.e. the path without its
/// `s3://<bucket>/`, `s3a://<bucket>/` or `s3n://<bucket>/` prefix.
///
/// Returns `None` when the path is not an S3 URI, points to another bucket, or has no key.
pub fn object_key_from_path<'a>(file_path: &'a str, bucket: &str) -> Option<&'a str> {
    match parse_s3_uri(file_path) {
        Some((b, key)) if b == bucket && !key.is_empty() => Some(key),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_uri_accepts_s3_family_schemes() {
        for scheme in ["s3", "s3a", "s3n", "S3A"] {
            assert_eq!(
                parse_s3_uri(&format!("{scheme}://my-bucket/warehouse")),
                Some(("my-bucket", "warehouse")),
                "scheme {scheme}"
            );
        }
    }

    #[test]
    fn test_parse_s3_uri_bucket_only() {
        assert_eq!(parse_s3_uri("s3://my-bucket"), Some(("my-bucket", "")));
        assert_eq!(parse_s3_uri("s3://my-bucket/"), Some(("my-bucket", "")));
    }

    #[test]
    fn test_parse_s3_uri_rejects_other_uris() {
        assert_eq!(parse_s3_uri("gs://my-bucket/warehouse"), None);
        assert_eq!(parse_s3_uri("file:///tmp/warehouse"), None);
        assert_eq!(parse_s3_uri("s3:///warehouse"), None);
        assert_eq!(parse_s3_uri("my-bucket/warehouse"), None);
        assert_eq!(parse_s3_uri(""), None);
    }

    #[test]
    fn test_object_key_from_path_strips_bucket_prefix() {
        // Data files of a table created by Spark/S3FileIO (the contract's warehouse layout).
        assert_eq!(
            object_key_from_path(
                "s3://my-bucket/warehouse/nyc_taxi_database/nyc_taxi_table/data/00000-0-a.parquet",
                "my-bucket"
            ),
            Some("warehouse/nyc_taxi_database/nyc_taxi_table/data/00000-0-a.parquet")
        );
        // Data files of a table created with an s3a:// location (the reference repo's layout).
        assert_eq!(
            object_key_from_path("s3a://my-bucket/warehouse/db/t/data/f.parquet", "my-bucket"),
            Some("warehouse/db/t/data/f.parquet")
        );
        assert_eq!(
            object_key_from_path("s3n://my-bucket/f.parquet", "my-bucket"),
            Some("f.parquet")
        );
    }

    #[test]
    fn test_object_key_from_path_rejects_other_buckets_and_schemes() {
        assert_eq!(
            object_key_from_path("s3://other-bucket/f.parquet", "my-bucket"),
            None
        );
        // A bucket whose name only starts with the requested bucket name.
        assert_eq!(
            object_key_from_path("s3://my-bucket-2/f.parquet", "my-bucket"),
            None
        );
        assert_eq!(
            object_key_from_path("gs://my-bucket/f.parquet", "my-bucket"),
            None
        );
        assert_eq!(object_key_from_path("s3://my-bucket/", "my-bucket"), None);
    }
}
