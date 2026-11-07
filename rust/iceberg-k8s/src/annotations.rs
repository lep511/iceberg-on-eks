use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use serde_json::Value;

use crate::error::AppError;

/// Attaches an annotation (key-value) to an S3 object.
pub async fn put_annotation(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    annotation_name: &str,
    annotation_value: &str,
) -> Result<(), AppError> {
    s3_client
        .put_object_annotation()
        .bucket(bucket)
        .key(key)
        .annotation_name(annotation_name)
        .annotation_payload(ByteStream::from(annotation_value.as_bytes().to_vec()))
        .send()
        .await?;

    Ok(())
}

/// Retrieves a specific annotation from an S3 object.
pub async fn get_annotation(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    annotation_name: &str,
) -> Result<Value, AppError> {
    let resp = s3_client
        .get_object_annotation()
        .bucket(bucket)
        .key(key)
        .annotation_name(annotation_name)
        .send()
        .await?;

    let body = resp.annotation_payload.collect().await?;
    let payload = String::from_utf8_lossy(&body.into_bytes()).to_string();

    Ok(serde_json::json!({
        "bucket": bucket,
        "key": key,
        "annotation_name": annotation_name,
        "annotation_value": payload,
    }))
}

/// Deletes a specific annotation from an S3 object.
pub async fn delete_annotation(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    annotation_name: &str,
) -> Result<(), AppError> {
    s3_client
        .delete_object_annotation()
        .bucket(bucket)
        .key(key)
        .annotation_name(annotation_name)
        .send()
        .await?;

    Ok(())
}

/// Lists all annotations attached to an S3 object.
pub async fn list_annotations(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<Value, AppError> {
    let resp = s3_client
        .list_object_annotations()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let annotations: Vec<Value> = resp
        .annotations()
        .iter()
        .map(|a| {
            serde_json::json!({
                "annotation_name": a.annotation_name(),
                "size": a.size(),
                "last_modified": format!("{:?}", a.last_modified()),
            })
        })
        .collect();

    Ok(serde_json::json!({
        "bucket": bucket,
        "key": key,
        "annotation_count": resp.annotation_count(),
        "annotations": annotations,
    }))
}
