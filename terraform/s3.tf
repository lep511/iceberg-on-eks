# Iceberg warehouse bucket.
#
# Only S3 APIs the IDE role may call are used: CreateBucket, PutBucketTagging,
# PutBucketPublicAccessBlock, PutEncryptionConfiguration, PutObject (plus s3:Get*/List*).
# There is deliberately no bucket policy, lifecycle configuration, or ownership controls
# resource (the IDE role lacks those permissions). New buckets already default to
# Object Ownership "BucketOwnerEnforced" (ACLs disabled).
#
# Versioning is left DISABLED on purpose. Iceberg never updates files in place: every
# commit writes new data, manifest, and metadata files, and snapshot expiration / orphan
# file cleanup / compaction delete the old ones. With versioning enabled each of those
# deletes would leave a noncurrent version behind, and without permission to add a
# lifecycle rule (s3:PutLifecycleConfiguration) nothing would ever expire them, so storage
# would keep growing. Iceberg snapshots already provide time travel and rollback.
resource "aws_s3_bucket" "warehouse" {
  bucket_prefix = "${var.name}-"

  # Sandbox / workshop setting: lets `terraform destroy` delete the bucket even when it
  # still contains Iceberg data. Remove it for anything you want to keep.
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# SSE-S3 (AES256). SSE-KMS would also need KMS permissions in the IRSA role and for the
# S3 Metadata tables, which this project does not grant.
resource "aws_s3_bucket_server_side_encryption_configuration" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Directory marker for the default Spark event log dir (s3a://<bucket>/lab/spark-event-logs).
# Spark refuses to start when spark.eventLog.dir does not exist, and S3A only sees a
# "directory" when at least one object exists under the prefix.
resource "aws_s3_object" "spark_event_log_dir" {
  bucket       = aws_s3_bucket.warehouse.id
  key          = local.event_log_dir_key
  content      = ""
  content_type = "application/x-directory"

  # Tagging an object needs s3:PutObjectTagging, which the IDE role lacks, so the provider
  # default_tags are not applied to this object.
  override_provider {
    default_tags {
      tags = {}
    }
  }

  depends_on = [
    aws_s3_bucket_public_access_block.warehouse,
    aws_s3_bucket_server_side_encryption_configuration.warehouse,
  ]
}
