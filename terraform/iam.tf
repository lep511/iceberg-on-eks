locals {
  warehouse_bucket_arn = aws_s3_bucket.warehouse.arn

  glue_arn_prefix = "arn:${local.partition}:glue:${local.region}:${local.account_id}"

  # AWS managed table bucket that holds the S3 Metadata journal and inventory tables of
  # every general purpose bucket in this account and Region.
  s3_metadata_table_bucket_arn = "arn:${local.partition}:s3tables:${local.region}:${local.account_id}:bucket/aws-s3"

  # One IRSA role per workload, each trusted by exactly one service account (least privilege):
  # Spark manages the Glue database and tables; the Rust CLI Jobs append rows, annotate
  # objects and manage S3 Metadata.
  irsa_roles = {
    spark = {
      subject     = local.spark_service_account_subject
      description = "IRSA role for the Iceberg Spark jobs (${var.spark_namespace}/${var.spark_service_account}) on EKS cluster ${var.cluster_name}"
      policy      = data.aws_iam_policy_document.spark.json
    }
    jobs = {
      subject     = local.jobs_service_account_subject
      description = "IRSA role for the iceberg-k8s Rust CLI Jobs (${var.jobs_namespace}/${var.jobs_service_account}) on EKS cluster ${var.cluster_name}"
      policy      = data.aws_iam_policy_document.jobs.json
    }
  }
}

################################################################################
# IRSA roles
################################################################################

data "aws_iam_policy_document" "irsa_trust" {
  for_each = local.irsa_roles

  statement {
    sid     = "EksIrsaServiceAccount"
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [data.aws_iam_openid_connect_provider.eks.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:sub"
      values   = [each.value.subject]
    }
  }
}

# The IDE role cannot call iam:UpdateAssumeRolePolicy. Tracking the trust policy here lets
# a role be REPLACED (DeleteRole + CreateRole, both allowed) instead of updated in place
# when a namespace or service account name changes.
resource "terraform_data" "irsa_trust_policy" {
  for_each = local.irsa_roles

  input = data.aws_iam_policy_document.irsa_trust[each.key].json
}

resource "aws_iam_role" "irsa" {
  for_each = local.irsa_roles

  # The IDE role may only create roles named spark-on-eks-* (enforced by var.name validation).
  name               = "${var.name}-${each.key}"
  description        = each.value.description
  assume_role_policy = data.aws_iam_policy_document.irsa_trust[each.key].json

  force_detach_policies = true

  lifecycle {
    replace_triggered_by = [terraform_data.irsa_trust_policy[each.key]]
  }
}

# Inline policies (aws_iam_role_policy) because the IDE role may only create managed policies
# with specific names; inline policies only need iam:PutRolePolicy on spark-on-eks-* roles.
resource "aws_iam_role_policy" "irsa" {
  for_each = local.irsa_roles

  name   = "${var.name}-${each.key}"
  role   = aws_iam_role.irsa[each.key].id
  policy = each.value.policy

  lifecycle {
    # Re-create the inline policy whenever its role is re-created.
    replace_triggered_by = [terraform_data.irsa_trust_policy[each.key]]
  }
}

################################################################################
# Spark jobs: load (creates the Glue database and table), analytics, cleanup (drops them)
################################################################################

data "aws_iam_policy_document" "spark" {
  #-----------------------------------------------------------------------------
  # Iceberg warehouse bucket (table data and metadata, PySpark scripts, input data,
  # default event logs)
  #-----------------------------------------------------------------------------
  statement {
    sid    = "WarehouseBucketList"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [local.warehouse_bucket_arn]
  }

  statement {
    sid    = "WarehouseObjectReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${local.warehouse_bucket_arn}/*"]
  }

  #-----------------------------------------------------------------------------
  # Optional platform bucket for Spark event logs (Spark History Server)
  #-----------------------------------------------------------------------------
  dynamic "statement" {
    for_each = var.spark_event_logs_bucket != "" ? [var.spark_event_logs_bucket] : []

    content {
      sid    = "SparkEventLogsBucketList"
      effect = "Allow"
      actions = [
        "s3:ListBucket",
        "s3:GetBucketLocation",
      ]
      resources = ["arn:${local.partition}:s3:::${statement.value}"]
    }
  }

  dynamic "statement" {
    for_each = var.spark_event_logs_bucket != "" ? [var.spark_event_logs_bucket] : []

    content {
      sid    = "SparkEventLogsObjectReadWrite"
      effect = "Allow"
      actions = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
      ]
      resources = ["arn:${local.partition}:s3:::${statement.value}/${local.spark_event_prefix}/*"]
    }
  }

  # S3A getFileStatus("s3a://<bucket>/spark-event-logs") first sends HEAD for the key
  # "spark-event-logs" (no trailing slash). Without s3:GetObject on that exact key S3 answers
  # 403 instead of 404, S3A raises AccessDeniedException, and Spark cannot open the log dir.
  dynamic "statement" {
    for_each = var.spark_event_logs_bucket != "" ? [var.spark_event_logs_bucket] : []

    content {
      sid       = "SparkEventLogsDirProbe"
      effect    = "Allow"
      actions   = ["s3:GetObject"]
      resources = ["arn:${local.partition}:s3:::${statement.value}/${local.spark_event_prefix}"]
    }
  }

  #-----------------------------------------------------------------------------
  # Glue Data Catalog: the Spark jobs create, change and drop the database and its tables
  #-----------------------------------------------------------------------------
  statement {
    sid    = "GlueIcebergCatalog"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateDatabase",
      "glue:UpdateDatabase",
      "glue:DeleteDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
    ]
    resources = [
      "${local.glue_arn_prefix}:catalog",
      "${local.glue_arn_prefix}:database/${var.glue_database}",
      "${local.glue_arn_prefix}:table/${var.glue_database}/*",
      # DeleteDatabase also needs every table and user-defined function in the database.
      "${local.glue_arn_prefix}:userDefinedFunction/${var.glue_database}/*",
    ]
  }

  # Needed when Lake Formation governs the catalog (a no-op in IAM-only mode).
  statement {
    sid       = "LakeFormationDataAccess"
    effect    = "Allow"
    actions   = ["lakeformation:GetDataAccess"]
    resources = ["*"]
  }
}

################################################################################
# Rust CLI Jobs: read/append the shared table, S3 Object Annotations, S3 Metadata
################################################################################

data "aws_iam_policy_document" "jobs" {
  #-----------------------------------------------------------------------------
  # Iceberg warehouse bucket: table data/metadata (read, append), CLI binary and payload
  #-----------------------------------------------------------------------------
  statement {
    sid    = "WarehouseBucketList"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [local.warehouse_bucket_arn]
  }

  # No s3:DeleteObject: the CLI only appends (fast-append commits never delete files).
  statement {
    sid    = "WarehouseObjectReadWrite"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${local.warehouse_bucket_arn}/*"]
  }

  #-----------------------------------------------------------------------------
  # Glue Data Catalog: load the existing table and commit appends (no create or drop)
  #-----------------------------------------------------------------------------
  statement {
    sid    = "GlueIcebergTableReadAppend"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:UpdateTable",
    ]
    resources = [
      "${local.glue_arn_prefix}:catalog",
      "${local.glue_arn_prefix}:database/${var.glue_database}",
      "${local.glue_arn_prefix}:table/${var.glue_database}/*",
    ]
  }

  # Needed when Lake Formation governs the catalog (a no-op in IAM-only mode).
  statement {
    sid       = "LakeFormationDataAccess"
    effect    = "Allow"
    actions   = ["lakeformation:GetDataAccess"]
    resources = ["*"]
  }

  #-----------------------------------------------------------------------------
  # S3 Object Annotations on warehouse objects
  #-----------------------------------------------------------------------------
  statement {
    sid    = "S3ObjectAnnotations"
    effect = "Allow"
    actions = [
      "s3:PutObjectAnnotation",
      "s3:GetObjectAnnotation",
      "s3:DeleteObjectAnnotation",
      "s3:ListObjectAnnotations",
    ]
    resources = ["${local.warehouse_bucket_arn}/*"]
  }

  #-----------------------------------------------------------------------------
  # S3 Metadata (journal + live inventory tables) for the warehouse bucket.
  # The IDE role cannot enable S3 Metadata, so an in-cluster Job does it with this role.
  #-----------------------------------------------------------------------------
  statement {
    sid    = "S3MetadataConfiguration"
    effect = "Allow"
    # Per the S3 API reference, the V2 operations (Create/Get/DeleteBucketMetadataConfiguration)
    # are authorized by these *MetadataTableConfiguration actions ("the IAM policy action name
    # is the same for the V1 and V2 API operations").
    actions = [
      "s3:CreateBucketMetadataTableConfiguration",
      "s3:GetBucketMetadataTableConfiguration",
      "s3:DeleteBucketMetadataTableConfiguration",
    ]
    resources = [local.warehouse_bucket_arn]
  }

  # The CLI reads the S3 Metadata tables through the S3 Tables API (no Glue access needed).
  statement {
    sid    = "S3MetadataTablesRead"
    effect = "Allow"
    actions = [
      "s3tables:GetTable",
      "s3tables:GetTableBucket",
      "s3tables:GetTableData",
      "s3tables:GetTableMetadataLocation",
      "s3tables:ListTables",
      "s3tables:GetNamespace",
      "s3tables:ListNamespaces",
    ]
    resources = [
      local.s3_metadata_table_bucket_arn,
      "${local.s3_metadata_table_bucket_arn}/*",
    ]
  }

  # Permissions S3 checks on the caller when it creates the metadata tables.
  statement {
    sid    = "S3MetadataTablesCreate"
    effect = "Allow"
    actions = [
      "s3tables:CreateTableBucket",
      "s3tables:CreateNamespace",
      "s3tables:CreateTable",
      "s3tables:PutTablePolicy",
      "s3tables:PutTableBucketPolicy",
      "s3tables:PutTableEncryption",
    ]
    resources = [
      local.s3_metadata_table_bucket_arn,
      "${local.s3_metadata_table_bucket_arn}/*",
    ]
  }
}
