provider "aws" {
  region = var.region

  default_tags {
    tags = merge(var.tags, {
      Project = var.name
    })
  }
}

# Same pattern as the platform blueprint (no exec plugin): a short-lived token from
# aws_eks_cluster_auth. The IDE role is the cluster creator, so it is cluster-admin.
provider "kubernetes" {
  host                   = data.aws_eks_cluster.this.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.this.certificate_authority[0].data)
  token                  = data.aws_eks_cluster_auth.this.token
}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  region     = var.region

  # Issuer without the scheme, as used in IRSA condition keys (<issuer>:aud, <issuer>:sub).
  oidc_issuer = trimprefix(data.aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://")

  spark_service_account_subject = "system:serviceaccount:${var.spark_namespace}:${var.spark_service_account}"
  jobs_service_account_subject  = "system:serviceaccount:${var.jobs_namespace}:${var.jobs_service_account}"

  # Iceberg warehouse layout (see the project contract).
  warehouse_bucket   = aws_s3_bucket.warehouse.bucket
  iceberg_warehouse  = "s3://${local.warehouse_bucket}/warehouse"
  lab_uri            = "s3://${local.warehouse_bucket}/lab"
  event_log_dir_key  = "lab/spark-event-logs/"
  spark_event_prefix = "spark-event-logs"

  # Spark Operator deployment of the platform, read by the check in data.tf.
  spark_operator_namespace  = "spark-operator"
  spark_operator_deployment = "spark-operator-controller"

  # Graviton5 NodePool (nodepool.tf). M9g and M9gd are the Graviton5 families (GRAVITON5.md).
  graviton5_node_pool         = "${var.name}-graviton5"
  graviton5_instance_families = ["m9g", "m9gd"]

  k8s_labels = {
    "app.kubernetes.io/part-of"    = var.name
    "app.kubernetes.io/managed-by" = "terraform"
  }
}
