# Every output is a string: scripts/render-manifests.sh turns output foo_bar into the
# manifest placeholder <FOO_BAR>. Keep the names in sync with the project contract.

output "region" {
  description = "AWS Region of the cluster and of every resource of this project."
  value       = var.region
}

output "cluster_name" {
  description = "Name of the existing EKS cluster."
  value       = data.aws_eks_cluster.this.name
}

output "warehouse_bucket" {
  description = "Name of the S3 bucket that holds the Iceberg warehouse and the lab artifacts."
  value       = aws_s3_bucket.warehouse.bucket
}

output "iceberg_warehouse" {
  description = "Iceberg warehouse root (Glue catalog warehouse property)."
  value       = local.iceberg_warehouse
}

output "lab_uri" {
  description = "S3 prefix for lab artifacts: input data, Spark scripts, the Rust CLI binary, default event logs."
  value       = local.lab_uri
}

output "glue_database" {
  description = "Glue database (Iceberg namespace) of the shared table. Created in-cluster by the first Spark job, not by Terraform."
  value       = var.glue_database
}

output "iceberg_table" {
  description = "Name of the shared Iceberg table inside glue_database."
  value       = var.iceberg_table
}

output "spark_irsa_role_arn" {
  description = "ARN of the IRSA role of the Spark service account (Glue database and tables, warehouse bucket, event logs)."
  value       = aws_iam_role.irsa["spark"].arn
}

output "jobs_irsa_role_arn" {
  description = "ARN of the IRSA role of the Rust CLI Jobs service account (append to the table, S3 Object Annotations, S3 Metadata)."
  value       = aws_iam_role.irsa["jobs"].arn
}

output "spark_namespace" {
  description = "Existing Kubernetes namespace (watched by the Spark Operator) where SparkApplications run."
  value       = data.kubernetes_namespace_v1.spark.metadata[0].name
}

output "spark_service_account" {
  description = "IRSA-annotated service account for the Spark driver and executors, in spark_namespace."
  value       = kubernetes_service_account_v1.spark.metadata[0].name
}

output "jobs_namespace" {
  description = "Kubernetes namespace created by this project for the Rust CLI Jobs."
  value       = kubernetes_namespace_v1.jobs.metadata[0].name
}

output "jobs_service_account" {
  description = "IRSA-annotated service account for the Rust CLI Jobs, in jobs_namespace."
  value       = kubernetes_service_account_v1.jobs.metadata[0].name
}

output "graviton5_node_pool" {
  description = "EKS Auto Mode NodePool of Graviton5 instances (M9g, M9gd; arm64) created by this project. Every Spark driver and executor and every Rust CLI Job selects it with karpenter.sh/nodepool."
  value       = kubernetes_manifest.graviton5_node_pool.manifest.metadata.name
}

output "graviton5_node_class" {
  description = "Existing EKS Auto Mode NodeClass used by the Graviton5 NodePool."
  value       = data.kubernetes_resource.graviton5_node_class.object.metadata.name
}

output "spark_event_log_dir" {
  description = "Value for spark.eventLog.dir (s3a URI): the platform logs bucket when spark_event_logs_bucket is set (visible in the Spark History Server), otherwise a prefix of the warehouse bucket."
  value = (
    var.spark_event_logs_bucket != ""
    ? "s3a://${var.spark_event_logs_bucket}/${local.spark_event_prefix}"
    : "s3a://${aws_s3_object.spark_event_log_dir.bucket}/${trimsuffix(aws_s3_object.spark_event_log_dir.key, "/")}"
  )
}

output "configure_kubectl" {
  description = "Command that adds the cluster to your kubeconfig."
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${data.aws_eks_cluster.this.name}"
}
