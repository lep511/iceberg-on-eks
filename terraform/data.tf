data "aws_partition" "current" {}

data "aws_caller_identity" "current" {}

data "aws_eks_cluster" "this" {
  name = var.cluster_name

  lifecycle {
    postcondition {
      condition     = self.status == "ACTIVE"
      error_message = "EKS cluster ${var.cluster_name} is not ACTIVE (status: ${self.status})."
    }
  }
}

data "aws_eks_cluster_auth" "this" {
  name = var.cluster_name
}

# The IAM OIDC provider is owned by the platform Terraform; this project only looks it up.
data "aws_iam_openid_connect_provider" "eks" {
  url = data.aws_eks_cluster.this.identity[0].oidc[0].issuer

  lifecycle {
    postcondition {
      condition     = contains(self.client_id_list, "sts.amazonaws.com")
      error_message = "The IAM OIDC provider of cluster ${var.cluster_name} does not list sts.amazonaws.com as a client ID (audience), so IRSA would not work."
    }
  }
}

# The Spark namespace must already exist (created by the platform and watched by the
# Spark Operator). It is read, never created or owned, by this project.
data "kubernetes_namespace_v1" "spark" {
  metadata {
    name = var.spark_namespace
  }

  lifecycle {
    postcondition {
      condition     = try(self.metadata[0].uid, "") != ""
      error_message = "Kubernetes namespace ${var.spark_namespace} does not exist in cluster ${var.cluster_name}. spark_namespace must be an existing namespace watched by the Spark Operator."
    }
  }
}

# Optional platform bucket for Spark event logs: fail at plan time on a typo.
data "aws_s3_bucket" "spark_event_logs" {
  count = var.spark_event_logs_bucket != "" ? 1 : 0

  bucket = var.spark_event_logs_bucket
}

# Warning only (check blocks never block plan/apply): SparkApplications in a namespace the
# Spark Operator does not watch are silently ignored.
check "spark_operator_watches_spark_namespace" {
  data "kubernetes_resource" "spark_operator_controller" {
    api_version = "apps/v1"
    kind        = "Deployment"

    metadata {
      name      = local.spark_operator_deployment
      namespace = local.spark_operator_namespace
    }
  }

  assert {
    condition = anytrue([
      for arg in flatten([
        for c in try(data.kubernetes_resource.spark_operator_controller.object.spec.template.spec.containers, []) :
        try(c.args, [])
      ]) :
      startswith(arg, "--namespaces=") && anytrue([
        for ns in split(",", trimprefix(arg, "--namespaces=")) : ns == var.spark_namespace || ns == ""
      ])
    ])
    error_message = "The Spark Operator (${local.spark_operator_namespace}/${local.spark_operator_deployment}) does not appear to watch namespace ${var.spark_namespace}: SparkApplications there will not run. Use one of the namespaces in its --namespaces argument."
  }
}
