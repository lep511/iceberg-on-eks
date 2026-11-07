################################################################################
# Jobs namespace (created and owned by this project): Rust CLI Jobs
################################################################################

resource "kubernetes_namespace_v1" "jobs" {
  metadata {
    name = var.jobs_namespace
    labels = merge(local.k8s_labels, {
      # Blocks privileged pods, host namespaces, and hostPath volumes. "restricted" is not
      # enforced because the AWS CLI init container image runs as root.
      "pod-security.kubernetes.io/enforce" = "baseline"
    })
  }

  lifecycle {
    precondition {
      condition     = var.jobs_namespace != var.spark_namespace
      error_message = "jobs_namespace must differ from spark_namespace: this project creates and owns jobs_namespace, while spark_namespace is an existing platform namespace."
    }
  }
}

resource "kubernetes_service_account_v1" "jobs" {
  metadata {
    name      = var.jobs_service_account
    namespace = kubernetes_namespace_v1.jobs.metadata[0].name
    labels    = local.k8s_labels
    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.irsa["jobs"].arn
    }
  }

  # These Jobs only call AWS APIs. IRSA injects its own projected web identity token
  # (aws-iam-token volume), so the Kubernetes API token is not mounted. A pod can still
  # opt in with automountServiceAccountToken: true.
  automount_service_account_token = false
}

################################################################################
# Spark service account in the EXISTING Spark Operator job namespace
################################################################################

resource "kubernetes_service_account_v1" "spark" {
  metadata {
    name      = var.spark_service_account
    namespace = data.kubernetes_namespace_v1.spark.metadata[0].name
    labels    = local.k8s_labels
    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.irsa["spark"].arn
    }
  }

  # The Spark driver talks to the Kubernetes API to manage its executors.
  automount_service_account_token = true
}

# Namespaced, least-privilege counterpart of the platform's spark-cluster-role: only what
# the Spark driver needs to run executors in its own namespace (no secrets, no RBAC objects,
# no cluster-scoped resources). The driver pod itself is created by the Spark Operator with
# its own service account.
resource "kubernetes_role_v1" "spark_driver" {
  metadata {
    name      = "${var.spark_service_account}-driver"
    namespace = data.kubernetes_namespace_v1.spark.metadata[0].name
    labels    = local.k8s_labels
  }

  # Executor pods, executor ConfigMaps, on-demand PVCs, and services: the driver creates,
  # watches, labels/patches (decommissioning, owner references, server-side apply), and
  # deletes them by label selector (deletecollection) on shutdown.
  rule {
    api_groups = [""]
    resources  = ["pods", "configmaps", "persistentvolumeclaims", "services"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"]
  }

  # Read-only diagnostics.
  rule {
    api_groups = [""]
    resources  = ["pods/log", "events"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_role_binding_v1" "spark_driver" {
  metadata {
    name      = "${var.spark_service_account}-driver"
    namespace = data.kubernetes_namespace_v1.spark.metadata[0].name
    labels    = local.k8s_labels
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.spark_driver.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account_v1.spark.metadata[0].name
    namespace = kubernetes_service_account_v1.spark.metadata[0].namespace
  }
}
