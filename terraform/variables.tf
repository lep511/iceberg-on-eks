variable "region" {
  description = "AWS Region of the existing EKS cluster. All resources of this project are created in this Region."
  type        = string
  default     = "us-west-2"
}

variable "cluster_name" {
  description = "Name of the existing EKS cluster (created by data-on-eks/analytics/terraform/spark-k8s-operator). This project only reads it."
  type        = string
  default     = "spark-on-eks"
}

variable "name" {
  description = "Prefix for every AWS resource created by this project (S3 bucket prefix, IAM role name). Must start with spark-on-eks because of the IDE role permissions."
  type        = string
  default     = "spark-on-eks-iceberg"

  validation {
    # The IDE instance role may only create IAM roles named spark-on-eks-* and S3 buckets named spark-on-eks*.
    condition     = can(regex("^spark-on-eks(-[a-z0-9]+)*$", var.name))
    error_message = "The name must be \"spark-on-eks\" or start with \"spark-on-eks-\", followed only by lowercase letters, digits, and single hyphens (no trailing hyphen). The IDE role that runs Terraform may only create IAM roles named spark-on-eks-* and S3 buckets named spark-on-eks*, so any other prefix fails with AccessDenied."
  }

  validation {
    # The bucket uses bucket_prefix = "<name>-", and S3 limits bucket_prefix to 37 characters
    # (63-character bucket names minus the 26-character unique suffix Terraform appends).
    condition     = length(var.name) <= 36
    error_message = "The name must be at most 36 characters long: it is used as the S3 bucket_prefix \"<name>-\", which S3 limits to 37 characters."
  }
}

variable "glue_database" {
  description = "Glue Data Catalog database (Iceberg namespace) for the Iceberg table. Terraform does not create it (the IDE role has no Glue permissions); the first Spark job creates it with the IRSA role."
  type        = string
  default     = "nyc_taxi_database"

  validation {
    # Glue stores names in lowercase; an uppercase name would not match the IAM resource ARNs.
    condition     = can(regex("^[a-z0-9_]{1,255}$", var.glue_database))
    error_message = "The glue_database must contain only lowercase letters, digits, and underscores (1 to 255 characters)."
  }
}

variable "iceberg_table" {
  description = "Name of the shared Iceberg table (23-column NYC Taxi schema) inside glue_database."
  type        = string
  default     = "nyc_taxi_table"

  validation {
    condition     = can(regex("^[a-z0-9_]{1,255}$", var.iceberg_table))
    error_message = "The iceberg_table must contain only lowercase letters, digits, and underscores (1 to 255 characters)."
  }
}

variable "spark_namespace" {
  description = "EXISTING Kubernetes namespace for SparkApplications. It must be one of the namespaces the Spark Operator watches (default, spark-team-a, spark-team-b, spark-team-c, spark-s3-express). This project does not create or own it."
  type        = string
  default     = "spark-team-a"
}

variable "spark_service_account" {
  description = "Service account created in spark_namespace for the Spark driver and executors (IRSA-annotated, with a namespaced Role to manage executor pods)."
  type        = string
  default     = "spark-iceberg"
}

variable "jobs_namespace" {
  description = "Kubernetes namespace created by this project for plain Kubernetes Jobs (the Rust CLI)."
  type        = string
  default     = "iceberg"
}

variable "jobs_service_account" {
  description = "Service account created in jobs_namespace for the Rust CLI Jobs (IRSA-annotated, no Kubernetes API permissions)."
  type        = string
  default     = "iceberg-jobs"
}

variable "graviton5_node_class" {
  description = "EXISTING EKS Auto Mode NodeClass (eks.amazonaws.com/v1) for the Graviton5 NodePool of this project: it sets the subnets, security groups, node IAM role and ephemeral storage of the M9g/M9gd nodes. The default is the platform NodeClass of its general-purpose NodePool (100 GiB gp3). This project only reads it."
  type        = string
  default     = "ebs-gp3-100gi"

  validation {
    condition     = can(regex("^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$", var.graviton5_node_class))
    error_message = "The graviton5_node_class must be the name of a Kubernetes object (lowercase letters, digits, '-' and '.')."
  }
}

variable "spark_event_logs_bucket" {
  description = "Optional existing S3 bucket for Spark event logs (written under spark-event-logs/). Set it to the platform logs bucket to see the jobs in the Spark History Server. Empty: event logs go to s3a://<warehouse_bucket>/lab/spark-event-logs."
  type        = string
  default     = ""

  validation {
    condition     = var.spark_event_logs_bucket == "" || can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.spark_event_logs_bucket))
    error_message = "The spark_event_logs_bucket must be empty or a plain S3 bucket name (no s3:// or s3a:// scheme, no key prefix)."
  }
}

variable "tags" {
  description = "Additional tags applied to every taggable AWS resource through the provider default_tags (Project = var.name is always added)."
  type        = map(string)
  default     = {}
}
