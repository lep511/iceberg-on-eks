terraform {
  # `terraform_data` and `replace_triggered_by` (used in iam.tf) need Terraform 1.4 or later;
  # `check` blocks (used in data.tf) need 1.5 or later.
  required_version = ">= 1.5.7, < 2.0.0"

  # Providers are pinned exactly. This project is independent of the platform blueprint
  # (data-on-eks/analytics/terraform/spark-k8s-operator), which pins its own, older versions.
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.72.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "3.5.0"
    }
  }

  # State is local (terraform.tfstate in this directory) by default.
  # To share state, create a bucket named spark-on-eks-* (the only names the IDE role may create)
  # and uncomment the block below. Re-run `terraform init -migrate-state` afterwards.
  #
  # backend "s3" {
  #   bucket       = "spark-on-eks-iceberg-tfstate-<unique-suffix>"
  #   key          = "iceberg-on-eks/terraform.tfstate"
  #   region       = "us-west-2"
  #   encrypt      = true
  #   use_lockfile = true
  # }
}
