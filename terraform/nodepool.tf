################################################################################
# Graviton5 NodePool (EKS Auto Mode): every pod of this project runs on it
################################################################################

# AWS Graviton5 (Neoverse V3, Armv9.2-a, 3.3 GHz) powers the M9g (EBS only) and M9gd (local
# NVMe) families; see GRAVITON5.md. Every Rust CLI Job and every Spark driver and executor
# selects this NodePool (karpenter.sh/nodepool) and kubernetes.io/arch=arm64, and the Rust CLI
# is compiled for Neoverse V3 (rust/.cargo/config.toml), so it only runs on Graviton5.
#
# The platform's own NodePools are not used: none of them is limited to Graviton5
# (compute-general-graviton, for example, also launches 7th and 8th generation Graviton).
resource "kubernetes_manifest" "graviton5_node_pool" {
  manifest = {
    apiVersion = "karpenter.sh/v1"
    kind       = "NodePool"
    metadata = {
      name   = local.graviton5_node_pool
      labels = local.k8s_labels
    }
    spec = {
      template = {
        spec = {
          # Existing NodeClass: subnets, security groups, node IAM role and ephemeral storage.
          nodeClassRef = {
            group = "eks.amazonaws.com"
            kind  = "NodeClass"
            name  = data.kubernetes_resource.graviton5_node_class.object.metadata.name
          }
          requirements = [
            {
              key      = "eks.amazonaws.com/instance-family"
              operator = "In"
              values   = local.graviton5_instance_families
            },
            {
              key      = "kubernetes.io/arch"
              operator = "In"
              values   = ["arm64"]
            },
            {
              key      = "kubernetes.io/os"
              operator = "In"
              values   = ["linux"]
            },
            {
              # Rust Jobs and Spark drivers ask for on-demand; Spark executors may use spot.
              key      = "karpenter.sh/capacity-type"
              operator = "In"
              values   = ["on-demand", "spot"]
            },
            {
              # Auto Mode needs more than 1 vCPU (no medium). The lab pods ask for at most
              # 2 CPUs and 4 GiB, so 2 to 32 vCPUs (8 to 128 GiB) is plenty.
              key      = "eks.amazonaws.com/instance-size"
              operator = "In"
              values   = ["large", "xlarge", "2xlarge", "4xlarge", "8xlarge"]
            },
          ]
        }
      }
      # Upper bound for the whole pool (a guard against runaway scaling, not a quota per job).
      limits = {
        cpu    = "256"
        memory = "1024Gi"
      }
      disruption = {
        consolidationPolicy = "WhenEmptyOrUnderutilized"
        consolidateAfter    = "60s"
      }
    }
  }
}
