output "cluster_name" {
  description = "Name of the Kind cluster managed by this Terraform root."
  value       = kind_cluster.banvic.name
}

output "kubernetes_context" {
  description = "Kubectl context associated with the Kind cluster."
  value       = "kind-${kind_cluster.banvic.name}"
}
