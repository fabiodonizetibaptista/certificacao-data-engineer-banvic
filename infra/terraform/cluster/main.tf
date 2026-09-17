resource "kind_cluster" "banvic" {
  name            = var.cluster_name
  node_image      = var.node_image
  wait_for_ready  = true
  kubeconfig_path = pathexpand("~/.kube/${var.cluster_name}-config")
}
