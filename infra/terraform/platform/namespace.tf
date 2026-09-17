resource "kubernetes_namespace_v1" "banvic" {
  metadata {
    name = var.namespace
  }
}
