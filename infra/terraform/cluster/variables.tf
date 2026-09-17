variable "cluster_name" {
  description = "Name of the local Kind cluster managed by Terraform."
  type        = string
  default     = "banvic-local"
}

variable "node_image" {
  description = "Kind node image pinned by tag and digest for reproducibility."
  type        = string
  default     = "kindest/node:v1.35.0@sha256:452d707d4862f52530247495d180205e029056831160e22870e37e3f6c1ac31f"
}
