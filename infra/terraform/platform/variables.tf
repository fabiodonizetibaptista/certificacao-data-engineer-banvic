variable "kubeconfig_path" {
  description = "Path to the kubeconfig used by the Kubernetes and Helm providers."
  type        = string
  default     = "~/.kube/banvic-local-config"
}

variable "kubernetes_context" {
  description = "Kubernetes context targeted by the platform Terraform root."
  type        = string
  default     = "kind-banvic-local"
}

variable "namespace" {
  description = "Kubernetes namespace used by the BanVic platform resources."
  type        = string
  default     = "banvic"
}

# ---------------------------------------------------------------------------
# PostgreSQL / Airflow runtime
# ---------------------------------------------------------------------------

variable "postgres_host" {
  description = "Kubernetes Service hostname used by Airflow to reach PostgreSQL."
  type        = string
  default     = "postgres"
}

variable "postgres_port" {
  description = "PostgreSQL service port."
  type        = number
  default     = 5432
}

variable "postgres_database" {
  description = "BanVic analytical PostgreSQL database name."
  type        = string
  default     = "banvic_dw"
}

variable "postgres_user" {
  description = "BanVic analytical PostgreSQL user."
  type        = string
  default     = "banvic_user"
}

variable "postgres_password" {
  description = "PostgreSQL password transported ephemerally to Kubernetes Secrets."
  type        = string
  sensitive   = true
  ephemeral   = true
}

# PostgreSQL and Airflow runtime credentials form one credential domain.
# Incrementing this revision only instructs Kubernetes to rewrite the Secrets.
# It is NOT, by itself, a complete PostgreSQL password-rotation procedure.
variable "postgres_credentials_revision" {
  description = "Explicit revision controlling writes of PostgreSQL/runtime write-only Secret data."
  type        = number
  default     = 1
}

# ---------------------------------------------------------------------------
# Airflow API secret
# ---------------------------------------------------------------------------

variable "airflow_api_secret_key" {
  description = "Internal Airflow API secret transported ephemerally to Kubernetes."
  type        = string
  sensitive   = true
  ephemeral   = true
}

variable "airflow_api_secret_revision" {
  description = "Explicit revision controlling writes of the Airflow API write-only Secret."
  type        = number
  default     = 1
}

# ---------------------------------------------------------------------------
# Airflow administrative user
# ---------------------------------------------------------------------------

variable "airflow_admin_role" {
  description = "Airflow administrative role."
  type        = string
  default     = "Admin"
}

variable "airflow_admin_username" {
  description = "Airflow administrative username."
  type        = string
  default     = "admin"
}

variable "airflow_admin_email" {
  description = "Airflow administrative email."
  type        = string
  default     = "admin@example.com"
}

variable "airflow_admin_first_name" {
  description = "Airflow administrative user's first name."
  type        = string
  default     = "BanVic"
}

variable "airflow_admin_last_name" {
  description = "Airflow administrative user's last name."
  type        = string
  default     = "Admin"
}

variable "airflow_admin_password" {
  description = "Airflow admin password transported ephemerally to Kubernetes."
  type        = string
  sensitive   = true
  ephemeral   = true
}

variable "airflow_admin_secret_revision" {
  description = "Explicit revision controlling writes of the Airflow admin write-only Secret."
  type        = number
  default     = 1
}
