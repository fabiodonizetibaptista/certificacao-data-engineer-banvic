# Kubernetes is the owner of the Secret objects through Terraform.
# Sensitive values enter Terraform only as ephemeral variables and are written
# through data_wo, so Terraform does not persist those values in state.

resource "kubernetes_secret_v1" "postgres" {
  metadata {
    name      = "postgres-secret"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
  }

  type = "Opaque"

  data_wo = {
    POSTGRES_USER     = var.postgres_user
    POSTGRES_PASSWORD = var.postgres_password
    POSTGRES_DB       = var.postgres_database
  }

  # PostgreSQL and Airflow runtime share the same credential revision because
  # both Secrets must remain consistent with the same database credentials.
  data_wo_revision = var.postgres_credentials_revision
}

resource "kubernetes_secret_v1" "airflow_runtime" {
  metadata {
    name      = "airflow-runtime-secret"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
  }

  type = "Opaque"

  data_wo = {
    TARGET_POSTGRES_HOST     = var.postgres_host
    TARGET_POSTGRES_PORT     = tostring(var.postgres_port)
    TARGET_POSTGRES_DATABASE = var.postgres_database
    TARGET_POSTGRES_USER     = var.postgres_user
    TARGET_POSTGRES_PASSWORD = var.postgres_password

    AIRFLOW_CONN_BANVIC_DW = jsonencode({
      conn_type = "postgres"
      host      = var.postgres_host
      login     = var.postgres_user
      password  = var.postgres_password
      schema    = var.postgres_database
      port      = var.postgres_port
    })
  }

  data_wo_revision = var.postgres_credentials_revision
}

resource "kubernetes_secret_v1" "airflow_api" {
  metadata {
    name      = "airflow-api-secret"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
  }

  type = "Opaque"

  data_wo = {
    "api-secret-key" = var.airflow_api_secret_key
  }

  data_wo_revision = var.airflow_api_secret_revision
}

resource "kubernetes_secret_v1" "airflow_admin" {
  metadata {
    name      = "airflow-admin-secret"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name
  }

  type = "Opaque"

  data_wo = {
    AIRFLOW_ADMIN_ROLE       = var.airflow_admin_role
    AIRFLOW_ADMIN_USERNAME   = var.airflow_admin_username
    AIRFLOW_ADMIN_EMAIL      = var.airflow_admin_email
    AIRFLOW_ADMIN_FIRST_NAME = var.airflow_admin_first_name
    AIRFLOW_ADMIN_LAST_NAME  = var.airflow_admin_last_name
    AIRFLOW_ADMIN_PASSWORD   = var.airflow_admin_password
  }

  data_wo_revision = var.airflow_admin_secret_revision
}
