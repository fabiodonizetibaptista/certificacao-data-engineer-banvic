# ---------------------------------------------------------------------------
# Apache Airflow
# ---------------------------------------------------------------------------
#
# Declarative equivalent of the proven baseline command:
#
#   helm upgrade --install airflow apache-airflow/airflow \
#     --version 1.22.0 \
#     --namespace banvic \
#     --values infra/k8s/airflow-values.yaml \
#     --atomic \
#     --timeout 15m
#
# The existing airflow-values.yaml remains the single source of truth for the
# chart configuration. Duplicating its contents in HCL would create two
# configuration surfaces and increase drift risk.


resource "helm_release" "airflow" {
  name       = "airflow"
  repository = "https://airflow.apache.org"
  chart      = "airflow"
  version    = "1.22.0"

  namespace = kubernetes_namespace_v1.banvic.metadata[0].name

  # The namespace is already owned by this Terraform root. Allowing Helm to
  # create it independently would create overlapping ownership.
  create_namespace = false

  # Reuse the validated baseline values instead of translating 200+ lines of
  # Helm configuration into Terraform set blocks.
  values = [
    file("${path.module}/../../k8s/airflow-values.yaml")
  ]

  # `--atomic` from the original deployment contract. A failed installation
  # should be rolled back rather than leave a partially deployed Airflow.
  atomic = true

  # Helm CLI used --timeout 15m. helm_release expects timeout in seconds.
  timeout = 900

  # Helm's atomic behavior implies waiting for resources. Keeping this explicit
  # documents the deployment contract and makes readiness part of Terraform's
  # success criterion.
  wait = true

  # These dependencies are intentionally explicit. airflow-values.yaml refers
  # to the Secrets only by their Kubernetes names, so Terraform cannot infer
  # the resource graph from the YAML file contents.
  depends_on = [
    kubernetes_secret_v1.airflow_runtime,
    kubernetes_secret_v1.airflow_api,
    kubernetes_secret_v1.airflow_admin,
  ]
}
