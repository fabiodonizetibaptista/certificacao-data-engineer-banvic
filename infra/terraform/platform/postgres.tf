# ---------------------------------------------------------------------------
# BanVic analytical PostgreSQL
# ---------------------------------------------------------------------------
#
# This file is the Terraform representation of the existing Kubernetes
# manifests:
#   - postgres-pvc.yaml
#   - postgres-deployment.yaml
#   - postgres-service.yaml
#
# The intent is behavioral parity with the proven baseline. Infrastructure
# changes should be made deliberately here rather than silently diverging from
# the original manifests.


# ---------------------------------------------------------------------------
# Persistent storage
# ---------------------------------------------------------------------------

resource "kubernetes_persistent_volume_claim_v1" "postgres_data" {
  metadata {
    name      = "postgres-data"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name

    labels = {
      app = "postgres"
    }
  }

  # Kind's "standard" StorageClass uses WaitForFirstConsumer. Therefore the
  # PVC is expected to remain Pending until the PostgreSQL Pod is scheduled.
  # Waiting for Bound here would deadlock Terraform before it could create the
  # Deployment that consumes the volume.
  wait_until_bound = false

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = "standard"

    resources {
      requests = {
        storage = "2Gi"
      }
    }
  }
}


# ---------------------------------------------------------------------------
# PostgreSQL workload
# ---------------------------------------------------------------------------

resource "kubernetes_deployment_v1" "postgres" {
  metadata {
    name      = "postgres"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name

    labels = {
      app = "postgres"
    }
  }

  spec {
    replicas = 1

    # The PVC is ReadWriteOnce. Recreate prevents Kubernetes from attempting
    # to keep the previous Pod and a replacement Pod active simultaneously
    # during an update, which could create a volume-attachment conflict.
    strategy {
      type = "Recreate"
    }

    selector {
      match_labels = {
        app = "postgres"
      }
    }

    template {
      metadata {
        labels = {
          app = "postgres"
        }
      }

      spec {
        # PostgreSQL has no reason to call the Kubernetes API. Disabling the
        # automatically mounted ServiceAccount token reduces unnecessary
        # credentials inside the container.
        automount_service_account_token  = false
        termination_grace_period_seconds = 30

        container {
          name = "postgres"

          # The tag documents the intended PostgreSQL major version while the
          # digest makes the container artifact reproducible.
          image = "postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20"

          image_pull_policy = "IfNotPresent"

          port {
            name           = "postgres"
            container_port = 5432
            protocol       = "TCP"
          }

          # Reference the Terraform-managed Secret directly instead of using a
          # literal Secret name. Besides preserving the Kubernetes contract,
          # this creates the correct Terraform dependency automatically.
          env {
            name = "POSTGRES_USER"

            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.postgres.metadata[0].name
                key  = "POSTGRES_USER"
              }
            }
          }

          env {
            name = "POSTGRES_PASSWORD"

            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.postgres.metadata[0].name
                key  = "POSTGRES_PASSWORD"
              }
            }
          }

          env {
            name = "POSTGRES_DB"

            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.postgres.metadata[0].name
                key  = "POSTGRES_DB"
              }
            }
          }

          # PostgreSQL stores its database cluster in a subdirectory of the
          # mounted volume. This avoids conflicts with directories that the
          # storage implementation may create at the volume root.
          env {
            name  = "PGDATA"
            value = "/var/lib/postgresql/data/pgdata"
          }

          # Startup gets a longer failure window because initial database
          # initialization can legitimately take longer than normal health
          # checks. Readiness and liveness retain the proven baseline values.
          startup_probe {
            exec {
              command = [
                "sh",
                "-c",
                "pg_isready -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\"",
              ]
            }

            period_seconds    = 2
            timeout_seconds   = 2
            failure_threshold = 30
          }

          readiness_probe {
            exec {
              command = [
                "sh",
                "-c",
                "pg_isready -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\"",
              ]
            }

            period_seconds    = 5
            timeout_seconds   = 2
            failure_threshold = 6
          }

          liveness_probe {
            exec {
              command = [
                "sh",
                "-c",
                "pg_isready -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\"",
              ]
            }

            period_seconds    = 10
            timeout_seconds   = 2
            failure_threshold = 6
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "128Mi"
            }

            limits = {
              cpu    = "500m"
              memory = "512Mi"
            }
          }

          volume_mount {
            name       = "postgres-storage"
            mount_path = "/var/lib/postgresql/data"
          }
        }

        volume {
          name = "postgres-storage"

          # Referencing the Terraform-managed PVC establishes creation order
          # without an artificial depends_on.
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.postgres_data.metadata[0].name
          }
        }
      }
    }
  }
}


# ---------------------------------------------------------------------------
# Internal PostgreSQL network endpoint
# ---------------------------------------------------------------------------

resource "kubernetes_service_v1" "postgres" {
  metadata {
    name      = "postgres"
    namespace = kubernetes_namespace_v1.banvic.metadata[0].name

    labels = {
      app = "postgres"
    }
  }

  spec {
    # PostgreSQL is consumed only by workloads inside the cluster. No
    # NodePort/LoadBalancer exposure is required for this POC.
    type = "ClusterIP"

    selector = {
      app = "postgres"
    }

    port {
      name        = "postgres"
      port        = 5432
      target_port = 5432
    }
  }
}
