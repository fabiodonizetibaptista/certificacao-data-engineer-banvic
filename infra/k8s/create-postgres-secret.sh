#!/usr/bin/env bash

# Interrompe o script em erros, variáveis não definidas ou falhas em pipelines.
set -euo pipefail

NAMESPACE="${NAMESPACE:-banvic}"

POSTGRES_SECRET_NAME="${POSTGRES_SECRET_NAME:-postgres-secret}"
AIRFLOW_RUNTIME_SECRET_NAME="${AIRFLOW_RUNTIME_SECRET_NAME:-airflow-runtime-secret}"

POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-banvic_dw}"
POSTGRES_USER="${POSTGRES_USER:-banvic_user}"

# O namespace deve existir antes da criação dos Secrets.
if ! kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
    echo "Erro: namespace '${NAMESPACE}' não encontrado." >&2
    echo "Crie o namespace antes de executar este script." >&2
    exit 1
fi

# A senha é solicitada sem ser exibida no terminal.
if [[ -z "${POSTGRES_PASSWORD:-}" ]]; then
    read -r -s -p "Informe a senha local do PostgreSQL: " POSTGRES_PASSWORD
    echo
fi

if [[ -z "${POSTGRES_PASSWORD}" ]]; then
    echo "Erro: a senha do PostgreSQL não pode ser vazia." >&2
    exit 1
fi

# Cria a conexão do Airflow em JSON para preservar corretamente
# caracteres especiais presentes no usuário ou na senha.
export POSTGRES_HOST
export POSTGRES_PORT
export POSTGRES_DB
export POSTGRES_USER
export POSTGRES_PASSWORD

AIRFLOW_CONN_BANVIC_DW="$(
python3 - <<'PY'
import json
import os

connection = {
    "conn_type": "postgres",
    "host": os.environ["POSTGRES_HOST"],
    "login": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "schema": os.environ["POSTGRES_DB"],
    "port": int(os.environ["POSTGRES_PORT"]),
}

print(json.dumps(connection, separators=(",", ":")))
PY
)"

# Os arquivos temporários recebem permissões restritas e são removidos
# automaticamente ao término do script.
umask 077
POSTGRES_ENV_FILE="$(mktemp)"
AIRFLOW_RUNTIME_ENV_FILE="$(mktemp)"

cleanup() {
    rm -f "${POSTGRES_ENV_FILE}" "${AIRFLOW_RUNTIME_ENV_FILE}"
    unset POSTGRES_PASSWORD AIRFLOW_CONN_BANVIC_DW
}

trap cleanup EXIT

# Secret consumido exclusivamente pelo container do PostgreSQL.
cat > "${POSTGRES_ENV_FILE}" <<SECRET_VALUES
POSTGRES_USER=${POSTGRES_USER}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
POSTGRES_DB=${POSTGRES_DB}
SECRET_VALUES

# Secret consumido pelos componentes do Airflow e pelo Meltano.
cat > "${AIRFLOW_RUNTIME_ENV_FILE}" <<SECRET_VALUES
TARGET_POSTGRES_HOST=${POSTGRES_HOST}
TARGET_POSTGRES_PORT=${POSTGRES_PORT}
TARGET_POSTGRES_DATABASE=${POSTGRES_DB}
TARGET_POSTGRES_USER=${POSTGRES_USER}
TARGET_POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
AIRFLOW_CONN_BANVIC_DW=${AIRFLOW_CONN_BANVIC_DW}
SECRET_VALUES

# Os manifestos são gerados em memória e enviados diretamente ao Kubernetes.
# Nenhum arquivo YAML contendo credenciais permanece no repositório.
kubectl create secret generic "${POSTGRES_SECRET_NAME}" \
    --namespace "${NAMESPACE}" \
    --from-env-file="${POSTGRES_ENV_FILE}" \
    --dry-run=client \
    --output=yaml |
kubectl apply -f -

kubectl create secret generic "${AIRFLOW_RUNTIME_SECRET_NAME}" \
    --namespace "${NAMESPACE}" \
    --from-env-file="${AIRFLOW_RUNTIME_ENV_FILE}" \
    --dry-run=client \
    --output=yaml |
kubectl apply -f -

echo "Secret '${POSTGRES_SECRET_NAME}' criado ou atualizado no namespace '${NAMESPACE}'."
echo "Secret '${AIRFLOW_RUNTIME_SECRET_NAME}' criado ou atualizado no namespace '${NAMESPACE}'."
echo "Nenhuma credencial foi gravada no repositório."
