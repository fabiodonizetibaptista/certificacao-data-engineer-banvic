#!/usr/bin/env bash

# Interrompe imediatamente em caso de erro, variável ausente ou falha em pipeline.
set -euo pipefail

NAMESPACE="${NAMESPACE:-banvic}"
SECRET_NAME="${SECRET_NAME:-airflow-api-secret}"
ROTATE_API_SECRET="${ROTATE_API_SECRET:-false}"

# O namespace precisa existir antes da criação do Secret.
if ! kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
    echo "Erro: namespace '${NAMESPACE}' não encontrado." >&2
    echo "Crie o namespace antes de executar este script." >&2
    exit 1
fi

# Preserva a chave existente por padrão para evitar rotação acidental,
# invalidação de tokens e reinícios desnecessários dos componentes do Airflow.
if kubectl get secret "${SECRET_NAME}" \
    --namespace "${NAMESPACE}" >/dev/null 2>&1; then

    if [[ "${ROTATE_API_SECRET}" != "true" ]]; then
        echo "Secret '${SECRET_NAME}' já existe no namespace '${NAMESPACE}'."
        echo "A chave atual foi preservada."
        echo "Para rotacioná-la conscientemente, execute com ROTATE_API_SECRET=true."
        exit 0
    fi

    echo "Rotacionando a chave interna da API do Airflow."
fi

# Gera uma chave criptograficamente aleatória sem exibi-la no terminal.
AIRFLOW_API_SECRET_KEY="$(
python3 - <<'PY'
import secrets

print(secrets.token_urlsafe(64))
PY
)"

umask 077
TEMP_ENV_FILE="$(mktemp)"

cleanup() {
    rm -f "${TEMP_ENV_FILE}"
    unset AIRFLOW_API_SECRET_KEY
}

trap cleanup EXIT

cat > "${TEMP_ENV_FILE}" <<SECRET_VALUES
api-secret-key=${AIRFLOW_API_SECRET_KEY}
SECRET_VALUES

# O manifesto é gerado em memória e aplicado diretamente ao Kubernetes.
# Nenhum YAML contendo a chave permanece no repositório.
kubectl create secret generic "${SECRET_NAME}" \
    --namespace "${NAMESPACE}" \
    --from-env-file="${TEMP_ENV_FILE}" \
    --dry-run=client \
    --output=yaml |
kubectl apply -f -

echo "Secret '${SECRET_NAME}' criado ou atualizado no namespace '${NAMESPACE}'."
echo "Nenhuma chave da API foi gravada no repositório ou exibida no terminal."
