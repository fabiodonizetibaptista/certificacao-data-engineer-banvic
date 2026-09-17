#!/usr/bin/env bash

# Interrompe imediatamente em caso de erro, variável ausente ou falha em pipeline.
set -euo pipefail

NAMESPACE="${NAMESPACE:-banvic}"
SECRET_NAME="${SECRET_NAME:-airflow-admin-secret}"

# O namespace precisa existir antes da criação do Secret.
if ! kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
    echo "Erro: namespace '${NAMESPACE}' não encontrado." >&2
    echo "Crie o namespace antes de executar este script." >&2
    exit 1
fi

# Dados não sensíveis podem ser informados interativamente.
read -r -p "Usuário administrativo do Airflow [admin]: " AIRFLOW_ADMIN_USERNAME
AIRFLOW_ADMIN_USERNAME="${AIRFLOW_ADMIN_USERNAME:-admin}"

read -r -p "E-mail administrativo [admin@example.com]: " AIRFLOW_ADMIN_EMAIL
AIRFLOW_ADMIN_EMAIL="${AIRFLOW_ADMIN_EMAIL:-admin@example.com}"

read -r -p "Primeiro nome [BanVic]: " AIRFLOW_ADMIN_FIRST_NAME
AIRFLOW_ADMIN_FIRST_NAME="${AIRFLOW_ADMIN_FIRST_NAME:-BanVic}"

read -r -p "Sobrenome [Admin]: " AIRFLOW_ADMIN_LAST_NAME
AIRFLOW_ADMIN_LAST_NAME="${AIRFLOW_ADMIN_LAST_NAME:-Admin}"

# A senha é solicitada duas vezes e nunca é exibida no terminal.
read -r -s -p "Informe a senha do administrador do Airflow: " AIRFLOW_ADMIN_PASSWORD
echo
read -r -s -p "Confirme a senha do administrador do Airflow: " AIRFLOW_ADMIN_PASSWORD_CONFIRMATION
echo

if [[ -z "${AIRFLOW_ADMIN_PASSWORD}" ]]; then
    echo "Erro: a senha do administrador não pode ser vazia." >&2
    exit 1
fi

if [[ "${AIRFLOW_ADMIN_PASSWORD}" != "${AIRFLOW_ADMIN_PASSWORD_CONFIRMATION}" ]]; then
    echo "Erro: as senhas informadas não coincidem." >&2
    exit 1
fi

# Cria um arquivo temporário com acesso restrito.
# O arquivo é removido automaticamente ao final do script.
umask 077
TEMP_ENV_FILE="$(mktemp)"
trap 'rm -f "${TEMP_ENV_FILE}"' EXIT

cat > "${TEMP_ENV_FILE}" <<SECRET_VALUES
AIRFLOW_ADMIN_ROLE=Admin
AIRFLOW_ADMIN_USERNAME=${AIRFLOW_ADMIN_USERNAME}
AIRFLOW_ADMIN_EMAIL=${AIRFLOW_ADMIN_EMAIL}
AIRFLOW_ADMIN_FIRST_NAME=${AIRFLOW_ADMIN_FIRST_NAME}
AIRFLOW_ADMIN_LAST_NAME=${AIRFLOW_ADMIN_LAST_NAME}
AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD}
SECRET_VALUES

# Gera o manifesto em memória e aplica diretamente no Kubernetes.
# Nenhum YAML com credenciais é criado dentro do repositório.
kubectl create secret generic "${SECRET_NAME}" \
    --namespace "${NAMESPACE}" \
    --from-env-file="${TEMP_ENV_FILE}" \
    --dry-run=client \
    --output=yaml |
kubectl apply -f -

# Remove as senhas das variáveis do processo antes de encerrar.
unset AIRFLOW_ADMIN_PASSWORD AIRFLOW_ADMIN_PASSWORD_CONFIRMATION

echo "Secret '${SECRET_NAME}' criado ou atualizado no namespace '${NAMESPACE}'."
echo "Nenhuma credencial administrativa foi gravada no repositório."
