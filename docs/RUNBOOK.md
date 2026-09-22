]633;E;sed -n '334,574p' README.md;64c61384-037b-42e8-94e9-44362a5ae207]633;C# Runbook Operacional — BanVic

Os comandos abaixo devem ser executados na raiz do repositório, em WSL ou Linux.

## Pré-requisitos

- Docker;
- Kind;
- `kubectl`;
- Terraform `1.16.x`;
- Python 3;
- Git.

## Construir a imagem customizada

```bash
docker build \
  --no-cache \
  --pull \
  --tag banvic-airflow-meltano:0.6.4 \
  --file infra/airflow/Dockerfile \
  .
```

A imagem é construída a partir do conteúdo versionado no repositório antes do provisionamento do cluster.

## Provisionar o cluster Kind com Terraform

Inicializar o root responsável pelo cluster:

```bash
terraform -chdir=infra/terraform/cluster init
```

Revisar o plano:

```bash
terraform -chdir=infra/terraform/cluster plan
```

Aplicar:

```bash
terraform -chdir=infra/terraform/cluster apply
```

Por padrão, o Terraform cria o cluster `banvic-local`, o contexto `kind-banvic-local` e o kubeconfig `~/.kube/banvic-local-config`.

Validar o node:

```bash
kubectl \
  --kubeconfig ~/.kube/banvic-local-config \
  --context kind-banvic-local \
  get nodes
```

O node deve atingir o estado `Ready`.

## Carregar a imagem no Kind

Depois que o cluster estiver disponível, carregar a imagem customizada no node:

```bash
kind load docker-image \
  banvic-airflow-meltano:0.6.4 \
  --name banvic-local
```

O `airflow-values.yaml` utiliza `pullPolicy: Never`, portanto a imagem precisa estar disponível no node antes do provisionamento do Airflow.

## Inicializar o Terraform da plataforma

Inicializar o segundo root Terraform:

```bash
terraform -chdir=infra/terraform/platform init
```

Esse root é responsável por:

- namespace `banvic`;
- Kubernetes Secrets;
- PVC, Deployment e Service do PostgreSQL analítico;
- release Helm do Apache Airflow.

## Revisar o plano da plataforma com o launcher seguro

Executar:

```bash
python3 scripts/deploy-platform.py plan
```

No primeiro bootstrap, o launcher solicita sem exibir no terminal:

- senha do PostgreSQL e confirmação;
- senha do administrador do Airflow e confirmação.

A chave interna da API do Airflow é gerada em memória.

O launcher envia os valores sensíveis ao Terraform apenas no ambiente do subprocesso. As variáveis correspondentes são `sensitive` e `ephemeral`, e os Secrets utilizam atributos write-only para evitar a persistência dos valores no state.

Em um cluster novo, a ausência do namespace `banvic` é tratada como estado válido de bootstrap. A criação do namespace permanece sob responsabilidade do Terraform.

## Provisionar a plataforma

Executar:

```bash
python3 scripts/deploy-platform.py apply
```

O launcher executa os preflights de segurança, gera um plano Terraform temporário e aplica exatamente o plano gerado.

## Validar a plataforma Kubernetes

Executar:

```bash
kubectl \
  --kubeconfig ~/.kube/banvic-local-config \
  --context kind-banvic-local \
  --namespace banvic \
  get pods,pvc,jobs
```

Resultado esperado:

- PostgreSQL analítico em `Running`;
- componentes ativos do Airflow em `Running`;
- PVC `postgres-data` em `Bound`;
- Jobs de migration e criação do usuário administrativo em `Complete`.

## Validar a DAG

Identificar o Pod do scheduler:

```bash
SCHEDULER_POD="$(
  kubectl \
    --kubeconfig ~/.kube/banvic-local-config \
    --context kind-banvic-local \
    --namespace banvic \
    get pods \
    -l component=scheduler \
    -o jsonpath='{.items[0].metadata.name}'
)"
```

Validar erros de importação:

```bash
kubectl \
  --kubeconfig ~/.kube/banvic-local-config \
  --context kind-banvic-local \
  --namespace banvic \
  exec -c scheduler "${SCHEDULER_POD}" -- \
  airflow dags list-import-errors
```

Resultado esperado:

```text
No data found
```

Confirmar a DAG:

```bash
kubectl \
  --kubeconfig ~/.kube/banvic-local-config \
  --context kind-banvic-local \
  --namespace banvic \
  exec -c scheduler "${SCHEDULER_POD}" -- \
  airflow dags list |
grep banvic_meltano_ingestion
```

A coluna `is_paused` deve aparecer como `False`. A DAG utiliza
`is_paused_upon_creation=False`, portanto não é necessário executar
`airflow dags unpause` após uma instalação limpa.

## Executar a DAG

A DAG foi configurada com `schedule=None`; nesta POC, a execução é manual e sob demanda.

Disparar uma execução:

```bash
kubectl \
  --kubeconfig ~/.kube/banvic-local-config \
  --context kind-banvic-local \
  --namespace banvic \
  exec -c scheduler "${SCHEDULER_POD}" -- \
  airflow dags trigger banvic_meltano_ingestion
```

Anote o `run_id` retornado.

Consultar as execuções:

```bash
kubectl \
  --kubeconfig ~/.kube/banvic-local-config \
  --context kind-banvic-local \
  --namespace banvic \
  exec -c scheduler "${SCHEDULER_POD}" -- \
  airflow dags list-runs banvic_meltano_ingestion
```

Resultado esperado para a execução mais recente:

```text
state = success
```

Para validar as contagens carregadas e os eventos de auditoria após a execução, siga [VALIDATION.md](VALIDATION.md).

## Validar a idempotência da infraestrutura

Após o provisionamento, uma nova revisão dos dois roots Terraform deve resultar em ausência de mudanças.

Validar a plataforma:

```bash
python3 scripts/deploy-platform.py plan
```

Validar o cluster:

```bash
terraform -chdir=infra/terraform/cluster plan
```

Resultado esperado em ambos os casos:

```text
No changes. Your infrastructure matches the configuration.
```

Essa validação confirma que reaplicar a configuração declarativa não produz alterações quando o ambiente já está aderente ao código versionado.

## Acesso à interface do Airflow

Executar o port-forward utilizando explicitamente o kubeconfig e o contexto do cluster:

```bash
kubectl \
  --kubeconfig ~/.kube/banvic-local-config \
  --context kind-banvic-local \
  --namespace banvic \
  port-forward service/airflow-api-server 8080:8080
```

Acessar:

```text
http://localhost:8080
```

Utilize o usuário administrativo configurado para o Airflow e a senha informada ao launcher seguro durante o bootstrap da plataforma. A credencial não deve ser registrada no repositório nem exibida no terminal.

A interface permite acompanhar:

- status das execuções;
- dependência entre tasks;
- tentativas e retries;
- logs de cada task;
- duração;
- histórico de runs;
- falhas operacionais.
