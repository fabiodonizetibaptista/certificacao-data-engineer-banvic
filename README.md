# Certificação Data Engineer — BanVic

Este repositório contém a solução desenvolvida para o desafio **Certificação Data Engineer by Indicium**, com foco na construção de uma prova de conceito reprodutível de infraestrutura e ingestão de dados para o Banco Vitória S.A. (BanVic).

A solução executa localmente em Kubernetes com **Kind**, utiliza **Apache Airflow 3.2.2** para orquestração, **Meltano 4.2.0** para ELT e **PostgreSQL 16** como destino centralizado dos dados.

## 1. Objetivo do projeto

O objetivo é disponibilizar uma infraestrutura local, segura e reproduzível para ingerir e centralizar as sete tabelas fornecidas no desafio BanVic.

A solução contempla:

- provisionamento de infraestrutura local em Kubernetes;
- execução do Airflow em ambiente conteinerizado;
- ingestão ELT com Meltano, `tap-csv` e `target-postgres`;
- armazenamento centralizado em PostgreSQL;
- validação quantitativa dos arquivos antes da carga;
- validação das tabelas após a carga;
- auditoria operacional por execução, task e tabela;
- retries e callbacks para início, sucesso, nova tentativa e falha do Meltano;
- idempotência por recriação controlada do schema de destino;
- gerenciamento de credenciais com Kubernetes Secrets;
- scripts SQL para validação dos dados e da auditoria.

## 2. Arquitetura da solução

![Arquitetura da solução BanVic](docs/images/banvic-data-architecture.png)

A arquitetura separa o fluxo de dados das responsabilidades operacionais.

| Componente | Papel |
|---|---|
| Docker | Constrói a imagem customizada do Airflow com Meltano e seus plugins |
| Kind | Cria o cluster Kubernetes local |
| Kubernetes | Orquestra os componentes da solução |
| Apache Airflow 3.2.2 | Agenda, executa e monitora o pipeline |
| Meltano 4.2.0 | Executa o processo ELT |
| `tap-csv` | Extrai os sete arquivos CSV |
| `target-postgres` | Carrega os registros no PostgreSQL analítico |
| PostgreSQL do Airflow | Armazena metadados internos do Airflow |
| PostgreSQL BanVic | Armazena os schemas `raw_banvic` e `control_banvic` |
| Kubernetes Secrets | Mantêm credenciais e chaves fora do código versionado |
| Scripts SQL | Validam contagens e eventos de auditoria |

A solução utiliza dois bancos PostgreSQL independentes:

1. **PostgreSQL interno do Helm Chart**, exclusivo para os metadados do Airflow.
2. **Deployment `postgres`**, utilizado como destino analítico do BanVic.

## 3. Versões fixadas

| Componente | Versão |
|---|---:|
| Imagem customizada | `banvic-airflow-meltano:0.6.3` |
| Apache Airflow | `3.2.2` |
| Meltano | `4.2.0` |
| Helm Chart do Airflow | `1.22.0` |
| PostgreSQL analítico | `16` |
| Kind node | `kindest/node:v1.35.0` |

O pinning reduz variações entre instalações e torna a execução mais previsível.

## 4. Estrutura do repositório

```text
.
├── dags/
│   └── banvic_meltano_ingestion_dag.py
├── data/
│   └── input/
│       └── banvic_raw/
│           ├── agencias.csv
│           ├── clientes.csv
│           ├── colaborador_agencia.csv
│           ├── colaboradores.csv
│           ├── contas.csv
│           ├── propostas_credito.csv
│           └── transacoes.csv
├── docs/
│   └── images/
│       └── banvic-data-architecture.png
├── infra/
│   ├── airflow/
│   │   └── Dockerfile
│   └── k8s/
│       ├── airflow-values.yaml
│       ├── create-airflow-admin-secret.sh
│       ├── create-airflow-api-secret.sh
│       ├── create-postgres-secret.sh
│       ├── namespace.yaml
│       ├── postgres-deployment.yaml
│       └── postgres-service.yaml
├── meltano_project/
│   ├── meltano.yml
│   └── plugins/
│       ├── extractors/
│       │   └── tap-csv--meltanolabs.lock
│       └── loaders/
│           └── target-postgres--meltanolabs.lock
├── sql/
│   ├── validate_audit_events.sql
│   └── validate_raw_counts.sql
├── .dockerignore
├── .gitignore
└── README.md
```

## 5. Fonte de dados

Os arquivos de entrada ficam em:

```text
data/input/banvic_raw/
```

Na imagem do Airflow, eles são copiados para:

```text
/opt/airflow/data/input/banvic_raw/
```

| Arquivo | Entidade | Registros esperados |
|---|---|---:|
| `agencias.csv` | Agências | 10 |
| `clientes.csv` | Clientes | 998 |
| `colaborador_agencia.csv` | Relação colaborador-agência | 100 |
| `colaboradores.csv` | Colaboradores | 100 |
| `contas.csv` | Contas | 999 |
| `propostas_credito.csv` | Propostas de crédito | 2.000 |
| `transacoes.csv` | Transações | 71.999 |

As contagens representam o snapshot fornecido para o desafio. Uma alteração legítima nos arquivos exige a atualização consciente dos valores esperados na DAG.

## 6. Estratégia de ingestão

O pipeline Meltano está definido em:

```text
meltano_project/meltano.yml
```

Plugins utilizados:

- extractor: `tap-csv`;
- loader: `target-postgres`.

As sete entidades são carregadas no schema:

```text
raw_banvic
```

A estratégia atual é **full refresh idempotente**:

1. os arquivos fonte são validados;
2. o schema `raw_banvic` é recriado;
3. o Meltano executa a carga;
4. as tabelas carregadas são validadas.

Executar a DAG novamente produz o mesmo estado final, sem acumular duplicidades.

## 7. Orquestração com Airflow

A DAG está em:

```text
dags/banvic_meltano_ingestion_dag.py
```

Identificador:

```text
banvic_meltano_ingestion
```

Fluxo:

```text
ensure_audit_table
    >> validate_source_files
    >> recreate_raw_schema
    >> run_meltano_tap_csv_to_target_postgres
    >> validate_loaded_tables
```

| Task | Responsabilidade |
|---|---|
| `ensure_audit_table` | Cria ou valida a estrutura de auditoria |
| `validate_source_files` | Valida existência, cabeçalho e contagem dos sete CSVs |
| `recreate_raw_schema` | Recria `raw_banvic` para garantir idempotência |
| `run_meltano_tap_csv_to_target_postgres` | Executa o pipeline Meltano |
| `validate_loaded_tables` | Confirma as contagens das sete tabelas carregadas |

A DAG possui:

```text
retries = 2
retry_delay = 1 minuto
```

A validação da fonte ocorre antes da exclusão do schema. Portanto, um arquivo ausente, vazio, truncado ou com contagem divergente interrompe a execução antes de alterar o destino.

## 8. Monitoramento, auditoria e falhas

A auditoria é persistida em:

```text
control_banvic.ingestion_audit
```

Campos registrados:

- `audit_id`;
- `dag_id`;
- `run_id`;
- `task_id`;
- `table_name`;
- `status`;
- `row_count`;
- `message`;
- `created_at`.

Eventos relevantes:

| Status | Significado |
|---|---|
| `success` | Etapa operacional concluída |
| `source_file_validated` | Arquivo fonte validado com contagem correta |
| `source_file_validation_failed` | Falha na validação do arquivo fonte |
| `meltano_started` | Execução do Meltano iniciada |
| `meltano_succeeded` | Execução do Meltano concluída |
| `loaded_table_validated` | Tabela de destino validada |
| `loaded_table_validation_failed` | Divergência ou falha na validação do destino |

O operador do Meltano possui callbacks para início, retry, sucesso e falha. Em caso de erro, a auditoria registra o tipo do evento sem armazenar credenciais.

Uma execução bem-sucedida gera 18 eventos:

```text
1  criação/validação da auditoria
7  validações dos arquivos fonte
1  recriação do schema
2  eventos do Meltano
7  validações das tabelas carregadas
```

## 9. Segurança e gerenciamento de segredos

Nenhuma senha ou chave operacional deve ser versionada no Git.

A solução utiliza quatro Kubernetes Secrets:

| Secret | Consumidor | Conteúdo |
|---|---|---|
| `postgres-secret` | PostgreSQL BanVic | usuário, senha e banco |
| `airflow-runtime-secret` | Airflow e Meltano | parâmetros de conexão e `AIRFLOW_CONN_BANVIC_DW` |
| `airflow-admin-secret` | Job de criação do usuário | credenciais administrativas da interface |
| `airflow-api-secret` | API do Airflow | chave interna estática da API |

Os scripts:

```text
infra/k8s/create-postgres-secret.sh
infra/k8s/create-airflow-admin-secret.sh
infra/k8s/create-airflow-api-secret.sh
```

implementam os seguintes controles:

- senhas digitadas sem exibição no terminal;
- confirmação da senha administrativa;
- geração criptograficamente segura da chave da API;
- arquivos temporários criados com `umask 077`;
- remoção automática dos temporários;
- manifestos enviados diretamente ao Kubernetes;
- ausência de arquivos YAML com credenciais no repositório;
- preservação da chave da API para evitar rotação acidental.

Não use `admin/admin`. As credenciais válidas são aquelas definidas durante a execução do script administrativo.

Também não execute comandos que imprimam Fernet Key, API key, senhas ou conteúdo completo de Secrets no terminal.

## 10. Execução local a partir de um ambiente limpo

Os comandos abaixo devem ser executados na raiz do repositório, em WSL ou Linux.

### 10.1 Pré-requisitos

- Docker;
- Kind;
- `kubectl`;
- Helm;
- Python 3;
- Git.

### 10.2 Criar o cluster Kind

```bash
kind create cluster \
  --name banvic \
  --image kindest/node:v1.35.0
```

Validar:

```bash
kubectl cluster-info --context kind-banvic
kubectl config use-context kind-banvic
```

### 10.3 Criar o namespace

```bash
kubectl apply -f infra/k8s/namespace.yaml
```

### 10.4 Criar os Secrets do PostgreSQL

```bash
bash infra/k8s/create-postgres-secret.sh
```

O script solicita a senha local e cria:

```text
postgres-secret
airflow-runtime-secret
```

### 10.5 Subir o PostgreSQL analítico

```bash
kubectl apply -f infra/k8s/postgres-deployment.yaml
kubectl apply -f infra/k8s/postgres-service.yaml
```

Aguardar disponibilidade:

```bash
kubectl rollout status \
  deployment/postgres \
  --namespace banvic \
  --timeout 5m
```

### 10.6 Criar os Secrets do Airflow

Criar as credenciais administrativas:

```bash
bash infra/k8s/create-airflow-admin-secret.sh
```

Criar a chave interna estática da API:

```bash
bash infra/k8s/create-airflow-api-secret.sh
```

Por padrão, o script da API preserva uma chave já existente. Uma rotação consciente pode ser feita com:

```bash
ROTATE_API_SECRET=true \
bash infra/k8s/create-airflow-api-secret.sh
```

A rotação invalida tokens existentes e pode provocar reinícios dos componentes.

### 10.7 Construir a imagem customizada

```bash
docker build \
  --tag banvic-airflow-meltano:0.6.3 \
  --file infra/airflow/Dockerfile \
  .
```

### 10.8 Carregar a imagem no Kind

```bash
kind load docker-image \
  banvic-airflow-meltano:0.6.3 \
  --name banvic
```

### 10.9 Instalar o Airflow

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

```bash
helm upgrade --install airflow apache-airflow/airflow \
  --version 1.22.0 \
  --namespace banvic \
  --values infra/k8s/airflow-values.yaml \
  --atomic \
  --timeout 15m
```

Validar:

```bash
kubectl get pods -n banvic
```

Os componentes ativos do Airflow devem ficar em `Running`, e os Jobs de migration e criação de usuário devem terminar em `Succeeded`.

### 10.10 Validar a DAG

```bash
SCHEDULER_POD="$(
  kubectl get pods -n banvic \
    -l component=scheduler \
    -o jsonpath='{.items[0].metadata.name}'
)"
```

Validar erros de importação:

```bash
kubectl exec -n banvic \
  -c scheduler \
  "${SCHEDULER_POD}" -- \
  airflow dags list-import-errors
```

Resultado esperado:

```text
No data found
```

Confirmar a DAG:

```bash
kubectl exec -n banvic \
  -c scheduler \
  "${SCHEDULER_POD}" -- \
  airflow dags list |
grep banvic_meltano_ingestion
```

A coluna `is_paused` deve aparecer como `False`. A DAG utiliza
`is_paused_upon_creation=False`, portanto não é necessário executar
`airflow dags unpause` após uma instalação limpa.

### 10.11 Executar a DAG

```bash
kubectl exec -n banvic \
  -c scheduler \
  "${SCHEDULER_POD}" -- \
  airflow dags trigger banvic_meltano_ingestion
```

Anote o `run_id` retornado.

Consultar execuções:

```bash
kubectl exec -n banvic \
  -c scheduler \
  "${SCHEDULER_POD}" -- \
  airflow dags list-runs banvic_meltano_ingestion
```

Resultado esperado para a execução mais recente:

```text
state = success
```

## 11. Validação dos dados carregados

Executar a consulta sem imprimir a senha:

```bash
kubectl exec -i -n banvic deployment/postgres -- sh -lc '
  export PGPASSWORD="${POSTGRES_PASSWORD}"

  psql \
    --username "${POSTGRES_USER}" \
    --dbname "${POSTGRES_DB}" \
    --set ON_ERROR_STOP=1 \
    --pset pager=off
' < sql/validate_raw_counts.sql
```

Resultado esperado:

| Tabela | Registros |
|---|---:|
| `agencias` | 10 |
| `clientes` | 998 |
| `colaborador_agencia` | 100 |
| `colaboradores` | 100 |
| `contas` | 999 |
| `propostas_credito` | 2.000 |
| `transacoes` | 71.999 |

## 12. Validação da auditoria

```bash
kubectl exec -i -n banvic deployment/postgres -- sh -lc '
  export PGPASSWORD="${POSTGRES_PASSWORD}"

  psql \
    --username "${POSTGRES_USER}" \
    --dbname "${POSTGRES_DB}" \
    --set ON_ERROR_STOP=1 \
    --pset pager=off
' < sql/validate_audit_events.sql
```

A consulta deve retornar eventos recentes de:

- criação ou validação da tabela de auditoria;
- validação quantitativa dos arquivos fonte;
- recriação do schema;
- início e término do Meltano;
- validação das tabelas carregadas.

## 13. Acesso à interface do Airflow

```bash
kubectl port-forward \
  service/airflow-api-server \
  8080:8080 \
  --namespace banvic
```

Acessar:

```text
http://localhost:8080
```

Utilize o usuário e a senha definidos por:

```text
infra/k8s/create-airflow-admin-secret.sh
```

A interface permite acompanhar:

- status das execuções;
- dependência entre tasks;
- tentativas e retries;
- logs de cada task;
- duração;
- histórico de runs;
- falhas operacionais.

## 14. Evidências de funcionamento

A versão `0.6.3` foi validada em 20 de julho de 2026 com:

- Helm revision `2`;
- Airflow `3.2.2`;
- Meltano `4.2.0`;
- Chart `1.22.0`;
- imagem `banvic-airflow-meltano:0.6.3`;
- cinco tasks concluídas com `success`;
- sete CSVs validados antes da carga;
- sete tabelas validadas após a carga;
- 18 eventos de auditoria;
- `meltano_started` e `meltano_succeeded`;
- contagens de origem e destino idênticas;
- componentes do Airflow iniciados sem reinícios no rollout da revisão;
- Secrets separados por responsabilidade;
- chave estática da API do Airflow;
- execução idempotente confirmada.

Execução de referência:

```text
manual__2026-07-20T16:37:15.728040+00:00
```

Estado final:

```text
success
```

## 15. Decisões técnicas

### PostgreSQL

Foi escolhido como destino por ser adequado a uma POC local de centralização de dados e por possuir integração direta com o Airflow e o Meltano.

### Meltano

Separa a lógica de extração e carga da orquestração. Os lockfiles dos plugins reduzem variações de instalação.

### Airflow

Orquestra dependências, retries, callbacks, auditoria e monitoramento visual.

### Kind

Permite executar Kubernetes localmente sem depender de infraestrutura em nuvem.

### LocalExecutor

É suficiente para a escala da POC e evita a complexidade operacional de Celery e Redis.

### Full refresh idempotente

A recriação do schema simplifica a reprodutibilidade do snapshot e elimina duplicidades entre execuções.

### Auditoria no destino

A tabela `control_banvic.ingestion_audit` fornece rastreabilidade independente da interface do Airflow.

### Segregação de Secrets

Cada componente recebe somente as credenciais necessárias à sua função, reduzindo exposição e acoplamento.

## 16. Limitações conhecidas

Esta solução é uma POC local. Portanto:

- não implementa carga incremental ou CDC;
- não possui alta disponibilidade;
- não utiliza um gerenciador externo de segredos;
- os logs do Airflow não estão configurados com persistência externa;
- os dados fonte são incorporados à imagem durante o build;
- mudanças legítimas no snapshot exigem atualização das contagens esperadas;
- a exclusão do cluster Kind remove os recursos locais e exige novo provisionamento;
- o ambiente depende dos recursos disponíveis no Docker Desktop e no WSL.

Em produção, a evolução natural incluiria armazenamento de objetos, observabilidade centralizada, logs persistentes, secret manager, CI/CD, políticas de rede, backup e estratégia incremental.

## 17. Repositório

```text
https://github.com/fabio-baptista/certificacao-data-engineer
```

## 18. Conclusão

A solução entrega uma POC funcional e auditável de engenharia de dados para o BanVic, cobrindo infraestrutura local com Kubernetes, orquestração com Airflow, ingestão com Meltano, armazenamento em PostgreSQL, segurança por Secrets, validação de origem e destino, idempotência e monitoramento operacional.

O pipeline centraliza as sete entidades do desafio em um ambiente reproduzível e constitui uma base consistente para futuras camadas analíticas e consumo por ferramentas de BI.