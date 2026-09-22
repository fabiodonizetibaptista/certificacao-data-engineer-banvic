# Certificação Data Engineer — BanVic

Este repositório contém a solução desenvolvida para o desafio **Certificação Data Engineer by Indicium**, com foco na construção de uma prova de conceito reprodutível de infraestrutura e ingestão de dados para o Banco Vitória S.A. (BanVic).

A solução utiliza **Terraform** para provisionamento declarativo, executa localmente em Kubernetes com **Kind**, utiliza **Apache Airflow 3.2.2** para orquestração, **Meltano 4.2.0** para ELT e **PostgreSQL 16** como destino centralizado dos dados.

## 1. Objetivo do projeto

O objetivo é disponibilizar uma infraestrutura local, segura e reproduzível para ingerir e centralizar as sete tabelas fornecidas no desafio BanVic.

A solução contempla:

- provisionamento declarativo da infraestrutura local com Terraform e Kubernetes;
- execução do Airflow em ambiente conteinerizado;
- ingestão ELT com Meltano, `tap-csv` e `target-postgres`;
- armazenamento centralizado em PostgreSQL;
- validação quantitativa dos arquivos antes da carga;
- validação das tabelas após a carga;
- auditoria operacional por execução, task e tabela;
- retries e callbacks para início, sucesso, nova tentativa e falha do Meltano;
- idempotência por recriação controlada do schema de destino;
- gerenciamento seguro de credenciais com Kubernetes Secrets, Terraform e launcher dedicado;
- scripts SQL para validação dos dados e da auditoria.

## 2. Arquitetura da solução

![Arquitetura da solução BanVic](docs/images/banvic-data-architecture.png)

A arquitetura separa o fluxo de dados das responsabilidades operacionais.

| Componente | Papel |
|---|---|
| Terraform | Provisiona declarativamente o cluster Kind e os recursos da plataforma |
| `scripts/deploy-platform.py` | Executa preflights de segurança e conduz o plan/apply da plataforma |
| Docker | Constrói a imagem customizada do Airflow com Meltano e seus plugins |
| Kind | Executa o cluster Kubernetes local |
| Kubernetes | Orquestra os componentes da solução |
| Apache Airflow 3.2.2 | Orquestra, executa e monitora o pipeline |
| Meltano 4.2.0 | Executa o processo ELT |
| `tap-csv` | Extrai os sete arquivos CSV |
| `target-postgres` | Carrega os registros no PostgreSQL analítico |
| PostgreSQL do Airflow | Armazena metadados internos do Airflow |
| PostgreSQL BanVic | Armazena os schemas `raw_banvic` e `control_banvic` |
| Kubernetes Secrets | Mantêm credenciais e chaves fora do código versionado, provisionados pelo Terraform |
| Scripts SQL | Validam contagens e eventos de auditoria |

A solução utiliza dois bancos PostgreSQL independentes:

1. **PostgreSQL interno do Helm Chart**, exclusivo para os metadados do Airflow.
2. **Deployment `postgres`**, utilizado como destino analítico do BanVic.

O PostgreSQL analítico utiliza o PVC `postgres-data`, com `2Gi` e acesso
`ReadWriteOnce`. O volume preserva os dados durante recriações e atualizações
do Pod. O PostgreSQL interno do Airflow mantém seu próprio PVC, independente
do banco analítico.

## 3. Versões fixadas

| Componente | Versão |
|---|---:|
| Terraform | `1.16.x` |
| Provider `tehcyx/kind` | `0.11.0` |
| Provider `hashicorp/kubernetes` | `3.2.1` |
| Provider `hashicorp/helm` | `3.3.0` |
| Imagem customizada | `banvic-airflow-meltano:0.6.4` |
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
│   ├── k8s/
│   │   ├── airflow-values.yaml
│   │   ├── create-airflow-admin-secret.sh
│   │   ├── create-airflow-api-secret.sh
│   │   ├── create-postgres-secret.sh
│   │   ├── namespace.yaml
│   │   ├── postgres-deployment.yaml
│   │   ├── postgres-pvc.yaml
│   │   └── postgres-service.yaml
│   └── terraform/
│       ├── cluster/
│       │   ├── .terraform.lock.hcl
│       │   ├── main.tf
│       │   ├── outputs.tf
│       │   ├── providers.tf
│       │   ├── variables.tf
│       │   └── versions.tf
│       └── platform/
│           ├── .terraform.lock.hcl
│           ├── airflow.tf
│           ├── namespace.tf
│           ├── postgres.tf
│           ├── providers.tf
│           ├── secrets.tf
│           ├── variables.tf
│           └── versions.tf
├── meltano_project/
│   ├── meltano.yml
│   └── plugins/
│       ├── extractors/
│       │   └── tap-csv--meltanolabs.lock
│       └── loaders/
│           └── target-postgres--meltanolabs.lock
├── scripts/
│   └── deploy-platform.py
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

As contagens representam o snapshot fornecido para o desafio e são usadas
como evidência de aceitação. A DAG não depende desses números fixos: ela calcula
as quantidades diretamente dos CSVs e valida a paridade entre origem e destino.

O conjunto é tratado nesta solução como material educacional e demonstrativo da POC. Em um ambiente real, a camada RAW deveria possuir acesso restrito, e as camadas de consumo deveriam aplicar classificação, mascaramento ou pseudonimização conforme a sensibilidade dos dados e as políticas de privacidade aplicáveis.

## 6. Estratégia de ingestão

O pipeline Meltano está definido em:

```text
meltano_project/meltano.yml
```

Plugins utilizados:

- extractor `tap-csv` `1.2.0`, fixado no commit
  `7af22d8e81ff2ac6bd391aec63fd1fef4eb24b22`;
- loader `meltanolabs-target-postgres` `0.8.0`.

O `meltano.yml` fixa os artefatos efetivamente instalados. Os lockfiles
preservam as definições dos plugins obtidas do Meltano Hub, e o Dockerfile
interrompe o build caso a versão ou o commit instalado seja diferente do
contrato validado.

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

Configuração operacional:

```text
schedule = None
catchup = False
max_active_runs = 1
```

A execução é sob demanda nesta POC. O limite de uma execução ativa por vez evita concorrência entre full refreshes sobre o mesmo schema de destino.

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
| `validate_source_files` | Valida existência, conteúdo, cabeçalho, estrutura, chaves obrigatórias e duplicidades dos sete CSVs |
| `recreate_raw_schema` | Recria `raw_banvic` para garantir idempotência |
| `run_meltano_tap_csv_to_target_postgres` | Executa o pipeline Meltano |
| `validate_loaded_tables` | Confirma a paridade de contagens, chaves obrigatórias e ausência de duplicidades no destino |

A DAG possui:

```text
retries = 2
retry_delay = 1 minuto
```

A validação da fonte ocorre antes da exclusão do schema. Portanto, um arquivo
ausente, vazio, estruturalmente inválido, com chave obrigatória ausente ou com
duplicidade de chave interrompe a execução antes de alterar o destino. Depois da
carga, a DAG compara as quantidades reais da origem com as tabelas carregadas.

## 8. Monitoramento, auditoria e falhas

A auditoria é persistida em:

```text
control_banvic.ingestion_audit
```

Os logs locais dos Pods do Airflow utilizam `emptyDir` e são efêmeros. Essa decisão evita um PVC de logs de `100Gi` desnecessário para a POC. A rastreabilidade operacional relevante permanece persistida no PostgreSQL por meio da tabela de auditoria.

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

O fluxo oficial de provisionamento dos Secrets é gerenciado pelo Terraform em conjunto com o launcher seguro `scripts/deploy-platform.py`.

Launcher:

- solicita valores sensíveis sem exibir os valores no terminal;
- gera a chave interna da API do Airflow em memória;
- preserva Secrets existentes quando o estado persistente é coerente;
- bloqueia a aplicação quando identifica um estado inconsistente que poderia resultar em sobrescrita acidental de credenciais;
- cria o plano Terraform em diretório temporário com permissões restritas e aplica exatamente o plano gerado.

No Terraform, as variáveis que carregam segredos são marcadas como `sensitive` e `ephemeral`. No provedor Kubernetes, os Secrets utilizam atributos write-only, evitando a persistência dos valores sensíveis no state.

Os scripts legados em `infra/k8s` permanecem no repositório, mas não fazem parte do fluxo oficial de provisionamento documentado nesta versão.

Não use `admin/admin`. As credenciais válidas são as definidas no bootstrap pelo launcher seguro.

Não execute comandos que imprimam Fernet Key, API key, senhas, tokens, strings de conexão ou o conteúdo completo de Secrets no terminal.

## 10. Execução local a partir de um ambiente limpo

Os comandos abaixo devem ser executados na raiz do repositório, em WSL ou Linux.

### 10.1 Pré-requisitos

- Docker;
- Kind;
- `kubectl`;
- Terraform `1.16.x`;
- Python 3;
- Git.

### 10.2 Construir a imagem customizada

```bash
docker build \
  --no-cache \
  --pull \
  --tag banvic-airflow-meltano:0.6.4 \
  --file infra/airflow/Dockerfile \
  .
```

A imagem é construída a partir do conteúdo versionado no repositório antes do provisionamento do cluster.

### 10.3 Provisionar o cluster Kind com Terraform

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

### 10.4 Carregar a imagem no Kind

Depois que o cluster estiver disponível, carregar a imagem customizada no node:

```bash
kind load docker-image \
  banvic-airflow-meltano:0.6.4 \
  --name banvic-local
```

O `airflow-values.yaml` utiliza `pullPolicy: Never`, portanto a imagem precisa estar disponível no node antes do provisionamento do Airflow.

### 10.5 Inicializar o Terraform da plataforma

Inicializar o segundo root Terraform:

```bash
terraform -chdir=infra/terraform/platform init
```

Esse root é responsável por:

- namespace `banvic`;
- Kubernetes Secrets;
- PVC, Deployment e Service do PostgreSQL analítico;
- release Helm do Apache Airflow.

### 10.6 Revisar o plano da plataforma com o launcher seguro

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

### 10.7 Provisionar a plataforma

Executar:

```bash
python3 scripts/deploy-platform.py apply
```

O launcher executa os preflights de segurança, gera um plano Terraform temporário e aplica exatamente o plano gerado.

### 10.8 Validar a plataforma Kubernetes

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

### 10.9 Validar a DAG

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

### 10.10 Executar a DAG

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

### 10.11 Validar a idempotência da infraestrutura

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

## 11. Validação dos dados carregados

Executar a consulta sem imprimir a senha:

```bash
kubectl \
  --kubeconfig ~/.kube/banvic-local-config \
  --context kind-banvic-local \
  --namespace banvic \
  exec -i deployment/postgres -- sh -lc '
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

Executar a consulta de auditoria sem imprimir a senha:

```bash
kubectl \
  --kubeconfig ~/.kube/banvic-local-config \
  --context kind-banvic-local \
  --namespace banvic \
  exec -i deployment/postgres -- sh -lc '
  export PGPASSWORD="${POSTGRES_PASSWORD}"

  psql \
    --username "${POSTGRES_USER}" \
    --dbname "${POSTGRES_DB}" \
    --set ON_ERROR_STOP=1 \
    --pset pager=off
' < sql/validate_audit_events.sql
```

A consulta deve retornar eventos recentes de:

- criação ou validação da tabela `control_banvic.ingestion_audit`;
- validação quantitativa dos sete arquivos fonte;
- recriação do schema `raw_banvic`;
- início e término da execução do Meltano;
- validação das sete tabelas carregadas.

## 13. Acesso à interface do Airflow

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

## 14. Evidências de funcionamento

A versão `0.6.4` foi revalidada em **17 de setembro de 2026** a partir de um clone novo do repositório oficial, na branch `feature/banvic-second-chance-iac`.

As evidências observadas no gate técnico foram:

- build da imagem `banvic-airflow-meltano:0.6.4` concluído com `--no-cache` e `--pull`;
- cluster `banvic-local` provisionado pelo Terraform com `1` recurso adicionado, `0` alterados e `0` destruídos;
- node do Kind em estado `Ready`;
- imagem customizada carregada explicitamente no node do Kind;
- plataforma provisionada pelo launcher seguro com `9` recursos adicionados, `0` alterados e `0` destruídos;
- PostgreSQL analítico e componentes ativos do Airflow em `Running`;
- Jobs de migration e criação do usuário administrativo em `Complete`;
- PVC analítico `postgres-data` em `Bound`;
- DAG `banvic_meltano_ingestion` carregada e não pausada;
- primeira execução manual concluída com `success`;
- segunda execução manual concluída com `success`;
- contagens de origem e destino idênticas nas duas execuções;
- `18` eventos de auditoria registrados em `control_banvic.ingestion_audit` por execução;
- ausência de multiplicação de linhas após a segunda execução, confirmando o comportamento idempotente do full refresh;
- persistência do PostgreSQL confirmada após exclusão controlada do Pod e criação automática de um novo Pod pelo Deployment;
- dados e eventos de auditoria preservados após a substituição do Pod;
- `python3 scripts/deploy-platform.py plan` retornando `No changes`;
- `terraform -chdir=infra/terraform/cluster plan` retornando `No changes`.

Contagens validadas após as execuções:

| Tabela | Registros |
|---|---:|
| `agencias` | 10 |
| `clientes` | 998 |
| `colaborador_agencia` | 100 |
| `colaboradores` | 100 |
| `contas` | 999 |
| `propostas_credito` | 2.000 |
| `transacoes` | 71.999 |

Execuções de referência:

```text
manual__2026-09-17T22:29:32.915628+00:00
manual__2026-09-17T22:35:31.872995+00:00
```

Estado final de ambas:

```text
success
```

Essas evidências cobrem reprodutibilidade da infraestrutura, execução do pipeline, validação quantitativa, auditoria, idempotência e persistência do banco dentro do ciclo de vida do cluster local.

## 15. Decisões técnicas

### Infraestrutura como código

A infraestrutura foi separada em dois roots Terraform independentes:

- `infra/terraform/cluster`: provisiona o cluster Kind `banvic-local`;
- `infra/terraform/platform`: provisiona namespace, Secrets, PostgreSQL analítico e release Helm do Airflow.

A separação reduz o acoplamento entre o ciclo de vida do cluster e o da plataforma. O build da imagem Docker e o `kind load docker-image` permanecem fora do Terraform por serem etapas de empacotamento e distribuição local da imagem, não recursos declarativos do cluster. Não são utilizados `local-exec` ou comandos imperativos ocultos dentro do Terraform.

### PostgreSQL e persistência

O PostgreSQL foi escolhido como destino por ser adequado à escala da POC e possuir integração direta com Airflow e Meltano. O banco analítico utiliza o PVC `postgres-data`, permitindo que os dados sobrevivam à exclusão e recriação do Pod. O `local-path` do Kind protege contra reinícios do workload, mas não contra a exclusão completa do cluster.

### Meltano e reprodutibilidade

O Meltano separa a extração e a carga da orquestração. O `tap-csv` está fixado por commit Git e o `target-postgres` por versão publicada. O Dockerfile valida esses artefatos durante o build e interrompe a criação da imagem em caso de divergência.

### Airflow e auditoria

O Airflow orquestra dependências, retries, callbacks e validações. Os logs dos Pods utilizam `emptyDir`, evitando um PVC local de `100Gi` desnecessário. Os eventos operacionais relevantes permanecem persistidos em `control_banvic.ingestion_audit`.

### Kind

O Kind permite reproduzir localmente a implantação em Kubernetes sem depender de infraestrutura em nuvem. Ele é adequado para desenvolvimento, demonstração e avaliação técnica, mas não oferece a durabilidade ou a alta disponibilidade esperadas em produção.

### LocalExecutor

O `LocalExecutor` é suficiente para a escala da POC e evita a complexidade operacional de Celery e Redis. Como o scheduler também executa as tasks nesse modo, a persistência dos workers foi explicitamente desabilitada no Helm Chart.

### Full refresh idempotente

A recriação do schema `raw_banvic` simplifica a reprodução do snapshot e impede acúmulo de duplicidades entre execuções. Em produção, uma evolução mais segura seria carregar em schema ou tabelas de staging, validar o resultado e promover a nova versão por troca controlada.

### Fonte incorporada à imagem

Os CSVs são copiados para a imagem porque o desafio utiliza um snapshot conhecido e disponível antes da execução. Por isso, não foi necessário implementar `FileSensor`. Em produção, a fonte deveria ser externalizada para object storage, SFTP ou volume compartilhado, com sensor, evento ou mecanismo equivalente quando a chegada fosse assíncrona.

### Tipagem da camada RAW

A camada RAW preserva os valores da fonte majoritariamente como texto, evitando conversões indevidas de identificadores, documentos, contas e códigos. Uma camada refinada de produção deveria aplicar contratos, tipos de negócio, regras de qualidade e proteção de dados para consumo analítico.

### Segregação e tratamento seguro de Secrets

Cada componente recebe apenas as credenciais necessárias à sua função. O launcher `scripts/deploy-platform.py` concentra o fluxo de bootstrap e preservação das credenciais sem exibir valores sensíveis no terminal.

As variáveis Terraform que carregam segredos são marcadas como `sensitive` e `ephemeral`. No provedor Kubernetes, os Secrets utilizam atributos write-only, evitando a persistência dos valores em planos ou no state Terraform.

O launcher também valida a coerência do estado persistente antes de qualquer aplicação. Se o PVC analítico já existe mas as credenciais do PostgreSQL estão ausentes ou inconsistentes, o processo é interrompido para evitar a substituição acidental de credenciais de um banco já inicializado.

### Execução sob demanda da POC

A DAG `banvic_meltano_ingestion` utiliza `schedule=None`. Essa é uma decisão de escopo: o desafio utiliza um snapshot estático de arquivos CSV e a POC é executada sob demanda para demonstrar reprodutibilidade, idempotência, validação e auditoria.

Em um cenário de produção, a cadência não deve ser inferida desta POC. Ela deve ser definida a partir do SLA do processo de negócio, da frequência de disponibilização da fonte e dos requisitos de atualização dos consumidores.

### Escopo analítico

Não foram adicionados dbt, modelo dimensional ou dashboard porque o desafio concentra-se em ingestão, orquestração, armazenamento, segurança, idempotência e monitoramento. Esses componentes são evoluções possíveis, não dependências para demonstrar o objetivo da POC.

## 16. Limitações conhecidas

Esta solução é uma POC local. Portanto:

- não implementa carga incremental ou CDC;
- não possui alta disponibilidade;
- não utiliza um gerenciador externo de segredos;
- os logs do Airflow não estão configurados com persistência externa;
- os dados fonte são incorporados à imagem durante o build;
- as contagens fixas dos scripts SQL representam apenas o snapshot de aceitação do desafio;
- o PVC `local-path` preserva dados na recriação do Pod, mas não após a exclusão do cluster Kind;
- a exclusão do cluster Kind remove os recursos locais e exige novo provisionamento;
- o state do Terraform é mantido localmente e não utiliza backend remoto com locking, criptografia e controle de acesso;
- o ambiente depende dos recursos disponíveis no Docker Desktop e no WSL.

Em produção, a evolução natural incluiria armazenamento de objetos, observabilidade centralizada, logs persistentes, secret manager, CI/CD, políticas de rede, backup e estratégia incremental.

## 17. Repositório

```text
https://github.com/fabiodonizetibaptista/certificacao-data-engineer-banvic
```

## 18. Conclusão

A solução entrega uma POC funcional, reprodutível e auditável de engenharia de dados para o BanVic, cobrindo provisionamento declarativo da infraestrutura com Terraform, Kubernetes com Kind, orquestração com Airflow, ingestão com Meltano, armazenamento em PostgreSQL, tratamento seguro de Secrets, validação, idempotência, persistência e auditoria operacional.

O cumprimento do gate técnico a partir de um clone limpo demonstra que o ambiente pode ser provisionado a partir do código versionado, que duas execuções consecutivas do pipeline preservam as contagens esperadas e que os eventos de auditoria e os dados persistem após a substituição do Pod do PostgreSQL.

Do ponto de vista do negócio, a POC cria uma fundação centralizada, validada e rastreável sobre clientes, contas, transações, agências e crédito, permitindo que, em evoluções posteriores, a área Comercial e a Diretoria construam indicadores e análises confiáveis para apoiar retenção, atividade dos clientes e tomada de decisão.

O escopo atual não pretende ser uma plataforma analítica de produção, e sim demonstrar uma base técnica confiável, reprodutível e extensível para evoluções futuras.
