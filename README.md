# Certificação Data Engineer - BanVic

Este repositório contém a solução desenvolvida para o desafio **Certificação Data Engineer by Indicium**, com foco na construção de uma POC de infraestrutura e pipeline de ingestão de dados para o Banco Vitória S.A. (BanVic).

A solução implementa um ambiente local em Kubernetes com Apache Airflow, PostgreSQL e Meltano, permitindo a ingestão centralizada das 7 tabelas disponibilizadas para o desafio.

---

## 1. Objetivo do Projeto

O objetivo do projeto é construir uma infraestrutura local reprodutível para ingestão de dados do BanVic, permitindo que analistas consumam os dados em um ambiente centralizado.

A solução contempla:

* Provisionamento de infraestrutura local com Kubernetes.
* Execução do Apache Airflow em ambiente conteinerizado.
* Uso de PostgreSQL como destino analítico centralizado.
* Ingestão ELT com Meltano.
* Orquestração da carga via DAG no Airflow.
* Validação de arquivos de entrada antes da ingestão.
* Validação das tabelas carregadas no destino.
* Auditoria de execução do pipeline.
* Estratégia de idempotência e retries.
* Scripts SQL de apoio para validação da carga e auditoria.

---

## 2. Arquitetura da Solução

A arquitetura implementada está organizada em camadas, separando o fluxo principal de dados das camadas de suporte operacional:

![Arquitetura da solução BanVic](docs/images/banvic-data-architecture.png)

### Componentes principais

| Componente      | Papel na solução                                   |
| --------------- | -------------------------------------------------- |
| Kind/Kubernetes | Ambiente local de orquestração de containers       |
| Docker          | Build da imagem customizada do Airflow com Meltano |
| Apache Airflow  | Orquestração do pipeline de ingestão               |
| Meltano         | Execução do processo ELT                           |
| tap-csv         | Extração dos arquivos CSV do BanVic                |
| target-postgres | Carga dos dados no PostgreSQL                      |
| PostgreSQL      | Destino centralizado dos dados                     |
| SQL scripts     | Validação das tabelas carregadas e da auditoria    |

---

## 3. Estrutura do Projeto

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
├── infra/
│   ├── airflow/
│   │   └── Dockerfile
│   └── k8s/
│       ├── airflow-values.yaml
│       ├── namespace.yaml
│       ├── postgres-deployment.yaml
│       └── postgres-service.yaml
├── meltano_project/
│   ├── meltano.yml
│   ├── requirements.txt
│   └── plugins/
├── sql/
│   ├── validate_audit_events.sql
│   └── validate_raw_counts.sql
├── .dockerignore
├── .gitignore
└── README.md
```

---

## 4. Fonte de Dados

A fonte de dados utilizada no pipeline é composta por 7 arquivos CSV representando as tabelas iniciais do BanVic:

| Arquivo                   | Entidade                           |
| ------------------------- | ---------------------------------- |
| `agencias.csv`            | Agências                           |
| `clientes.csv`            | Clientes                           |
| `colaborador_agencia.csv` | Relacionamento colaborador-agência |
| `colaboradores.csv`       | Colaboradores                      |
| `contas.csv`              | Contas                             |
| `propostas_credito.csv`   | Propostas de crédito               |
| `transacoes.csv`          | Transações                         |

Esses arquivos ficam disponíveis no diretório:

```text
data/input/banvic_raw/
```

No container do Airflow, os arquivos são copiados para:

```text
/opt/airflow/data/input/banvic_raw/
```

---

## 5. Estratégia de Ingestão

A ingestão foi implementada com **Meltano**, utilizando:

* `tap-csv` como extractor.
* `target-postgres` como loader.

O arquivo principal de configuração é:

```text
meltano_project/meltano.yml
```

O pipeline realiza a carga das 7 entidades para o schema:

```text
raw_banvic
```

No PostgreSQL.

A estratégia adotada é de carga idempotente. Antes de cada execução, a DAG recria o schema `raw_banvic`, evitando duplicidade de registros e garantindo reprodutibilidade da carga.

---

## 6. Orquestração com Airflow

A DAG principal está em:

```text
dags/banvic_meltano_ingestion_dag.py
```

Nome da DAG:

```text
banvic_meltano_ingestion
```

### Tasks da DAG

A DAG possui as seguintes etapas:

| Task                                     | Descrição                                                          |
| ---------------------------------------- | ------------------------------------------------------------------ |
| `ensure_audit_table`                     | Cria/verifica a tabela de auditoria                                |
| `validate_source_files`                  | Valida a disponibilidade dos arquivos de entrada                   |
| `recreate_raw_schema`                    | Recria o schema `raw_banvic` para garantir idempotência            |
| `run_meltano_tap_csv_to_target_postgres` | Executa o pipeline Meltano                                         |
| `validate_loaded_tables`                 | Valida se as tabelas foram carregadas com as quantidades esperadas |

### Dependência das tasks

```text
ensure_audit_table
    >> validate_source_files
    >> recreate_raw_schema
    >> run_meltano_tap_csv_to_target_postgres
    >> validate_loaded_tables
```

---

## 7. Monitoramento, Auditoria e Tratamento de Falhas

A solução implementa auditoria em PostgreSQL por meio da tabela:

```text
control_banvic.ingestion_audit
```

Essa tabela registra eventos da execução da DAG, incluindo:

* `dag_id`
* `run_id`
* `task_id`
* `table_name`
* `status`
* `row_count`
* `message`
* `created_at`

Exemplos de status registrados:

| Status                           | Significado                            |
| -------------------------------- | -------------------------------------- |
| `success`                        | Etapa executada com sucesso            |
| `source_file_validated`          | Arquivo de entrada validado            |
| `source_file_validation_failed`  | Falha na validação de arquivo          |
| `loaded_table_validated`         | Tabela carregada e validada            |
| `loaded_table_validation_failed` | Falha na validação da tabela carregada |

A DAG também possui configuração de retries:

```python
"retries": 2
```

Dessa forma, em caso de falha transitória, o Airflow realiza novas tentativas antes de marcar a execução como falha.

---

## 8. Segurança e Gerenciamento de Segredos

As credenciais não são expostas diretamente no código da DAG.

Para esta POC local, algumas credenciais de demonstração podem existir em arquivos de configuração do ambiente Kubernetes/Airflow. Em um ambiente produtivo, esses valores devem ser substituídos por mecanismos apropriados de gerenciamento de segredos, como Kubernetes Secrets, Vault, Secret Manager ou serviço equivalente.

A configuração do `target-postgres` utiliza variáveis de ambiente no `meltano.yml`:

```yaml
host: ${TARGET_POSTGRES_HOST}
port: ${TARGET_POSTGRES_PORT}
database: ${TARGET_POSTGRES_DATABASE}
user: ${TARGET_POSTGRES_USER}
password: ${TARGET_POSTGRES_PASSWORD}
```

Arquivos locais de ambiente e estados internos de ferramenta são ignorados pelo Git:

```text
.env
*.env
meltano_project/.env
meltano_project/.meltano/
.venv/
```

---

## 9. Como Executar Localmente

### 9.1 Pré-requisitos

É necessário ter instalado:

* Docker
* Kind
* kubectl
* Helm
* Python 3
* Git
* WSL ou ambiente Linux equivalente

---

### 9.2 Criar o cluster Kind

```bash
kind create cluster --name banvic
```

Validar o cluster:

```bash
kubectl cluster-info --context kind-banvic
```

---

### 9.3 Criar o namespace

```bash
kubectl apply -f infra/k8s/namespace.yaml
```

---

### 9.4 Subir o PostgreSQL

```bash
kubectl apply -f infra/k8s/postgres-deployment.yaml
kubectl apply -f infra/k8s/postgres-service.yaml
```

Validar os pods:

```bash
kubectl get pods -n banvic
```

---

### 9.5 Construir a imagem customizada do Airflow

```bash
docker build -t banvic-airflow-meltano:0.4.0 -f infra/airflow/Dockerfile .
```

Carregar a imagem no cluster Kind:

```bash
kind load docker-image banvic-airflow-meltano:0.4.0 --name banvic
```

---

### 9.6 Instalar o Airflow via Helm

Adicionar o repositório Helm do Airflow:

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

Instalar ou atualizar o Airflow:

```bash
helm upgrade --install airflow apache-airflow/airflow \
  --namespace banvic \
  -f infra/k8s/airflow-values.yaml
```

Validar os pods:

```bash
kubectl get pods -n banvic
```

---

### 9.7 Validar importação da DAG

Obter o pod do scheduler:

```bash
SCHEDULER_POD=$(kubectl get pods -n banvic -l component=scheduler -o jsonpath='{.items[0].metadata.name}')
echo $SCHEDULER_POD
```

Validar erros de importação:

```bash
kubectl exec -n banvic -c scheduler $SCHEDULER_POD -- airflow dags list-import-errors
```

Resultado esperado:

```text
No data found
```

Listar a DAG:

```bash
kubectl exec -n banvic -c scheduler $SCHEDULER_POD -- airflow dags list | grep banvic_meltano_ingestion
```

---

### 9.8 Executar a DAG

```bash
kubectl exec -n banvic -c scheduler $SCHEDULER_POD -- airflow dags trigger banvic_meltano_ingestion
```

Consultar execuções:

```bash
kubectl exec -n banvic -c scheduler $SCHEDULER_POD -- airflow dags list-runs banvic_meltano_ingestion
```

Resultado esperado para a execução mais recente:

```text
state = success
```

---

## 10. Validação dos Dados Carregados

A validação das contagens finais pode ser executada com:

```bash
kubectl exec -i -n banvic deployment/postgres -- psql -U banvic_user -d banvic_dw < sql/validate_raw_counts.sql
```

Resultado esperado:

```text
       tabela        | registros 
---------------------+-----------
 agencias            |        10
 clientes            |       998
 colaborador_agencia |       100
 colaboradores       |       100
 contas              |       999
 propostas_credito   |      2000
 transacoes          |     71999
```

---

## 11. Validação da Auditoria

A consulta dos eventos de auditoria pode ser feita com:

```bash
kubectl exec -i -n banvic deployment/postgres -- psql -U banvic_user -d banvic_dw < sql/validate_audit_events.sql
```

Essa consulta retorna os eventos mais recentes registrados na tabela:

```text
control_banvic.ingestion_audit
```

A execução bem-sucedida registra eventos de:

* validação da tabela de auditoria;
* validação dos arquivos fonte;
* recriação do schema raw;
* validação das tabelas carregadas.

---

## 12. Acesso à Interface do Airflow

Para acessar a interface do Airflow localmente:

```bash
kubectl port-forward svc/airflow-api-server 8080:8080 --namespace banvic
```

Depois, acessar no navegador:

```text
http://localhost:8080
```

Na interface, é possível acompanhar a DAG:

```text
banvic_meltano_ingestion
```

E verificar visualmente:

* status da execução;
* dependências entre tasks;
* retries;
* logs;
* tempo de execução;
* histórico de runs.

---

## 13. Evidências de Funcionamento

A solução foi validada considerando:

* criação e execução do ambiente Kubernetes local;
* build da imagem customizada do Airflow;
* execução da DAG no Airflow;
* validação da importação da DAG sem erros;
* execução do Meltano pelo Airflow;
* carga das 7 tabelas no PostgreSQL;
* validação das contagens finais;
* registro dos eventos na tabela de auditoria;
* configuração de retries;
* execução idempotente por recriação do schema `raw_banvic`.

---

## 14. Decisões Técnicas

### Uso de PostgreSQL

O PostgreSQL foi escolhido como destino por representar um Data Warehouse local simples, adequado para uma POC de centralização de dados.

### Uso de Meltano

O Meltano foi escolhido para separar claramente o processo de extração e carga da orquestração. A configuração declarativa em `meltano.yml` facilita manutenção e evolução do pipeline.

### Uso de Airflow

O Airflow foi utilizado para orquestrar o processo, definir dependências, controlar retries e permitir monitoramento visual da execução.

### Uso de Kubernetes local com Kind

O Kind foi utilizado para simular um ambiente conteinerizado e reprodutível, atendendo ao requisito de execução local com Kubernetes.

### Estratégia de idempotência

A DAG recria o schema `raw_banvic` antes da carga. Com isso, múltiplas execuções do pipeline produzem o mesmo resultado final, sem duplicidade de registros.

### Estratégia de auditoria

A tabela `control_banvic.ingestion_audit` permite rastrear a execução do pipeline por DAG run, task, tabela e status, apoiando monitoramento e troubleshooting.

---

## 15. Repositório

Repositório do projeto:

```text
https://github.com/fabio-baptista/certificacao-data-engineer
```

---

## 16. Roteiro Sugerido para o Vídeo

Para a apresentação final de 3 a 5 minutos, recomenda-se seguir este roteiro:

1. Apresentar rapidamente o objetivo do desafio.
2. Mostrar a arquitetura no README.
3. Mostrar a estrutura de pastas do projeto.
4. Explicar a DAG `banvic_meltano_ingestion`.
5. Mostrar o ambiente Kubernetes com `kubectl get pods -n banvic`.
6. Abrir a interface do Airflow e mostrar a DAG executada com sucesso.
7. Executar ou mostrar a validação das contagens em `raw_banvic`.
8. Mostrar a tabela de auditoria com os eventos da execução.
9. Finalizar explicando idempotência, retries e segurança de credenciais.

---

## 17. Conclusão

A solução entrega uma POC funcional de engenharia de dados para o BanVic, cobrindo infraestrutura local com Kubernetes, orquestração com Airflow, ingestão com Meltano, armazenamento em PostgreSQL, validação de dados e auditoria operacional.

O pipeline implementado permite centralizar as tabelas iniciais do BanVic em um ambiente estruturado, reprodutível e monitorável, servindo como base para futuras camadas analíticas e consumo por ferramentas de BI.
