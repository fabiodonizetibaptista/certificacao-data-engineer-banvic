]633;E;sed -n '25,73p' README.md;64c61384-037b-42e8-94e9-44362a5ae207]633;C# Arquitetura da Solução BanVic

![Arquitetura da solução BanVic](images/banvic-data-architecture.png)

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

## Versões fixadas

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

## Fonte de dados

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

## Estratégia de ingestão

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

## Orquestração com Airflow

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

## Monitoramento, auditoria e falhas

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

## Segurança e gerenciamento de segredos

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

## Decisões técnicas

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

## Limitações conhecidas

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
