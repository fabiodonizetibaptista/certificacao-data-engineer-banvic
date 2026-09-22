]633;E;sed -n '575,634p' README.md;64c61384-037b-42e8-94e9-44362a5ae207]633;C# Validação Técnica — BanVic

## Validação dos dados carregados

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

## Validação da auditoria

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
- validação dos sete arquivos fonte, incluindo existência, conteúdo, cabeçalho, estrutura, chaves obrigatórias, duplicidades e contagem;
- recriação do schema `raw_banvic`;
- início e término da execução do Meltano;
- validação das sete tabelas carregadas.

## Evidências de funcionamento

A versão `0.6.4` foi revalidada em **17 de setembro de 2026** a partir de um clone novo do repositório oficial.

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
