from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


DATA_DIR = Path("/opt/airflow/data/input/banvic_raw")
RAW_SCHEMA_NAME = "raw_banvic"
CONTROL_SCHEMA_NAME = "control_banvic"
AUDIT_TABLE_NAME = "ingestion_audit"

# Contrato estrutural das entidades fornecidas no banvic_data.zip.
#
# A DAG não fixa a quantidade de linhas. A contagem real dos CSVs é calculada
# em cada execução e comparada com o destino depois da carga.
TABLE_CONFIG = {
    "agencias": {
        "filename": "agencias.csv",
        "columns": (
            "cod_agencia",
            "nome",
            "endereco",
            "cidade",
            "uf",
            "data_abertura",
            "tipo_agencia",
        ),
        "key_columns": ("cod_agencia",),
    },
    "clientes": {
        "filename": "clientes.csv",
        "columns": (
            "cod_cliente",
            "primeiro_nome",
            "ultimo_nome",
            "email",
            "tipo_cliente",
            "data_inclusao",
            "cpfcnpj",
            "data_nascimento",
            "endereco",
            "cep",
        ),
        "key_columns": ("cod_cliente",),
    },
    "colaborador_agencia": {
        "filename": "colaborador_agencia.csv",
        "columns": (
            "cod_colaborador",
            "cod_agencia",
        ),
        "key_columns": (
            "cod_colaborador",
            "cod_agencia",
        ),
    },
    "colaboradores": {
        "filename": "colaboradores.csv",
        "columns": (
            "cod_colaborador",
            "primeiro_nome",
            "ultimo_nome",
            "email",
            "cpf",
            "data_nascimento",
            "endereco",
            "cep",
        ),
        "key_columns": ("cod_colaborador",),
    },
    "contas": {
        "filename": "contas.csv",
        "columns": (
            "num_conta",
            "cod_cliente",
            "cod_agencia",
            "cod_colaborador",
            "tipo_conta",
            "data_abertura",
            "saldo_total",
            "saldo_disponivel",
            "data_ultimo_lancamento",
        ),
        "key_columns": ("num_conta",),
    },
    "propostas_credito": {
        "filename": "propostas_credito.csv",
        "columns": (
            "cod_proposta",
            "cod_cliente",
            "cod_colaborador",
            "data_entrada_proposta",
            "taxa_juros_mensal",
            "valor_proposta",
            "valor_financiamento",
            "valor_entrada",
            "valor_prestacao",
            "quantidade_parcelas",
            "carencia",
            "status_proposta",
        ),
        "key_columns": ("cod_proposta",),
    },
    "transacoes": {
        "filename": "transacoes.csv",
        "columns": (
            "cod_transacao",
            "num_conta",
            "data_transacao",
            "nome_transacao",
            "valor_transacao",
        ),
        "key_columns": ("cod_transacao",),
    },
}


def quote_identifier(identifier: str) -> str:
    """Escapa identificadores SQL definidos no contrato estático da DAG."""
    return '"' + identifier.replace('"', '""') + '"'


def get_postgres_hook() -> PostgresHook:
    """
    Centraliza o acesso ao PostgreSQL analítico.

    A conexão ``banvic_dw`` é injetada no runtime pelo Kubernetes Secret
    ``airflow-runtime-secret``. Nenhuma credencial fica versionada na DAG.
    """
    return PostgresHook(postgres_conn_id="banvic_dw")


def write_audit_event(
    dag_id: str,
    run_id: str,
    task_id: str,
    status: str,
    table_name: str | None = None,
    row_count: int | None = None,
    message: str | None = None,
) -> None:
    """Registra um evento em ``control_banvic.ingestion_audit``."""
    hook = get_postgres_hook()

    sql = f"""
        INSERT INTO {CONTROL_SCHEMA_NAME}.{AUDIT_TABLE_NAME}
        (
            dag_id,
            run_id,
            task_id,
            table_name,
            status,
            row_count,
            message,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
    """

    hook.run(
        sql,
        parameters=(
            dag_id,
            run_id,
            task_id,
            table_name,
            status,
            row_count,
            message,
        ),
    )


def write_meltano_audit_event(
    context: dict,
    status: str,
    message: str,
) -> None:
    """
    Registra eventos da execução do Meltano sem persistir argumentos,
    credenciais ou mensagens brutas potencialmente sensíveis.
    """
    task_instance = context["task_instance"]

    write_audit_event(
        dag_id=task_instance.dag_id,
        run_id=task_instance.run_id,
        task_id=task_instance.task_id,
        status=status,
        table_name=None,
        row_count=None,
        message=message,
    )


def audit_meltano_started(context: dict) -> None:
    """Registra o início de uma tentativa da task Meltano."""
    task_instance = context["task_instance"]

    write_meltano_audit_event(
        context=context,
        status="meltano_started",
        message=(
            "Execução do pipeline Meltano iniciada. "
            f"Tentativa={task_instance.try_number}."
        ),
    )


def audit_meltano_retry(context: dict) -> None:
    """Registra que a task Meltano será executada novamente."""
    task_instance = context["task_instance"]

    write_meltano_audit_event(
        context=context,
        status="meltano_retrying",
        message=(
            "Execução do pipeline Meltano será repetida. "
            f"Tentativa={task_instance.try_number}."
        ),
    )


def audit_meltano_succeeded(context: dict) -> None:
    """Registra a conclusão bem-sucedida da task Meltano."""
    task_instance = context["task_instance"]

    write_meltano_audit_event(
        context=context,
        status="meltano_succeeded",
        message=(
            "Pipeline Meltano concluído com sucesso. "
            f"Tentativa={task_instance.try_number}."
        ),
    )


def audit_meltano_failed(context: dict) -> None:
    """Registra a falha final do Meltano sem salvar a exceção bruta."""
    task_instance = context["task_instance"]
    exception = context.get("exception")

    exception_type = (
        type(exception).__name__
        if exception is not None
        else "UnknownError"
    )

    write_meltano_audit_event(
        context=context,
        status="meltano_failed",
        message=(
            "Pipeline Meltano finalizado com falha. "
            f"Tentativa={task_instance.try_number}; "
            f"TipoErro={exception_type}."
        ),
    )


def ensure_audit_table(dag_id: str, run_id: str, task_id: str) -> None:
    """Cria o schema e a tabela de auditoria quando necessário."""
    hook = get_postgres_hook()

    hook.run(f"CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA_NAME};")

    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA_NAME}.{AUDIT_TABLE_NAME} (
            audit_id BIGSERIAL PRIMARY KEY,
            dag_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            table_name TEXT NULL,
            status TEXT NOT NULL,
            row_count INTEGER NULL,
            message TEXT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    write_audit_event(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        status="success",
        table_name=None,
        row_count=None,
        message="Tabela de auditoria verificada/criada com sucesso.",
    )

    print(
        "Tabela de auditoria disponível: "
        f"{CONTROL_SCHEMA_NAME}.{AUDIT_TABLE_NAME}"
    )


def validate_csv_file(
    table_name: str,
    file_path: Path,
    expected_columns: tuple[str, ...],
    key_columns: tuple[str, ...],
) -> int:
    """
    Valida estrutura e integridade básica de um CSV.

    Controles aplicados:
    - arquivo existente e não vazio;
    - cabeçalho exato;
    - quantidade de colunas consistente;
    - pelo menos um registro;
    - chaves obrigatórias preenchidas;
    - ausência de chaves duplicadas.

    Valores de negócio e chaves não são escritos nos logs.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    if file_path.stat().st_size == 0:
        raise ValueError(f"Arquivo vazio: {file_path}")

    with file_path.open(
        mode="r",
        encoding="utf-8-sig",
        newline="",
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        actual_columns = tuple(reader.fieldnames or ())

        if actual_columns != expected_columns:
            raise ValueError(
                f"Cabeçalho divergente para {table_name}. "
                f"Esperado={expected_columns}; recebido={actual_columns}."
            )

        seen_keys: set[tuple[str, ...]] = set()
        row_count = 0

        for line_number, row in enumerate(reader, start=2):
            # DictReader usa a chave None quando há colunas extras e valores
            # None quando faltam colunas.
            if None in row or any(value is None for value in row.values()):
                raise ValueError(
                    f"Estrutura de colunas inválida em {table_name}, "
                    f"linha={line_number}."
                )

            normalized_values = {
                column: value.strip()
                for column, value in row.items()
            }

            if not any(normalized_values.values()):
                continue

            key = tuple(
                normalized_values[column]
                for column in key_columns
            )

            if any(not value for value in key):
                raise ValueError(
                    f"Chave obrigatória vazia em {table_name}, "
                    f"linha={line_number}."
                )

            if key in seen_keys:
                raise ValueError(
                    f"Chave duplicada em {table_name}, "
                    f"linha={line_number}."
                )

            seen_keys.add(key)
            row_count += 1

    if row_count == 0:
        raise ValueError(f"Arquivo sem registros de dados: {file_path}")

    return row_count


def validate_source_files(
    dag_id: str,
    run_id: str,
    task_id: str,
) -> dict[str, int]:
    """
    Valida os sete CSVs e retorna suas contagens reais.

    O dicionário retornado é pequeno e é usado pela task final para conferir
    a paridade entre origem e destino na mesma Dag Run.
    """
    source_row_counts: dict[str, int] = {}

    for table_name, metadata in TABLE_CONFIG.items():
        file_path = DATA_DIR / metadata["filename"]
        row_count = None

        try:
            row_count = validate_csv_file(
                table_name=table_name,
                file_path=file_path,
                expected_columns=metadata["columns"],
                key_columns=metadata["key_columns"],
            )

            source_row_counts[table_name] = row_count

            write_audit_event(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                status="source_file_validated",
                table_name=table_name,
                row_count=row_count,
                message=(
                    "Arquivo fonte validado com sucesso. "
                    f"Registros={row_count}."
                ),
            )

            print(
                f"Arquivo fonte validado: {file_path} "
                f"| registros={row_count}"
            )

        except Exception as exception:
            write_audit_event(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                status="source_file_validation_failed",
                table_name=table_name,
                row_count=row_count,
                message=(
                    "Validação do arquivo fonte concluída com falha. "
                    f"TipoErro={type(exception).__name__}."
                ),
            )
            raise

    return source_row_counts


def recreate_raw_schema(dag_id: str, run_id: str, task_id: str) -> None:
    """
    Recria ``raw_banvic`` antes da carga.

    A origem é validada integralmente antes desta task. Assim, um arquivo
    inválido não remove o último estado válido do destino.
    """
    hook = get_postgres_hook()

    try:
        hook.run(f"DROP SCHEMA IF EXISTS {RAW_SCHEMA_NAME} CASCADE;")
        hook.run(f"CREATE SCHEMA {RAW_SCHEMA_NAME};")

        write_audit_event(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            status="success",
            table_name=None,
            row_count=None,
            message=f"Schema recriado com sucesso: {RAW_SCHEMA_NAME}",
        )

        print(f"Schema recriado com sucesso: {RAW_SCHEMA_NAME}")

    except Exception as exception:
        write_audit_event(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            status="failed",
            table_name=None,
            row_count=None,
            message=(
                "Recriação do schema RAW concluída com falha. "
                f"TipoErro={type(exception).__name__}."
            ),
        )
        raise


def validate_loaded_tables(
    dag_id: str,
    run_id: str,
    task_id: str,
    source_row_counts: dict[str, int],
) -> None:
    """
    Valida paridade de linhas e integridade das chaves no PostgreSQL.

    O destino deve reproduzir a contagem real dos CSVs processados na mesma
    execução, sem depender de números hardcoded.
    """
    if not isinstance(source_row_counts, dict):
        raise TypeError(
            "As contagens da origem não foram recebidas como dicionário."
        )

    expected_tables = set(TABLE_CONFIG)
    received_tables = set(source_row_counts)

    if received_tables != expected_tables:
        missing = sorted(expected_tables - received_tables)
        unexpected = sorted(received_tables - expected_tables)

        raise ValueError(
            "Mapa de contagens da origem inválido. "
            f"Ausentes={missing}; inesperadas={unexpected}."
        )

    hook = get_postgres_hook()

    for table_name, metadata in TABLE_CONFIG.items():
        expected_rows = int(source_row_counts[table_name])
        row_count = None

        schema_sql = quote_identifier(RAW_SCHEMA_NAME)
        table_sql = quote_identifier(table_name)
        qualified_table = f"{schema_sql}.{table_sql}"

        key_columns_sql = [
            quote_identifier(column)
            for column in metadata["key_columns"]
        ]

        try:
            row_count = int(
                hook.get_first(
                    f"SELECT COUNT(*) FROM {qualified_table};"
                )[0]
            )

            if row_count != expected_rows:
                raise ValueError(
                    f"Paridade inválida para {RAW_SCHEMA_NAME}.{table_name}. "
                    f"Origem={expected_rows}; destino={row_count}."
                )

            null_conditions = " OR ".join(
                (
                    f"{column} IS NULL "
                    f"OR BTRIM(CAST({column} AS TEXT)) = ''"
                )
                for column in key_columns_sql
            )

            invalid_key_count = int(
                hook.get_first(
                    f"""
                    SELECT COUNT(*)
                    FROM {qualified_table}
                    WHERE {null_conditions};
                    """
                )[0]
            )

            if invalid_key_count != 0:
                raise ValueError(
                    f"Chaves obrigatórias inválidas em "
                    f"{RAW_SCHEMA_NAME}.{table_name}. "
                    f"Registros={invalid_key_count}."
                )

            group_by_columns = ", ".join(key_columns_sql)

            duplicate_group_count = int(
                hook.get_first(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT {group_by_columns}
                        FROM {qualified_table}
                        GROUP BY {group_by_columns}
                        HAVING COUNT(*) > 1
                    ) AS duplicate_keys;
                    """
                )[0]
            )

            if duplicate_group_count != 0:
                raise ValueError(
                    f"Chaves duplicadas em "
                    f"{RAW_SCHEMA_NAME}.{table_name}. "
                    f"Grupos={duplicate_group_count}."
                )

            write_audit_event(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                status="loaded_table_validated",
                table_name=table_name,
                row_count=row_count,
                message=(
                    "Tabela validada com sucesso. "
                    f"Origem={expected_rows}; destino={row_count}; "
                    "chaves_invalidas=0; grupos_duplicados=0."
                ),
            )

            print(
                f"Tabela validada: {RAW_SCHEMA_NAME}.{table_name} "
                f"| origem={expected_rows} "
                f"| destino={row_count}"
            )

        except Exception as exception:
            write_audit_event(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                status="loaded_table_validation_failed",
                table_name=table_name,
                row_count=row_count,
                message=(
                    "Validação da tabela carregada concluída com falha. "
                    f"TipoErro={type(exception).__name__}."
                ),
            )
            raise


with DAG(
    dag_id="banvic_meltano_ingestion",
    description=(
        "Orquestra tap-csv -> target-postgres para as sete entidades BanVic, "
        "com qualidade, auditoria e idempotência."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    # Evita intervenção manual após uma instalação limpa.
    is_paused_upon_creation=False,
    tags=[
        "banvic",
        "meltano",
        "tap-csv",
        "target-postgres",
        "raw-data",
        "audit",
        "data-quality",
    ],
    default_args={
        "owner": "banvic-data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
) as dag:
    ensure_audit = PythonOperator(
        task_id="ensure_audit_table",
        python_callable=ensure_audit_table,
        op_kwargs={
            "dag_id": "{{ dag.dag_id }}",
            "run_id": "{{ run_id }}",
            "task_id": "ensure_audit_table",
        },
    )

    validate_files = PythonOperator(
        task_id="validate_source_files",
        python_callable=validate_source_files,
        op_kwargs={
            "dag_id": "{{ dag.dag_id }}",
            "run_id": "{{ run_id }}",
            "task_id": "validate_source_files",
        },
    )

    recreate_schema = PythonOperator(
        task_id="recreate_raw_schema",
        python_callable=recreate_raw_schema,
        op_kwargs={
            "dag_id": "{{ dag.dag_id }}",
            "run_id": "{{ run_id }}",
            "task_id": "recreate_raw_schema",
        },
    )

    run_meltano = BashOperator(
        task_id="run_meltano_tap_csv_to_target_postgres",
        bash_command=(
            "cd /opt/airflow/meltano_project "
            "&& meltano run tap-csv target-postgres"
        ),
        on_execute_callback=audit_meltano_started,
        on_retry_callback=audit_meltano_retry,
        on_success_callback=audit_meltano_succeeded,
        on_failure_callback=audit_meltano_failed,
    )

    validate_load = PythonOperator(
        task_id="validate_loaded_tables",
        python_callable=validate_loaded_tables,
        op_kwargs={
            "dag_id": "{{ dag.dag_id }}",
            "run_id": "{{ run_id }}",
            "task_id": "validate_loaded_tables",
            "source_row_counts": validate_files.output,
        },
    )

    ensure_audit >> validate_files >> recreate_schema >> run_meltano >> validate_load
