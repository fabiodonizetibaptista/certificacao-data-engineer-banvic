from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


DATA_DIR = Path("/opt/airflow/data/input/banvic_synthetic")
RAW_SCHEMA_NAME = "raw_banvic"
CONTROL_SCHEMA_NAME = "control_banvic"
AUDIT_TABLE_NAME = "ingestion_audit"

EXPECTED_FILES = {
    "agencias": {
        "path": DATA_DIR / "agencias.csv",
        "expected_rows": 5,
    },
    "clientes": {
        "path": DATA_DIR / "clientes.csv",
        "expected_rows": 6,
    },
    "colaborador_agencia": {
        "path": DATA_DIR / "colaborador_agencia.csv",
        "expected_rows": 7,
    },
    "colaboradores": {
        "path": DATA_DIR / "colaboradores.csv",
        "expected_rows": 5,
    },
    "contas": {
        "path": DATA_DIR / "contas.csv",
        "expected_rows": 6,
    },
    "propostas_credito": {
        "path": DATA_DIR / "propostas_credito.csv",
        "expected_rows": 6,
    },
    "transacoes": {
        "path": DATA_DIR / "transacoes.csv",
        "expected_rows": 8,
    },
}


def get_postgres_hook() -> PostgresHook:
    """
    Centraliza a criação do hook do PostgreSQL.

    A connection 'banvic_dw' foi configurada no Airflow para apontar
    para o serviço PostgreSQL dentro do namespace Kubernetes.
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
    """
    Registra um evento de auditoria da ingestão.

    A tabela de auditoria permite rastrear:
    - qual DAG executou;
    - qual run executou;
    - qual task registrou o evento;
    - qual tabela foi afetada;
    - qual foi o status;
    - quantos registros foram carregados/validados;
    - mensagem operacional de apoio.
    """
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


def ensure_audit_table(dag_id: str, run_id: str, task_id: str) -> None:
    """
    Cria o schema e a tabela de auditoria, caso ainda não existam.
    """
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

    print(f"Tabela de auditoria disponível: {CONTROL_SCHEMA_NAME}.{AUDIT_TABLE_NAME}")


def validate_source_files(dag_id: str, run_id: str, task_id: str) -> None:
    """
    Valida a disponibilidade dos arquivos de entrada antes da execução do Meltano.

    Esta etapa cumpre o papel de checagem/sensor lógico:
    se algum arquivo esperado não existir ou estiver vazio, a DAG falha antes da carga.
    """
    for table_name, metadata in EXPECTED_FILES.items():
        file_path = metadata["path"]

        try:
            if not file_path.exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

            if file_path.stat().st_size == 0:
                raise ValueError(f"Arquivo vazio: {file_path}")

            write_audit_event(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                status="source_file_validated",
                table_name=table_name,
                row_count=None,
                message=f"Arquivo validado: {file_path}",
            )

            print(f"Arquivo validado para {table_name}: {file_path}")

        except Exception as exception:
            write_audit_event(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                status="source_file_validation_failed",
                table_name=table_name,
                row_count=None,
                message=str(exception),
            )

            raise


def recreate_raw_schema(dag_id: str, run_id: str, task_id: str) -> None:
    """
    Recria o schema raw_banvic antes da carga.

    Esta estratégia garante idempotência:
    a DAG pode ser executada várias vezes sem duplicar dados.
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
            message=str(exception),
        )

        raise


def validate_loaded_tables(dag_id: str, run_id: str, task_id: str) -> None:
    """
    Valida se as 7 tabelas foram carregadas com as quantidades esperadas.
    Também registra uma linha de auditoria por tabela validada.
    """
    hook = get_postgres_hook()

    for table_name, metadata in EXPECTED_FILES.items():
        expected_rows = metadata["expected_rows"]

        try:
            sql = f'SELECT COUNT(*) FROM "{RAW_SCHEMA_NAME}"."{table_name}";'
            row_count = hook.get_first(sql)[0]

            if row_count != expected_rows:
                raise ValueError(
                    f"Quantidade inesperada para {RAW_SCHEMA_NAME}.{table_name}. "
                    f"Esperado={expected_rows}, recebido={row_count}"
                )

            write_audit_event(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                status="loaded_table_validated",
                table_name=table_name,
                row_count=row_count,
                message=f"Tabela validada com sucesso. Esperado={expected_rows}, recebido={row_count}.",
            )

            print(f"Tabela validada: {RAW_SCHEMA_NAME}.{table_name} | registros={row_count}")

        except Exception as exception:
            write_audit_event(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                status="loaded_table_validation_failed",
                table_name=table_name,
                row_count=None,
                message=str(exception),
            )

            raise


with DAG(
    dag_id="banvic_meltano_ingestion",
    description="Orquestra pipeline Meltano tap-csv -> target-postgres para ingestão das 7 entidades BanVic",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["banvic", "meltano", "tap-csv", "target-postgres", "synthetic-data", "audit"],
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
        bash_command="cd /opt/airflow/meltano_project && meltano run tap-csv target-postgres",
    )

    validate_load = PythonOperator(
        task_id="validate_loaded_tables",
        python_callable=validate_loaded_tables,
        op_kwargs={
            "dag_id": "{{ dag.dag_id }}",
            "run_id": "{{ run_id }}",
            "task_id": "validate_loaded_tables",
        },
    )

    ensure_audit >> validate_files >> recreate_schema >> run_meltano >> validate_load
