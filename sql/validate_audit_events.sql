SELECT
    run_id,
    task_id,
    status,
    table_name,
    row_count,
    created_at
FROM control_banvic.ingestion_audit
ORDER BY audit_id DESC
LIMIT 50;
