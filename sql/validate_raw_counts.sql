SELECT 'agencias' AS tabela, COUNT(*) AS registros FROM raw_banvic.agencias
UNION ALL
SELECT 'clientes', COUNT(*) FROM raw_banvic.clientes
UNION ALL
SELECT 'colaborador_agencia', COUNT(*) FROM raw_banvic.colaborador_agencia
UNION ALL
SELECT 'colaboradores', COUNT(*) FROM raw_banvic.colaboradores
UNION ALL
SELECT 'contas', COUNT(*) FROM raw_banvic.contas
UNION ALL
SELECT 'propostas_credito', COUNT(*) FROM raw_banvic.propostas_credito
UNION ALL
SELECT 'transacoes', COUNT(*) FROM raw_banvic.transacoes
ORDER BY tabela;
