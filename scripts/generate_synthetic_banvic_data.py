from pathlib import Path
import csv

OUTPUT_DIR = Path("data/input/banvic_synthetic")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def write_csv(filename: str, header: list[str], rows: list[list]) -> None:
    path = OUTPUT_DIR / filename

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)

    print(f"Arquivo criado: {path} ({len(rows)} registros)")


write_csv(
    "agencias.csv",
    [
        "cod_agencia",
        "nome",
        "endereco",
        "cidade",
        "uf",
        "data_abertura",
        "tipo_agencia",
    ],
    [
        [1, "Agência Paulista", "Av Paulista 1000", "São Paulo", "SP", "2010-01-01", "Fisica"],
        [2, "Agência Campinas", "Rua Centro 200", "Campinas", "SP", "2012-03-15", "Fisica"],
        [3, "Agência Porto Alegre", "Av Ipiranga 300", "Porto Alegre", "RS", "2015-06-10", "Fisica"],
        [4, "Agência Digital", "Online", "São Paulo", "SP", "2020-11-01", "Digital"],
        [5, "Agência Curitiba", "Rua XV 500", "Curitiba", "PR", "2018-09-20", "Fisica"],
    ],
)

write_csv(
    "clientes.csv",
    [
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
    ],
    [
        [101, "Mariana", "Silva", "mariana.silva@email.com", "PF", "2018-01-10", 12345678901, "1989-04-20", "Rua A 10", 11000000],
        [102, "Carlos", "Souza", "carlos.souza@email.com", "PF", "2019-05-21", 23456789012, "1978-08-11", "Rua B 20", 22000000],
        [103, "Empresa Alfa", "Ltda", "contato@alfa.com", "PJ", "2020-02-03", 34567890000199, "2005-01-01", "Av Comercial 300", 33000000],
        [104, "Fernanda", "Lima", "fernanda.lima@email.com", "PF", "2021-07-14", 45678901234, "1992-12-01", "Rua C 40", 44000000],
        [105, "João", "Pereira", "joao.pereira@email.com", "PF", "2022-10-05", 56789012345, "1985-03-30", "Rua D 50", 55000000],
        [106, "Patricia", "Oliveira", "patricia.oliveira@email.com", "PF", "2022-12-18", 67890123456, "1995-09-15", "Rua E 60", 66000000],
    ],
)

write_csv(
    "colaboradores.csv",
    [
        "cod_colaborador",
        "primeiro_nome",
        "ultimo_nome",
        "email",
        "cpf",
        "data_nascimento",
        "endenreco",
        "cep",
    ],
    [
        [201, "Ana", "Costa", "ana.costa@banvic.com", 11122233344, "1982-02-12", "Rua Banco 1", 10000000],
        [202, "Bruno", "Martins", "bruno.martins@banvic.com", 22233344455, "1987-06-25", "Rua Banco 2", 20000000],
        [203, "Camila", "Diniz", "camila.diniz@banvic.com", 33344455566, "1980-09-09", "Rua Banco 3", 30000000],
        [204, "Lucas", "Johnson", "lucas.johnson@banvic.com", 44455566677, "1993-11-22", "Rua Banco 4", 40000000],
        [205, "André", "Tech", "andre.tech@banvic.com", 55566677788, "1979-01-18", "Rua Banco 5", 50000000],
    ],
)

write_csv(
    "colaborador_agencia.csv",
    [
        "cod_colaborador",
        "cod_agencia",
    ],
    [
        [201, 1],
        [202, 2],
        [203, 4],
        [204, 4],
        [205, 1],
        [201, 3],
        [202, 5],
    ],
)

write_csv(
    "contas.csv",
    [
        "num_conta",
        "cod_cliente",
        "cod_agencia",
        "cod_colaborador",
        "data_abertura",
        "tipo_conta",
        "saldo",
        "data_ultimo_lancamento",
    ],
    [
        [10001, 101, 1, 201, "2018-01-12 10:15:00", 1, 2500.50, "2023-01-10 12:00:00"],
        [10002, 102, 2, 202, "2019-05-25 09:30:00", 1, 870.20, "2022-12-20 08:10:00"],
        [10003, 103, 4, 203, "2020-03-01 14:00:00", 2, 15200.00, "2023-01-15 11:45:00"],
        [10004, 104, 4, 204, "2021-07-20 16:20:00", 1, 320.75, "2022-11-30 18:05:00"],
        [10005, 105, 5, 202, "2022-10-08 13:50:00", 1, 4300.10, "2023-01-05 09:40:00"],
        [10006, 106, 3, 201, "2022-12-22 15:10:00", 1, 980.00, "2023-01-12 17:25:00"],
    ],
)

write_csv(
    "transacoes.csv",
    [
        "cod_transacao",
        "num_conta",
        "data_transacao",
        "nome_transacao",
        "valor_transacao",
    ],
    [
        [5001, 10001, "2023-01-01 10:00:00", "Deposito", 1000.00],
        [5002, 10001, "2023-01-03 12:30:00", "PIX", -150.00],
        [5003, 10002, "2022-12-20 08:10:00", "Saque", -200.00],
        [5004, 10003, "2023-01-15 11:45:00", "TED", 5000.00],
        [5005, 10004, "2022-11-30 18:05:00", "PIX", -75.50],
        [5006, 10005, "2023-01-05 09:40:00", "Deposito", 2200.00],
        [5007, 10006, "2023-01-12 17:25:00", "Transferencia", -300.00],
        [5008, 10003, "2022-10-10 10:10:00", "Pagamento", -980.75],
    ],
)

write_csv(
    "propostas_credito.csv",
    [
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
    ],
    [
        [9001, 101, 201, "2022-01-10 09:00:00", 1.20, 10000.00, 8000.00, 2000.00, 750.00, 12, 0, "Aprovada"],
        [9002, 102, 202, "2022-03-15 10:30:00", 1.80, 5000.00, 5000.00, 0.00, 480.00, 12, 1, "Aprovada"],
        [9003, 103, 203, "2022-05-20 14:45:00", 2.10, 50000.00, 40000.00, 10000.00, 3900.00, 12, 2, "Em análise"],
        [9004, 104, 204, "2022-08-01 16:00:00", 2.50, 3000.00, 3000.00, 0.00, 310.00, 10, 0, "Recusada"],
        [9005, 105, 202, "2022-11-12 11:20:00", 1.40, 15000.00, 12000.00, 3000.00, 1150.00, 12, 0, "Aprovada"],
        [9006, 106, 201, "2023-01-05 15:35:00", 1.95, 8000.00, 7000.00, 1000.00, 690.00, 12, 1, "Em análise"],
    ],
)

print("\nMassa sintética gerada com sucesso.")
print(f"Diretório: {OUTPUT_DIR}")