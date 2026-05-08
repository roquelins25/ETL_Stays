# ETL Stays — Lenon Collect

Pipeline de extração, transformação e carga (ETL) de dados financeiros da plataforma **Stays**, com armazenamento em banco de dados PostgreSQL.

---

## Visão Geral

A aplicação conecta à API da Stays, extrai dados de proprietários e transações financeiras, e os carrega em um banco de dados PostgreSQL. O pipeline é executado diariamente e respeita o conceito de **temporada** para controle dos dados.

---

## Estrutura do Projeto

```
lenon_collect/
├── main.py                          # Ponto de entrada — orquestra os pipelines
├── config/
│   ├── configDB.py                  # Conexão com o PostgreSQL via variáveis de ambiente
│   └── CRUD.py                      # Operações de banco: insert, insert_many, update, upsert
└── src/
    └── 01.collect/
        ├── db_owers_extract.py      # Extração dos proprietários (tabela owners)
        └── db_finance.py            # Extração das transações financeiras (tabela finance)
```

---

## Fluxo de Execução

```
main.py
  ├── pipeline_owners   → API Stays → upsert → tabela owners
  └── pipeline_finance  → tabela owners → API Stays (por listing) → insert → tabela finance
```

**Sequência obrigatória:** `owners` é executado antes do `finance`, pois o finance usa os `_id` e `listing_id` salvos na tabela `owners` como parâmetros para as chamadas à API.

---

## Tabelas do Banco de Dados

### `owners`
Proprietários e seus listings extraídos da API.

| Coluna | Tipo | Descrição |
|---|---|---|
| `_id` | VARCHAR | ID do proprietário (chave de conflito no upsert) |
| `name` | VARCHAR | Nome do proprietário |
| `listing_id` | VARCHAR | ID do listing |
| `dateinternal` | VARCHAR | Período de extração (from / to) |

### `finance`
Transações financeiras por listing e temporada.

| Coluna | Tipo | Descrição |
|---|---|---|
| `owner_id` | VARCHAR(100) | ID do proprietário |
| `account_id` | VARCHAR(100) | ID da conta da transação |
| `date` | DATE | Data da transação |
| `transaction_name` | VARCHAR(255) | Nome da transação |
| `valor` | NUMERIC(10,2) | Valor em BRL |
| `internal_note` | TEXT | Nota interna |
| `type` | VARCHAR(50) | Tipo da transação |
| `temporada` | VARCHAR(10) | Temporada (ex: `2025-2026`) |

---

## Lógica de Temporada

A temporada vai de **05/05/YYYY** até **04/05/YYYY+1**.

| Data de execução | Temporada | from | to |
|---|---|---|---|
| Qualquer dia entre 05/05/2025 e 04/05/2026 | `2025-2026` | 2025-05-05 | 2026-05-04 |
| Qualquer dia entre 05/05/2026 e 04/05/2027 | `2026-2027` | 2026-05-05 | 2027-05-04 |

A cada execução, os registros da temporada vigente são **deletados e reinseridos**, garantindo dados sempre atualizados sem duplicatas. Temporadas anteriores são preservadas.

---

## Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto:

```env
DB_HOST=
DB_PORT=
DB_NAME=
DB_USER=
DB_PASSWORD=
USER_NAME=
USER_PASSWORD=
```

---

## Como Executar

```bash
python main.py
```

---

## Dependências

Gerenciadas via `pyproject.toml`:

- `pandas` — manipulação de dados
- `psycopg2` — conexão com PostgreSQL
- `requests` — chamadas à API
- `python-dotenv` — variáveis de ambiente
- `pyarrow` / `fastparquet` — suporte a parquet
- `sqlalchemy` — utilitários ORM
