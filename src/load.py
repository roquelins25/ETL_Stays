import csv
import logging
import os
import sys
from io import StringIO

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.configDB import conn as connect_db

logger = logging.getLogger(__name__)

_SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")


def create_table_if_not_exists(connection, table: str) -> None:
    sql_path = os.path.join(_SQL_DIR, f"{table}.sql")
    if not os.path.exists(sql_path):
        logger.warning("SQL não encontrado para %s — tabela não será criada", table)
        return
    with open(sql_path, encoding="utf-8") as f:
        ddl = f.read()
    with connection.cursor() as cur:
        cur.execute(ddl)
    connection.commit()


def _copy_to_buffer(df: pd.DataFrame) -> StringIO:
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
    buffer.seek(0)
    return buffer


def _load_reservations(connection, df: pd.DataFrame, table: str, date_column: str) -> None:
    cols = df.columns.tolist()
    cols_str = ", ".join(cols)
    copy_sql = f"COPY {table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, NULL '')"

    data_min = pd.to_datetime(df[date_column]).min().date()
    data_max = pd.to_datetime(df[date_column]).max().date()

    delete_sql = f"DELETE FROM {table} WHERE {date_column} BETWEEN %s AND %s"

    with connection.cursor() as cur:
        cur.execute(delete_sql, (data_min, data_max))
        deleted = cur.rowcount
        cur.copy_expert(copy_sql, _copy_to_buffer(df))

    connection.commit()
    logger.info(
        "%s: %d deletados, %d inseridos (período %s → %s, coluna %s)",
        table, deleted, len(df), data_min, data_max, date_column,
    )


def _upsert_owners(connection, df: pd.DataFrame, table: str) -> None:
    cols = df.columns.tolist()
    cols_str = ", ".join(cols)
    tmp = f"tmp_{table}"

    update_cols = [c for c in cols if c != "listing_id"]
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    copy_sql = f"COPY {tmp} ({cols_str}) FROM STDIN WITH (FORMAT CSV, NULL '')"
    upsert_sql = f"""
        INSERT INTO {table} ({cols_str})
        SELECT {cols_str} FROM {tmp}
        ON CONFLICT (listing_id) DO UPDATE SET {update_set}
    """

    with connection.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE {tmp} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
        cur.copy_expert(copy_sql, _copy_to_buffer(df))
        cur.execute(upsert_sql)

    connection.commit()
    logger.info("%s: %d registros upserted", table, len(df))


def process_reservations(df: pd.DataFrame, date_column: str = "data_de_criacao") -> None:
    table = "reservations"
    connection = connect_db()
    try:
        create_table_if_not_exists(connection, table)
        _load_reservations(connection, df, table, date_column)
    except Exception:
        connection.rollback()
        logger.exception("Falha ao processar %s", table)
        raise
    finally:
        connection.close()


def process_owners(df: pd.DataFrame) -> None:
    table = "owners"
    connection = connect_db()
    try:
        create_table_if_not_exists(connection, table)
        _upsert_owners(connection, df, table)
    except Exception:
        connection.rollback()
        logger.exception("Falha ao processar %s", table)
        raise
    finally:
        connection.close()
