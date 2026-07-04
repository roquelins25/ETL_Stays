import logging
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, date
import calendar

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "config"))

from airflow import DAG
from airflow.operators.python import PythonOperator

from config.apiconect import StaysConnection
from config.configDB import conn as connect_db
from src.extract import StaysExtract
from src.Transform import FinanceTransform
from src.load import process_finance

logger = logging.getLogger(__name__)

_HISTORICO_INICIO = date(2025, 5, 1)
_HISTORICO_FIM = date(2026, 7, 31)
_RATE_LIMIT_SLEEP = 5

_DEFAULT_ARGS = {
    "owner": "lenon",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _get_owner_ids() -> list[str]:
    connection = connect_db()
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT DISTINCT owner_id FROM owners WHERE owner_id IS NOT NULL")
            return [row[0] for row in cur.fetchall()]
    finally:
        connection.close()


def _run_finance_mes(data_inicial: str, data_final: str) -> None:
    conn = StaysConnection()
    extractor = StaysExtract(conn)
    transformer = FinanceTransform()

    owner_ids = _get_owner_ids()
    total_owners = len(owner_ids)
    logger.info("[%s -> %s] %d owners a processar", data_inicial, data_final, total_owners)

    dfs_mes = []
    for i, owner_id in enumerate(owner_ids, start=1):
        raw = extractor.extract_finance(data_inicial, data_final, owner_id)
        df = transformer.transform_finance(raw)

        if df.empty:
            logger.info("[%s -> %s] (%d/%d) owner_id=%s sem dados — ignorado", data_inicial, data_final, i, total_owners, owner_id)
        else:
            logger.info("[%s -> %s] (%d/%d) owner_id=%s: %d registros", data_inicial, data_final, i, total_owners, owner_id, len(df))
            dfs_mes.append(df)

        time.sleep(_RATE_LIMIT_SLEEP)

    if not dfs_mes:
        logger.warning("[%s -> %s] nenhum owner com dados — nada a carregar", data_inicial, data_final)
        return

    df_final = pd.concat(dfs_mes, ignore_index=True)
    logger.info("[%s -> %s] carregando %d registros de %d owners", data_inicial, data_final, len(df_final), len(dfs_mes))
    process_finance(df_final)


def _gerar_meses(inicio: date, fim: date):
    cursor = inicio.replace(day=1)
    while cursor <= fim:
        ultimo_dia = calendar.monthrange(cursor.year, cursor.month)[1]
        data_final = min(date(cursor.year, cursor.month, ultimo_dia), fim)
        yield cursor.strftime("%Y-%m-%d"), data_final.strftime("%Y-%m-%d"), cursor.strftime("%Y_%m")
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)


with DAG(
    dag_id="lenon_finance_historico",
    default_args=_DEFAULT_ARGS,
    description="Backfill histórico de finance mês a mês de 2025-05 a 2026-07, por owner_id — trigger manual",
    schedule=None,
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=["lenon", "finance", "historico"],
) as dag:

    tasks = []

    for data_ini, data_fim, label in _gerar_meses(_HISTORICO_INICIO, _HISTORICO_FIM):
        t = PythonOperator(
            task_id=f"finance_{label}",
            python_callable=_run_finance_mes,
            op_kwargs={"data_inicial": data_ini, "data_final": data_fim},
        )
        if tasks:
            tasks[-1] >> t
        tasks.append(t)
