import logging
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, date

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
from src.load import process_finance_incremental

logger = logging.getLogger(__name__)

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


def _run_finance_incremental() -> None:
    hoje = date.today()
    data_inicial = hoje.replace(day=1).strftime("%Y-%m-%d")
    data_final = hoje.strftime("%Y-%m-%d")

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

    df_mes = pd.concat(dfs_mes, ignore_index=True) if dfs_mes else pd.DataFrame()

    logger.info("[%s -> %s] carregando %d registros de %d owners (apagando mês corrente antes)", data_inicial, data_final, len(df_mes), len(dfs_mes))

    # Apaga todos os proprietários do mês corrente e recarrega do zero,
    # mesmo que a API não tenha retornado nada para nenhum owner.
    process_finance_incremental(df_mes, data_inicial, data_final)


with DAG(
    dag_id="lenon_finance_incremental",
    default_args=_DEFAULT_ARGS,
    description="Carga incremental de finance — apaga e recarrega todos os owners do mês corrente, a cada 2 horas",
    schedule="0 */2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lenon", "finance", "incremental"],
) as dag:

    PythonOperator(
        task_id="finance_mes_corrente",
        python_callable=_run_finance_incremental,
    )
