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

    dfs_mes = []
    for owner_id in owner_ids:
        raw = extractor.extract_finance(data_inicial, data_final, owner_id)
        df = transformer.transform_finance(raw)

        if not df.empty:
            dfs_mes.append(df)

        time.sleep(_RATE_LIMIT_SLEEP)

    df_mes = pd.concat(dfs_mes, ignore_index=True) if dfs_mes else pd.DataFrame()

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
