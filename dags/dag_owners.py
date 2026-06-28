import sys
from pathlib import Path
from datetime import datetime, timedelta, date

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "config"))

from airflow import DAG
from airflow.operators.python import PythonOperator

from config.apiconect import StaysConnection
from src.extract import StaysExtract
from src.Transform import OwnersTransform
from src.load import process_owners

_DEFAULT_ARGS = {
    "owner": "lenon",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _run_owners() -> None:
    hoje = date.today()
    data_inicial = date(hoje.year, 1, 1).strftime("%Y-%m-%d")
    data_final = hoje.strftime("%Y-%m-%d")

    conn = StaysConnection()
    extractor = StaysExtract(conn)

    raw = extractor.extract_owners(data_inicial, data_final)

    transformer = OwnersTransform()
    df = transformer.transform(raw)

    if not df.empty:
        process_owners(df)


with DAG(
    dag_id="lenon_owners",
    default_args=_DEFAULT_ARGS,
    description="Carga full de owners — UPSERT por listing_id, a cada 4 horas",
    schedule="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lenon", "owners", "dimensao"],
) as dag:

    PythonOperator(
        task_id="owners_full_load",
        python_callable=_run_owners,
    )
