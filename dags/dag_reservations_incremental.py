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
from src.Transform import ReservationsTransform
from src.load import process_reservations

_JANELA_DIAS = 90

_DEFAULT_ARGS = {
    "owner": "lenon",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _run_reservations_incremental() -> None:
    hoje = date.today()
    data_inicial = (hoje - timedelta(days=_JANELA_DIAS)).strftime("%Y-%m-%d")
    data_final = hoje.strftime("%Y-%m-%d")

    conn = StaysConnection()
    extractor = StaysExtract(conn)

    df_raw = extractor.extract_reservations(data_inicial, data_final, date_type="creation")

    transformer = ReservationsTransform()
    df = transformer.transform_reservations(df_raw)

    if not df.empty:
        process_reservations(df, date_column="data_de_criacao")


with DAG(
    dag_id="lenon_reservations_incremental",
    default_args=_DEFAULT_ARGS,
    description="Carga incremental de reservations — últimos 90 dias por data de criação, a cada 2 horas",
    schedule="0 */2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lenon", "reservations", "incremental"],
) as dag:

    PythonOperator(
        task_id="reservations_ultimos_90_dias",
        python_callable=_run_reservations_incremental,
    )
