import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, date
import calendar

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

_HISTORICO_INICIO = date(2024, 1, 1)
_RATE_LIMIT_SLEEP = 15

_DEFAULT_ARGS = {
    "owner": "lenon",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _run_reservations_mes(data_inicial: str, data_final: str) -> None:
    conn = StaysConnection()
    extractor = StaysExtract(conn)

    df_raw = extractor.extract_reservations(data_inicial, data_final, date_type="creation")

    transformer = ReservationsTransform()
    df = transformer.transform_reservations(df_raw)

    if not df.empty:
        process_reservations(df, date_column="data_de_criacao")

    time.sleep(_RATE_LIMIT_SLEEP)


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
    dag_id="lenon_reservations_historico",
    default_args=_DEFAULT_ARGS,
    description="Backfill histórico de reservations mês a mês desde 2024-01 — trigger manual",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lenon", "reservations", "historico"],
) as dag:

    tasks = []
    hoje = date.today()

    for data_ini, data_fim, label in _gerar_meses(_HISTORICO_INICIO, hoje):
        t = PythonOperator(
            task_id=f"reservations_{label}",
            python_callable=_run_reservations_mes,
            op_kwargs={"data_inicial": data_ini, "data_final": data_fim},
        )
        if tasks:
            tasks[-1] >> t
        tasks.append(t)
