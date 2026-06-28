import logging
import time
from datetime import date, timedelta

from config.apiconect import StaysConnection
from src.extract import StaysExtract
from src.Transform import OwnersTransform, ReservationsTransform
from src.load import process_owners, process_reservations

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


def run_owners(data_inicial: str, data_final: str) -> None:
    conn = StaysConnection()
    extractor = StaysExtract(conn)

    logger.info("Extraindo owners de %s a %s", data_inicial, data_final)
    raw = extractor.extract_owners(data_inicial, data_final)

    transformer = OwnersTransform()
    df = transformer.transform(raw)

    if df.empty:
        logger.warning("Nenhum owner retornado — nada a carregar")
        return

    logger.info("Carregando %d registros em owners", len(df))
    process_owners(df)


def run_reservations(data_inicial: str, data_final: str, date_type: str = "creation") -> None:
    conn = StaysConnection()
    extractor = StaysExtract(conn)

    logger.info("Extraindo reservations de %s a %s (dateType=%s)", data_inicial, data_final, date_type)
    df_raw = extractor.extract_reservations(data_inicial, data_final, date_type=date_type)

    transformer = ReservationsTransform()
    df = transformer.transform_reservations(df_raw)

    if df.empty:
        logger.warning("Nenhum registro após transformação — nada a carregar")
        return

    logger.info("Carregando %d registros em reservations", len(df))
    process_reservations(df, date_column="data_de_criacao")


def main():
    inicio = time.perf_counter()

    hoje = date.today()
    data_inicial = (hoje - timedelta(days=90)).strftime("%Y-%m-%d")
    data_final = hoje.strftime("%Y-%m-%d")

    erros = []

    try:
        run_owners(data_inicial, data_final)
    except Exception:
        logger.exception("Falha ao processar owners")
        erros.append("owners")

    try:
        run_reservations(data_inicial, data_final)
    except Exception:
        logger.exception("Falha ao processar reservations")
        erros.append("reservations")

    if erros:
        raise RuntimeError(f"Falha em: {', '.join(erros)}")

    logger.info("Pipeline concluído em %.1fs", time.perf_counter() - inicio)


if __name__ == "__main__":
    main()
