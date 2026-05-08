import importlib.util
import logging
from pathlib import Path

from config.configDB import conn, encerra_conexao
from config.CRUD import upsert, insert_many

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")


def _load_module(relative_path: str, module_name: str):
    module_path = Path(__file__).parent / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def pipeline_owners(connection):
    logger.info(">>> [OWNERS] Iniciando pipeline de owners")
    module = _load_module("src/01.collect/db_owers_extract.py", "db_owers_extract")
    extractor = module.OwnersExtractor()
    df = extractor.extract()
    logger.info("[OWNERS] %d linhas extraídas da API", len(df))
    records = extractor.to_records(df)
    success, failed = 0, 0
    for record in records:
        if upsert(connection, "owners", record, ["_id"]):
            success += 1
        else:
            failed += 1
    logger.info("<<< [OWNERS] Finalizado — sucesso: %d | falhas: %d", success, failed)


def pipeline_finance(connection):
    logger.info(">>> [FINANCE] Iniciando pipeline de finance")
    module = _load_module("src/01.collect/db_finance.py", "db_finance")
    extractor = module.FinanceExtractor(connection)
    extractor.run(insert_many_fn=insert_many)
    logger.info("<<< [FINANCE] Pipeline de finance finalizado")


def main():
    logger.info("=" * 60)
    logger.info("INICIANDO MAIN PIPELINE")
    logger.info("=" * 60)

    logger.info("Conectando ao banco de dados...")
    connection = conn()
    if connection is None:
        logger.critical("Falha ao conectar ao banco. Pipeline encerrado.")
        return
    logger.info("Conexão estabelecida com sucesso")

    try:
        pipeline_owners(connection)
        pipeline_finance(connection)
    except Exception as e:
        logger.exception("Erro inesperado no pipeline: %s", e)
    finally:
        logger.info("Encerrando conexão com o banco...")
        encerra_conexao(connection)
        logger.info("=" * 60)
        logger.info("MAIN PIPELINE FINALIZADO")
        logger.info("=" * 60)


if __name__ == "__main__":
    main()
