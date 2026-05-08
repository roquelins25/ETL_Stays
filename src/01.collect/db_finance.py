# %%
import logging
import base64
import os
import time
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _get_temporada(hoje: date) -> tuple[str, str, str]:
    """
    Retorna (temporada, from_date, to_date) com base na data atual.
    Temporada vai de 05/05/YYYY até 04/05/YYYY+1.
    """
    inicio_temporada_atual = date(hoje.year, 5, 5)

    if hoje >= inicio_temporada_atual:
        ano_inicio = hoje.year
    else:
        ano_inicio = hoje.year - 1

    ano_fim = ano_inicio + 1
    temporada = f"{ano_inicio}-{ano_fim}"
    from_date = date(ano_inicio, 5, 5).strftime("%Y-%m-%d")
    to_date = date(ano_fim, 5, 4).strftime("%Y-%m-%d")

    return temporada, from_date, to_date


class FinanceExtractor:
    URL = "https://lit.stays.com.br/external/v1/finance/owners"

    def __init__(self, connection):
        load_dotenv()
        user_name = os.getenv("USER_NAME")
        user_password = os.getenv("USER_PASSWORD")
        if not user_name or not user_password:
            logger.error("USER_NAME ou USER_PASSWORD não encontrados no .env")
        encoded = base64.b64encode(f"{user_name}:{user_password}".encode()).decode()
        self.headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/json",
        }
        self.connection = connection
        logger.info("FinanceExtractor iniciado com sucesso")

    def _fetch_owners(self) -> list[dict]:
        logger.info("Buscando _id e listing_id da tabela owners no banco...")
        cursor = self.connection.cursor()
        cursor.execute("SELECT _id, listing_id FROM owners")
        rows = cursor.fetchall()
        cursor.close()
        logger.info("%d listings encontrados na tabela owners", len(rows))
        return [{"_id": row[0], "listing_id": row[1]} for row in rows]

    def _delete_temporada(self, temporada: str):
        logger.info("Deletando registros da temporada '%s' no banco...", temporada)
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM finance WHERE temporada = %s", (temporada,))
        deleted = cursor.rowcount
        self.connection.commit()
        cursor.close()
        logger.info("%d registros deletados da temporada '%s'", deleted, temporada)

    def _extract_listing(self, _id: str, listing_id: str, from_date: str, to_date: str) -> pd.DataFrame:
        url = f"{self.URL}/{_id}/{listing_id}"
        response = requests.get(url, headers=self.headers, params={"from": from_date, "to": to_date})

        logger.info("API %s | %s → status %s", _id, listing_id, response.status_code)

        if response.status_code != 200:
            logger.warning("Falha na API para %s | %s: %s", _id, listing_id, response.text)
            return pd.DataFrame()

        data = response.json()
        df = pd.DataFrame(data) if isinstance(data, list) else pd.json_normalize(data)
        df["_id"] = _id
        df["listing_id"] = listing_id
        return df

    def _transform(self, df: pd.DataFrame, temporada: str) -> pd.DataFrame:
        if df.empty or "accounts" not in df.columns:
            return pd.DataFrame()

        df_exploded = df[["_id", "accounts"]].explode("accounts", ignore_index=True)
        df_exploded = df_exploded[df_exploded["accounts"].notna()]

        df_accounts = pd.json_normalize(df_exploded["accounts"])
        df_accounts = df_accounts.rename(columns={"_id": "account_id"})

        df_final = pd.concat([df_exploded[["_id"]], df_accounts], axis=1)

        df_final["valor"] = df_final["_mcval.BRL"] if "_mcval.BRL" in df_final.columns else None

        df_final = df_final[[
            "_id",
            "account_id",
            "date",
            "transactionName",
            "valor",
            "internalNote",
            "type",
        ]]
        df_final.columns = [
            "owner_id",
            "account_id",
            "date",
            "transaction_name",
            "valor",
            "internal_note",
            "type",
        ]

        df_final["date"] = pd.to_datetime(df_final["date"], errors="coerce").dt.date
        df_final["valor"] = pd.to_numeric(df_final["valor"], errors="coerce").round(2)
        df_final["temporada"] = temporada

        return df_final

    def _processar_listing(self, owner: dict, from_date: str, to_date: str, temporada: str) -> pd.DataFrame:
        _id = owner["_id"]
        listing_id = owner["listing_id"]
        try:
            time.sleep(3)
            df_raw = self._extract_listing(_id, listing_id, from_date, to_date)
            df_transformed = self._transform(df_raw, temporada)
            if df_transformed.empty:
                logger.warning("Sem dados para %s | %s", _id, listing_id)
            else:
                logger.info("Transformado %s | %s — %d transações", _id, listing_id, len(df_transformed))
            return df_transformed
        except Exception as e:
            logger.exception("Erro inesperado em %s | %s: %s", _id, listing_id, e)
            return pd.DataFrame()

    def run(self, insert_many_fn, max_workers: int = 8):
        logger.info("=" * 60)
        logger.info("INICIANDO PIPELINE — finance extract → banco de dados")
        logger.info("=" * 60)

        temporada, from_date, to_date = "2025-2026", "2025-05-09", "2026-05-08"
        logger.info("Temporada: %s | Período: %s até %s", temporada, from_date, to_date)

        owners = self._fetch_owners()

        self._delete_temporada(temporada)

        resultados = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._processar_listing, owner, from_date, to_date, temporada): owner
                for owner in owners
            }
            for future in as_completed(futures):
                df_result = future.result()
                if not df_result.empty:
                    resultados.append(df_result)

        if not resultados:
            logger.warning("Nenhum dado extraído da API.")
            return

        df_all = pd.concat(resultados, ignore_index=True)
        logger.info("Total de transações extraídas: %d", len(df_all))

        records = df_all.where(pd.notna(df_all), None).to_dict(orient="records")
        inserted = insert_many_fn(self.connection, "finance", records)
        logger.info("Registros inseridos no banco: %d", inserted)

        logger.info("=" * 60)
        logger.info("PIPELINE FINALIZADO — temporada %s | %d registros salvos", temporada, inserted)
        logger.info("=" * 60)
