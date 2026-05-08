# %%
import logging
import requests
import base64
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class OwnersExtractor:
    URL = "https://lit.stays.com.br/external/v1/finance/owners"

    def __init__(self):
        logger.info("Iniciando OwnersExtractor — carregando credenciais")
        load_dotenv()
        user_name = os.getenv("USER_NAME")
        user_password = os.getenv("USER_PASSWORD")
        if not user_name or not user_password:
            logger.error("USER_NAME ou USER_PASSWORD não encontrados no .env")
        credentials = f"{user_name}:{user_password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        self.headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/json",
        }
        logger.info("Credenciais carregadas com sucesso")

    def _build_querystring(self) -> dict:
        data_atual = datetime.today()
        data_um_ano_atras = data_atual.replace(year=data_atual.year - 1) + pd.Timedelta(days=1)
        qs = {
            "from": data_um_ano_atras.strftime("%Y-%m-%d"),
            "to": data_atual.strftime("%Y-%m-%d"),
        }
        logger.info("Período de extração: %s até %s", qs["from"], qs["to"])
        return qs

    def extract(self) -> pd.DataFrame:
        querystring = self._build_querystring()

        logger.info("Chamando API: %s", self.URL)
        response = requests.get(self.URL, headers=self.headers, params=querystring)
        logger.info("Resposta da API: status %s", response.status_code)
        response.raise_for_status()

        raw = response.json()
        logger.info("Registros brutos recebidos: %d owners", len(raw))

        df = pd.DataFrame(raw)
        df_owners = df[["_id", "name", "listings"]].copy()

        df_owners = df_owners.explode("listings", ignore_index=True)
        df_owners = df_owners[df_owners["listings"].notna()]
        logger.info("Após explode de listings: %d linhas", len(df_owners))

        df_listings = pd.json_normalize(df_owners["listings"])
        df_listings = df_listings.rename(columns={"_id": "listing_id"})
        logger.info("Colunas expandidas de listings: %s", df_listings.columns.tolist())

        df_final = pd.concat(
            [df_owners.drop(columns=["listings"]), df_listings],
            axis=1,
        )
        df_final["dateinternal"] = f"{querystring['from']} / {querystring['to']}"

        logger.info("DataFrame final: %d linhas, %d colunas", len(df_final), len(df_final.columns))
        return df_final

    def to_records(self, df: pd.DataFrame) -> list[dict]:
        records = df.where(pd.notna(df), None).to_dict(orient="records")
        logger.info("Registros preparados para o banco: %d", len(records))
        return records
