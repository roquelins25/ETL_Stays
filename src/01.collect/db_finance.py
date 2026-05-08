# %%
import pandas as pd
import os
import pyarrow as pq
import base64
import requests
from dotenv import load_dotenv

# %%
pasta = './../../data/owners'

arquivos = sorted(os.listdir(pasta), reverse=True)

arquivo_mais_recente = arquivos[0]

caminho = os.path.join(pasta, arquivo_mais_recente)

df = pd.read_parquet(caminho, columns=['_id', 'listing_id'], engine='fastparquet')
# %%
df.head()
#%%
class FinanceDataExtractor:
    def __init__(self):
        load_dotenv()
        self.user_name = os.getenv("USER_NAME")
        self.user_password = os.getenv("USER_PASSWORD")
        self.credentials = f"{self.user_name}:{self.user_password}"
        self.encoded = base64.b64encode(self.credentials.encode()).decode()

        self.url = "https://lit.stays.com.br/external/v1/finance/owners"
        self.headers = {
            "Authorization": f"Basic {self.encoded}",
            "Content-Type": "application/json"
        }
    def extract_data_finance(self,_id: str, listing_id: str, from_date: str, to_date: str) -> pd.DataFrame:
        url = f"{self.url}/{_id}/{listing_id}"
        querystring = {"from": from_date, "to": to_date}
        response = requests.get(url, headers=self.headers, params=querystring)

        print(f"Fetching {_id} | {listing_id} → {response.status_code}")

        if response.status_code != 200:
            print("Erro na API:")
            print(response.text)
            return pd.DataFrame()

        data = response.json()

        # tratamento correto do JSON
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = pd.json_normalize(data)

        # contexto extra (muito útil depois)
        df['_id'] = _id
        df['listing_id'] = listing_id

        return df
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:

        if df.empty or 'accounts' not in df.columns:
            return pd.DataFrame()

        df_exploded = df[['_id', 'accounts']].explode('accounts', ignore_index=True)
        df_exploded = df_exploded[df_exploded['accounts'].notna()]

        df_accounts = pd.json_normalize(df_exploded['accounts'])

        df_accounts = df_accounts.rename(columns={'_id': 'account_id'})

        df_final = pd.concat([df_exploded[['_id']], df_accounts], axis=1)

        # extrair valor
        df_final['valor'] = df_final['_mcval.BRL']

        # selecionar colunas finais
        df_final = df_final[[
            '_id',
            'account_id',  # account id (ajuste se necessário)
            'date',
            'transactionName',
            'valor',
            'internalNote',
            'type'
        ]]

        df_final.columns = [
            'owner_id',
            'account_id',
            'date',
            'transaction_name',
            'valor',
            'internal_note',
            'type'
        ]

        return df_final
    def save_data(self, df: pd.DataFrame, _id: str, listing_id: str):
        if df.empty:
            print(f"Nenhum dado para salvar para {_id} | {listing_id}")
            return

        pasta_destino = './../../data/finance'
        os.makedirs(pasta_destino, exist_ok=True)

        nome_arquivo = f"{_id}_{listing_id}.parquet"
        caminho_completo = os.path.join(pasta_destino, nome_arquivo)

        df.to_parquet(caminho_completo, index=False)

        print(f"Dados salvos em: {caminho_completo}")


    def processar_dados(self, _id: str, listing_id: str, from_date: str, to_date: str) -> pd.DataFrame:
        df_raw = self.extract_data_finance(_id, listing_id, from_date, to_date)
        if df_raw.empty:
            return pd.DataFrame()
        df_transformed = self.transform_data(df_raw)
        self.save_data(df_transformed, _id, listing_id)

        return df_transformed
    
    
# %%
api = FinanceDataExtractor()

df_teste = api.processar_dados(
    _id='64b8c9e5e1d2c90001a7f0b4',
    listing_id='667acdd88e45ed576f980d3a',
    from_date='2025-05-01',
    to_date='2026-04-30'
)
# %%
# %%
