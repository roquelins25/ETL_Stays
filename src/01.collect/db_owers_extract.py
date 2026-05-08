# %%
import requests
import os
import base64
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv


#%%
load_dotenv()
user_name = os.getenv("USER_NAME")
user_password = os.getenv("USER_PASSWORD")
credentials = f"{user_name}:{user_password}"
encoded = base64.b64encode(credentials.encode()).decode()
# %%
url = "https://lit.stays.com.br/external/v1/finance/owners"

headers = {
    "Authorization": f"Basic {encoded}",
    "Content-Type": "application/json"
}
querystring = {"from":"2025-12-01","to":"2025-12-31"}

response = requests.get(url, headers=headers, params=querystring)
# %%

response_json = response.json()
df = pd.DataFrame(response_json)
# %%
columns = ['_id', 'name','listings']
df_owners = df[columns].copy()

# %%
df_owners = df_owners.explode('listings', ignore_index=True)
df_owners = df_owners[df_owners['listings'].notna()]
df_listings = pd.json_normalize(df_owners['listings'])
df_listings = df_listings.rename(columns={'_id': 'listing_id'})
df_final = pd.concat(
    [df_owners.drop(columns=['listings']), df_listings],
    axis=1
)

df_final.head()
# %%
hoje = datetime.today().strftime('%Y-%m-%d')

caminho = f'./../../data/owners/db_owners_{hoje}.parquet'

# cria a pasta automaticamente
os.makedirs(os.path.dirname(caminho), exist_ok=True)

df_final.to_parquet(caminho, index=False)
# %%
