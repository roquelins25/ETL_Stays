# %%
import pandas as pd
from io import BytesIO
from config.apiconect import StaysConnection

class StaysExtract:
    def __init__(self, connection: StaysConnection):
        self.connection = connection

    def extract_reservations(self, from_date: str, to_date: str, date_type: str = "creation"):
        payload = {
            "from": from_date,
            "to": to_date,
            "dateType": date_type
        }
        response = self.connection.get("booking/reservations-export", payload)

        df = pd.read_excel(
                BytesIO(response.content))

        return df

    def extract_owners(self, from_date: str , to_date: str):
        playload = {
            "from": from_date,
            "to": to_date
        }    
        
        response = self.connection.get("finance/owners", params=playload)

        return response.json()
# %%
