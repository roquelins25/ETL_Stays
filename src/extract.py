# %%
import pandas as pd
import requests
from io import BytesIO
from config.apiconect import StaysConnection

_NO_LISTINGS_MESSAGE = "No listings were found for this owner"

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
        
        filename = f"{from_date}_{to_date}.xlsx"

        # with open(filename, "wb") as f: f.write(response.content)
             
        return df

    def extract_owners(self, from_date: str , to_date: str):
        payload = {
            "from": from_date,
            "to": to_date
        }    
        
        response = self.connection.get("finance/owners", params=payload)

        return response.json()
    
    def extract_finance(self, from_date: str, to_date: str, _id: str):
        payload = {
            "from": from_date,
            "to": to_date
        }
        try:
            response = self.connection.get(f"finance/owners/{_id}", params=payload)
        except requests.exceptions.HTTPError as exc:
            error_response = exc.response
            if error_response is not None and error_response.status_code == 404:
                try:
                    body = error_response.json()
                except ValueError:
                    body = {}
                if body.get("message") == _NO_LISTINGS_MESSAGE:
                    return {}
            raise

        df = response.json()

        return df
    
    def extract_finance2(self, from_date: str, to_date: str, _id: str):
        payload = {
            "from": from_date,
            "to": to_date,
            "ownerId": _id,
            "dateType": "creation"
        }
        try:
            response = self.connection.get(f"/finance/accounting-transactions", params=payload)
        except requests.exceptions.HTTPError as exc:
            error_response = exc.response
            if error_response is not None and error_response.status_code == 404:
                try:
                    body = error_response.json()
                except ValueError:
                    body = {}
                if body.get("message") == _NO_LISTINGS_MESSAGE:
                    return {}
            raise

        df = response.json()

        return df


# %%
