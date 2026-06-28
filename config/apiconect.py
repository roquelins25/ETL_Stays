from pathlib import Path
import os
import base64

import requests
from dotenv import load_dotenv

_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


class StaysConnection:
    BASE_URL = "https://lit.stays.com.br/external/v1"

    def __init__(self):
        load_dotenv(_ENV_PATH, override=True)

        self.client_id = os.getenv("USER_NAME")
        self.client_secret = os.getenv("USER_PASSWORD")

        if not self.client_id or not self.client_secret:
            raise ValueError(
                "USER_NAME e USER_PASSWORD devem estar definidos no .env"
            )

        # Gera o Basic Auth
        credentials = f"{self.client_id}:{self.client_secret}"
        token = base64.b64encode(credentials.encode()).decode()

        self.session = requests.Session()

        self.session.headers.update({
            "Authorization": f"Basic {token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        })

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict | None = None,
        payload: dict | None = None
    ):
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"

        response = self.session.request(
            method=method.upper(),
            url=url,
            params=params,
            json=payload,
            timeout=120
        )

        response.raise_for_status()

        return response

    def get(self, endpoint: str, params: dict | None = None):
        return self.request(
            method="GET",
            endpoint=endpoint,
            params=params
        )

    def post(self, endpoint: str, payload: dict | None = None):
        return self.request(
            method="POST",
            endpoint=endpoint,
            payload=payload
        )