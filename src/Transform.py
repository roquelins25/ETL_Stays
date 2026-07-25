import logging
import re

import pandas as pd
import unidecode

logger = logging.getLogger(__name__)

_MESES_PT = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04",
    "mai": "05", "jun": "06", "jul": "07", "ago": "08",
    "set": "09", "out": "10", "nov": "11", "dez": "12",
}
_MESES_RE = re.compile(r"(\d{1,2})\s+(" + "|".join(_MESES_PT) + r")\s+(\d{4})", re.IGNORECASE)


def _parse_data_pt(valor):
    if pd.isna(valor):
        return pd.NaT
    m = _MESES_RE.search(str(valor))
    if m:
        dia, mes, ano = m.group(1), _MESES_PT[m.group(2).lower()], m.group(3)
        return pd.Timestamp(f"{ano}-{mes}-{dia.zfill(2)}")
    return pd.to_datetime(valor, errors="coerce")


class OwnersTransform:

    def transform(self, data: list[dict]) -> pd.DataFrame:

        df = pd.json_normalize(data)

        df = df.explode("listings")

        listings = pd.json_normalize(df["listings"])

        df = df.reset_index(drop=True)
        listings = listings.reset_index(drop=True)

        final_df = pd.concat(
            [
                df[["_id", "name"]],
                listings[["_id", "id", "internalName"]]
            ],
            axis=1
        )

        final_df.columns = [
            'owner_id',
            'owner_name',
            'listing_id',
            'listing_code',
            'listing_internal_name'
        ]

        return final_df

class ReservationsTransform:

    COLUNAS_DESEJADAS = [
        'mes',
        'chegada',
        'data de checkout',
        'id do anuncio',
        'nome interno do anuncio',
        'reserva',
        'nome do hospede',
        'codigo externo de reserva ota',
        'canal',
        'data de criacao',
        'data do repasse',
        'numero de noites',
        'moeda',
        'valor por noite',
        'total da reserva',
        'base de calculo do repasse',
        'preco de venda corrigido',
        'comissao da empresa',
        'preco de compra',
        'total da fatura de hospedagem',
        'total de servicos extras',
        'taxas: taxa de limpeza',
        'taxa do proprietario: tp',
        'primeiro nome do hospede',
        'sobrenome do hospede',
        'status da reserva',
        'agente'
    ]

    def transform_reservations(self, dataset: pd.DataFrame) -> pd.DataFrame:

        if dataset.empty or dataset.dropna(how="all").empty:
            logger.info("transform_reservations: dataset bruto vazio — nada a transformar")
            return pd.DataFrame()

        df = dataset.copy()
        logger.info("transform_reservations: %d linhas brutas recebidas", len(df))

        df.columns = [
            unidecode.unidecode(str(col)).lower().strip()
            for col in df.columns
        ]

        colunas_existentes = [
            c for c in self.COLUNAS_DESEJADAS
            if c in df.columns
        ]
        colunas_faltando = [c for c in self.COLUNAS_DESEJADAS if c not in df.columns]
        if colunas_faltando:
            logger.warning("transform_reservations: colunas ausentes no export: %s", colunas_faltando)

        df = df[colunas_existentes]

        if "status da reserva" in df.columns:
            antes = len(df)
            df = df[df["status da reserva"] != "pré-reserva"]
            logger.info(
                "transform_reservations: filtro status != pré-reserva removeu %d linhas (restam %d)",
                antes - len(df), len(df),
            )

        if "id do anuncio" in df.columns:
            antes = len(df)
            df = df[
                df["id do anuncio"].notna() &
                (df["id do anuncio"].astype(str).str.strip() != "")
            ]
            logger.info(
                "transform_reservations: filtro id do anuncio vazio removeu %d linhas (restam %d)",
                antes - len(df), len(df),
            )

        df = df.reset_index(drop=True)

        _COLUNAS_DATA = ["chegada", "data de checkout", "data de criacao", "data do repasse"]
        for col in _COLUNAS_DATA:
            if col in df.columns:
                df[col] = df[col].apply(_parse_data_pt)

        df.columns = [col.replace(" ", "_").replace(":", "") for col in df.columns]

        logger.info("transform_reservations: %d linhas finais após transformação", len(df))
        return df


class FinanceTransform:

    def transform_finance(self, data: dict) -> pd.DataFrame:

        registros = []

        if isinstance(data, dict):
            data = [data]

        for proprietario in data:

            id_proprietario = proprietario.get("_id")
            nome_proprietario = proprietario.get("name")

            for imovel in proprietario.get("listings", []):

                id_imovel = imovel.get("_id")
                id_ref = imovel.get("id")
                nome_interno = imovel.get("internalName")

                for lancamento in imovel.get("accounts", []):

                    registros.append({
                        "id_proprietario": id_proprietario,
                        "nome_proprietario": nome_proprietario,

                        "id_imovel": id_imovel,
                        "id_ref": id_ref,
                        "nome_interno": nome_interno,

                        "id_lancamento": lancamento.get("_id"),
                        "data": lancamento.get("date"),
                        "nome_lancamento": lancamento.get("transactionName"),
                        "valor": lancamento.get("_mcval", {}).get("BRL"),
                        "tipo": lancamento.get("type"),
                        "status": lancamento.get("status")
                    })

        df = pd.DataFrame(registros)
        
        return df