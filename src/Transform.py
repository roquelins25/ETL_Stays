import pandas as pd
import unidecode


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
            "owner_id",
            "owner_name",
            "listing_id",
            "listing_code",
            "listing_internal_name"
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
            return pd.DataFrame()

        df = dataset.copy()

        df.columns = [
            unidecode.unidecode(str(col)).lower().strip()
            for col in df.columns
        ]

        colunas_existentes = [
            c for c in self.COLUNAS_DESEJADAS
            if c in df.columns
        ]

        df = df[colunas_existentes]

        if "status da reserva" in df.columns:
            df = df[df["status da reserva"] != "pré-reserva"]

        if "id do anuncio" in df.columns:
            df = df[
                df["id do anuncio"].notna() &
                (df["id do anuncio"].astype(str).str.strip() != "")
            ]

        df = df.reset_index(drop=True)

        df.columns = [col.replace(" ", "_").replace(":", "") for col in df.columns]

        return df