CREATE TABLE IF NOT EXISTS finance (
    id_proprietario     VARCHAR(50),
    nome_proprietario   VARCHAR(100),
    id_imovel           VARCHAR(50),
    id_ref              VARCHAR(50),
    nome_interno        VARCHAR(100),
    id_lancamento       VARCHAR(50),
    data                DATE,
    nome_lancamento     VARCHAR(100),
    valor               DECIMAL(10, 2),
    tipo                VARCHAR(10),
    status              VARCHAR(20),
    periodo_referencia  DATE
);

ALTER TABLE finance ADD COLUMN IF NOT EXISTS periodo_referencia DATE;