CREATE OR REPLACE TABLE silver.silver_credit_card_fraud (

    index_op INT COMMENT 'Identificador único da operação (renomeado de index_op)',

    trans_date_trans_time TIMESTAMP COMMENT 'Data e hora da transação padronizada',

    merchant STRING COMMENT 'Nome do estabelecimento',

    category STRING COMMENT 'Categoria do estabelecimento',

    amt DECIMAL(18,2) COMMENT 'Valor da transação',

    city STRING COMMENT 'Cidade da transação normalizada',

    state STRING COMMENT 'Estado da transação (UF padronizada)',

    latitude DOUBLE COMMENT 'Latitude do cliente convertida para numérico',

    longitude DOUBLE COMMENT 'Longitude do cliente convertida para numérico',

    city_pop INT COMMENT 'População da cidade',

    job STRING COMMENT 'Profissão do cliente padronizada',

    dt_birthday DATE COMMENT 'Data de nascimento do cliente (convertida de timestamp)',

    trans_num STRING COMMENT 'Identificador único da transação',

    merch_lat DOUBLE COMMENT 'Latitude do estabelecimento',

    merch_long DOUBLE COMMENT 'Longitude do estabelecimento',

    is_fraud INT COMMENT 'Indicador de fraude (1 = fraude, 0 = não fraude)'

)
USING DELTA
COMMENT 'Tabela Silver com dados tratados, tipados e padronizados para análise de fraude';
