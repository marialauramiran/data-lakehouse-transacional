CREATE OR REPLACE TABLE silver.silver_credit_card_fraud (

    transaction_id INT COMMENT 'Identificador único da operação (renomeado de index_op)',

    transaction_timestamp TIMESTAMP COMMENT 'Data e hora da transação padronizada',

    merchant_name STRING COMMENT 'Nome do estabelecimento',

    merchant_category STRING COMMENT 'Categoria do estabelecimento',

    transaction_amount DECIMAL(18,2) COMMENT 'Valor da transação',

    city STRING COMMENT 'Cidade da transação normalizada',

    state STRING COMMENT 'Estado da transação (UF padronizada)',

    customer_latitude DOUBLE COMMENT 'Latitude do cliente convertida para numérico',

    customer_longitude DOUBLE COMMENT 'Longitude do cliente convertida para numérico',

    city_population INT COMMENT 'População da cidade',

    customer_job STRING COMMENT 'Profissão do cliente padronizada',

    dt_birthday DATE COMMENT 'Data de nascimento do cliente (convertida de timestamp)',

    transaction_number STRING COMMENT 'Identificador único da transação',

    merchant_latitude DOUBLE COMMENT 'Latitude do estabelecimento',

    merchant_longitude DOUBLE COMMENT 'Longitude do estabelecimento',

    is_fraud INT COMMENT 'Indicador de fraude (1 = fraude, 0 = não fraude)'

)
USING DELTA
COMMENT 'Tabela Silver com dados tratados, tipados e padronizados para análise de fraude';
