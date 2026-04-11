CREATE OR REPLACE TABLE gold.gold_credit_card_fraud (

    merchant STRING COMMENT 'Nome do estabelecimento',

    category STRING COMMENT 'Categoria do estabelecimento',

    total_amount_trans DECIMAL(18,2) COMMENT 'Valor total transacionado',

    total_trans BIGINT COMMENT 'Quantidade total de transações',

    avg_city_pop DECIMAL(18,2) COMMENT 'Média da população das cidades das transações',

    sum_is_fraud BIGINT COMMENT 'Quantidade total de transações fraudulentas',
)
USING DELTA
COMMENT 'Tabela Gold agregada para análise de fraude pelo time de negócio';
