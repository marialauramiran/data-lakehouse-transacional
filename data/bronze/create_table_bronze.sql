CREATE TABLE bronze.bronze_credit_card_fraud (
    
    index_op INT COMMENT 'Identificador único da operação',
    
    trans_date_trans_time TIMESTAMP COMMENT 'Data e hora da transação',
    
    merchant STRING COMMENT 'Nome do estabelecimento comercial',
    
    category STRING COMMENT 'Categoria do estabelecimento (usada para particionamento)',
    
    amt DECIMAL(18,2) COMMENT 'Valor da transação em moeda',
    
    city STRING COMMENT 'Cidade onde a transação ocorreu',
    
    state STRING COMMENT 'Estado da transação',
    
    latitude STRING COMMENT 'Latitude do cliente',
    
    longitude STRING COMMENT 'Longitude do cliente',
    
    city_pop INT COMMENT 'População da cidade do cliente',
    
    job STRING COMMENT 'Profissão do cliente',
    
    dob TIMESTAMP COMMENT 'Data de nascimento do cliente',
    
    trans_num STRING COMMENT 'Identificador único da transação',
    
    merch_lat STRING COMMENT 'Latitude do estabelecimento',
    
    merch_long STRING COMMENT 'Longitude do estabelecimento',
    
    is_fraud INT COMMENT 'Indicador de fraude (1 = fraude, 0 = não fraude)'
    
)
USING DELTA
PARTITIONED BY (category)
COMMENT 'Tabela Bronze contendo dados brutos de transações para detecção de fraude';
