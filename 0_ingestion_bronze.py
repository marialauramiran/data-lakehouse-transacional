# Databricks notebook source
# Bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# Configuração para Delta Lake 
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Cell 2
# Criando schemas para armazenar as tabelas no formato da Arquitetura Medallion 
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

# DBTITLE 1,Cell 3
# Criação da tabela fonte na camada bronze particionando por categoria
spark.sql("""
    CREATE OR REPLACE TABLE bronze.bronze_credit_card_fraud (
        index_op INT,
        trans_date_trans_time TIMESTAMP,	
        merchant STRING,
        category STRING,
        amt	DECIMAL(18,2),
        city STRING,
        state STRING,
        latitude STRING,
        longitude STRING,	
        city_pop INT,	
        job	STRING,
        dob	TIMESTAMP,
        trans_num STRING,
        merch_lat STRING,	
        merch_long STRING,	
        is_fraud INT
    )
    USING DELTA 
    PARTITIONED BY (category)         
""")

# COMMAND ----------

#Leitura dos dados da fonte (CSV)
df_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("sep", ";") \
    .load("/Volumes/workspace/default/data_lakehouse_trabalho_mba/credit_card_fraud.csv")

df_bronze.show()

# COMMAND ----------

# Criação de uma visão temporária
df_bronze.createOrReplaceTempView("credit_card_fraud")

# COMMAND ----------

# Execução do MERGE INTO
spark.sql("""
    MERGE INTO bronze.bronze_credit_card_fraud AS tb_final
    USING credit_card_fraud AS tb_temporaria
    ON tb_final.index_op = tb_temporaria.index_op
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

# COMMAND ----------

# Verifica qtd de linhas no dataframe 
row_count = spark.sql("SELECT COUNT(*) FROM bronze.bronze_credit_card_fraud").collect()[0][0]
print(f"Total de linhas na tabela bronze_credit_card_fraud: {row_count}")

# COMMAND ----------

# Atualização do timestamp da última execução
last_run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"Processo concluído. Novo timestamp da última execução: {last_run_timestamp}")

# COMMAND ----------

spark.sql("""
DESCRIBE FORMATTED bronze.bronze_credit_card_fraud
""").show(truncate=False)

# COMMAND ----------

# Ver informações sobre partições
spark.sql("SHOW PARTITIONS bronze.bronze_credit_card_fraud").show()

# COMMAND ----------

#Snapshot
spark.sql("DESCRIBE HISTORY bronze.bronze_credit_card_fraud")

# COMMAND ----------

#Snapshot 0 (Quando cria a tabela)
spark.sql("SELECT * FROM bronze.bronze_credit_card_fraud VERSION AS OF 0")

# COMMAND ----------

#Snapshot 1 (Quando realiza o merge da tabela com o CSV)
spark.sql("SELECT * FROM bronze.bronze_credit_card_fraud VERSION AS OF 1")