# Databricks notebook source
# Dados agregados por comerciante para consumo pelo time de negócio
df_gold = spark.sql("""
    SELECT 
        merchant, 
        category, 
        CAST(SUM(amt) AS DECIMAL(18,2)) AS total_amount_trans, 
        COUNT(*) AS total_trans, 
        CAST(AVG(city_pop) AS DECIMAL(18,2)) AS avg_city_pop, 
        SUM(is_fraud) AS sum_is_fraud
    FROM silver.silver_credit_card_fraud
    GROUP BY merchant, category
""")

display(df_gold)

# COMMAND ----------

# Utilizar um dos Formatos Open Data (Delta Lake, Apache Iceberg ou Apache Hudi) para armazenar a tabela final
df_gold.write \
    .format("iceberg") \
    .mode("overwrite") \
    .saveAsTable("gold.gold_credit_card_fraud")

# COMMAND ----------

# Demonstrar uma operação de UPSERT (Update ou Insert) ou DELETE na tabela final

# Adicionando um novo comerciante (INSERT)
spark.sql("""
    INSERT INTO gold.gold_credit_card_fraud VALUES
    ('LvC S.A.', 'travel', 20000.00, 4, 3000.00, 0)
""")

print("Comerciante LVC S.A. adicionado na tabela final!")

# COMMAND ----------

# Removendo um comerciante
spark.sql("""
    DELETE FROM gold.gold_credit_card_fraud WHERE merchant = 'LSC S.A.'
""")

print("Comerciante LSC S.A. removido da tabela final!")

# COMMAND ----------

# Atualizando os dados de um comerciante
spark.sql("""
    UPDATE gold.gold_credit_card_fraud
    SET category = 'tecnology'
    WHERE merchant = 'Huel-Langworth'
""")

print("Dados do comerciante Huel-Langworth atualizado!!")

display(spark.sql("SELECT * FROM gold.gold_credit_card_fraud WHERE merchant = 'Huel-Langworth'"))

# COMMAND ----------

#Snapshot 0 (Quando insere dados usando a tabela silver como fonte na camada gold com as visões agregadas)
spark.sql("SELECT * FROM gold.gold_credit_card_fraud VERSION AS OF 0")

# COMMAND ----------

#Snapshot 1 (Quando insere um comerciante novo na tabela gold)
spark.sql("SELECT * FROM gold.gold_credit_card_fraud VERSION AS OF 1")

# COMMAND ----------

#Snapshot 2 (Quando insere outro comerciante novo na tabela gold)
spark.sql("SELECT * FROM gold.gold_credit_card_fraud VERSION AS OF 2")

# COMMAND ----------

#Snapshot 3 (Quando deleta um comerciante da tabela gold)
spark.sql("SELECT * FROM gold.gold_credit_card_fraud VERSION AS OF 3")

# COMMAND ----------

#Snapshot 3 (Quando atualiza os dados de um comerciante na tabela gold)
spark.sql("SELECT * FROM gold.gold_credit_card_fraud VERSION AS OF 4")