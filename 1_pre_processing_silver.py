# Databricks notebook source
# Bibliotecas
from pyspark.sql.functions import regexp_replace, col, split, to_date, date_format

# COMMAND ----------

# Leitura dos dados na camada bronze
df_bronze = spark.table("bronze.bronze_credit_card_fraud")

# COMMAND ----------

# Aplicação de 3 transformações de qualidade na camada silver
df_silver = (
    df_bronze

    # 1- Transformação (casting das colunas de longitude e latitude para inteiro)

    .withColumn("latitude", split(col("latitude"), r"\.")[0].cast("int"))
    .withColumn("longitude", split(col("longitude"), r"\.")[0].cast("int"))

    .withColumn("merch_lat", split(col("merch_lat"), r"\.")[0].cast("int"))
    .withColumn("merch_long", split(col("merch_long"), r"\.")[0].cast("int"))

    # 2- Transformação (mudar o tipo da coluna de data de aniversário para date)
    .withColumn("dob", col("dob").cast("date"))
                
    # 3- Transformação (renomeando nome da coluna de aniversário)
    .withColumnRenamed("dob", "dt_birthday")

    # 4- Transformação da coluna trans_date_trans_time
   .withColumn("trans_date_trans_time", date_format(col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss"))
)

# COMMAND ----------

# Salvar base na camada silver
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.silver_credit_card_fraud")

# COMMAND ----------

# Consulta na base da camada silver
display(df_silver)

# COMMAND ----------

# Consultando a tabela usando partição

spark.sql("SELECT merchant FROM silver.silver_credit_card_fraud WHERE category = 'grocery_pos'").show()

# COMMAND ----------

#Snapshot 0 (Quando insere dados usando a tabela bronze como fonte na camada silver com os devidos tratamentos)
spark.sql("SELECT * FROM silver.silver_credit_card_fraud VERSION AS OF 0")