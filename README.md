# Data Lakehouse Transacional 

## Objetivo

Criação de um pipeline de dados que simula a ingestão e o processamento de dados transacionais, aplicando as técnicas de pré-processamento e os formatos Open Data para garantir ACIDidade (Atomicidade, Consistência, Isolamento, Durabilidade).

## Arquitetura

Fonte de Dados → Bronze → Silver → Gold → BI

---

## Bronze
- Ingestão de dados brutos
- Sem tratamento
- Dados no formato original

---

## Silver
- Limpeza de dados
- Conversão de tipos
- Padronização de colunas

Exemplo:
- latitude: STRING → DOUBLE
- dob: TIMESTAMP → DATE

---

##  Gold
- Dados agregados
- Métricas de negócio

KPIs:
- Total de transações
- Volume financeiro
- Soma de fraudes

---

## Tecnologias
- Databricks (Serverless)
- Delta Lake
- PySpark / SQL

---

## 🔒 ACID e Versionamento

O projeto utiliza Delta Lake garantindo:

- Atomicidade
- Consistência
- Isolamento
- Durabilidade

