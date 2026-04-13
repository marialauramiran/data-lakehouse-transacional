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
- latitude: STRING → INT
- rename: dob → dt_birthday
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

## Explicação dos códigos:

1- Coleta/Aquisição:

No notebook 0_ingestion_bronze, foi realizada a ingestão de dados na camada Bronze de uma arquitetura Medallion utilizando Delta Lake no Databricks. Inicialmente, é criada a sessão Spark e os schemas das camadas Bronze, Silver e Gold, organizando o ambiente de dados. Em seguida, é definida a tabela Bronze no formato Delta, particionada pela coluna de categoria, com o objetivo de armazenar os dados brutos de forma otimizada para leitura.

Os dados são lidos a partir de um arquivo CSV e carregados em um DataFrame, que é posteriormente transformado em uma view temporária para permitir manipulação via SQL. A carga na tabela Bronze é realizada por meio de uma operação MERGE, garantindo processamento incremental (upsert), evitando duplicidades e mantendo a consistência dos dados com base na chave index_op.

Após a ingestão, são realizadas validações como contagem de registros e registro do timestamp de execução, possibilitando controle e auditoria do processo. Também são consultadas informações da tabela, como schema, partições e histórico de versões, utilizando recursos do Delta Lake.

Por fim, o uso do Delta Lake garante propriedades ACID, versionamento dos dados (time travel) e melhor performance, tornando a camada Bronze confiável e preparada para alimentar as etapas seguintes do pipeline (Silver e Gold).

---

2- Pré-processamento:

No notebook 1_pre_processing_silver, realiza o processamento da camada Silver a partir dos dados da Bronze, aplicando transformações de qualidade e padronização. Inicialmente, os dados são lidos da tabela Bronze e, em seguida, passam por ajustes de tipo e formato.

As principais transformações incluem a conversão das colunas de latitude e longitude (do cliente e do estabelecimento) para valores inteiros, a alteração do tipo da coluna de data de nascimento para DATE e sua renomeação para dt_birthday. Também é realizada a padronização do campo de data e hora da transação (trans_date_trans_time) para um formato consistente.

Após o tratamento, os dados são gravados na camada Silver em formato Delta, substituindo a versão anterior da tabela. Por fim, são feitas consultas para validação dos dados, incluindo filtros por partição e uso de versionamento (time travel), garantindo rastreabilidade e consistência.

---

3- Persistência: 

Por fim, no notebook 2_final_gold, foi implementada na camada Gold do pipeline, responsável por disponibilizar dados agregados e prontos para consumo pelo time de negócio. Inicialmente, os dados da camada Silver são agregados por comerciante e categoria, gerando métricas como valor total transacionado, quantidade de transações, média da população das cidades e total de fraudes.

Em seguida, o resultado é armazenado como tabela final em formato aberto (Delta Lake), garantindo eficiência, confiabilidade e suporte a operações transacionais. A tabela Gold representa uma visão consolidada, otimizada para consultas e uso em ferramentas de BI.

O código também demonstra operações de manipulação de dados (INSERT, DELETE e UPDATE), simulando cenários reais de manutenção da tabela analítica, como inclusão de novos comerciantes, remoção de registros e atualização de informações.

Por fim, são realizadas consultas utilizando versionamento (time travel), permitindo visualizar diferentes estados da tabela ao longo do tempo. Isso garante rastreabilidade, auditoria e controle sobre as alterações realizadas na camada Gold.

