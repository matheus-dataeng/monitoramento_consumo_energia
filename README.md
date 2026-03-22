# 📊 Monitoramento de Consumo de Energia no Brasil

## 📌 Visão Geral

Este projeto implementa um pipeline completo de dados para análise de consumo de energia no Brasil, seguindo a arquitetura de Data Lake (Arquitetura Medalhão):

- **Bronze** → dados brutos  
- **Silver** → dados tratados e validados  
- **Gold** → dados modelados para análise  

Além disso, o projeto disponibiliza uma **API REST** para consulta dos dados e métricas.

---

## 🧱 Arquitetura do Projeto

Pipeline ETL → Data Lake → Data Warehouse → API

### Camadas:

- **Bronze**
  - Extração do CSV
  - Armazenamento em formato Parquet

- **Silver**
  - Limpeza e transformação dos dados
  - Conversão de tipos
  - Validação de dados
  - Remoção de duplicados
  - Criação de features temporais

- **Gold**
  - Modelagem dimensional (Star Schema)
  - Criação de:
    - `dim_tempo`
    - `dim_subsistema`
    - `fato_carga_energia`

---

## ⚙️ Tecnologias Utilizadas

- Python
- Pandas
- PostgreSQL
- SQLAlchemy
- FastAPI
- Docker
- Apache Airflow (em desenvolvimento)

---

## 🔄 Pipeline de Dados

### 1. Extract
- Leitura de arquivo CSV
- Persistência na camada Bronze

### 2. Transform
- Padronização de colunas
- Conversão de tipos
- Validação de dados
- Criação de colunas temporais

### 3. Build Metrics
- Criação de dimensões e fato
- Persistência na camada Gold

### 4. Load
- Inserção dos dados no PostgreSQL
- Controle de duplicidade (DELETE + INSERT)

---

## 🗄️ Modelagem de Dados

### ⭐ Dimensão Tempo
- Id_tempo
- Data
- Ano
- Mês
- Dia
- Trimestre
- Dia da semana

### ⭐ Dimensão Subsistema
- Id_subsistema
- Sigla
- Subsistema

### ⭐ Fato Carga Energia
- Id_tempo
- Id_subsistema
- Carga_energia
- Data

---

## 🌐 API

A API permite consultar os dados e métricas:

### Rotas presentes na API:

- `/tempo`  
  Retorna registros da dimensão tempo (datas, ano, mês, dia, trimestre e dia da semana).

- `/tempo/{mes}`  
  Retorna os dados de carga de energia filtrados por mês (1 a 12).

- `/subsistema`  
  Retorna todos os subsistemas disponíveis (ex: Norte, Nordeste, Sudeste/Centro-Oeste, Sul).

- `/subsistema/{subsistema}`  
  Representa o nome da região. Exemplo: `Nordeste`.  
  Retorna a carga de energia associada ao subsistema informado.

- `/carga_energia`  
  Retorna registros da tabela fato de carga de energia.

- `/carga_energia/ids/{id_tempo}/{id_subsistema}`  
  Retorna dados específicos combinando um `Id_tempo` e um `Id_subsistema`.

- `/carga_energia/datas/{data_start}/{data_end}`  
  Retorna dados dentro de um intervalo de datas.  
  Exemplo: `2025-01-01` até `2025-03-31`.

- `/metricas/carga-por-subsistema/{subsistema}`  
  Retorna a carga total de energia agregada por subsistema.

- `/metricas/carga-media/{mes}`  
  Retorna a média de carga de energia para um determinado mês.

---

## 🚀 Como Executar o Projeto

### 1. Criar ambiente virtual

python -m venv venv
source venv/bin/activate

### 2. Instalar dependências

pip install -r requirements.txt

### 3. Configurar variáveis de ambiente (.env)

PG_USER=
PG_PASSWORD=
PG_HOST=
PG_PORT=
PG_DBNAME=
CAMINHO_ARQUIVO=

### 4. Rodar pipeline atraves do comando make disponibilizado pelo arquivo Makefile

make run-pipeline

### 5. Rodar API atraves do comando make disponibilizado pelo arquivo Makefile

make run-api

---

## 🧠 Boas Práticas Aplicadas

- Logging estruturado em todas as etapas do pipeline
- Separação por camadas (Bronze, Silver, Gold)
- Idempotência no processo de carga (DELETE + INSERT)
- Uso de variáveis de ambiente (.env)
- Organização modular do código

---

## 🐳 Containerização (Próximos Passos)

O projeto está sendo preparado para execução com Docker:

- API containerizada  
- PostgreSQL em container  
- Airflow para orquestração  

---

## ☁️ Deploy (Próximos Passos)

Planejamento de deploy em nuvem:

- Data Lake → AWS S3  
- API → AWS Lambda ou ECS  
- Banco → AWS RDS  
- Orquestração → Airflow  

---

## 📈 Possíveis Evoluções

- Dashboard para visualização (React ou BI)
- Autenticação na API
- Paginação e filtros avançados
- Uso de Spark para processamento distribuído
- Otimização de carga com COPY (PostgreSQL)
- CI/CD com GitHub Actions

---

## 👥 Colaboração

Projeto desenvolvido em equipe com foco em:

- Engenharia de Dados  
- DevOps    

---

## 📊 Dataset

Dados de consumo de energia elétrica no Brasil (ONS).

---

## 🎯 Objetivo

Construir um projeto completo de engenharia de dados, cobrindo:

- Pipeline ETL  
- Modelagem de dados  
- API  
- Orquestração  
- Deploy em nuvem  

---

## 📌 Status do Projeto

🚧 Em desenvolvimento — pipeline e API finalizados, iniciando containerização e deploy.

