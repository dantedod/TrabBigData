Projeto: Pipeline de Dados COVID-19 Brasil

Este projeto implementa um pipeline de dados para coletar, processar, transmitir e consultar informações sobre a COVID-19 no Brasil, utilizando Python, Apache Kafka, Apache Spark e MongoDB.

## Estrutura do Projeto

```
├── data/
│   └── dados_covid_brasil_io.csv      # Dados históricos extraídos da API Brasil.IO
├── src/
│   ├── extract_historical_data.py     # Extrai dados da API Brasil.IO e salva em CSV
│   ├── spark_kafka_producer.py        # Processa dados com Spark e envia para o Kafka
│   ├── kafka_consumer.py              # Consome dados do Kafka e armazena no MongoDB
│   ├── mongo_query_tool.py            # Ferramenta de consulta aos dados no MongoDB
│   ├── config.py                      # Configurações de conexão (Kafka, MongoDB)
│   └── utils/
│       └── logger.py                  # Utilitário de logging
├── requirements.txt                   # Dependências Python
├── docker-compose.yaml                # Sobe Kafka e Zookeeper via Docker
└── README.md                          # Este arquivo
```

## Pré-requisitos

- Python 3.7+
- Docker e Docker Compose
- Conta no MongoDB Atlas (ou instância local)

## Instalação

1. **Clone o repositório:**
   ```bash
   git clone <url-do-repo>
   cd TrabBigData-master
   ```

2. **Instale as dependências Python:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure o MongoDB:**
   - Edite o arquivo `src/config.py` e insira sua string de conexão do MongoDB Atlas em `MONGO_CONNECTION_STRING`.

## Subindo o Kafka com Docker

Na raiz do projeto, execute:
```bash
docker-compose up -d
```
Isso irá iniciar os serviços do Zookeeper e do Kafka.

## Pipeline de Execução

1. **Extração dos dados históricos:**
   ```bash
   python src/extract_historical_data.py
   ```
   - Extrai dados da API Brasil.IO e salva em `data/dados_covid_brasil_io.csv`.

2. **Processamento e envio para o Kafka:**
   ```bash
   python src/spark_kafka_producer.py
   ```
   - Lê o CSV, processa com Spark e envia os dados para o tópico Kafka.

3. **Consumo e armazenamento no MongoDB:**
   ```bash
   python src/kafka_consumer.py
   ```
   - Consome mensagens do Kafka e armazena no MongoDB.

4. **Consulta aos dados no MongoDB:**
   ```bash
   python src/mongo_query_tool.py
   ```
   - Permite consultar e salvar queries sobre os dados armazenados.

## Dependências

- requests
- pandas
- kafka-python
- pymongo[srv]
- pyspark

## Observações

- O arquivo `data/dados_covid_brasil_io.csv` pode ser grande. Ele contém os dados históricos extraídos da API Brasil.IO.
- O utilitário de logging (`src/utils/logger.py`) padroniza logs em todos os scripts.
- As configurações de conexão (Kafka, MongoDB) estão centralizadas em `src/config.py`.

## Contato

Dúvidas ou sugestões? Abra uma issue ou entre em contato com os autores do projeto.

## Instalação de Dependências

Para instalar todas as dependências necessárias, execute:

```bash
pip install -r requirements.txt
```

O arquivo `requirements.txt` inclui:

- requests
- pandas
- kafka-python
- pymongo[srv]
- pyspark
