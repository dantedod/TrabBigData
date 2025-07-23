# src/spark_kafka_producer.py
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, to_date
from pyspark.sql.types import IntegerType, DateType
from kafka import KafkaProducer
from config import KAFKA_SERVER, KAFKA_TOPIC
import os
from utils.logger import setup_logger

logger = setup_logger('SparkKafkaProducer')

def create_spark_session():
    """Cria e retorna uma sessão Spark."""
    logger.info("Criando SparkSession...")
    spark = SparkSession.builder.appName("CovidETLProducer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession criada.")
    return spark


def create_kafka_producer():
    """Cria e retorna um produtor Kafka."""
    logger.info(f"Criando KafkaProducer para {KAFKA_SERVER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(0, 10, 1)
        )
        logger.info("KafkaProducer criado.")
        return producer
    except Exception as e:
        logger.error(f"Erro ao criar KafkaProducer: {e}")
        raise


def process_data_and_send(spark, kafka_producer):
    """Lê os dados do CSV, processa com Spark e envia para o Kafka."""
    csv_filename = "data/dados_covid_brasil_io.csv"
    csv_filepath = os.path.join(os.path.dirname(os.path.dirname(__file__)), csv_filename)

    logger.info(f"Lendo o arquivo CSV: {csv_filepath}...")
    if not os.path.exists(csv_filepath):
        logger.critical(f"Erro: Arquivo CSV '{csv_filepath}' não encontrado. Por favor, execute 'extract_historical_data.py' primeiro.")
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {csv_filepath}")

    df = spark.read.csv(csv_filepath, header=True, inferSchema=True)
    logger.info("CSV lido. Esquema inferido:")
    df.printSchema()

    logger.info("Convertendo tipos de dados para numérico e data...")
    df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
           .withColumn("deaths", col("deaths").cast(IntegerType())) \
           .withColumn("confirmed", col("confirmed").cast(IntegerType()))

    logger.info("Processando os dados para obter o último registro por estado (considerando data e city.isNull())...")
    
    df_estados = df.filter(col("city").isNull())

    df_latest_date = df_estados.groupBy("state").agg(
        spark_max("date").alias("latest_date")
    )

    logger.info("Juntando tabelas para selecionar os registros mais recentes por estado...")
    df_final = (
        df_estados.alias("e")
        .join(
            df_latest_date.alias("l"),
            (col("e.state") == col("l.state"))
            & (col("e.date") == col("l.latest_date")),
        )
        .select(col("e.state"), col("e.deaths"), col("e.confirmed"), col("e.date"))
    )

    logger.info("Dados processados. Enviando para o Kafka...")

    records = df_final.collect()
    total_records = len(records)
    logger.info(f"Total de {total_records} registros processados para envio.")

    for i, row in enumerate(records):
        record = row.asDict()

        if record.get("date"):
            record["date"] = record["date"].strftime("%Y-%m-%d")

        kafka_producer.send(KAFKA_TOPIC, record)
        logger.info(f"Enviado registro {i+1}/{total_records}: {record['state']} ({record['date']})")

    kafka_producer.flush()
    logger.info(
        f"\n{total_records} registros enviados com sucesso para o tópico '{KAFKA_TOPIC}'!"
    )


if __name__ == "__main__":
    try:
        spark = create_spark_session()
        producer = create_kafka_producer()

        process_data_and_send(spark, producer)

    except Exception as e:
        logger.critical(f"Erro fatal no produtor Spark/Kafka: {e}", exc_info=True)
    finally:
        if 'spark' in locals() and spark:
            spark.stop()
            logger.info("SparkSession parada.")
        if 'producer' in locals() and producer:
            producer.close()
            logger.info("Conexão com o Kafka producer fechada.")