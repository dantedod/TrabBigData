# producer.py (VERSÃO FINALíssima CORRIGIDA)

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.types import IntegerType
from kafka import KafkaProducer
from config import KAFKA_SERVER, KAFKA_TOPIC


def create_spark_session():
    """Cria e retorna uma sessão Spark."""
    return SparkSession.builder.appName("CovidETLProducer").getOrCreate()


def create_kafka_producer():
    """Cria e retorna um produtor Kafka."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def process_data_and_send(spark, kafka_producer):
    """Lê os dados do CSV, processa com Spark e envia para o Kafka."""
    print("Lendo o arquivo CSV...")
    df = spark.read.csv("dados_covid_brasil_io.csv", header=True, inferSchema=True)

    print("Convertendo tipos de dados para numérico...")
    df = df.withColumn("deaths", col("deaths").cast(IntegerType()))
    df = df.withColumn("confirmed", col("confirmed").cast(IntegerType()))

    print("Processando os dados para obter o último registro por estado...")
    df_estados = df.filter(col("city").isNull())

    df_latest_date = df_estados.groupBy("state").agg(
        spark_max("date").alias("latest_date")
    )

    print("Juntando tabelas com alias para evitar ambiguidade...")
    df_final = (
        df_estados.alias("e")
        .join(
            df_latest_date.alias("l"),
            (col("e.state") == col("l.state"))
            & (col("e.date") == col("l.latest_date")),
        )
        .select(col("e.state"), col("e.deaths"), col("e.confirmed"), col("e.date"))
    )

    print("Dados processados. Enviando para o Kafka...")

    records = df_final.collect()
    total_records = len(records)

    for i, row in enumerate(records):
        record = row.asDict()

        # --- LINHA DE CORREÇÃO FINAL ---
        # Converte o objeto de data para uma string no formato ISO (AAAA-MM-DD)
        if record.get("date"):
            record["date"] = record["date"].isoformat()
        # -----------------------------

        kafka_producer.send(KAFKA_TOPIC, record)
        print(f"Enviado registro {i+1}/{total_records}: {record['state']}")

    kafka_producer.flush()
    print(
        f"\n{total_records} registros enviados com sucesso para o tópico '{KAFKA_TOPIC}'!"
    )


if __name__ == "__main__":
    spark = create_spark_session()
    producer = create_kafka_producer()

    process_data_and_send(spark, producer)

    spark.stop()
    producer.close()
