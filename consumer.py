# consumer.py
import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from config import (
    KAFKA_SERVER,
    KAFKA_TOPIC,
    MONGO_CONNECTION_STRING,
    MONGO_DATABASE,
    MONGO_COLLECTION_MAIN,
)


def get_mongo_collection():
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client[MONGO_DATABASE]
    return db[MONGO_COLLECTION_MAIN]


def consume_and_store():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    collection = get_mongo_collection()
    print("Ouvindo o tópico do Kafka... Pressione Ctrl+C para parar.")
    for message in consumer:
        record = message.value
        state = record["state"]
        collection.update_one({"state": state}, {"$set": record}, upsert=True)
        print(f"Registro de {state} salvo/atualizado no MongoDB.")


if __name__ == "__main__":
    if MONGO_CONNECTION_STRING == "SUA_CONNECTION_STRING_AQUI":
        print("ATENÇÃO: Configure sua MONGO_CONNECTION_STRING no arquivo config.py")
    else:
        consume_and_store()
