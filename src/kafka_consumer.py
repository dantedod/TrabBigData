# src/kafka_consumer.py
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
from utils.logger import setup_logger

logger = setup_logger('KafkaConsumer')

def get_mongo_collection():
    """Conecta ao MongoDB e retorna a coleção principal."""
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE]
        logger.info(f"Conectado ao MongoDB: DB='{MONGO_DATABASE}', Coleção='{MONGO_COLLECTION_MAIN}'")
        return db[MONGO_COLLECTION_MAIN]
    except Exception as e:
        logger.error(f"Erro ao conectar ao MongoDB: {e}")
        raise

def consume_and_store():
    """Consome mensagens do Kafka e as armazena no MongoDB."""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id='covid_data_consumer_group',
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            api_version=(0, 10, 1)
        )
        collection = get_mongo_collection()
        logger.info(f"Ouvindo o tópico do Kafka '{KAFKA_TOPIC}'... Pressione Ctrl+C para parar.")
        
        for message in consumer:
            record = message.value
            
            state = record.get("state")
            date = record.get("date")
            
            if state and date:
                collection.update_one({"state": state, "date": date}, {"$set": record}, upsert=True)
                logger.info(f"Registro de {state} ({date}) salvo/atualizado no MongoDB.")
            else:
                logger.warning(f"Registro inválido recebido do Kafka (sem 'state' ou 'date'): {record}")

    except KeyboardInterrupt:
        logger.info("Consumidor interrompido pelo usuário.")
    except Exception as e:
        logger.critical(f"Erro fatal no consumidor Kafka: {e}", exc_info=True)
    finally:
        if 'consumer' in locals() and consumer:
            consumer.close()
            logger.info("Conexão com o Kafka consumer fechada.")
        if 'client' in locals() and client:
            client.close()
            logger.info("Conexão com o MongoDB fechada.")

if __name__ == "__main__":
    if MONGO_CONNECTION_STRING == "YOUR_MONGODB_ATLAS_CONNECTION_STRING":
        logger.critical("ATENÇÃO: Configure sua MONGO_CONNECTION_STRING no arquivo src/config.py antes de executar.")
    else:
        consume_and_store()