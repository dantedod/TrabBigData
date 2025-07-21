# src/mongo_query_tool.py
from pymongo import MongoClient
from src.config import ( 
    MONGO_CONNECTION_STRING,
    MONGO_DATABASE,
    MONGO_COLLECTION_MAIN,
    MONGO_COLLECTION_QUERIES,
)
from datetime import datetime, timezone 
from src.utils.logger import setup_logger

logger = setup_logger('MongoQueryTool')

def get_mongo_collections():
    """Conecta ao MongoDB e retorna as coleções principal e de queries."""
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE]
        logger.info(f"Conectado ao MongoDB: DB='{MONGO_DATABASE}'")
        return db[MONGO_COLLECTION_MAIN], db[MONGO_COLLECTION_QUERIES]
    except Exception as e:
        logger.error(f"Erro ao conectar ao MongoDB: {e}")
        raise

def query_tool():
    """Permite ao usuário consultar os dados de COVID no MongoDB e salvar consultas."""
    main_collection, queries_collection = get_mongo_collections()
    logger.info("\n--- Ferramenta de Consulta de Dados de COVID ---")
    while True:
        try:
            threshold_str = input("\nDigite o número mínimo de mortes (ou 'sair'): ")
            if threshold_str.lower() == "sair":
                logger.info("Saindo da ferramenta de consulta.")
                break
            
            try:
                threshold = int(threshold_str)
            except ValueError:
                logger.warning("Entrada inválida. Por favor, digite um número ou 'sair'.")
                continue

            query = {"deaths": {"$gt": threshold}}

            projection = {"_id": 0, "state": 1, "deaths": 1, "confirmed": 1, "date": 1}
            
            results = list(main_collection.find(query, projection).sort("deaths", -1))

            if not results:
                logger.info(f"Nenhum estado com mais de {threshold} mortes encontrado.")
            else:
                logger.info("\n--- Resultado da Consulta ---")
                for r in results:
                    state = r.get('state', 'N/A')
                    deaths = r.get('deaths', 'N/A')
                    confirmed = r.get('confirmed', 'N/A')
                    date = r.get('date', 'N/A')
                    print(f"Estado: {state}, Mortes: {deaths}, Casos Confirmados: {confirmed}, Data: {date}")
                logger.info("----------------------------\n")

                save_query = input("Salvar esta consulta no banco? (s/n): ").lower()
                if save_query == "s":
                    query_doc = {
                        "query_description": f"Estados com mais de {threshold} mortes",
                        "query_threshold": threshold,
                        "timestamp": datetime.now(timezone.utc), 
                        "results": results,
                    }
                    queries_collection.insert_one(query_doc)
                    logger.info("Consulta salva com sucesso!")
        
        except Exception as e:
            logger.error(f"Ocorreu um erro na ferramenta de consulta: {e}", exc_info=True)


if __name__ == "__main__":
    if MONGO_CONNECTION_STRING == "YOUR_MONGODB_ATLAS_CONNECTION_STRING":
        logger.critical("ATENÇÃO: Configure sua MONGO_CONNECTION_STRING no arquivo src/config.py antes de executar.")
    else:
        query_tool()