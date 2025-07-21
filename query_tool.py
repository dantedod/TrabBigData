# query_tool.py (VERSÃO FINAL CORRIGIDA)

from pymongo import MongoClient
from config import (
    MONGO_CONNECTION_STRING,
    MONGO_DATABASE,
    MONGO_COLLECTION_MAIN,
    MONGO_COLLECTION_QUERIES,
)
import datetime


def get_mongo_collections():
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client[MONGO_DATABASE]
    return db[MONGO_COLLECTION_MAIN], db[MONGO_COLLECTION_QUERIES]


def query_tool():
    main_collection, queries_collection = get_mongo_collections()
    print("\n--- Ferramenta de Consulta de Dados de COVID ---")
    while True:
        try:
            threshold_str = input("\nDigite o número mínimo de mortes (ou 'sair'): ")
            if threshold_str.lower() == "sair":
                break
            threshold = int(threshold_str)

            query = {"deaths": {"$gt": threshold}}

            # CORREÇÃO 1: Pedir "confirmed" em vez de "totalCases"
            projection = {"_id": 0, "state": 1, "deaths": 1, "confirmed": 1, "date": 1}
            results = list(main_collection.find(query, projection).sort("deaths", -1))

            if not results:
                print(f"Nenhum estado com mais de {threshold} mortes.")
            else:
                print("\n--- Resultado ---")
                for r in results:
                    # CORREÇÃO 2: Acessar r['confirmed'] e ajustar o texto do print
                    print(
                        f"Estado: {r['state']}, Mortes: {r['deaths']}, Casos Confirmados: {r['confirmed']}, Data: {r['date']}"
                    )
                print("-----------------\n")

                if input("Salvar esta consulta no banco? (s/n): ").lower() == "s":
                    query_doc = {
                        "query_description": f"Estados com mais de {threshold} mortes",
                        "query_threshold": threshold,
                        "timestamp": datetime.datetime.now(datetime.UTC),
                        "results": results,
                    }
                    queries_collection.insert_one(query_doc)
                    print("Consulta salva com sucesso!")
        except KeyError as e:
            # Tratamento de erro mais específico para a gente saber a causa
            print(
                f"Ocorreu um erro de chave: o campo {e} não foi encontrado no documento."
            )
        except Exception as e:
            print(f"Ocorreu um erro: {e}")


if __name__ == "__main__":
    if MONGO_CONNECTION_STRING == "SUA_CONNECTION_STRING_AQUI":
        print("ATENÇÃO: Configure sua MONGO_CONNECTION_STRING no arquivo config.py")
    else:
        query_tool()
