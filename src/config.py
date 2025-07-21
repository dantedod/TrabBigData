# src/config.py

# --- Configurações do Kafka ---
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "dados_covid"

# --- Configurações do MongoDB Atlas ---
# IMPORTANTE: Substitua pela sua string de conexão do MongoDB Atlas
# Exemplo: "mongodb+srv://meu_usuario:minha_senha@meucluster.12345.mongodb.net/?retryWrites=true&w=majority"
MONGO_CONNECTION_STRING = "mongodb+srv://danted2300:wjAaKvRPndv9elXS@cluster0.hpxattc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DATABASE = "db_covid_brasil"
MONGO_COLLECTION_MAIN = "casos_por_estado"
MONGO_COLLECTION_QUERIES = "consultas_salvas"