# src/extract_historical_data.py
import pandas as pd
import requests
import os
import time 
from utils.logger import setup_logger

logger = setup_logger('HistoricalDataExtractor')

TOKEN = "ea94c3c2b68857d0dab35d8fae9faf4ba8c97c90" 

headers = {"Authorization": f"Token {TOKEN}"}
url = "https://api.brasil.io/v1/dataset/covid19/caso/data/"

data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
output_filename = "dados_covid_brasil_io.csv" 
output_filepath = os.path.join(data_dir, output_filename)

os.makedirs(data_dir, exist_ok=True)

MAX_PAGES = 40

logger.info("Iniciando a extração de dados da API do Brasil.IO...")
logger.info(f"Isso pode levar alguns minutos (limitado a {MAX_PAGES} páginas)...")

all_data = []
page_num = 1
while url and page_num <= MAX_PAGES:
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status() 
        data = response.json()
        all_data.extend(data["results"])
        url = data.get("next")
        if url and page_num < MAX_PAGES:
            logger.info(f"Página {page_num} buscada com sucesso. Indo para a próxima...")
            page_num += 1
            time.sleep(1.5)
        elif page_num == MAX_PAGES:
            logger.info(f"ATINGIU O LIMITE DE {MAX_PAGES} PÁGINAS. Parando a extração.")
            url = None
        else:
            logger.info(f"Página {page_num} buscada com sucesso. Última página.")
            url = None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao buscar dados da API: {e}")
        if hasattr(e, 'response') and e.response.status_code == 429:
            logger.error("Rate limit excedido (429). A extração será finalizada com os dados coletados até agora.")
        url = None

if all_data:
    logger.info("Extração concluída! Convertendo para DataFrame e salvando...")
    df = pd.DataFrame(all_data)
    df.to_csv(output_filepath, index=False)
    logger.info(f"SUCESSO! {len(df)} registros salvos em '{output_filepath}'")
else:
    logger.warning("Nenhum dado foi coletado. Verifique seu token, conexão e o status da API.")