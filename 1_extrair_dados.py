# 1_extrair_dados.py
import pandas as pd
import requests

# --- INFORME SEU TOKEN DE AUTENTICAÇÃO AQUI ---
TOKEN = "ea94c3c2b68857d0dab35d8fae9faf4ba8c97c90	"
# ---------------------------------------------

if TOKEN == "SEU_TOKEN_AQUI":
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("!!! ATENÇÃO: Substitua 'SEU_TOKEN_AQUI' pelo seu token !!!")
    print("!!! do Brasil.IO antes de executar.                  !!!")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    exit()

headers = {"Authorization": f"Token {TOKEN}"}
url = "https://api.brasil.io/v1/dataset/covid19/caso/data/"
output_filename = "dados_covid_brasil_io.csv"

print("Iniciando a extração de dados da API do Brasil.IO...")
print("Isso pode levar alguns minutos...")

all_data = []
page_num = 1
while url:
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        all_data.extend(data["results"])
        url = data.get("next")
        if url:
            print(f"Página {page_num} buscada com sucesso. Indo para a próxima...")
            page_num += 1
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar dados: {e}")
        url = None

if all_data:
    print("\nExtração concluída! Convertendo para DataFrame e salvando...")
    df = pd.DataFrame(all_data)
    df.to_csv(output_filename, index=False)
    print(f"SUCESSO! {len(df)} registros salvos em '{output_filename}'")
else:
    print("Nenhum dado foi coletado. Verifique seu token e conexão.")
