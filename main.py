import pandas as pd
import requests
import mysql.connector
from datetime import datetime

# Extração - Buscar dados da API CoinGecko
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",  # Converte os preços para dólares
    "order": "market_cap_desc",  # Ordena por valor de mercado (maiores primeiro)
    "per_page": 15,  # Pegamos apenas as 5 principais criptomoedas
    "page": 1,  # Primeira página de resultados
    "sparkline": False  # Não inclui gráficos de tendência
}

response = requests.get(url, params=params) # Envia a requisição HTTP para a API com os parâmetros definidos.

if response.status_code == 200: # 200 = ok
    data = response.json() # Converte a resposta JSON para um dicionário
else:
    print({response.status_code}) # printa o status do problema
    exit()  # Encerra o programa caso a API falhe


cripto_df = pd.DataFrame(data, columns=["symbol", "name", "current_price", "market_cap"]) #cria um dataframe com os nomes que tem os cabeçalhos das coins na api

cripto_df.rename(columns={
    "symbol": "simbolo",
    "name": "nome",
    "current_price": "preco",
    "market_cap": "valor_mercado"
}, inplace=True) #renomeia todas para as do MySQL,  inplace=True) == alterar o original

cripto_df["ultima_atualizacao"] = datetime.now()  # Adiciona a data e hora da atualização

try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="cripto_info"
    )
    cursor = conn.cursor() #Aciona a conexão e cria um cursor
    
    for _, row in cripto_df.iterrows(): #Aqui em cada linha ocorre a iteração dos dados definidos em cima
        cursor.execute("""
        INSERT INTO cripto_precos (simbolo, nome, preco, valor_mercado, ultima_atualizacao)
        VALUES (%s, %s, %s, %s, %s)
    """, (row["simbolo"], row["nome"], row["preco"], row["valor_mercado"], row["ultima_atualizacao"]))
    conn.commit()#Acionar
    print("Dados inseridos com sucesso!")

except mysql.connector.Error as err:
    print(f"Erro: {err}")

finally:
    cursor.close()
    conn.close()

