import pandas as pd
import requests
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.conexao_mysql import conectar_mysql
import logging

logging.basicConfig(
    filename = "log_execucao.txt",
    level = logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",  # Formato do log
)

# Extração - Buscar dados da API CoinGecko
def coletar_dados():
    logging.info("Inicializando coleta de CRIPTO DATA")
    
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",  # Converte os preços para dólares
        "order": "market_cap_desc",  # Ordena por valor de mercado (maiores primeiro)
        "per_page": 15,  # Pegamos apenas as 5 principais criptomoedas
        "page": 1,  # Primeira página de resultados
        "sparkline": False  # Não inclui gráficos de tendência
    }

    try:
        response = requests.get(url, params=params) # Envia a requisição HTTP para a API com os parâmetros definidos.
        response.raise_for_status() 
        data = response.json() # Converte a resposta JSON para um dicionário
        logging.info("Coleta de CRIPTO BEM-sucedida.")
    except requests.exceptions.RequestException as e:
        logging.error("Coleta de CRIPTO MAL-sucedida. ERROR = {e}")
        return  # Encerra a função sem continuar


    cripto_df = pd.DataFrame(data, columns=["symbol", "name", "current_price", "market_cap"]) #cria um dataframe com os nomes que tem os cabeçalhos das coins na api

    cripto_df.rename(columns={
        "symbol": "simbolo",
        "name": "nome",
        "current_price": "preco",
        "market_cap": "valor_mercado"
    }, inplace=True) #renomeia todas para as do MySQL,  inplace=True) == alterar o original

    cripto_df["ultima_atualizacao"] = datetime.now()  # Adiciona a data e hora da atualização

    # Conectar ao MySQL e inserir os dados
    conn = conectar_mysql()
    if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("TRUNCATE TABLE cripto_precos;")
                for _, row in cripto_df.iterrows():
                    cursor.execute("""
                        INSERT INTO cripto_precos (simbolo, nome, preco, valor_mercado, ultima_atualizacao)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (row["simbolo"], row["nome"], row["preco"], row["valor_mercado"], row["ultima_atualizacao"]))
                conn.commit()
                logging.info("Dados inseridos com sucesso.")
            except Exception as err:
                logging.info("Falha ao inserir dados: {err}.")
            finally:
                cursor.close()
                conn.close()
                logging.info("Conexão ao banco de dados finalizada.")
                logging.info("-------------------------------------")
                
# Execução da função
if __name__ == "__main__":
    coletar_dados()


