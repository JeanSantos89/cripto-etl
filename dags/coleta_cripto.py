import pandas as pd
import requests
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.conexao_mysql import conectar_mysql
import logging

# Definição de log base e formato
logging.basicConfig( 
    filename = "log_execucao.txt",
    level = logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",  
)

# 1) Extração - Buscar dados da API (CoinGecko)
def coletar_dados():
    logging.info("Inicializando coleta de CRIPTO DATA")
    
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",  # Converte os preços para dólares
        "order": "market_cap_desc",  # Ordena por ordem crescente de valor de mercado
        "per_page": 15,  # Apenas as 15 primeiras moedads
        "page": 1,  # Apenas a primeira página de resultados
        "sparkline": False  # Não inclui gráficos de tendência
    }

    # Enviar requisição para a API com os parâmetros (params)
    try:
        response = requests.get(url, params=params) # Envia a requisição HTTP para a API com os parâmetros definidos
        response.raise_for_status() # Checagem se ocorreu algum erro
        data = response.json() # Converte JSON para dicionário Python
        logging.info("Coleta de CRIPTO BEM-sucedida.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Coleta de CRIPTO MAL-sucedida. ERROR = {e}") # {e} = erro resultante
        return  # Encerra a função sem continuar


    cripto_df = pd.DataFrame(data, columns=["symbol", "name", "current_price", "market_cap"]) # Cria dataframe com os cabeçalhos da API

    cripto_df.rename(columns={
        "symbol": "simbolo",
        "name": "nome",
        "current_price": "preco",
        "market_cap": "valor_mercado"
    }, inplace=True) # Renomeia todas para a nomenclatura do MySQL,  (inplace=True == alterar o original)

    cripto_df["ultima_atualizacao"] = datetime.now()  # Adiciona a data e hora da última atualização

    # Conectar ao MySQL e inserir os dados
    conn = conectar_mysql()
    if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("TRUNCATE TABLE cripto_precos;") # Limpa os dados dos preços passados
                for _, row in cripto_df.iterrows(): #iteração sobre o dataframe
                    cursor.execute("""
                        INSERT INTO cripto_precos (simbolo, nome, preco, valor_mercado, ultima_atualizacao)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (row["simbolo"], row["nome"], row["preco"], row["valor_mercado"], row["ultima_atualizacao"]))
                conn.commit()
                logging.info("Dados inseridos com sucesso.")
            except Exception as err:
                logging.info(f"Falha ao inserir dados: {err}.")
            finally:
                cursor.close()
                conn.close()
                logging.info("Conexão ao banco de dados finalizada.")
                logging.info("-------------------------------------")
                
# Execução da função
if __name__ == "__main__":
    coletar_dados()


