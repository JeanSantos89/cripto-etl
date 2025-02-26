import mysql.connector

def conectar_mysql():
    try:
        conn = mysql.connector.connect(
            host="",
            user="",
            password="",
            database=""
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Erro na conexão: {err}")
        return None
