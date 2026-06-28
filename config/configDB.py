import psycopg2 as pg
import os
from dotenv import load_dotenv
from psycopg2 import Error

load_dotenv()

def conn():
    senha = os.getenv('LENON_DB_PASSWORD')
    user = os.getenv('LENON_DB_USER')
    host = os.getenv('LENON_DB_HOST')
    port = os.getenv('LENON_DB_PORT')
    database = os.getenv('LENON_DB_NAME')
    try:
        connection = pg.connect(
            user=user,
            password=senha,
            host=host,
            port=port,
            database=database
        )
        print("Connection to PostgreSQL successful")
        return connection
    except Error as e:
        print(f"Error while connecting to PostgreSQL: {e}")
        return None

def encerra_conexao(connection):
    if connection:
        connection.close()
        print("PostgreSQL connection closed")