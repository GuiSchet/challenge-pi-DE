import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import logging
#import schedule
import time

# Configuración del logging.
logging.basicConfig(filename='process.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Parámetros de conexión a la base de datos.
# Modificar que las contraseñas se guarden en dotenv y .gitignore
DATABASE_URL = 'mssql+pyodbc://SA:YourStrong!Passw0rd@localhost:1433/YourDataBase?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'
engine = create_engine(DATABASE_URL)

# URL del archivo CSV
CSV_URL = 'https://adlssynapsetestfrancis.blob.core.windows.net/challenge/nuevas_filas.csv?sp=r&st=2024-01-02T18:53:16Z&se=2025-01-01T02:53:16Z&spr=https&sv=2022-11-02&sr=b&sig=hIwTpJsf1xZCR45bu5YTNUxiKgpxqudWecn7VeNiR2s%3D'

def download_csv(url, filename='nuevas_filas.csv'):
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(filename, 'wb') as file:
            file.write(response.content)
        logging.info(f'CSV descargado y guardado como: {filename}.')
    except Exception as e:
        logging.error(f'Error al descargar: {e}')

def insert_data_to_db(filename='nuevas_filas.csv'):
    try:
        # Leer el archivo CSV
        df = pd.read_csv(filename)
        logging.info(f'Archivo CSV en pandas correctamente.')

        # columna FECHA_COPIA con la fecha actual
        df['FECHA_COPIA'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Insertar los datos en la base de datos
        with engine.connect() as connection:
            result = pd.read_sql('SELECT COUNT(*) FROM unificado', con=connection)
            rows_before = result.iloc[0,0]
            df.to_sql('unificado', con=connection, if_exists='append', index=False)
            result = pd.read_sql('SELECT COUNT(*) FROM unificado', con=connection)
            rows_after = result.iloc[0,0]

            added_rows = rows_after - rows_before
            logging.info(f'Se agregaron {added_rows} filas a la bd.')
        # Verificar que la cantidad de filas del archivo coincide con la diferencia de filas de la tabla.
        if added_rows == len(df):
            logging.info('Todas las filas se cargaron correctamente.')
        else:
            logging.error('Discrepancia en el número de filas cargadas.')
        logging.info(f'Datos insertados en la tabla Unificado correctamente.')
    except SQLAlchemyError as e:
        logging.error(f'Error al insertar los datos en la base de datos: {e}')
    except Exception as e:
        logging.error(f'Error al leer o procesar el archivo CSV: {e}')

# Una opción para programar el proceso podría ser con schedule.
# Programar el proceso para que se ejecute los lunes a las 5:00 AM
# schedule.every().monday.at("07:00").do(job)
def job():
    logging.info('Iniciando el proceso de actualización de datos.')
    download_csv(CSV_URL)
    insert_data_to_db()
    logging.info('Proceso completado.')


if __name__ == "__main__":
    job()
    #while True:
        #schedule.run_pending()
        #time.sleep(60)  # Espera 1 minuto
        
