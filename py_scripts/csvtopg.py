import os
import logging
import psycopg2
import traceback
import numpy as np
import pandas as pd
import urllib.request

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

postgres_host = os.environ.get('localhost')
postgres_database = os.environ.get('postgres')
postgres_user = os.environ.get('lumiana')
postgres_password = os.environ.get('liana135')
postgres_port = os.environ.get('5432')

destination_path = 'csv_files/churn_modelling.csv'
url = "https://raw.githubusercontent.com/dogukannulu/datasets/master/Churn_Modelling.csv"

try:
    conn = psycopg2.connect(
        host='localhost',
        database='postgres',
        user='lumiana',
        password='liana135',
        port=5432
    )

    cur = conn.cursor()
    logging.info('Postgres server connection is successful')
except Exception as e:
    traceback.print_exc()
    logging.error("Couldn't create the Postgres connection")


def download_file_from_url(url: str, destination_path: str):
    dest_folder = os.path.dirname(destination_path)
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    try:
        urllib.request.urlretrieve(url, destination_path)
        logging.info('CSV file downloaded successfully to the working directory')
    except Exception as e:
        logging.error(f'Error while downloading the CSV file due to: {e}')
        traceback.print_exc()


def download_file_from_url_main():
    download_file_from_url(url, destination_path)


def create_postgres_table():
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling (RowNumber INTEGER PRIMARY KEY, CustomerId INTEGER, 
        Surname VARCHAR(50), CreditScore INTEGER, Geography VARCHAR(50), Gender VARCHAR(20), Age INTEGER, 
        Tenure INTEGER, Balance FLOAT, NumOfProducts INTEGER, HasCrCard INTEGER, IsActiveMember INTEGER, EstimatedSalary FLOAT, Exited INTEGER)""")

        logging.info(' New table churn_modelling created successfully to postgres server')
    except:
        logging.warning(' Check if the table churn_modelling exists')


def write_to_postgres():
    df = pd.read_csv(destination_path)
    inserted_row_count = 0

    for _, row in df.iterrows():
        count_query = f"""SELECT COUNT(*) FROM churn_modelling WHERE RowNumber = {row['RowNumber']}"""
        cur.execute(count_query)
        result = cur.fetchone()

        if result[0] == 0:
            inserted_row_count += 1
            cur.execute("""INSERT INTO churn_modelling (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, 
            Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)""",
                        (int(row[0]), int(row[1]), str(row[2]), int(row[3]), str(row[4]), str(row[5]), int(row[6]),
                         int(row[7]), float(row[8]), int(row[9]), int(row[10]), int(row[11]), float(row[12]),
                         int(row[13])))

    logging.info(f' {inserted_row_count} rows from csv file inserted into churn_modelling table successfully')


def write_csv_to_postgres_main():
    download_file_from_url(url, destination_path)
    create_postgres_table()
    write_to_postgres()
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    download_file_from_url(url, destination_path)
    create_postgres_table()
    write_to_postgres()
    conn.commit()
    cur.close()
    conn.close()


def csvtopg_main():
    return None