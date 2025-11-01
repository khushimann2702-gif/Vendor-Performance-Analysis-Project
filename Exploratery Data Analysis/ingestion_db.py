import pandas as pd
import os
import time
import logging
from sqlalchemy import create_engine

# Setup logging
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Create SQLite engine
engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    """This function ingests the dataframe into a database table."""
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f'Table "{table_name}" ingested successfully.')

def load_raw_data():
    """This function loads CSV files as dataframes and ingests them into the database."""
    start = time.time()
    
    folder_path = '/Users/khushimann/Desktop/data/'
    
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            full_path = os.path.join(folder_path, file)
            df = pd.read_csv(full_path)
            table_name = file[:-4]  # Remove ".csv"
            logging.info(f'Ingesting {file} into database...')
            ingest_db(df, table_name, engine)
    
    end = time.time()
    total_time = (end - start) / 60
    logging.info(f"Finished ingesting all files in {total_time:.2f} minutes.")
if __name__ == '__main__':
    load_raw_data()