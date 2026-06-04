import sys
import time
import logging
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from utils.database import db_engine, check_connection

def load_data_to_db(file_path: str, chunk_size: int = 50000):
    """
    Pre process and load data to the database in chunks
    
    Args:
        file_path (str): Path to the parquet file
    """
    try:
        start_time = time.time()
        logging.info("Checking database connection")
        check_connection()

        year = file_path.split("_")[1]
        month = file_path.split("_")[2].split(".")[0]

        with db_engine.begin() as connection:
            logging.info(f"Truncating data for {year}-{int(month):02d}")
            truncate_query = text("""
                DELETE FROM raw.nyc_yellow_trip
                WHERE EXTRACT(year FROM tpep_pickup_datetime) = :year
                  AND EXTRACT(month FROM tpep_pickup_datetime) = :month
            """)
            connection.execute(truncate_query, {"year": year, "month": month})

            logging.info(f"Reading parquet file from {file_path}")
            parquet_file = pq.ParquetFile(file_path)

            logging.info("Starting chunk pre processing and load to the database")
            
            rows_loaded = 0
            for batch in parquet_file.iter_batches(batch_size=chunk_size):
                df_chunk = batch.to_pandas()
                df_chunk.columns = df_chunk.columns.str.lower().str.strip()
                df_chunk["loaded_at"] = datetime.utcnow()

                df_chunk.to_sql(
                    name="nyc_yellow_trip",
                    con=connection,
                    schema="raw",
                    if_exists="append",
                    index=False,
                    method="multi"
                )
                rows_loaded += len(df_chunk)
            
            duration = time.time() - start_time
            logging.info(f"Successfully loaded {rows_loaded:,} rows for {year}-{int(month):02d} in {duration:.2f} seconds.")
            
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Error reading parquet file: {e}")
    except OperationalError as e:
        raise ConnectionError(f"Error in database connection: {e}")
    except Exception as e:
        raise Exception(f"Error trying to load data to the database: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    if len(sys.argv) > 1:
        load_data_to_db(sys.argv[1])
    else:
        logging.error("Usage: python -m scripts.load <file_path>")
        sys.exit(1)