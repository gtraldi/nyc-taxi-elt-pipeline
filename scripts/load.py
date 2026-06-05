# import sys
import time
import logging
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from utils.database import db_engine, check_connection


def check_data_in_db(year: int, month: int):
    """
    Checks if data already exists in the database
    
    Returns:
        bool: True if data exists, False otherwise
    """
    try:
        start_time = time.time()
        logging.info("Checking database connection")
        check_connection()

        table_name = "nyc_yellow_trip"

        with db_engine.begin() as connection:
            logging.info(f"Checking data in {table_name} table")
            check_query = text("""
                SELECT 1
                FROM raw.nyc_yellow_trip
                WHERE file_year = :year AND file_month = :month
                LIMIT 1
            """)
            result = connection.execute(check_query, {"year": year, "month": month}).fetchone()
            exists = result is not None
            
            duration = time.time() - start_time
            logging.info(f"Checked data in {table_name} table in {duration:.2f} seconds. Exists: {exists}")
            
            return exists
    except OperationalError as e:
        raise ConnectionError(f"Error in database connection: {e}")
    except Exception as e:
        raise Exception(f"Error trying to check data in the database: {e}")

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

        table_name = "nyc_yellow_trip"

        year = file_path.split("_")[1]
        month = file_path.split("_")[2].split(".")[0]

        with db_engine.begin() as connection:
            logging.info(f"Truncating data for {year}-{int(month):02d}")
            truncate_query = text("""
                DELETE FROM raw.nyc_yellow_trip
                WHERE file_year = :year
                  AND file_month = :month
            """)
            connection.execute(truncate_query, {"year": year, "month": month})

            logging.info(f"Reading parquet file from {file_path}")
            parquet_file = pq.ParquetFile(file_path)
            total_rows = parquet_file.metadata.num_rows
            logging.info(f"Total rows to load: {total_rows:,}")

            logging.info("Starting chunk pre processing and load to the database")
            
            rows_loaded = 0
            for batch in parquet_file.iter_batches(batch_size=chunk_size):
                df_chunk = batch.to_pandas()
                df_chunk.columns = df_chunk.columns.str.lower().str.strip()
                df_chunk["loaded_at"] = datetime.utcnow()
                df_chunk["file_year"] = int(year)
                df_chunk["file_month"] = int(month)

                df_chunk.to_sql(
                    name=table_name,
                    con=connection,
                    schema="raw",
                    if_exists="append",
                    index=False,
                    method="multi"
                )
                rows_loaded += len(df_chunk)
                
                # Progress logging
                pct = (rows_loaded / total_rows) * 100
                bar_length = 20
                filled = int(bar_length * rows_loaded // total_rows)
                bar = '=' * filled + '-' * (bar_length - filled)
                logging.info(f"Ingestion progress: [{bar}] {pct:.1f}% ({rows_loaded:,}/{total_rows:,} rows)")
            
            duration = time.time() - start_time
            logging.info(f"Successfully loaded {rows_loaded:,} rows for {year}-{int(month):02d} in {duration:.2f} seconds into {table_name} table.")
            
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Error reading parquet file: {e}")
    except OperationalError as e:
        raise ConnectionError(f"Error in database connection: {e}")
    except Exception as e:
        raise Exception(f"Error trying to load data to the database: {e}")

# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
#     if len(sys.argv) > 1:
#         load_data_to_db(sys.argv[1])
#     else:
#         logging.error("Usage: python -m scripts.load <file_path>")
#         sys.exit(1)