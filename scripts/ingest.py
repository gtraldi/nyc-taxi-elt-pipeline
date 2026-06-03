import os
import requests
from dotenv import load_dotenv
import argparse
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)

def check_files(year: int, month: int) -> bool:
    """
        Checks if the file already exists

        Returns:
            bool: True if the file exists, False otherwise
    """
    return os.path.exists(f"./data/raw_{year}_{month}.parquet")

def generate_url(year: int, month: int) -> str:
    """
        Generates the url for parquet file download
    
        Args:
            year (int): Year of the file
            month (int): Month of the file
    
        Returns:
            str: Url for the file download
    """
    base_url = os.getenv("BASE_URL")
    return f"{base_url}{year}-{month:02d}.parquet"


def download_parquet_files(year: int, month: int) -> None:
    """
        Downloads parquet files from NYC TLC for a given period

        Args:
            year (int): Year of the file
            month (int): Month of the file

        Returns:
            None
    """
    if check_files(year=year, month=month):
        logging.info(f"File {year}-{month:02d} already exists")
        return

    url = generate_url(year=year, month=month)
    logging.info(f"File url: {url}")

    try:
        logging.info(f"Downloading file for {year}-{month:02d}")
        with requests.get(url=url, stream=True) as req:
            req.raise_for_status()
            with open(f"/opt/airflow/data/landing/raw_{year}_{month}.parquet", "wb") as f:
                for chunk in req.iter_content(chunk_size=8192):
                    f.write(chunk)
        logging.info(f"File {year}-{month:02d} downloaded successfully")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during download: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("year", type=str, help="Initial date in yyyy")
    parser.add_argument("month", type=str, help="End date in mm format")

    args = parser.parse_args()

    year = int(args.year)
    month = int(args.month)

    download_parquet_files(year=year, month=month)
    