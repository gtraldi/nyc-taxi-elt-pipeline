import os
import requests
import logging
from utils.settings import BASE_URL, LANDING_PATH


def check_source_availability(year: int, month: int) -> bool:
    """
        Checks if the source file is available for download

        Returns:
            bool: True if the file is available, False otherwise
    """
    url = generate_url(year=year, month=month)
    try:
        req = requests.head(url)
        return req.status_code == 200
    except requests.exceptions.RequestException as e:
        logging.error(f"No data available for {year}-{month:02d}")
        return False


def check_files(year: int, month: int) -> bool:
    """
        Checks if the file already exists

        Returns:
            bool: True if the file exists, False otherwise
    """
    return os.path.exists(f"{LANDING_PATH}/raw_{year}_{month}.parquet")

def generate_url(year: int, month: int) -> str:
    """
        Generates the url for parquet file download
    
        Args:
            year (int): Year of the file
            month (int): Month of the file
    
        Returns:
            str: Url for the file download
    """
    return f"{BASE_URL}{year}-{month:02d}.parquet"


def download_parquet_files(year: int, month: int) -> str:
    """
        Downloads parquet files from NYC TLC for a given period

        Args:
            year (int): Year of the file
            month (int): Month of the file

        Returns:
            str: The path to the downloaded file
    """
    os.makedirs(LANDING_PATH, exist_ok=True)

    url = generate_url(year=year, month=month)
    logging.info(f"File url: {url}")

    try:
        logging.info(f"Downloading file for {year}-{month:02d}")
        with requests.get(url=url, stream=True) as req:
            req.raise_for_status()
            with open(f"{LANDING_PATH}/raw_{year}_{month}.parquet", "wb") as f:
                for chunk in req.iter_content(chunk_size=8192):
                    f.write(chunk)
        logging.info(f"File {year}-{month:02d} downloaded successfully")
        return f"{LANDING_PATH}/raw_{year}_{month}.parquet"
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during download: {e}")
        raise

