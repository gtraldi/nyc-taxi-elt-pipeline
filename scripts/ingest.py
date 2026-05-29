import requests
from dotenv import load_dotenv
from datetime import datetime
import argparse


load_dotenv()

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
    url = generate_url(year=year, month=month)

    try:
        with requests.get(url=url, stream=True) as req:
            req.raise_for_status()
            with open(f"/opt/airflow/data/landing/raw_{year}_{month}.parquet", "wb") as f:
                for chunk in req.iter_content(chunk_size=8192):
                    f.write(chunk)
    except requests.exceptions.RequestException as e:
        print(f"Error during download: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("start_date", type=str, help="Initial date in yyyy-mm format")
    parser.add_argument("end_date", type=str, help="End date in yyyy-mm format")

    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m")
    end_date = datetime.strptime(args.end_date, "%Y-%m")

    ingest_script(start_date=start_date, end_date=end_date)
    