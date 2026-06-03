import os
from dotenv import load_dotenv

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(dotenv_path=os.path.join(root_dir, '.env'))

BASE_URL = os.getenv("BASE_URL")
LANDING_PATH = os.getenv("LANDING_PATH", "./data")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASSWORD = os.getenv("DB_PASSWORD", "airflow")
DB_NAME = os.getenv("DB_NAME", "airflow")