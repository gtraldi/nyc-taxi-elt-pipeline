import sqlalchemy
from utils.settings import (
    DB_HOST,
    DB_PORT,
    DB_USER,
    DB_PASSWORD,
    DB_NAME
)

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

db_engine = sqlalchemy.create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=5,
    pool_recycle=1800,
    pool_pre_ping=True
)

def check_connection():
    """
    Test the database connection

    Returns:
        bool: True if the connection is successful, False otherwise
    """
    try:
        with db_engine.connect() as connection:
            connection.execute(sqlalchemy.text("SELECT 1"))
        return True
    except Exception as e:
        raise ConnectionError(f"Error trying to connect to the database: {e}")
