# database.py
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
import pandas as pd
# Load environment variables from .env file
load_dotenv()
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
database = os.getenv('DB_NAME')
# Database Configuration
SQLALCHEMY_DATABASE_URL = f'postgresql://{username}:{password}@localhost/biologicalsamples'
engine = create_engine(SQLALCHEMY_DATABASE_URL)
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency to get DB session


def get_dburl():
    return SQLALCHEMY_DATABASE_URL


def get_db():
    db = Session()
    try:
        yield db
    finally:
        db.close()


def execute_query_to_dataframe(sql_query: str, params: dict = None) -> pd.DataFrame:
    with engine.connect() as connection:
        df = pd.read_sql_query(sql_query, connection, params=params)
    return df
