from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
mongo_url = os.getenv('MONGO_URL')
db_name = os.getenv('MONGO_DB_NAME')

# Function to get MongoDB connection


def get_mongo_client():
    return MongoClient(mongo_url)

# Function to get the MongoDB database


def get_mongo_db():
    client = get_mongo_client()
    return client[db_name]


# Example usage of the connection functions
if __name__ == "__main__":
    db = get_mongo_db()
    print(f"Connected to MongoDB database: {db.name}")
