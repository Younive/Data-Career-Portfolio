import os

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.database import Database


def init_dotenv():
    """Initialize dotenv"""
    global DB_USERNAME, DB_PASSWORD, DB_PORT, DB_NAME, MONGODB_HOST

    load_dotenv()

    DB_USERNAME = os.getenv("MONGODB_USERNAME")
    DB_PASSWORD = os.getenv("MONGODB_PASSWORD")
    MONGODB_HOST = os.getenv("MONGODB_HOST")
    DB_PORT = os.getenv("MONGODB_PORT")
    DB_NAME = os.getenv("DATABASE_NAME")


def get_database() -> Database:
    """Get database client from MongoDB

    Returns:
        Database: Database client
    """
    CONNECTION_STRING = f"mongodb://{DB_USERNAME}:{DB_PASSWORD}@{MONGODB_HOST}:{DB_PORT}/"

    client = MongoClient(CONNECTION_STRING)

    return client.get_database(DB_NAME)
