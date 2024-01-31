import os

import pandas as pd
import pymongo
from pymongo.database import Database
from utils.database import get_database, init_dotenv

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def read_data() -> tuple[list[dict], list[dict]]:
    """Read data from parquet file

    Returns:
        dict: JSON parsed data from parquet file
    """
    user_df = pd.read_parquet(os.path.join(DATA_DIR, "user.parquet"))
    restaurant_df = pd.read_parquet(os.path.join(DATA_DIR, "restaurant.parquet"))

    user_data = user_df.to_dict("records")
    restaurant_data = restaurant_df.to_dict("records")

    return user_data, restaurant_data

def process_restaurant_data(restaurant_data: list[dict]):
    for restaurant in restaurant_data:
        restaurant["location"] = [float(restaurant["longitude"]), float(restaurant["latitude"])]
        restaurant.pop("latitude")
        restaurant.pop("longitude")

def insert_data(
    user_data: dict, restaurant_data: dict, db: Database, reset: bool = True
):
    """Insert data to database

    Args:
        data (dict): Data to be inserted
        db (Database): Database client
        reset (bool, optional): Reset collection before inserting data. Defaults to True.
    """
    if reset:
        db.restaurants.drop()
        db.users.drop()

    print("Inserting restaurant data to database...")
    db.restaurants.insert_many(restaurant_data)
    db.restaurants.create_index([("location", pymongo.GEOSPHERE)])

    print("Inserting user data to database...")
    db.users.insert_many(user_data)
    db.users.create_index("user_id", unique=True)


if __name__ == "__main__":
    init_dotenv()
    db = get_database()
    user_data, restaurant_data = read_data()
    process_restaurant_data(restaurant_data)

    insert_data(user_data, restaurant_data, db)
