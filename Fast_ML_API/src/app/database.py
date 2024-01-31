import numpy as np
import pymongo
from bson.son import SON
from fastapi import HTTPException
from pymongo.database import Database


def get_user_feature(db: Database, user_id: str) -> np.ndarray:
    """Get user feature from database by user_id

    Args:
        db (Database): MongoDB database
        user_id (str): user_id of the user from path param.

    Raises:
        HTTPException: If user not found

    Returns:
        np.ndarray: user feature with shape (1, 1000)
    """
    user_data = db.users.find_one({"user_id": user_id}, {"_id": 0, "user_id": 0})

    if user_data is None:
        raise HTTPException(status_code=404, detail="User not found")
    user_data = np.array(list(user_data.values())).reshape(1, -1)

    return user_data


def get_restaurant_by_indices(
    db: Database,
    indices: np.array,
    latitude: float,
    longitude: float,
    sort_dis: int,
    size: int,
    max_dis: int,
) -> list:
    """Get restaurant data from database by indices

    Args:
        db (Database): MongoDB database
        indices (np.array): Array of indices of restaurants
        latitude (float): Latitude of the user
        longitude (float): Longitude of the user
        sort_dis (int): Sort response by score from the model (0 to sort)
        size (int): Limit size of the response
        max_dis (int): Max distance of spherical distance between user and restaurant

    Raises:
        HTTPException: If restaurant not found

    Returns:
        list: List of restaurant data
    """
    indices = indices.tolist()

    DISTANCE_QUERY = {
        "$geoNear": {
            "near": {"type": "Point", "coordinates": [longitude, latitude]},
            "distanceField": "distance",
            "maxDistance": max_dis,
            "spherical": True,
        }
    }

    if sort_dis == 0:
        SORT_QUERY = [
            {"$addFields": {"orderIndex": {"$indexOfArray": [indices, "$index"]}}},
            {"$sort": SON([("orderIndex", 1)])},
        ]
    else:
        SORT_QUERY = []

    PROJECT_QUERY = {
        "$project": {
            "_id": 0,
            "location": 0,
            "index": 0,
            "restaurant_id": 0,
            "orderIndex": 0,
        }
    }

    restaurant_data = db.restaurants.aggregate(
        [
            DISTANCE_QUERY,
            {"$match": {"index": {"$in": indices}}},
            *SORT_QUERY,
            {"$limit": size},
            {"$set": {"distance": {"$toInt": "$distance"}, "id": "$restaurant_id"}},
            PROJECT_QUERY,
        ]
    )

    if restaurant_data is None:
        raise HTTPException(status_code=404, detail="Restaurant not found")

    return list(restaurant_data)
