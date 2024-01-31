from typing import List

from pydantic import BaseModel


class Restaurant(BaseModel):
    id: str
    score: float
    distance: int


class RestaurantResponse(BaseModel):
    restaurants: List[Restaurant]
