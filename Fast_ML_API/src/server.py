import os
from time import time
from typing import Annotated

from fastapi import FastAPI, Path, Query

from app.database import get_restaurant_by_indices, get_user_feature
from app.dto import RestaurantResponse
from app.model import load_model, predict_user
from app.process import add_score_field, sort_scores_idx
from utils.database import get_database, init_dotenv

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
MODEL_DIR = os.path.join(DATA_DIR, "models")

app = FastAPI()


@app.on_event("startup")
def startup_event():
    global model, db

    init_dotenv()
    model = load_model(MODEL_DIR)
    db = get_database()


@app.get("/")
def root():
    return {"message": "Hello World"}


@app.get(
    "/recommend/{user_id}",
    #esponse_model=RestaurantResponse,
    responses={404: {"model": None}},
)
def recommend(
    user_id: Annotated[str, Path(..., title="The ID of the user")],
    latitude: Annotated[float, Query(..., title="The latitude of the user")],
    longitude: Annotated[float, Query(..., title="The longitude of the user")],
    sort_dis: Annotated[float, Query(..., title="The sort distance of the user")] = 0.0,
    size: Annotated[int, Query(..., title="The size of the user")] = 20,
    max_dis: Annotated[int, Query(..., title="The max distance of the user")] = 5000,
):
    s1 = time()
    user_data = get_user_feature(db, user_id)
    print(f"---- io (get_user_feature)---- {(time() - s1)*1000} ms")

    s2 = time()
    scores, indices = predict_user(model, user_data)
    print(f"---- model (predict_user)---- {(time() - s2)*1000} ms")

    sort_dis = int(sort_dis)
    if sort_dis == 0:
        scores, indices = sort_scores_idx(scores, indices)

    s3 = time()
    restaurants = get_restaurant_by_indices(
        db, indices, latitude, longitude, sort_dis, size, max_dis
    )
    print(f"---- io (get_restaurant_by_indices)---- {(time() - s3)*1000} ms")

    s4 = time()
    processed_restaurants = add_score_field(restaurants, scores, size)
    print(f"---- io (processed_restaurants_data)---- {(time() - s4)*1000} ms")

    return {"restaurants": processed_restaurants}
