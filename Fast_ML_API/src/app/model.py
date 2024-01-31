import os
import pickle

from sklearn.neighbors import NearestNeighbors


def load_model(model_dir: str) -> NearestNeighbors:
    """Load NearestNeighbors model

    Args:
        model_dir (str): Path to model directory

    Returns:
        NearestNeighbors: NearestNeighbors model
    """
    model_path = os.path.join(model_dir, "model.pkl")
    with open(model_path, "rb") as f:
        model: NearestNeighbors = pickle.load(f)

    return model

def predict_user(
    model: NearestNeighbors, user_data: dict
) -> tuple[list[float], list[int]]:
    """Predict user data

    Args:
        model (NearestNeighbors): NearestNeighbors model
        user_data (dict): User features data.

    Returns:
        tuple[list[float], list[int]]: Tuple of scores and indices of restaurants
    """
    dist, ind = model.kneighbors(user_data, n_neighbors=2000)

    return dist[0], ind[0]
