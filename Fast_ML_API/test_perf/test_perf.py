import os
from time import time

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(CURRENT_DIR, "..", "data")


def test_perf(reqs: dict) -> tuple[float, list[float]]:
    """Test performance of the API

    Args:
        reqs (dict): Request data

    Returns:
        tuple[float, list[float]]: Total time and time for each request
    """
    total_time = 0
    time_all = []
    for req in tqdm(reqs):
        user_id = req["user_id"]
        req.pop("user_id")

        s = time()
        requests.get(f"http://127.0.0.1:80/recommend/{user_id}", params=req)
        time_use = (time() - s) * 1000  # Convert to ms
        total_time += time_use
        time_all.append(time_use)

    return total_time, time_all


def load_request() -> dict:
    """Load request data from parquet file

    Returns:
        dict: Request data in dict
    """
    df = pd.read_parquet(os.path.join(DATA_DIR, "request.parquet"))
    reqs = df.to_dict(orient="records")

    return reqs


def process_request(reqs: dict):
    """Pop sort_dis and max_dis if it is nan (assume that user doesn't fill it)

    Args:
        reqs (dict): Request data
    """
    for req in reqs:
        # if sort_dis is nan then pop it
        if pd.isna(req["sort_dis"]):
            req.pop("sort_dis")
        if pd.isna(req["max_dis"]):
            req.pop("max_dis")


def print_result(req_size: int, total_time: float, time_all: list[float]):
    """Print result of the test

    Args:
        req_size (int): _description_
        total_time (float): _description_
        time_all (list[float]): _description_
    """
    percentile_value = np.percentile(time_all, 90)
    req_per_sec = req_size / (total_time / 1000)

    print(f"90th percentile: {percentile_value:.2f} ms")
    print(f"{req_per_sec:.2f} requests per second")
    print(f"Average time: {(total_time / len(time_all)):.2f} ms")
    print(f"Min time: {min(time_all):.2f} ms")
    print(f"Max time: {max(time_all):.2f} ms")


if __name__ == "__main__":
    reqs = load_request()
    req_size = len(reqs)
    process_request(reqs)

    total_time, time_all = test_perf(reqs)
    print_result(req_size, total_time, time_all)
