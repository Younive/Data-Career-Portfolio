import numpy as np


def sort_scores_idx(scores: np.array, indices: np.array) -> tuple[np.array, np.array]:
    """Sort scores and indices by scores only

    Args:
        scores (np.array): Score of restaurants
        indices (np.array): Index of restaurants

    Returns:
        tuple[np.array, np.array]: Tuple of sorted scores and indices
    """
    scores_sort_idx = np.argsort(scores)[::-1]
    scores_sorted = scores[scores_sort_idx]
    indices_sorted = indices[scores_sort_idx]

    return scores_sorted, indices_sorted


def add_score_field(data: dict, scores: list[float], size: int) -> dict:
    """Add score field to the data

    Args:
        data (dict): _description_
        scores (list[float]): _description_
        size (int): _description_

    Returns:
        dict: _description_
    """
    n_loop = min(size, len(data))
    for i in range(n_loop):
        data[i]["score"] = round(scores[i], 1)

    return data
