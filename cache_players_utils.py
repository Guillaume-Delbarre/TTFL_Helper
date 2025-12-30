import os
import pandas as pd

CACHE_DIR = "cache/players/"
JSON_NAME = "players.csv"

def is_cached(path: str = CACHE_DIR+JSON_NAME) -> bool:
    return os.path.exists(path)

def load_df_cache(path: str = CACHE_DIR+JSON_NAME) -> pd.DataFrame:
    return pd.read_csv(path)

def save_df_cache(df: pd.DataFrame, path: str = CACHE_DIR+JSON_NAME) -> None:
    df.to_csv(path)
