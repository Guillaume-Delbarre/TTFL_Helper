import os
import json

CACHE_DIR = "cache"

def ensure_cache_dir() -> None:
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)

def cache_file_for_date(date_str) -> str:
    ensure_cache_dir()
    return os.path.join(CACHE_DIR, f"stats_{date_str}.json")

def is_cached(date_str) -> bool:
    return os.path.exists(cache_file_for_date(date_str))

def load_cache(date_str):
    with open(cache_file_for_date(date_str), "r", encoding="utf-8") as f:
        return json.load(f)

def save_cache(date_str, data) -> None:
    with open(cache_file_for_date(date_str), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)