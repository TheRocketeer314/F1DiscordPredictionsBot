# config.py
from pathlib import Path
import fastf1
import os

CACHE_DIR = Path("fastf1cache")
CACHE_DIR.mkdir(exist_ok=True)
fastf1.Cache.enable_cache(str(CACHE_DIR))

CONSTRUCTOR_ERGAST_MAP = {
    "Red Bull Racing": "red_bull",
    "Racing Bulls": "rb",
    "Aston Martin": "aston_martin",
    "Mercedes": "mercedes",
    "Ferrari": "ferrari",
    "McLaren": "mclaren",
    "Williams": "williams",
    "Alpine": "alpine",
    "Haas": "haas",
    "Cadillac": "cadillac",
    "Audi": "audi"
}

CRAZY_PRED_POINTS = {
    "Easy": int(os.getenv("CRAZY_EASY_POINTS", 5)),
    "Medium": int(os.getenv("CRAZY_MEDIUM_POINTS", 10)),
    "Hard": int(os.getenv("CRAZY_HARD_POINTS", 20)),
}