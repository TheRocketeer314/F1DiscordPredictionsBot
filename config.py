# config.py
from pathlib import Path
import fastf1

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