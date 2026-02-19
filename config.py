# config.py
from pathlib import Path
import fastf1

CACHE_DIR = Path("fastf1cache")
CACHE_DIR.mkdir(exist_ok=True)
fastf1.Cache.enable_cache(str(CACHE_DIR))