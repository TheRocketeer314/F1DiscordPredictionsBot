from datetime import datetime, timezone, timedelta
import os
from dotenv import load_dotenv

real_time = datetime.now(timezone.utc)

load_dotenv()

#to go to a specific date, enter the datetime in this format in environment variables: 2025-11-25T13:00:00+00:00

_target_str = os.getenv('MOVING_TARGET', None)
try:
    MOVING_TARGET = datetime.fromisoformat(_target_str) if _target_str else None
except ValueError:
    MOVING_TARGET = None

TIME_MULTIPLE = float(os.getenv('TIME_MULTIPLE', 1.0))
if MOVING_TARGET:
    OFFSET = real_time - MOVING_TARGET

_test_time_str = os.getenv('STATIONARY_TARGET', None)
try:
    STATIONARY_TARGET = datetime.fromisoformat(_test_time_str) if _test_time_str else None
except ValueError:
    STATIONARY_TARGET = None

try:
    SEASON = int(os.getenv('SEASON', datetime.now().year))
except ValueError:
    SEASON = datetime.now().year

def get_now():
    if STATIONARY_TARGET:
        return STATIONARY_TARGET 
    if MOVING_TARGET:
        real_elapsed = datetime.now(timezone.utc) - real_time
        accelerated_elapsed = real_elapsed.total_seconds() * TIME_MULTIPLE
        return MOVING_TARGET + timedelta(seconds=accelerated_elapsed)
        #return datetime.now(timezone.utc) - OFFSET
    return datetime.now(timezone.utc)
