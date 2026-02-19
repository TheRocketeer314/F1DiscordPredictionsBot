from datetime import datetime, timezone, timedelta
import os


real_time = datetime.now(timezone.utc)
TARGET = None #to go to a specific date, enter the datetime in this format: datetime(2025, 11, 25, 13, 00, tzinfo=timezone.utc)
if TARGET:
    OFFSET = real_time - TARGET
TEST_TIME = None
TIME_MULTIPLE = 1.0
try:
    SEASON = int(os.getenv('SEASON', 2026))
except ValueError:
    SEASON = 2026

def get_now():
    if TEST_TIME:
        return TEST_TIME 
    if TARGET:
        real_elapsed = datetime.now(timezone.utc) - real_time
        accelerated_elapsed = real_elapsed.total_seconds() * TIME_MULTIPLE
        return TARGET + timedelta(seconds=accelerated_elapsed)
        #return datetime.now(timezone.utc) - OFFSET
    return datetime.now(timezone.utc)
