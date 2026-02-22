from datetime import datetime, timezone, timedelta
import os


real_time = datetime.now(timezone.utc)

#to go to a specific date, enter the datetime in this format in environment variables: datetime(2025, 11, 25, 13, 00, tzinfo=timezone.utc)

_target_str = os.getenv('TARGET', None)
try:
    TARGET = datetime.fromisoformat(_target_str) if _target_str else None
except ValueError:
    TARGET = None

TIME_MULTIPLE = float(os.getenv('TIME_MULTIPLE', 1.0))
if TARGET:
    OFFSET = real_time - TARGET

_test_time_str = os.getenv('TEST_TIME', None)
try:
    TEST_TIME = datetime.fromisoformat(_test_time_str) if _test_time_str else None
except ValueError:
    TEST_TIME = None

try:
    SEASON = int(os.getenv('SEASON', datetime.now().year))
except ValueError:
    SEASON = datetime.now().year

def get_now():
    if TEST_TIME:
        return TEST_TIME 
    if TARGET:
        real_elapsed = datetime.now(timezone.utc) - real_time
        accelerated_elapsed = real_elapsed.total_seconds() * TIME_MULTIPLE
        return TARGET + timedelta(seconds=accelerated_elapsed)
        #return datetime.now(timezone.utc) - OFFSET
    return datetime.now(timezone.utc)
