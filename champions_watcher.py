# final_champions_watcher.py
import asyncio
from FastF1_service import get_final_champions_if_ready, get_season_end_time
from database import get_connection, save_final_champions, update_leaderboard
from scoring import score_final_champions
from datetime import datetime, timezone, timedelta

real_time = datetime.now(timezone.utc)
TARGET = datetime(2025, 11, 25, 13, 00, tzinfo=timezone.utc)
OFFSET = real_time - TARGET
TEST_TIME = None
TIME_MULTIPLE = 600.0

def get_now():
    if TEST_TIME:
        return TEST_TIME 
    if TARGET:
        if TIME_MULTIPLE:
            real_elapsed = datetime.now(timezone.utc) - real_time
            accelerated_elapsed = real_elapsed.total_seconds() * TIME_MULTIPLE
            return TARGET + timedelta(seconds=accelerated_elapsed)
        return datetime.now(timezone.utc) - OFFSET
    return datetime.now(timezone.utc)

async def final_champions_loop(bot):
    await bot.wait_until_ready()
    loop = asyncio.get_running_loop()

    season_end_time = get_season_end_time()

    delay = (season_end_time - get_now()).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay/TIME_MULTIPLE)

    # One or two guarded attempts (data lag protection)
    for _ in range(2):
        result = await loop.run_in_executor(None, get_final_champions_if_ready)

        if result:
            wdc_winner, wcc_winner = result

            wdc = wdc_winner[:3].upper()
            wcc = wcc_winner

            save_final_champions(wdc, wcc)

            score_final_champions()
            update_leaderboard()
            return

        await asyncio.sleep(3600/TIME_MULTIPLE)  # rare delay fallback
