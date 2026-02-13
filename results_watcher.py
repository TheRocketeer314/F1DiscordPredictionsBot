# results_watcher.py
import asyncio
from FastF1_service import race_results, sprint_results, get_race_end_time
from database import get_connection, save_race_results, save_sprint_results, safe_fetch_one, update_leaderboard
from scoring import score_race
from datetime import datetime, timezone, timedelta

real_time = datetime.now(timezone.utc)
TARGET = None #to go to a specific date, enter the datetime in this format: datetime(2025, 11, 25, 13, 00, tzinfo=timezone.utc)
if TARGET:
    OFFSET = real_time - TARGET
TEST_TIME = None
TIME_MULTIPLE = 1.0
SEASON = 2026

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


async def poll_results_loop(bot):
    await bot.wait_until_ready()
    loop = asyncio.get_running_loop()

    while not bot.is_closed():
        try:
            race_end_time = get_race_end_time(get_now())

            if race_end_time is None:
                print("No upcoming races found, season may be over. Waiting 24 hours...")
                await asyncio.sleep(24 * 60 * 60 / TIME_MULTIPLE)
                continue

            delay = (race_end_time - get_now()).total_seconds()
            
            # Wait until race ends, checking periodically
            while delay > 0:
                print(f"Waiting {delay/3600/TIME_MULTIPLE:.2f} hours until next race ends...")
                await asyncio.sleep(min(delay / TIME_MULTIPLE, 600 / TIME_MULTIPLE))
                # Recalculate in case something changed
                race_end_time = get_race_end_time(get_now())
                delay = (race_end_time - get_now()).total_seconds()
                continue 
            
            # If race_end_time became None, restart from top
            if race_end_time is None:
                continue
            
            # If we get here, delay <= 0, meaning race has ended
            print("Race has ended, fetching results...")
            race_data = await loop.run_in_executor(None, race_results)

            if race_data:
                race_num = race_data['race_number']
                
                # Check if already scored
                existing_scores = safe_fetch_one(
                    "SELECT 1 FROM race_scores WHERE race_number = %s LIMIT 1",
                    (race_num,)
                )
                
                if existing_scores:
                    print(f"Race {race_num} already scored.")
                    await asyncio.sleep(24 * 60 * 60 / TIME_MULTIPLE)
                    continue
                
                print(f"Processing race {race_num}...")
                save_race_results(race_data)
                
                sprint_data = await loop.run_in_executor(None, sprint_results)
                if sprint_data:
                    save_sprint_results(sprint_data)
                
                print(f"Scoring race {race_num}...")
                score_race(race_num)
                print(f"Race {race_num} scored successfully!")
                
                update_leaderboard()
                print(f"Leaderboard updated successfully!")

                 # CRITICAL: Sleep before checking for next race
                print("Sleeping 24 hours before checking for next race...")
                await asyncio.sleep(24 * 60 * 60 / TIME_MULTIPLE)
            
        except Exception as e:
            print(f"Error in poll_results_loop: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(60 * 60 / TIME_MULTIPLE)