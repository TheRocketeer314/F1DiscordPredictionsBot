# results_watcher.py
import asyncio
from FastF1_service import race_results, sprint_results, get_race_end_time
from database import save_race_results, save_sprint_results, safe_fetch_one, update_leaderboard
from scoring import score_race, score_race_for_guild
from get_now import get_now, TIME_MULTIPLE

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

            while delay > 0:
                print(f"Waiting {delay/3600/TIME_MULTIPLE:.2f} hours until next race ends...")
                await asyncio.sleep(min(delay / TIME_MULTIPLE, 600 / TIME_MULTIPLE))
                race_end_time = get_race_end_time(get_now())
                delay = (race_end_time - get_now()).total_seconds()

            print("Race has ended, fetching results...")
            race_data = await loop.run_in_executor(None, race_results)
            if not race_data:
                await asyncio.sleep(60 * 60 / TIME_MULTIPLE)
                continue

            race_num = race_data['race_number']
            save_race_results(race_data)

            sprint_data = await loop.run_in_executor(None, sprint_results)
            if sprint_data:
                save_sprint_results(sprint_data)

            # Score per guild
            for guild in bot.guilds:
                guild_id = guild.id

                # Only score if this guild hasn't scored this race yet
                existing_scores = safe_fetch_one(
                    "SELECT 1 FROM race_scores WHERE race_number = %s AND guild_id = %s LIMIT 1",
                    (race_num, guild_id)
                )
                if existing_scores:
                    print(f"Race {race_num} already scored for guild {guild.name}")
                    continue

                print(f"Scoring race {race_num} for guild {guild.name}...")
                score_race_for_guild(race_num, guild_id)  # new function
                update_leaderboard(guild_id)
                print(f"Race {race_num} scored and leaderboard updated for guild {guild.name}")

            print("Sleeping 24 hours before checking for next race...")
            await asyncio.sleep(24 * 60 * 60 / TIME_MULTIPLE)

        except Exception as e:
            print(f"Error in poll_results_loop: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(60 * 60 / TIME_MULTIPLE)
