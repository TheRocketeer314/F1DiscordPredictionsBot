# final_champions_watcher.py
import asyncio
import fastf1
from config import CACHE_DIR
import os
import shutil
from FastF1_service import get_final_champions_if_ready, get_season_end_time
from database import save_final_champions, update_leaderboard, safe_fetch_one
from scoring import score_final_champions, score_final_champions_for_guild
from get_now import get_now, TIME_MULTIPLE

async def final_champions_loop(bot):
    await bot.wait_until_ready()
    loop = asyncio.get_running_loop()

    season_end_time = await get_season_end_time()

    delay = (season_end_time - get_now()).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay / TIME_MULTIPLE)

    # One or two guarded attempts (data lag protection)
    for _ in range(2):
        result = await get_final_champions_if_ready()

        if result:
            wdc_winner, wcc_winner = result

            wdc = wdc_winner[:3].upper()
            wcc = wcc_winner

            save_final_champions(wdc, wcc)

            shutil.rmtree(CACHE_DIR, ignore_errors=True)
            CACHE_DIR.mkdir(exist_ok=True)
            fastf1.Cache.enable_cache(str(CACHE_DIR))

            # Score per guild (like race loop)
            for guild in bot.guilds:
                guild_id = guild.id

                # Prevent double scoring
                existing_scores = safe_fetch_one(
                    "SELECT 1 FROM final_scores WHERE guild_id = %s LIMIT 1",
                    (guild_id,)
                )

                if existing_scores:
                    print(f"Final champions already scored for guild {guild.name}")
                    continue

                print(f"Scoring final champions for guild {guild.name}...")
                score_final_champions_for_guild(guild_id)
                update_leaderboard(guild_id)
                print(f"Final champions scored for guild {guild.name}")

            return

        await asyncio.sleep(3600 / TIME_MULTIPLE)
