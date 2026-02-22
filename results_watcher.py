# results_watcher.py
import asyncio
import fastf1
from config import CACHE_DIR
import logging
import traceback
import shutil
from FastF1_service import race_results, sprint_results, get_race_end_time
from database import (save_race_results,
                      save_sprint_results, 
                      update_leaderboard, 
                      get_prediction_channel,
                      is_race_scored,
                      mark_race_scored)
from scoring import score_race_for_guild
from get_now import get_now, TIME_MULTIPLE

logger = logging.getLogger(__name__)

async def poll_results_loop(bot):
    await bot.wait_until_ready()
    loop = asyncio.get_running_loop()

    while not bot.is_closed():
        try:
            race_end_time = await get_race_end_time(get_now())
            if race_end_time is None:
                logger.info("No upcoming races found, season may be over. Waiting 24 hours...")
                await asyncio.sleep(24 * 60 * 60 / TIME_MULTIPLE)
                continue

            delay = (race_end_time - get_now()).total_seconds()

            while delay > 0:
                logger.info("Waiting %.2f hours until next race ends...", delay/3600/TIME_MULTIPLE)
                await asyncio.sleep(min(delay / TIME_MULTIPLE, 600 / TIME_MULTIPLE))
                race_end_time = await get_race_end_time(get_now())
                delay = (race_end_time - get_now()).total_seconds()

            logger.info("Race has ended, fetching results...")
            race_data = await race_results()
            if not race_data:
                await asyncio.sleep(60 * 60 / TIME_MULTIPLE)
                continue

            race_num = race_data['race_number']
            save_race_results(race_data)

            sprint_data = await sprint_results()
            if sprint_data:
                save_sprint_results(sprint_data)

            shutil.rmtree(CACHE_DIR, ignore_errors=True)
            CACHE_DIR.mkdir(exist_ok=True)
            fastf1.Cache.enable_cache(str(CACHE_DIR))

            # Score per guild
            for guild in bot.guilds:
                guild_id = guild.id

                # Only score if this guild hasn't scored this race yet

                race_scored_for_guild = is_race_scored(guild_id, race_num)
                if race_scored_for_guild:
                    logger.info("Race %s already scored for guild %s", race_num, guild.name)
                    continue
                
                logger.info(
                    "race_num=%s | guild_id=%s | race_scored_for_guild=%s | scoring race %s for guild %s",
                    race_num,
                    guild_id,
                    race_scored_for_guild,
                    race_num,
                    guild.name,
                )

                score_race_for_guild(race_num, guild_id)  
                update_leaderboard(guild_id)
                mark_race_scored(guild_id, race_num)

                logger.info("Race %s scored and leaderboard updated for guild %s", race_num, guild.id)

                channel_id = get_prediction_channel(guild_id)
                if channel_id:
                    channel = guild.get_channel(channel_id)
                    if channel:
                        await channel.send(
                        f"✅ **The {race_data['race_name']} has been scored!**\n"
                        f"🥇 {race_data['pos1']}  🥈 {race_data['pos2']}  🥉 {race_data['pos3']}\n"
                        f"🏁 Pole: {race_data['pole']}  ⚡ Fastest Lap: {race_data['fastest_lap']}\n"
                        f"🏗️ Constructor: {race_data['winning_constructor']}"
                    )

            logger.info("Sleeping 24 hours before checking for next race...")
            await asyncio.sleep(24 * 60 * 60 / TIME_MULTIPLE)

        except Exception :
            logger.exception("pole_results_loop crashed")
            await asyncio.sleep(60 * 60 / TIME_MULTIPLE)
