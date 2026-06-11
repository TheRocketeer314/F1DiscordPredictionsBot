# results_watcher.py
import asyncio
import fastf1
from config import CACHE_DIR
import logging
import traceback
import shutil
from FastF1_service import race_results, sprint_results, get_race_end_time, get_standings_leaders
from database import (save_race_results,
                      save_sprint_results, 
                      update_leaderboard, 
                      get_prediction_channel,
                      is_race_scored,
                      mark_race_scored,
                      save_championship_leaders)
from scoring import score_race_for_guild
from get_now import get_now, TIME_MULTIPLE, SEASON

logger = logging.getLogger(__name__)

async def poll_results_loop(bot):
    await bot.wait_until_ready()

    while not bot.is_closed():
        try:
            race_end_time = await get_race_end_time(get_now())
            if race_end_time is None:
                logger.info("No upcoming races found, season may be over. Waiting 24 hours...")
                await asyncio.sleep(24 * 60 * 60 / TIME_MULTIPLE)
                continue

            delay = (race_end_time - get_now()).total_seconds()

            while delay > 0:
                logger.info("Waiting %.2f hours until next race ends...", delay / 3600 / TIME_MULTIPLE)

                if delay > 24 * 3600:
                    sleep = 6 * 3600
                else:
                    sleep = 30 * 60

                await asyncio.sleep(min(delay, sleep) / TIME_MULTIPLE)
                race_end_time = await get_race_end_time(get_now())
                if race_end_time is None:
                    break
                delay = (race_end_time - get_now()).total_seconds()

            logger.info("Race has ended, fetching results...")
            race_data = await race_results()
            if not race_data:
                await asyncio.sleep(60 * 60 / TIME_MULTIPLE)
                continue

            race_num = race_data['race_number']
            results_saved = save_race_results(race_data)

            sprint_data = await sprint_results()
            if sprint_data:
                save_sprint_results(sprint_data)

            standings = await get_standings_leaders(race_num=race_num)
            if standings:
                wdc_leader, wcc_leader = standings
                save_championship_leaders(SEASON, wdc_leader, wcc_leader)
                logger.info("Standings leaders saved: WDC=%s, WCC=%s", wdc_leader, wcc_leader)

            shutil.rmtree(CACHE_DIR, ignore_errors=True)
            CACHE_DIR.mkdir(exist_ok=True)
            fastf1.Cache.enable_cache(str(CACHE_DIR))

            for guild in bot.guilds:
                guild_id = guild.id

                if is_race_scored(guild_id, race_num):
                    logger.info("Race %s already scored for guild %s", race_num, guild.name)
                    continue

                channel_id = get_prediction_channel(guild_id)
                channel = None
                if channel_id:
                    channel = guild.get_channel(channel_id)

                try:
                    score_race_for_guild(race_num, guild_id)
                    update_leaderboard(guild_id)
                    mark_race_scored(guild_id, race_num)
                    logger.info("Race %s scored and leaderboard updated for guild %s", race_num, guild.name)

                    if channel:
                        if sprint_data:
                            await channel.send(
                                f"**The {race_data['race_name']} has been scored!**\n"
                                f"*Race Results:*\n"
                                f"| 1. {race_data['pos1']} | 2. {race_data['pos2']} | 3. {race_data['pos3']}\n"
                                f"| Pole: {race_data['pole']} | Fastest Lap: {race_data['fastest_lap']}\n"
                                f"| Constructor: {race_data['winning_constructor']}\n\n"
                                f"*Sprint Results:*\n"
                                f"| Winner: {sprint_data['sprint_winner']} | Pole: {sprint_data['sprint_pole']}"
                            )
                        else:
                            await channel.send(
                                f"**The {race_data['race_name']} has been scored!**\n"
                                f"*Race Results:*\n"
                                f"| 1. {race_data['pos1']} | 2. {race_data['pos2']} | 3. {race_data['pos3']}\n"
                                f"| Pole: {race_data['pole']} | Fastest Lap: {race_data['fastest_lap']}\n"
                                f"| Constructor: {race_data['winning_constructor']}\n\n"
                            )

                except Exception:
                    logger.exception("Scoring failed for guild %s race %s", guild_id, race_num)
                    if channel:
                        await channel.send(
                            f"⚠️ **Automatic scoring failed for the {race_data['race_name']}.**\n"
                            f"Please use `/force_score_race` to score manually."
                        )

            logger.info("Sleeping 24 hours before checking for next race...")
            await asyncio.sleep(24 * 60 * 60 / TIME_MULTIPLE)

        except Exception:
            logger.exception("poll_results_loop crashed")
            await asyncio.sleep(60 * 60 / TIME_MULTIPLE)

        finally:
            #Cleaning cache and recreating it
            shutil.rmtree(CACHE_DIR, ignore_errors=True)
            CACHE_DIR.mkdir(exist_ok=True)
            fastf1.Cache.enable_cache(str(CACHE_DIR))

