# final_champions_watcher.py
import asyncio
import fastf1
from config import CACHE_DIR
import logging
import shutil
from FastF1_service import get_final_champions_if_ready, get_season_end_time
from database import (save_final_champions, 
                      update_leaderboard, 
                      get_prediction_channel, 
                      is_season_scored, 
                      mark_season_scored)
from scoring import score_final_champions_for_guild
from get_now import get_now, TIME_MULTIPLE, SEASON

logger = logging.getLogger(__name__)

async def final_champions_loop(bot):
    await bot.wait_until_ready()
    loop = asyncio.get_running_loop()
    season = SEASON

    season_end_time = await get_season_end_time()

    delay = (season_end_time - get_now()).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay / TIME_MULTIPLE)

    # Three guarded attempts (data lag protection)
    for _ in range(3):
        try:
            result = await get_final_champions_if_ready()
        except Exception:
            logger.exception("Failed fetching final champions")
            await asyncio.sleep(3600 / TIME_MULTIPLE)
            continue

        if result:
            wdc_winner, wcc_winner = result

            wdc = wdc_winner[:3].upper()
            wcc = wcc_winner

            save_final_champions(season, wdc, wcc)

            shutil.rmtree(CACHE_DIR, ignore_errors=True)
            CACHE_DIR.mkdir(exist_ok=True)
            fastf1.Cache.enable_cache(str(CACHE_DIR))

            # Score per guild (like race loop)
            for guild in bot.guilds:
                try:
                    guild_id = guild.id

                    # Prevents double scoring 
                                        
                    if is_season_scored(guild_id, season):
                        logger.info("Final champions already scored for guild %s", guild.name)
                        continue

                    logger.info("Scoring final champions for guild %s...", guild.name)
                    score_final_champions_for_guild(guild_id)
                    update_leaderboard(guild_id)
                    mark_season_scored(guild_id, season)

                    channel_id = get_prediction_channel(guild_id)
                    if channel_id:
                        channel = guild.get_channel(channel_id)
                        if channel:
                            await channel.send(
                            f"✅ **The {season} Formula 1 season has ended!**\n"
                            f"👑 The {season} F1 WDC- {wdc_winner.title()}\n"
                            f"🏎️ The {season} F1 WCC- {wcc_winner.title()}\n" 
                            "Championship predictions have been scored!"
                        )

                    logger.info("Final champions scored for guild %s", guild.name)

                except Exception:
                    logger.exception("Failed scoring guild %s", guild.id)
                    continue

            return

        await asyncio.sleep(3600 / TIME_MULTIPLE)
