# final_champions_watcher.py
import asyncio
import fastf1
from config import CACHE_DIR
import logging
import shutil
from FastF1_service import get_final_champions_if_ready, get_season_end_time
from database import save_final_champions, update_leaderboard, safe_fetch_one, get_prediction_channel
from scoring import score_final_champions, score_final_champions_for_guild
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

    # One or two guarded attempts (data lag protection)
    for _ in range(2):
        try:
            result = await get_final_champions_if_ready()
        except Exception:
            logger.exception("Failed fetching final champions")
            await asyncio.sleep(3600)
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

                    # Prevent double scoring
                    existing_scores = safe_fetch_one(
                        "SELECT 1 FROM final_scores WHERE guild_id = %s LIMIT 1",
                        (guild_id,)
                    )

                    if existing_scores:
                        logger.info(f"Final champions already scored for guild {guild.name}")
                        continue

                    logger.info(f"Scoring final champions for guild {guild.name}...")
                    score_final_champions_for_guild(guild_id)
                    update_leaderboard(guild_id)
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

                    logger.info(f"Final champions scored for guild {guild.name}")

                except Exception:
                    logger.exception("Failed scoring guild %s", guild.id)
                    continue

            return

        await asyncio.sleep(3600 / TIME_MULTIPLE)
