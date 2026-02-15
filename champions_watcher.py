# final_champions_watcher.py
import asyncio
from FastF1_service import get_final_champions_if_ready, get_season_end_time
from database import save_final_champions, update_leaderboard
from scoring import score_final_champions
from get_now import get_now, TIME_MULTIPLE


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
            for guild in bot.guilds:
                update_leaderboard(guild.id)
            return

        await asyncio.sleep(3600/TIME_MULTIPLE)  # rare delay fallback
