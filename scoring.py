from database import safe_execute, safe_fetch_all, safe_fetch_one, has_led_championship
import threading
import logging
from get_now import SEASON
from config import CONSTRUCTOR_ERGAST_MAP

logger = logging.getLogger(__name__)

db_lock = threading.Lock()

def score_top3(pred, result):
    pred_set = {pred["pos1"], pred["pos2"], pred["pos3"]}
    res_set = {result["pos1"], result["pos2"], result["pos3"]}

    if pred["pos1"] == result["pos1"] and \
       pred["pos2"] == result["pos2"] and \
       pred["pos3"] == result["pos3"]:
        return 10
    if (
        (pred["pos1"] == result["pos1"] and pred["pos2"] == result["pos2"]) or
        (pred["pos1"] == result["pos1"] and pred["pos3"] == result["pos3"]) or
        (pred["pos2"] == result["pos2"] and pred["pos3"] == result["pos3"])
        ):
        return 7
    if pred_set == res_set and (
        pred["pos1"] == result["pos1"] or
        pred["pos2"] == result["pos2"] or
        pred["pos3"] == result["pos3"] 
        ):
        return 5
    if (
        pred["pos1"] == result["pos1"] or
        pred["pos2"] == result["pos2"] or
        pred["pos3"] == result["pos3"] 
        ):
        return 2
    
    if pred_set == res_set:
        return 4

    return 0

def score_pole(pred, result):
    if pred["pole"] == result["pole"]:
        return 3
    if pred["pole"] == result["quali_second"]:
        return 1
    return 0

def score_fastest_lap(pred, result):
    if pred["fastest_lap"] == result["fastest_lap"]:
        return 3
    return 0

def score_sprint_winner(pred, result):
    if result["sprint_winner"] is not None:
        if pred["sprint_winner"] == result["sprint_winner"]:
            return 3
        return 0
    return 0

def score_sprint_pole(pred, result):
    if result["sprint_pole"] is not None:
        if pred["sprint_pole"] == result["sprint_pole"]:
            return 3
        return 0
    return 0
    
def score_constructor(pred, result):
    if pred["constructor_winner"] == result["constructor"]:
        return 3
    return 0

def score_weekend(pred, result):
    points = 0
    points += score_top3(pred, result)
    points += score_pole(pred, result)
    points += score_fastest_lap(pred, result)
    points += score_constructor(pred, result)

    # Sprint automatically gives 0 if None
    points += score_sprint_winner(pred, result)
    points += score_sprint_pole(pred, result)

    return points

def score_race_for_guild(race_number, guild_id):
    try:
        predictions = safe_fetch_all(
            "SELECT * FROM race_predictions WHERE race_number=%s AND guild_id=%s",
            (race_number, guild_id)
        )

        result = safe_fetch_one(
            "SELECT * FROM race_results WHERE race_number=%s",
            (race_number,)
        )
        if not result:
            return
        
        race_name = result['race_name']

        if not predictions:
            return
        
        for pred in predictions:
            points = score_weekend(pred, result)
            safe_execute(
                """
                INSERT INTO race_scores (
                    guild_id, user_id, username, race_number, race_name, points
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(guild_id, user_id, race_number) DO UPDATE SET
                    username = excluded.username,
                    points = excluded.points
                """,
                (guild_id, pred['user_id'], pred['username'], race_number, race_name, points)
            )

    except Exception:
        logger.exception("score_race_for_guild error")

def normalize_constructor(name):
    return CONSTRUCTOR_ERGAST_MAP.get(name, name.lower().replace(" ", "_"))

def score_final_champions_for_guild(guild_id):
    try:
        # Fetch all predictions for this guild
        predictions = safe_fetch_all(
            "SELECT * FROM season_predictions WHERE guild_id=%s",
            (guild_id,)
        )
        if not predictions:
            return

        # Fetch real final champions
        result = safe_fetch_one(
            "SELECT wdc, wdc_second, wcc, wcc_second FROM final_champions WHERE season = %s",
            (SEASON,)
        )
        if not result:
            return

        real_wdc = result['wdc']
        real_wdc_second = result['wdc_second']
        real_wcc = result['wcc']
        real_wcc_second = result['wcc_second']

        for pred in predictions:
            score = 0

            if pred['wdc'] == real_wdc:
                score += 25
            elif pred['wdc'] == real_wdc_second:
                score += 15
            elif has_led_championship(SEASON, pred['wdc'], 'wdc'):
                score += 5

            if normalize_constructor(pred['wcc']) == real_wcc:
                score += 20
            elif normalize_constructor(pred['wcc']) == real_wcc_second:
                score += 10
            elif has_led_championship(SEASON, normalize_constructor(pred['wcc']), 'wcc'):
                score += 5

            safe_execute(
                """
                INSERT INTO final_scores (
                    guild_id, user_id, username, points
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT(guild_id, user_id) DO UPDATE SET
                    username = excluded.username,
                    points = excluded.points
                """,
                (guild_id, pred['user_id'], pred['username'], score)
            )
    except Exception:
        logger.exception("score_final_champions_for_guild error")