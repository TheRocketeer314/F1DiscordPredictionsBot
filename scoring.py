from database import safe_execute, safe_fetch_all, get_connection, safe_fetch_one
import threading

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
        return 2
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

def score_race(race_number):
    print(f"=== Starting score_race for race {race_number} ===")
    # Fetch official results
    result = safe_fetch_one(
        "SELECT * FROM race_results WHERE race_number=%s",
        (race_number,)
    )
    print(f"Result fetched: {result}")
    if not result:
        return
    
    race_name = result['race_name']
    print(f"Race name: {race_name}")

    # Fetch predictions
    predictions = safe_fetch_all(
        "SELECT * FROM race_predictions WHERE race_number=%s",
        (race_number,)
    )

    print("Scoring race", race_number)
    print("Result keys:", result.keys())
    print("Predictions found:", len(predictions))

    for pred in predictions:
        print("Scoring user", pred['user_id'])
        points = score_weekend(pred, result)
        print(f"Points calculated: {points}")

        print(f"Inserting score into database...")

        safe_execute("""
            INSERT INTO race_scores (
                guild_id,
                user_id,
                username,
                race_number,
                race_name,
                points
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT(guild_id, user_id, race_number)
            DO UPDATE SET
                username = excluded.username,
                points = excluded.points
        """, (
            pred['guild_id'],
            pred['user_id'],
            pred['username'],
            race_number,
            race_name,
            points
        ))
        print(f"Score inserted for user {pred['user_id']}")
    
    print(f"=== Finished score_race for race {race_number} ===")

def score_race_for_guild(race_number, guild_id):
    predictions = safe_fetch_all(
        "SELECT * FROM race_predictions WHERE race_number=%s AND guild_id=%s",
        (race_number, guild_id)
    )
    if not predictions:
        return

    result = safe_fetch_one(
        "SELECT * FROM race_results WHERE race_number=%s",
        (race_number,)
    )
    if not result:
        return

    race_name = result['race_name']

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


def score_final_champions():
    """
    Fetch all user predictions and the actual final WDC/WCC from DB,
    compare, and update scores table.
    """
    with db_lock:
        conn = get_connection()
        try:
            cur = conn.cursor()

            # Fetch actual champions
            cur.execute("SELECT wdc, wcc FROM final_champions")
            row = cur.fetchone()
            if not row:
                print("No final champions data found")
                cur.close()
                conn.close()
                return  # No final results yet
            
            real_wdc, real_wcc = row
            print(f"Final champions: WDC={real_wdc}, WCC={real_wcc}")

            # Fetch all user predictions
            cur.execute("SELECT guild_id, user_id, username, wdc, LOWER(wcc) FROM season_predictions")
            user_preds = cur.fetchall()
            print(f"Found {len(user_preds)} season predictions to score")

            # Loop over users and calculate score
            for guild_id, user_id, username, user_wdc, user_wcc in user_preds:
                score = 0
                if user_wdc == real_wdc:
                    score += 20  # points for WDC
                    print(f"User {username} got WDC correct!")
                if user_wcc == real_wcc:
                    score += 20 # points for WCC
                    print(f"User {username} got WCC correct!")

                # Insert or update
                cur.execute("""
                    INSERT INTO final_scores (guild_id, user_id, username, points)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT(guild_id, user_id) DO UPDATE SET
                        points = excluded.points
                """, (guild_id, user_id, username, score))
                
                print(f"Scored user {username}: {score} points")

            conn.commit()
            print("Final champions scored successfully!")
            cur.close()
        except Exception as e:
            conn.rollback()
            print(f"Error scoring final champions: {e}")
            import traceback
            traceback.print_exc()
        finally:
            conn.close()

def score_final_champions_for_guild(guild_id):
    # Fetch all predictions for this guild
    predictions = safe_fetch_all(
        "SELECT * FROM season_predictions WHERE guild_id=%s",
        (guild_id,)
    )
    if not predictions:
        return

    # Fetch real final champions
    result = safe_fetch_one(
        "SELECT wdc, wcc FROM final_champions"
    )
    if not result:
        return

    real_wdc = result['wdc']
    real_wcc = result['wcc']

    for pred in predictions:
        score = 0

        if pred['wdc'] == real_wdc:
            score += 20

        if pred['wcc'].lower() == real_wcc.lower():
            score += 20

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
