import threading
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import os
import logging
import socket

load_dotenv()
logger = logging.getLogger(__name__)

BOLD_PRED_POINTS = int(os.getenv('BOLD_PRED_POINTS', 10))

#Force IPv4 
orig_getaddrinfo = socket.getaddrinfo
def getaddrinfo_ipv4(*args, **kwargs):
    return [ai for ai in orig_getaddrinfo(*args, **kwargs) if ai[0] == socket.AF_INET]
socket.getaddrinfo = getaddrinfo_ipv4

DATABASE_URL = os.getenv('DATABASE_URL')
try:
    conn = psycopg2.connect(DATABASE_URL)
    logger.info("Connected")
    cur = conn.cursor()
except Exception:
    logger.exception("Database connection failed")

def get_connection():
    return psycopg2.connect(DATABASE_URL)

db_lock = threading.Lock()

def init_db():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS race_predictions(
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            username TEXT NOT NULL,
            race_number INTEGER NOT NULL,
            race_name TEXT NOT NULL,

            pos1 TEXT,
            pos2 TEXT,
            pos3 TEXT,
            pole TEXT,
            fastest_lap TEXT,

            constructor_winner TEXT,

            sprint_winner TEXT,
            sprint_pole TEXT,

            PRIMARY KEY (guild_id, user_id, race_number)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS prediction_state (
                guild_id BIGINT NOT NULL PRIMARY KEY,
                season_open BIGINT NOT NULL DEFAULT 0
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS season_predictions (
                guild_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                username TEXT NOT NULL,
                wdc TEXT,
                wcc TEXT,
                    
                PRIMARY KEY (guild_id, user_id)
            );
        """)

        cur.execute("""CREATE TABLE IF NOT EXISTS prediction_locks (
        guild_id BIGINT NOT NULL,
        type TEXT,
        manual_override TEXT,
                    
        PRIMARY KEY (guild_id, type)
        );
    """)
        
        cur.execute("""CREATE TABLE IF NOT EXISTS race_results(
                    race_number INTEGER PRIMARY KEY,
                    race_name TEXT,
                    pos1 TEXT,
                    pos2 TEXT,
                    pos3 TEXT,
                    pole TEXT,
                    quali_second TEXT,
                    fastest_lap TEXT,
                    constructor TEXT,
                    sprint_winner TEXT,
                    sprint_pole TEXT,
                    is_sprint BOOLEAN DEFAULT FALSE)""")

        cur.execute("""CREATE TABLE IF NOT EXISTS race_scores (
        guild_id BIGINT NOT NULL,
        user_id BIGINT NOT NULL,
        username TEXT NOT NULL,
        race_number INTEGER NOT NULL,
        race_name TEXT,
        points INTEGER,
        PRIMARY KEY (guild_id, user_id, race_number)
    );
    """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS scored_races (
            guild_id BIGINT NOT NULL,
            race_number INTEGER NOT NULL,
            PRIMARY KEY (guild_id, race_number)
        );
    """)
        
        cur.execute("""
                CREATE TABLE IF NOT EXISTS championship_leaders (
                season INTEGER NOT NULL,
                type TEXT NOT NULL, 
                leader TEXT NOT NULL,
                PRIMARY KEY (season, type, leader)
            );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS final_champions (
            season INTEGER PRIMARY KEY NOT NULL,
            wdc TEXT,
            wdc_second TEXT,
            wcc TEXT,
            wcc_second TEXT
        );
    """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS final_scores (
                guild_id BIGINT NOT NULL,
                user_id BIGINT,
                username TEXT,
                points INTEGER,
                    
                PRIMARY KEY (guild_id, user_id)
            );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS scored_seasons (
                guild_id BIGINT NOT NULL,
                season INTEGER NOT NULL,
                PRIMARY KEY (guild_id, season)
            );
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS force_points_log (
            id SERIAL PRIMARY KEY,
            guild_id BIGINT NOT NULL,
            userid BIGINT NOT NULL,
            username TEXT NOT NULL,
            points_given INTEGER NOT NULL,
            reason TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS total_force_points (
            guild_id BIGINT NOT NULL,
            user_id BIGINT,
            username TEXT NOT NULL,
            points INTEGER DEFAULT 0,
                    
            PRIMARY KEY (guild_id, user_id)
            );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS leaderboard (
            guild_id BIGINT NOT NULL,
            user_id BIGINT,
            username TEXT,
            total_points INTEGER,
            fully_correct_podiums INTEGER DEFAULT 0,
            correct_podiums INTEGER DEFAULT 0,
            correct_poles INTEGER DEFAULT 0,
            correct_fastest_laps INTEGER DEFAULT 0,
            correct_constructors INTEGER DEFAULT 0,
            PRIMARY KEY (guild_id, user_id)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS crazy_predictions (
                id SERIAL PRIMARY KEY,
                guild_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                username TEXT NOT NULL,
                season INT NOT NULL,
                prediction TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL
            )
        """)

        cur.execute("""CREATE INDEX IF NOT EXISTS idx_crazy_predictions_guild_timestamp
                    ON crazy_predictions 
                    (guild_id, timestamp DESC);
            """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scored_crazy_predictions (
                id SERIAL PRIMARY KEY,
                guild_id BIGINT NOT NULL,
                crazy_pred_id INTEGER NOT NULL,
                user_id BIGINT NOT NULL,
                username TEXT NOT NULL,
                difficulty TEXT NOT NULL,
                points INTEGER NOT NULL,
                UNIQUE (guild_id, crazy_pred_id)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bold_predictions (
                guild_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                race_number INTEGER NOT NULL,
                username TEXT NOT NULL,
                race_name TEXT NOT NULL,
                prediction TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                PRIMARY KEY (guild_id, user_id, race_number)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS correct_bold_predictions (
                guild_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                username TEXT NOT NULL,
                race_name TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id, race_name)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS prediction_lock_log (
            id SERIAL PRIMARY KEY,
            guild_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            username TEXT NOT NULL,
            command TEXT NOT NULL,
            prediction TEXT NOT NULL,
            state TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
        
        cur.execute("""CREATE TABLE IF NOT EXISTS persistent_messages (
        guild_id BIGINT NOT NULL,
        key TEXT,
        channel_id BIGINT NOT NULL,
        message_id BIGINT NOT NULL,
                    
        PRIMARY KEY (guild_id, key)
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS guild_config (
        guild_id BIGINT PRIMARY KEY,
        prediction_channel_id BIGINT NOT NULL
    );
    """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS guilds (
                    guild_id BIGINT PRIMARY KEY,
                    guild_name TEXT NOT NULL
                );
            """)
        
        conn.commit()
        cur.close()
        conn.close()

    except Exception:
        logger.exception("Failed to initialize DB")

def safe_execute(query, params=()):
    """For writes: prevents concurrent write issues."""
    try:
        with db_lock:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
            cur.close()
            conn.close()
    except Exception:
        logger.exception("Failed to write to DB for params %s", params)

def safe_fetch_all(query, params=()):
    """For reads: no lock needed."""
    try:
        with db_lock:
            conn = get_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query, params)
            result = cur.fetchall()
            cur.close()
            conn.close()
            return result
    except Exception:
        logger.exception("Failed to fetch all from DB for params %s", params)

def safe_fetch_one(query, params=()):
    try:
        with db_lock:
            conn = get_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query, params)
            result = cur.fetchone()
            cur.close()
            conn.close()
            return result
    except Exception:
        logger.exception("Failed to fetch one from DB for params %s", params)

def upsert_guild(guild_id: int, guild_name: str):
        safe_execute("""
            INSERT INTO guilds (guild_id, guild_name)
            VALUES (%s, %s)
            ON CONFLICT (guild_id)
            DO UPDATE SET guild_name = excluded.guild_name;
        """, (guild_id, guild_name))

def save_race_predictions(guild_id, user_id, username, race_number, race_name, preds):
    safe_execute(
        """
        INSERT INTO race_predictions (
            guild_id, user_id, username, race_number, race_name,
            pos1, pos2, pos3, pole, fastest_lap
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(guild_id, user_id, race_number) DO UPDATE SET
            race_name = excluded.race_name,
            username = excluded.username,
            pos1 = excluded.pos1,
            pos2 = excluded.pos2,
            pos3 = excluded.pos3,
            pole = excluded.pole,
            fastest_lap = excluded.fastest_lap
        """,
        (guild_id, user_id, username, race_number, race_name, *preds)
    )

def save_constructor_prediction(guild_id, user_id, username, race_number, race_name, constructor):
    safe_execute(
        """
        INSERT INTO race_predictions (
            guild_id, user_id, username, race_number, race_name, constructor_winner
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT(guild_id, user_id, race_number) DO UPDATE SET
            race_name = excluded.race_name,
            username = excluded.username,
            constructor_winner = excluded.constructor_winner
        """,
        (guild_id, user_id, username, race_number, race_name, constructor)
    )

def save_sprint_predictions(guild_id, user_id, username, race_number, race_name, sprint_winner, sprint_pole):
    safe_execute(
        """
        INSERT INTO race_predictions (
            guild_id, user_id, username, race_number, race_name,
            sprint_winner, sprint_pole
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(guild_id, user_id, race_number) DO UPDATE SET
            race_name = excluded.race_name,
            username = excluded.username,
            sprint_winner = excluded.sprint_winner,
            sprint_pole = excluded.sprint_pole
        """,
        (guild_id, user_id, username, race_number, race_name, sprint_winner, sprint_pole)
    )

# ---------- lock state ----------
def set_season_state(guild_id, open_: bool):
    safe_execute("""
        INSERT INTO prediction_state (guild_id, season_open)
        VALUES (%s, %s)
        ON CONFLICT (guild_id)
        DO UPDATE SET season_open = EXCLUDED.season_open
    """, (guild_id, int(open_)))

def is_season_open(guild_id) -> bool:
    row = safe_fetch_one(
        "SELECT season_open FROM prediction_state WHERE guild_id = %s",
        (guild_id,)
    )
    return bool(row["season_open"]) if row else False


# ---------- predictions ----------
def save_season_prediction(guild_id, user_id: int, username:str,*, wdc: str, wcc: str):
    safe_execute("""
        INSERT INTO season_predictions (guild_id, user_id,username, wdc, wcc)
        VALUES (%s, %s,%s, %s, %s)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET
            wdc = excluded.wdc,
            wcc = excluded.wcc
    """, (guild_id, user_id,username, wdc, wcc))

def guild_default_lock(guild_id: int):
    safe_execute("""
        INSERT INTO prediction_state (guild_id, season_open)
        VALUES (%s, 0)
        ON CONFLICT (guild_id) DO NOTHING;
    """, (guild_id,))

def ensure_lock_rows(guild_id: int):
    safe_execute("""
        INSERT INTO prediction_locks (guild_id, type, manual_override)
        VALUES (%s, 'race', NULL),
               (%s, 'sprint', NULL)
        ON CONFLICT (guild_id, type) DO NOTHING;
    """, (guild_id, guild_id))

def set_manual_lock(guild_id, pred_type: str, state: str | None):
    safe_execute(
        "UPDATE prediction_locks SET manual_override = %s WHERE guild_id = %s AND type = %s",
        (state, guild_id, pred_type)
    )

def get_manual_lock(guild_id, pred_type: str) -> str | None:
    row = safe_fetch_one(
        "SELECT manual_override FROM prediction_locks WHERE guild_id = %s AND type = %s",
        (guild_id, pred_type)
    )
    return row["manual_override"] if row else None

def reset_locks_on_cache_refresh(guild_id):
    safe_execute("""
        UPDATE prediction_locks
        SET manual_override = NULL
        WHERE guild_id = %s AND type IN ('race', 'sprint')
    """, (guild_id,))

def save_race_results(data):
    # Check if race already exists
    row = safe_fetch_one(
        "SELECT 1 FROM race_results WHERE race_number = %s",
        (data["race_number"],)
    )
    if row:
        return False  # already saved

    safe_execute("""
        INSERT INTO race_results (
            race_number, race_name,
            pos1, pos2, pos3,
            pole, quali_second, fastest_lap, constructor,
            is_sprint
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        data["race_number"],
        data["race_name"],
        data["pos1"],
        data["pos2"],
        data["pos3"],
        data["pole"],
        data["quali_second"],
        data["fastest_lap"],
        data["winning_constructor"],
        False
    ))

    return True

def save_sprint_results(data):
    safe_execute("""
        UPDATE race_results
        SET sprint_winner= %s,
            sprint_pole=%s
        WHERE race_number=%s
    """, (
        data["sprint_winner"],
        data["sprint_pole"],
        data["race_number"]
    ))

def save_final_champions(season, wdc, wdc_second, wcc, wcc_second):
    safe_execute(
        """INSERT INTO final_champions (season, wdc, wdc_second, wcc, wcc_second) 
            VALUES (%s , %s, %s, %s, %s)
            ON CONFLICT (season) DO UPDATE SET
            wdc = excluded.wdc,
            wdc_second = excluded.wdc_second,
            wcc = excluded.wcc,
            wcc_second = excluded.wcc_second
            """,
        (season, wdc, wdc_second, wcc, wcc_second)
    )

def save_championship_leaders(season, wdc_leader, wcc_leader):
    safe_execute("""
        INSERT INTO championship_leaders (season, type, leader)
        VALUES (%s, 'wdc', %s)
        ON CONFLICT DO NOTHING
    """, (season, wdc_leader))
    
    safe_execute("""
        INSERT INTO championship_leaders (season, type, leader)
        VALUES (%s, 'wcc', %s)
        ON CONFLICT DO NOTHING
    """, (season, wcc_leader))

def has_led_championship(season, leader, type):
    row = safe_fetch_one("""
        SELECT 1 FROM championship_leaders 
        WHERE season = %s AND type = %s AND leader = %s
    """, (season, type, leader))
    return row is not None

def is_race_scored(guild_id, race_number):
    row = safe_fetch_one(
        "SELECT 1 FROM scored_races WHERE guild_id = %s AND race_number = %s",
        (guild_id, race_number)
    )
    return row is not None

def mark_race_scored(guild_id, race_number):
    safe_execute("""
        INSERT INTO scored_races (guild_id, race_number)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
    """, (guild_id, race_number))

def is_season_scored(guild_id, season):
    row = safe_fetch_one(
        "SELECT 1 FROM scored_seasons WHERE guild_id = %s AND season = %s",
        (guild_id, season)
    )
    return row is not None

def mark_season_scored(guild_id, season):
    safe_execute("""
        INSERT INTO scored_seasons (guild_id, season)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
    """, (guild_id, season))

def add_points(guild_id, user_id: str, username: str, points: int, reason: str):
    safe_execute(
        "INSERT INTO force_points_log (guild_id, userid, username, points_given, reason) VALUES (%s, %s, %s, %s, %s)",
        (guild_id, user_id, username, points, reason)
    )

    # Update or insert total points
    safe_execute("""
        INSERT INTO total_force_points (guild_id, user_id, username, points)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT(guild_id, user_id) DO UPDATE SET
            points = total_force_points.points + excluded.points,
            username = excluded.username
    """, (guild_id, user_id, username, points))

def update_leaderboard(guild_id):
    with db_lock:
        conn = get_connection()
        try:
            cur = conn.cursor()

            cur.execute("DELETE FROM leaderboard WHERE guild_id = %s;", (guild_id,))

            cur.execute("""
                WITH combined_points AS (
                    SELECT guild_id, user_id, username, points
                    FROM race_scores
                    WHERE guild_id = %s

                    UNION ALL

                    SELECT guild_id, user_id, username, points
                    FROM final_scores
                    WHERE guild_id = %s

                    UNION ALL

                    SELECT guild_id, user_id, username, points
                    FROM total_force_points
                    WHERE guild_id = %s

                    UNION ALL

                    SELECT guild_id, user_id, username, %s as points
                    FROM correct_bold_predictions
                    WHERE guild_id = %s
                        
                    UNION ALL

                    SELECT guild_id, user_id, username, points
                    FROM scored_crazy_predictions
                    WHERE guild_id = %s
                ),
                total AS (
                    SELECT guild_id, user_id, MAX(username) as username, SUM(points) as total_points
                    FROM combined_points
                    GROUP BY guild_id, user_id
                ),
                tiebreakers AS (
                    SELECT 
                        rp.guild_id,
                        rp.user_id,
                        COUNT(CASE WHEN rp.pos1 = rr.pos1 AND rp.pos2 = rr.pos2 AND rp.pos3 = rr.pos3 THEN 1 END) AS fully_correct_podiums,
                        COUNT(CASE WHEN rp.pos1 = rr.pos1 THEN 1 END) +
                        COUNT(CASE WHEN rp.pos2 = rr.pos2 THEN 1 END) +
                        COUNT(CASE WHEN rp.pos3 = rr.pos3 THEN 1 END) AS correct_podiums,
                        COUNT(CASE WHEN rp.pole = rr.pole THEN 1 END) AS correct_poles,
                        COUNT(CASE WHEN rp.fastest_lap = rr.fastest_lap THEN 1 END) AS correct_fastest_laps,
                        COUNT(CASE WHEN rp.constructor_winner = rr.constructor THEN 1 END) AS correct_constructors
                    FROM race_predictions rp
                    LEFT JOIN race_results rr ON rr.race_number = rp.race_number
                    WHERE rp.guild_id = %s
                    GROUP BY rp.guild_id, rp.user_id
                )
                INSERT INTO leaderboard (guild_id, user_id, username, total_points, fully_correct_podiums, correct_podiums, correct_poles, correct_fastest_laps, correct_constructors)
                SELECT 
                    t.guild_id,
                    t.user_id,
                    t.username,
                    t.total_points,
                    COALESCE(tb.fully_correct_podiums, 0),
                    COALESCE(tb.correct_podiums, 0),
                    COALESCE(tb.correct_poles, 0),
                    COALESCE(tb.correct_fastest_laps, 0),
                    COALESCE(tb.correct_constructors, 0)
                FROM total t
                LEFT JOIN tiebreakers tb ON tb.user_id = t.user_id AND tb.guild_id = t.guild_id
                ORDER BY 
                    t.total_points DESC,
                    fully_correct_podiums DESC,
                    correct_podiums DESC,
                    correct_poles DESC,
                    correct_fastest_laps DESC,
                    correct_constructors DESC;
            """, (guild_id, guild_id, guild_id, BOLD_PRED_POINTS, guild_id, guild_id, guild_id))

            conn.commit()
            cur.close()

        except Exception as e:
            conn.rollback()
            logger.exception("Error updating leaderboard for guild %s", guild_id)
        finally:
            conn.close()

def get_top_n(guild_id, n):
    return safe_fetch_all("""
        SELECT username, total_points
        FROM leaderboard
        WHERE guild_id = %s
        ORDER BY 
            total_points DESC,
            fully_correct_podiums DESC,
            correct_podiums DESC,
            correct_poles DESC,
            correct_fastest_laps DESC,
            correct_constructors DESC
        LIMIT %s
    """, (guild_id, n))

def get_user_rank(guild_id, username):
    return safe_fetch_one("""
        SELECT rank, total_points FROM (
            SELECT username, total_points,
                   RANK() OVER (
                       ORDER BY 
                        total_points DESC,
                        fully_correct_podiums DESC,
                        correct_podiums DESC,
                        correct_poles DESC,
                        correct_fastest_laps DESC,
                        correct_constructors DESC
                   ) AS rank
            FROM leaderboard
            WHERE guild_id = %s
        ) sub
        WHERE username = %s
    """, (guild_id, username))

def save_crazy_prediction(guild_id, user_id, username, season, prediction, timestamp):
    safe_execute(
        """
        INSERT INTO crazy_predictions (guild_id, user_id, username, season, prediction, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (guild_id, user_id, username, season, prediction, timestamp)
    )

def get_crazy_predictions(guild_id, user_id, season):
    return safe_fetch_all(
        """
        SELECT prediction, timestamp
        FROM crazy_predictions
        WHERE guild_id = %s AND user_id = %s AND season = %s
        ORDER BY timestamp ASC
        """,
        (guild_id, user_id, season)
    )


def count_crazy_predictions(guild_id, user_id, season):
    result = safe_fetch_all(
        """
        SELECT COUNT(*) AS count
        FROM crazy_predictions
        WHERE guild_id = %s AND user_id = %s AND season = %s
        """,
        (guild_id, user_id, season)
    )
    return result[0]["count"]

def save_scored_crazy_prediction(guild_id, crazy_pred_id, user_id, username, difficulty, points):
    safe_execute("""
        INSERT INTO scored_crazy_predictions (guild_id, crazy_pred_id, user_id, username, difficulty, points)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (guild_id, crazy_pred_id) DO UPDATE SET
            difficulty = excluded.difficulty,
            points = excluded.points
    """, (guild_id, crazy_pred_id, user_id, username, difficulty, points))

def get_all_crazy_predictions_for_user(guild_id, user_id, season):
    return safe_fetch_all("""
        SELECT cp.id, cp.user_id, cp.username, cp.prediction, cp.timestamp,
               scp.difficulty, scp.points
        FROM crazy_predictions cp
        LEFT JOIN scored_crazy_predictions scp
            ON scp.crazy_pred_id = cp.id AND scp.guild_id = %s
        WHERE cp.guild_id = %s AND cp.user_id = %s AND cp.season = %s
        ORDER BY cp.timestamp ASC
    """, (guild_id, guild_id, user_id, season))

def get_all_crazy_predictions(guild_id, season):
    return safe_fetch_all("""
        SELECT cp.id, cp.user_id, cp.username, cp.prediction, cp.timestamp,
               scp.difficulty, scp.points
        FROM crazy_predictions cp
        LEFT JOIN scored_crazy_predictions scp 
            ON scp.crazy_pred_id = cp.id AND scp.guild_id = %s
        WHERE cp.guild_id = %s AND cp.season = %s
        ORDER BY cp.timestamp ASC
    """, (guild_id, guild_id, season))

def remove_scored_crazy_prediction(guild_id, crazy_pred_id):
    safe_execute("""
        DELETE FROM scored_crazy_predictions
        WHERE guild_id = %s AND crazy_pred_id = %s
    """, (guild_id, crazy_pred_id))

def save_bold_prediction(guild_id, user_id, race_number, username, race_name, prediction, timestamp):
    safe_execute(
        """
        INSERT INTO bold_predictions
        (guild_id, user_id, race_number, username, race_name, prediction, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(guild_id, user_id, race_number) DO UPDATE SET
            username = excluded.username,
            race_name = excluded.race_name,
            prediction = excluded.prediction,
            timestamp = excluded.timestamp
        """,
        (guild_id, user_id, race_number, username, race_name, prediction, timestamp)
    )

def fetch_bold_predictions(guild_id, race_number=None, race_name=None):
    if race_number is not None:
        return safe_fetch_all("""
            SELECT username, prediction
            FROM bold_predictions
            WHERE guild_id = %s AND race_number = %s
            ORDER BY timestamp ASC
            """,
            (guild_id, race_number))
    
    elif race_name is not None:
        return safe_fetch_all("""
            SELECT username, prediction
            FROM bold_predictions
            WHERE guild_id = %s AND race_name = %s
            ORDER BY timestamp ASC
            """,
            (guild_id, race_name))
    else:
        return []
    
def save_correct_bold_prediction(guild_id, user_id, username, race_name):
    safe_execute("""
        INSERT INTO correct_bold_predictions (guild_id, user_id, username, race_name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (guild_id, user_id, username, race_name))

def get_correct_bold_predictions(guild_id, user_id):
    return safe_fetch_all("""
        SELECT cbp.race_name, bp.prediction
        FROM correct_bold_predictions cbp
        LEFT JOIN bold_predictions bp 
            ON cbp.guild_id = bp.guild_id 
            AND cbp.user_id = bp.user_id 
            AND cbp.race_name = bp.race_name
        WHERE cbp.guild_id = %s AND cbp.user_id = %s
        ORDER BY cbp.race_name ASC
    """, (guild_id, user_id))

def prediction_state_log(guild_id, user_id, username, command, prediction, state):
    safe_execute("""
        INSERT INTO prediction_lock_log
        (guild_id, user_id, username, command, prediction, state)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (guild_id, user_id, username, command, prediction, state))
        
def get_persistent_message(guild_id, key):
    return safe_fetch_one(
        "SELECT channel_id, message_id FROM persistent_messages WHERE guild_id = %s AND key = %s",
        (guild_id, key)
    )

def save_persistent_message(guild_id, key, channel_id, message_id):
    safe_execute(
        """
        INSERT INTO persistent_messages (guild_id, key, channel_id, message_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (guild_id, key)
        DO UPDATE SET channel_id = excluded.channel_id,
                      message_id = excluded.message_id
        """,
        (guild_id, key, channel_id, message_id)
    )

def set_prediction_channel(guild_id: int, channel_id: int):
    query = """
        INSERT INTO guild_config (guild_id, prediction_channel_id)
        VALUES (%s, %s)
        ON CONFLICT (guild_id)
        DO UPDATE SET prediction_channel_id = EXCLUDED.prediction_channel_id;
    """
    safe_execute(query, (guild_id, channel_id))


def get_prediction_channel(guild_id: int):
    """Returns an int channel ID or None"""
    row = safe_fetch_one(
        "SELECT prediction_channel_id FROM guild_config WHERE guild_id = %s",
        (guild_id,)
    )
    if row is None:
        return None

    # safe-fetch returns a dict
    return row.get("prediction_channel_id")
