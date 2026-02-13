import threading
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
#print (DATABASE_URL)
try:
    conn = psycopg2.connect(DATABASE_URL)
    #print("Connected")
    cur = conn.cursor()
    #cur.execute("SELECT current_database(), current_schema(), inet_server_addr()")
    #print(cur.fetchone())
except Exception as e:
    print("Failed:", e)

def get_connection():
    return psycopg2.connect(DATABASE_URL)

'''conn = sqlite3.connect("predictionstest2.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
def get_connection():
    return sqlite3.connect("predictionstest2.db")'''

db_lock = threading.Lock()

def init_db():
    #cur.execute("DROP TABLE IF EXISTS race_predictions")
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS race_predictions(
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

        PRIMARY KEY (user_id, race_number)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS prediction_state (
            id BIGINT PRIMARY KEY,
            season_open BIGINT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS season_predictions (
            user_id BIGINT PRIMARY KEY,
            username TEXT NOT NULL,
            wdc TEXT,
            wcc TEXT
        );
    """)

    # ensure single row exists
    cur.execute("""
        INSERT INTO prediction_state (id, season_open)
        VALUES (1, 0)
        ON CONFLICT (id) DO NOTHING;
    """)

    cur.execute("""CREATE TABLE IF NOT EXISTS prediction_locks (
    type TEXT PRIMARY KEY,
    manual_override TEXT
    );
""")

    cur.execute("""INSERT INTO prediction_locks (type, manual_override) VALUES
    ('race', NULL),
    ('sprint', NULL)
    ON CONFLICT (type) DO NOTHING;
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
    user_id BIGINT NOT NULL,
    username TEXT NOT NULL,
    race_number INTEGER NOT NULL,
    race_name TEXT,
    points INTEGER,
    PRIMARY KEY (user_id, race_number)
);
""")
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS final_champions (
        wdc TEXT,
        wcc TEXT
    );
""")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS final_scores (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            points INTEGER
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS force_points_log (
        id SERIAL PRIMARY KEY ,
        userid BIGINT NOT NULL,
        username TEXT NOT NULL,
        points_given INTEGER NOT NULL,
        reason TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS total_force_points (
        userid BIGINT PRIMARY KEY,
        username TEXT NOT NULL,
        points INTEGER DEFAULT 0
        );
    """)

    cur.execute("""CREATE TABLE IF NOT EXISTS leaderboard (
        user_id BIGINT PRIMARY KEY,
        username TEXT,
        total_points INTEGER
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS crazy_predictions (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            username TEXT NOT NULL,
            season INT NOT NULL,
            prediction TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bold_predictions (
            user_id BIGINT NOT NULL,
            race_number INTEGER NOT NULL,
            username TEXT NOT NULL,
            race_name TEXT NOT NULL,
            prediction TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            PRIMARY KEY (user_id, race_number)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS prediction_lock_log (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        username TEXT NOT NULL,
        command TEXT NOT NULL,
        prediction TEXT NOT NULL,
        state TEXT NOT NULL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS persistent_messages (
    key TEXT PRIMARY KEY,
    channel_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL
    )""")

    conn.commit()
    cur.close()
    conn.close()

def safe_execute(query, params=()):
    """For writes: prevents concurrent write issues."""
    with db_lock:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        cur.close()
        conn.close()
    
def safe_fetch_all(query, params=()):
    """For reads: no lock needed."""
    with db_lock:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query, params)
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result

def safe_fetch_one(query, params=()):
    with db_lock:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query, params)
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result

def save_race_predictions(user_id, username, race_number, race_name, preds):
    safe_execute(
        """
        INSERT INTO race_predictions (
            user_id, username, race_number, race_name,
            pos1, pos2, pos3, pole, fastest_lap
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(user_id, race_number) DO UPDATE SET
            race_name = excluded.race_name,
            username = excluded.username,
            pos1 = excluded.pos1,
            pos2 = excluded.pos2,
            pos3 = excluded.pos3,
            pole = excluded.pole,
            fastest_lap = excluded.fastest_lap
        """,
        (user_id, username, race_number, race_name, *preds)
    )

def save_constructor_prediction(user_id, username, race_number, race_name, constructor):
    safe_execute(
        """
        INSERT INTO race_predictions (
            user_id, username, race_number, race_name, constructor_winner
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(user_id, race_number) DO UPDATE SET
            race_name = excluded.race_name,
            username = excluded.username,
            constructor_winner = excluded.constructor_winner
        """,
        (user_id, username, race_number, race_name, constructor)
    )

def save_sprint_predictions(user_id, username, race_number, race_name, sprint_winner, sprint_pole):
    safe_execute(
        """
        INSERT INTO race_predictions (
            user_id, username, race_number, race_name,
            sprint_winner, sprint_pole
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT(user_id, race_number) DO UPDATE SET
            race_name = excluded.race_name,
            username = excluded.username,
            sprint_winner = excluded.sprint_winner,
            sprint_pole = excluded.sprint_pole
        """,
        (user_id, username, race_number, race_name, sprint_winner, sprint_pole)
    )

# ---------- lock state ----------
def set_season_state(open_: bool):
    safe_execute(
        "UPDATE prediction_state SET season_open = %s WHERE id = 1",
        (int(open_),)
    )

def is_season_open() -> bool:
    row = safe_fetch_one("SELECT season_open FROM prediction_state WHERE id = %s", (1,))
    return bool(row["season_open"]) if row else False

# ---------- predictions ----------
def save_season_prediction(user_id: int, username:str,*, wdc: str, wcc: str):
    safe_execute("""
        INSERT INTO season_predictions (user_id,username, wdc, wcc)
        VALUES (%s,%s, %s, %s)
        ON CONFLICT(user_id)
        DO UPDATE SET
            wdc = excluded.wdc,
            wcc = excluded.wcc
    """, (user_id,username, wdc, wcc))

def set_manual_lock(pred_type: str, state: str | None):
    safe_execute(
        "UPDATE prediction_locks SET manual_override = %s WHERE type = %s",
        (state, pred_type)
    )

def get_manual_lock(pred_type: str) -> str | None:
    row = safe_fetch_one(
        "SELECT manual_override FROM prediction_locks WHERE type = %s",
        (pred_type,)
    )
    return row["manual_override"] if row else None

def reset_locks_on_cache_refresh():
    safe_execute("""
        UPDATE prediction_locks
        SET manual_override = NULL
        WHERE type IN ('race', 'sprint')
    """)

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

def save_final_champions(wdc, wcc):
    # Only one row ever for 2026, so replace any existing entry
    safe_execute("DELETE FROM final_champions")
    safe_execute(
        "INSERT INTO final_champions (wdc, wcc) VALUES (%s, %s)",
        (wdc, wcc)
    )

def add_points(user_id: str, username: str, points: int, reason: str):
    safe_execute(
        "INSERT INTO force_points_log (userid, username, points_given, reason) VALUES (%s, %s, %s, %s)",
        (user_id, username, points, reason)
    )

    # Update or insert total points
    safe_execute("""
        INSERT INTO total_force_points (userid, username, points)
        VALUES (%s, %s, %s)
        ON CONFLICT(userid) DO UPDATE SET
            points = total_force_points.points + excluded.points,
            username = excluded.username
    """, (user_id, username, points))

def update_leaderboard():
    with db_lock:
        conn = get_connection()
        try:
            cur = conn.cursor()
            
            # First, let's see what we're combining
            cur.execute("""
                SELECT user_id, username, points, 'race_scores' as source FROM race_scores
                UNION ALL
                SELECT user_id, username, points, 'final_scores' as source FROM final_scores
                UNION ALL
                SELECT userid AS user_id, username, points, 'total_force_points' as source FROM total_force_points
                ORDER BY user_id, source;
            """)
            
            all_data = cur.fetchall()
            #print("\n=== All data before grouping ===")
            #for row in all_data:
                #print(f"user_id: {row[0]}, username: {row[1]}, points: {row[2]}, source: {row[3]}")
            
            # Now do the actual update
            cur.execute("DELETE FROM leaderboard;")
            cur.execute("""
                WITH combined AS (
                    SELECT user_id, username, points FROM race_scores
                    UNION ALL
                    SELECT user_id, username, points FROM final_scores
                    UNION ALL
                    SELECT userid AS user_id, username, points FROM total_force_points
                )
                INSERT INTO leaderboard (user_id, username, total_points)
                SELECT
                    user_id,
                    MAX(username) AS username,
                    SUM(points) AS total_points
                FROM combined
                GROUP BY user_id
                ORDER BY total_points DESC;
            """)
            
            # Check what got inserted
            cur.execute("SELECT user_id, username, total_points FROM leaderboard ORDER BY total_points DESC;")
            results = cur.fetchall()
            #print("\n=== Leaderboard after update ===")
            for row in results:
                print(f"user_id: {row[0]}, username: {row[1]}, total_points: {row[2]}")
            
            conn.commit()
            cur.close()
        except Exception as e:
            conn.rollback()
            print("Error updating leaderboard:", e)
            import traceback
            traceback.print_exc()
        finally:
            conn.close() 

def get_top_n(n):
    """Return top n users with points"""
    return safe_fetch_all(
        "SELECT username, total_points FROM leaderboard ORDER BY total_points DESC LIMIT %s",
        (n,)
    )

def get_user_rank(username):
    """Return rank and points for a given user"""
    row = safe_fetch_one("""
        SELECT rank, total_points FROM (
            SELECT username, total_points, RANK() OVER (ORDER BY total_points DESC) AS rank
            FROM leaderboard
        ) sub
        WHERE username = %s
    """, (username,))
    return row  # row is a dict if using DictCursor; None if user not found

def save_crazy_prediction(user_id, username, season, prediction, timestamp):
    safe_execute(
        """
        INSERT INTO crazy_predictions (user_id, username, season, prediction, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (user_id, username, season, prediction, timestamp)
    )

def get_crazy_predictions(user_id, season):
    return safe_fetch_all(
        """
        SELECT prediction, timestamp
        FROM crazy_predictions
        WHERE user_id = %s AND season = %s
        ORDER BY timestamp ASC
        """,
        (user_id, season)
    )


def count_crazy_predictions(user_id, season):
    result = safe_fetch_all(
        """
        SELECT COUNT(*) AS count
        FROM crazy_predictions
        WHERE user_id = %s AND season = %s
        """,
        (user_id, season)
    )
    return result[0]["count"]

def save_bold_prediction(user_id, race_number, username, race_name, prediction, timestamp):
    safe_execute(
        """
        INSERT INTO bold_predictions
        (user_id, race_number, username, race_name, prediction, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT(user_id, race_number) DO UPDATE SET
            username = excluded.username,
            race_name = excluded.race_name,
            prediction = excluded.prediction,
            timestamp = excluded.timestamp
        """,
        (user_id, race_number, username, race_name, prediction, timestamp)
    )

def fetch_bold_predictions(race_number):
    return safe_fetch_all("""
            SELECT username, prediction
            FROM bold_predictions
            WHERE race_number = %s
            ORDER BY timestamp ASC
            """,
            (race_number,)
        ) or []

def prediction_state_log(user_id, username, command, prediction, state):
        safe_execute("""
        INSERT INTO prediction_lock_log (user_id, username, command, prediction, state)
        VALUES (%s, %s, %s, %s, %s)
    """, (user_id, username, command, prediction, state)
    )
        
def get_persistent_message(key):
        return safe_fetch_one(
            "SELECT channel_id, message_id FROM persistent_messages WHERE key = %s",
            (key,)
        )

def save_persistent_message(key, channel_id, message_id):
        safe_execute(
            """
            INSERT INTO persistent_messages (key, channel_id, message_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (key)
            DO UPDATE SET channel_id = excluded.channel_id,
                          message_id = excluded.message_id
            """,
            (key, channel_id, message_id)
        )
