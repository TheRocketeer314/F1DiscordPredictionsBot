import asyncio
from datetime import datetime, timedelta, timezone
import fastf1
from fastf1.ergast import Ergast
import pandas as pd
from database import safe_fetch_one
from get_now import get_now, SEASON

async def season_calender(season):
    schedule = await asyncio.to_thread(fastf1.get_event_schedule, season)
    return schedule['EventName'].tolist()

        
async def refresh_race_cache(now=None, year=None):
    if year is None:
        year = SEASON

    if now is None:
        now = datetime.now(timezone.utc)
    #print("Fetching F1 schedule...")
    # Fetch schedule once
    schedule = await asyncio.to_thread(fastf1.get_event_schedule, year)

    #print("Schedule fetched!")

    # Convert only the columns we need to UTC-aware datetime
    date_cols = ["Session1DateUtc", "Session2DateUtc", "Session4DateUtc", "Session5DateUtc"]
    for col in date_cols:
        schedule[col] = pd.to_datetime(schedule[col], utc=True, errors='coerce')#.dt.to_pydatetime()
    # Keep only future races
    future_races = schedule[schedule['Session5DateUtc'].notna() & 
                            (schedule['Session5DateUtc'] > now)]

    if future_races.empty:
        return None  # no upcoming races
    #print (future_races)
    next_race = future_races.iloc[0]
    cache_race = future_races.iloc[1] if len(future_races) > 1 else None

    # Race info
    race_number = int(next_race["RoundNumber"])
    race_name = next_race["EventName"]
    location = next_race["Location"]
    country = next_race["Country"]
    event_format = next_race["EventFormat"]

    is_sprint = "sprint" in event_format.lower()

    #lock_time = next_race["Session4DateUtc"]
    #sprint_lock_time = next_race["Session2DateUtc"] if is_sprint else None

    lock_time_panda = next_race["Session4DateUtc"]
    if pd.isna(lock_time_panda):
        lock_time_panda = None

    if lock_time_panda is not None:
        lock_time = lock_time_panda.to_pydatetime().astimezone(timezone.utc)
    else: 
        lock_time = None

    sprint_lock_time_panda = next_race["Session2DateUtc"] if is_sprint and not pd.isna(next_race["Session2DateUtc"]) else None

    if sprint_lock_time_panda is not None:
        sprint_lock_time = sprint_lock_time_panda.to_pydatetime().astimezone(timezone.utc)
    else:
        sprint_lock_time = None

    if cache_race is not None:
        cache_race_time = cache_race["Session5DateUtc"].to_pydatetime().astimezone(timezone.utc)
        next_refresh = (cache_race_time - timedelta(days=5))
    else:
        next_refresh = None

    return {
        "race_number": race_number,
        "race_name": race_name,
        "location": location,
        "country": country,
        "event_format": event_format,
        "is_sprint": is_sprint,
        "lock_time": lock_time,
        "sprint_lock_time": sprint_lock_time,
        "next_refresh": next_refresh,
    }

async def race_results(year=None):
    if year is None:
        year = SEASON

    schedule = await asyncio.to_thread(fastf1.get_event_schedule, year)


    # FORCE UTC-awareness
    schedule["Session5DateUtc"] = pd.to_datetime(
        schedule["Session5DateUtc"],
        utc=True,
        errors="coerce"
    )

    schedule["race_end"] = schedule["Session5DateUtc"] + pd.Timedelta(hours=12)

    now = get_now()

    finished = schedule[schedule["race_end"] < now]

    if finished.empty:
        return None

    finished_race_list = finished.iloc[-1]  # LAST finished race

    finished_race = finished_race_list["EventName"]
    race_number = int(finished_race_list["RoundNumber"])
    race_end_panda = finished_race_list["Session5DateUtc"] 
    race_end = race_end_panda.to_pydatetime().astimezone(timezone.utc)
    race_end += timedelta(hours=6)
    if now < race_end:
        return None # Too early
    # Race session
    race_session = fastf1.get_session(year, finished_race, "Race")
    race_session.load(laps=True, telemetry=False, weather=False, messages=False)
    
    race_results = race_session.results
    if race_results.empty:
        return None
    pos1 = race_results.iloc[0]["Abbreviation"]
    pos2 = race_results.iloc[1]["Abbreviation"]
    pos3 = race_results.iloc[2]["Abbreviation"]
    fastest_lap_driver = race_session.laps.pick_fastest()['Driver']
    constructor_points = race_results.groupby("TeamName")["Points"].sum()
    winning_constructor = constructor_points.idxmax()

    # Qualifying session
    quali_session = fastf1.get_session(year,finished_race, "Qualifying")
    quali_session.load(laps=False, telemetry=False, weather=False, messages=False)
    quali_results = quali_session.results

    
    pole = quali_results.iloc[0]["Abbreviation"] if not quali_results.empty else None
    quali_second = quali_results.iloc[1]["Abbreviation"] if not quali_results.empty else None


    return {
        "race_number": race_number,
        "race_name": finished_race,
        "pos1": pos1,
        "pos2": pos2,
        "pos3": pos3,
        "pole": pole,
        "quali_second": quali_second,
        "fastest_lap": fastest_lap_driver,
        "winning_constructor": winning_constructor
    }

async def sprint_results(year=None):
    if year is None:
        year = SEASON

    schedule = await asyncio.to_thread(fastf1.get_event_schedule, year)

    # Get timing of the next Quali session
    schedule["Session5DateUtc"] = pd.to_datetime(
        schedule["Session5DateUtc"], utc=True, errors="coerce"
    )
    schedule["race_end"] = schedule["Session5DateUtc"] + pd.Timedelta(hours=12)

    now = get_now()
    finished = schedule[schedule["race_end"] < now]

    if finished.empty:
        return None

    finished_race_list = finished.iloc[-1]  # LAST finished race

    finished_race = finished_race_list["EventName"]
    race_number = int(finished_race_list["RoundNumber"])
    race_end_panda = finished_race_list["Session5DateUtc"] 
    race_end = race_end_panda.to_pydatetime().astimezone(timezone.utc)
    race_end += timedelta(hours=6)
    event_format = finished_race_list["EventFormat"]

    # Check if it's a sprint weekend using EventFormat column
    is_sprint = "sprint" in event_format.lower()
    if not is_sprint:
        return None
    if  now < race_end:
        return None # Too early
    # Race session
    sprint_session = fastf1.get_session(year, finished_race, "Sprint")
    sprint_session.load(laps=False, telemetry=False, weather=False, messages=False)
    sprint_results = sprint_session.results

    sprint_winner = sprint_results.iloc[0]["Abbreviation"]

    # Qualifying session
    sprintquali_session = fastf1.get_session(year, finished_race, "Sprint Qualifying")
    sprintquali_session.load(laps=False, telemetry=False, weather=False, messages=False)
    sprintquali_results = sprintquali_session.results

    sprint_pole = sprintquali_results.iloc[0]["Abbreviation"] if not sprintquali_results.empty else None

    return {
        "race_number": race_number,
        "race_name": finished_race,
        "sprint_winner": sprint_winner,
        "is_sprint": is_sprint,
        "sprint_pole": sprint_pole
    }
 
async def get_final_champions_if_ready(year=None):
    if year is None:
        year = SEASON

    now = get_now()

    """
    Checks if the current time is at least 1 day after the last race of the season.
    If yes, fetches WDC and WCC winners from Ergast and returns them.
    Returns (WDC_winner, WCC_winner) or None if not ready.
    """
    # Get season calendar
    calendar = await asyncio.to_thread(fastf1.get_event_schedule, year)

    last_race = calendar.iloc[-1]
    race_date = pd.to_datetime(last_race["Session5DateUtc"],utc=True,errors="coerce")

    if pd.isna(race_date):
        return None

    # Check if 0.5 day has passed since last race
    if now < race_date + timedelta(hours = 12):
        return None  # Not yet 0.5 day after last race

    # Fetch standings from Ergast
    ergast = Ergast()
    driver_standings = ergast.get_driver_standings(season=year, round='last').content[0]
    constructor_standings = ergast.get_constructor_standings(season=year, round='last').content[0]

    # Winners are first in the list
    wdc_winner = driver_standings.iloc[0]['driverId']
    wcc_winner = constructor_standings.iloc[0]['constructorId']

    return wdc_winner, wcc_winner

async def get_race_end_time(now):
    """Get the end time of the next unscored race."""
    schedule = await asyncio.to_thread(fastf1.get_event_schedule, SEASON)

    
    schedule["Session5DateUtc"] = pd.to_datetime(
        schedule["Session5DateUtc"],
        utc=True,
        errors="coerce"
    )
    
    schedule["race_end"] = schedule["Session5DateUtc"] + pd.Timedelta(hours=12)
    
    # Get all finished races
    finished = schedule[schedule["race_end"] < now]
    
    if finished.empty:
        # No races finished yet, return the next one
        upcoming = schedule[schedule["race_end"] >= now]
        if not upcoming.empty:
            return upcoming.iloc[0]["race_end"].to_pydatetime().astimezone(timezone.utc)
        return None
    
    # Check finished races from most recent backwards
    for idx in range(len(finished) - 1, -1, -1):
        race = finished.iloc[idx]
        race_number = int(race["RoundNumber"])
        
        # Check if this race has been scored
        existing_scores = safe_fetch_one(
            "SELECT 1 FROM race_scores WHERE race_number = %s LIMIT 1",
            (race_number,)
        )
        
        if not existing_scores:
            # This race hasn't been scored yet
            return race["race_end"].to_pydatetime().astimezone(timezone.utc)
    
    # All finished races are scored, wait for the next race
    upcoming = schedule[schedule["race_end"] >= now]
    if not upcoming.empty:
        return upcoming.iloc[0]["race_end"].to_pydatetime().astimezone(timezone.utc)
    
    return None

async def get_season_end_time(year=None):
    if year is None:
        year = SEASON

    calendar = await asyncio.to_thread(fastf1.get_event_schedule, year)

    last_race = calendar.iloc[-1]

    race_start = pd.to_datetime(
        last_race["Session5DateUtc"],
        utc=True,
        errors="coerce"
    )

    if pd.isna(race_start):
        return None

    season_end_time = race_start + timedelta(hours=12)
    return season_end_time.to_pydatetime()


'''# Check and print final champions
result = get_final_champions_if_ready()
if result:
    wdc_winner, wcc_winner = result
    print(f"Final WDC Winner: {wdc_winner}")
    print(f"Final WCC Winner: {wcc_winner}")
else:
    print("Not yet 1 day after the last race, final standings not available.")'''


