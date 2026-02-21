import asyncio
import logging
from datetime import datetime, timedelta, timezone
import fastf1
from fastf1.ergast import Ergast
import pandas as pd
from database import safe_fetch_one
from get_now import get_now, SEASON

logger = logging.getLogger(__name__)

async def season_calender(season):
    try:
        schedule = await asyncio.to_thread(fastf1.get_event_schedule, season)
        return schedule['EventName'].tolist()
    except Exception:
        logger.exception("Failed to fetch season calendar for %s", season) 

        
async def refresh_race_cache(now=None, year=None):
    if year is None:
        year = SEASON

    if now is None:
        now = datetime.now(timezone.utc)
    try:
        schedule = await asyncio.to_thread(fastf1.get_event_schedule, year)
    except Exception:
        logger.exception("Failed to fetch F1 schedule for %s", year)
        return None

    try:
        # Convert dates
        date_cols = ["Session1DateUtc","Session2DateUtc","Session4DateUtc","Session5DateUtc"]
        for col in date_cols:
            schedule[col] = pd.to_datetime(schedule[col], utc=True, errors='coerce')
    except Exception:
        logger.exception("Failed processing schedule dates for %s", year)
        return None    
    
    # Keep only future races
    future_races = schedule[schedule['Session5DateUtc'].notna() & 
                            (schedule['Session5DateUtc'] > now)]

    if future_races.empty:
        return None  # no upcoming races
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

    try:
        schedule = await asyncio.to_thread(fastf1.get_event_schedule, year)
    except Exception:
        logger.exception("Failed to fetch season calendar for %s", year)
        return None

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
    
    race_results = pd.DataFrame()  # init before the try
    quali_results = pd.DataFrame() 

    try:
        # Race session
        race_session = fastf1.get_session(year, finished_race, "Race")
        race_session.load(laps=True, telemetry=False, weather=False, messages=False)
        race_results = race_session.results
        if race_results.empty:
            logger.warning("Race results are empty for %s", finished_race)

    except Exception:
        logger.exception("Error when fetching race results for %s", finished_race)

    if race_results.empty:
        return None
    pos1 = race_results.iloc[0]["Abbreviation"]
    pos2 = race_results.iloc[1]["Abbreviation"]
    pos3 = race_results.iloc[2]["Abbreviation"]
    fastest_lap_driver = race_session.laps.pick_fastest()['Driver']
    constructor_points = race_results.groupby("TeamName")["Points"].sum()
    winning_constructor = constructor_points.idxmax()

    try:
        # Qualifying session
        quali_session = fastf1.get_session(year, finished_race, "Qualifying")
        quali_session.load(laps=False, telemetry=False, weather=False, messages=False)
        quali_results = quali_session.results
        if quali_results.empty:
            logger.warning("Quali results are empty for %s", finished_race)

    except Exception:
        logger.exception("Failed to get quali results for %s", finished_race)
    
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

    try:
        schedule = await asyncio.to_thread(fastf1.get_event_schedule, year)
    except Exception:
        logger.exception("Failed to fetch season calendar for %s", year)
        return None

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
    
    sprint_results = pd.DataFrame()  # init before the try
    sprintquali_results = pd.DataFrame() 

    try:
    # Sprint session
        sprint_session = fastf1.get_session(year, finished_race, "Sprint")
        sprint_session.load(laps=False, telemetry=False, weather=False, messages=False)
        sprint_results = sprint_session.results
        if sprint_results.empty:
            logger.warning("Sprint results are empty for %s", finished_race)

    except Exception:
        logger.exception("Failed to load Sprint results for %s", finished_race)

    sprint_winner = sprint_results.iloc[0]["Abbreviation"]

    try:
        # Sprint Qualifying session
        sprintquali_session = fastf1.get_session(year, finished_race, "Sprint Qualifying")
        sprintquali_session.load(laps=False, telemetry=False, weather=False, messages=False)
        sprintquali_results = sprintquali_session.results
        if sprintquali_results.empty:
            logger.warning("Sprint Quali results are empty for %s", finished_race)
    except Exception:
        logger.exception("Failed to load Sprint Quali for %s", finished_race)

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
    logger.info("CHAMPIONS: now=%s, year=%s", now, year)

    # Get season calendar
    try:
        calendar = await asyncio.to_thread(fastf1.get_event_schedule, year)
    except Exception:
        logger.exception("Failed to fetch season calendar for %s", year)
        return None

    last_race = calendar.iloc[-1]
    race_date = pd.to_datetime(last_race["Session5DateUtc"],utc=True,errors="coerce")
    logger.info("CHAMPIONS: last race=%s, race_date=%s", last_race['EventName'], race_date)
    logger.info("CHAMPIONS: threshold=%s, passed=%s", race_date + timedelta(hours=12), now >= race_date + timedelta(hours=12))
    if pd.isna(race_date):
        logger.info("CHAMPIONS: race_date is NaT, returning None")
        return None

    # Check if 0.5 day has passed since last race
    if now < race_date + timedelta(hours = 12):
        logger.info("CHAMPIONS: not ready yet")
        return None  # Not yet 0.5 day after last race

    try:
        logger.info("CHAMPIONS: fetching Ergast standings...")
        # Fetch standings from Ergast
        ergast = Ergast()
        driver_standings = ergast.get_driver_standings(season=year, round='last').content[0]
        constructor_standings = ergast.get_constructor_standings(season=year, round='last').content[0]
        logger.info("CHAMPIONS: WDC=%s, WCC=%s", driver_standings.iloc[0]['driverId'], constructor_standings.iloc[0]['constructorId'])
        if driver_standings.empty or constructor_standings.empty:
            logger.warning("Standings returned empty for season %s", year)
            return None
    except Exception:
        logger.exception("Failed to load standings for %s", year)
        return None
    
    wdc_winner = driver_standings.iloc[0]['driverId']
    wcc_winner = constructor_standings.iloc[0]['constructorId']
    return wdc_winner, wcc_winner

async def get_race_end_time(now):
    """Get the end time of the next unscored race."""
    try:
        schedule = await asyncio.to_thread(fastf1.get_event_schedule, SEASON)
    except Exception:
        logger.exception("Failed to fetch season calendar for %s", SEASON)
        return None

    
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

    try:
        calendar = await asyncio.to_thread(fastf1.get_event_schedule, year)
    except Exception:
        logger.exception("Failed to fetch season calendar for %s", year)
        return None

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
