import discord
from discord import app_commands
from discord.ext import commands, tasks
import os
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
import asyncio
from config import CACHE_DIR
from pathlib import Path
from collections import defaultdict
from FastF1_service import refresh_race_cache, season_calender
from database import (init_db, save_race_predictions,
                      save_constructor_prediction,
                      save_sprint_predictions,
                      set_season_state,
                      is_season_open,
                      save_season_prediction,
                      set_manual_lock,
                      get_manual_lock,
                      reset_locks_on_cache_refresh,
                      add_points,
                      get_top_n,
                      get_user_rank,
                      save_crazy_prediction,
                      save_bold_prediction,
                      update_leaderboard,
                      prediction_state_log,
                      fetch_bold_predictions,
                      get_crazy_predictions,
                      count_crazy_predictions,
                      set_prediction_channel,
                      get_prediction_channel,
                      guild_default_lock,
                      ensure_lock_rows,
                      upsert_guild)

from results_watcher import poll_results_loop
from champions_watcher import final_champions_loop
from get_now import get_now, TIME_MULTIPLE, SEASON
from keep_alive import keep_alive

# Load environment variables
load_dotenv()
token = os.getenv('DISCORD_TOKEN')
handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')

keep_alive()

# Set up bot intents
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='/', intents=intents)

#Globals
RACE_CACHE: dict = {}
SEASON_CALENDER = []

main_predictions = {}
sprint_predictions = {}
constructors_predictions = {}

leaderboard_update_time = None

if RACE_CACHE.get("lock_time") is not None:
    leaderboard_update_time = RACE_CACHE.get("lock_time") + timedelta(days=3)

leaderboard_task = None

# Helpers

async def race_cache_watcher():
    await bot.wait_until_ready()

    while not bot.is_closed():
        next_refresh = RACE_CACHE.get("next_refresh")

        if next_refresh is None:
            # Nothing scheduled yet → check again later
            await asyncio.sleep(3600/TIME_MULTIPLE)
            continue

        now = get_now()
        delay = (next_refresh - now).total_seconds()

        if delay <= 0:
            # Missed it or startup case
            new_cache = await refresh_race_cache(now)
            if new_cache:
                RACE_CACHE.clear()
                RACE_CACHE.update(new_cache)

            for guild in bot.guilds:
                reset_locks_on_cache_refresh(guild.id)

            await asyncio.sleep(60/TIME_MULTIPLE)
            continue

        try:
            await asyncio.sleep(delay/TIME_MULTIPLE)
        except asyncio.CancelledError:
            return  # task cancelled on shutdown or restart

        # Time reached
        new_cache = await refresh_race_cache(now)
        if new_cache:
            RACE_CACHE.clear()
            RACE_CACHE.update(new_cache)

        for guild in bot.guilds:
            reset_locks_on_cache_refresh(guild.id)


# Function to check if predictions are open
def predictions_open(guild_id, now: datetime, RACE_CACHE) -> bool:
    lock_time = RACE_CACHE.get("lock_time")
    manual = get_manual_lock(guild_id, "race")

    if manual == "LOCKED":
        return False
    if manual == "OPEN":
        return True
    if manual == "AUTO":
        if lock_time is None:
            return False
        return now < lock_time
    
    if lock_time is None:
        return False
    
    return now < lock_time

# race_number -> { user_id -> [predictions] }
user_predictions = {}

def sprint_predictions_open(guild_id, now: datetime, RACE_CACHE) -> bool:
    sprint_lock_time = RACE_CACHE.get("sprint_lock_time")
    sprint_manual = get_manual_lock(guild_id, "sprint")

    if sprint_manual == "LOCKED":
        return False
    if sprint_manual == "OPEN":
        return True
    if sprint_manual == "AUTO":
        if sprint_lock_time is None:
            return False
        return now < sprint_lock_time
    
    if sprint_lock_time is None:
        return False
    
    return sprint_lock_time is not None and now < sprint_lock_time

@bot.event
async def on_ready():
    global SEASON_CALENDER
    init_db()
    await bot.tree.sync()
    initial = await refresh_race_cache(get_now())
    if initial:
        RACE_CACHE.update(initial)
    print("Bot is ready.")
    SEASON_CALENDER = await season_calender(SEASON)

    #print(lock_time,type(lock_time) )
    if getattr(bot, "cache_task_started", False):
        return # Prevent multiple tasks
    
    if not hasattr(bot, "race_cache_task"):
        bot.race_cache_task = asyncio.create_task(race_cache_watcher())
    
    if not hasattr(bot, "poll_results_task"):
        bot.poll_results_task = asyncio.create_task(poll_results_loop(bot))

    if not hasattr(bot, "final_champions_task"):
        bot.final_champions_task = asyncio.create_task(final_champions_loop(bot))

    if not bold_predictions_publisher.is_running():
        bold_predictions_publisher.start()
        print("Bold loop started")

    for guild in bot.guilds:
        upsert_guild(guild.id, guild.name)
        #print(f"Synced guild {guild.name} ({guild.id})")

    for guild in bot.guilds:
        upsert_guild(guild.id, guild.name)
        guild_default_lock(guild.id)
        ensure_lock_rows(guild.id)


    '''for cmd in bot.tree.walk_commands():
        print(cmd.name)
    print(race_name, type(race_name), int(RACE_CACHE.get("race_number")), type(race_number))'''


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction,
    error: discord.app_commands.AppCommandError
):
    print(f"Command error: {error}")
    import traceback
    traceback.print_exc()

    if interaction.response.is_done():
        await interaction.followup.send(
            f"❌ Error: {error}",
            ephemeral=True
        )
    else:
        await interaction.response.send_message(
            f"❌ Error: {error}",
            ephemeral=True
        )

@bot.event
async def on_guild_join(guild: discord.Guild):
    guild_id = guild.id
    print(f"Joined new guild: {guild.name} ({guild_id})")

    guild_name = guild.name

    # Adds Guild to Guild Table
    upsert_guild(guild.id, guild_name)
    # Set up default season state
    guild_default_lock(guild_id)

    # Add default prediction locks for this guild
    ensure_lock_rows(guild_id)

@bot.event
async def on_guild_update(before, after):
    if before.name != after.name:
        upsert_guild(after.id, after.name)

user_predictions = {}

prediction_locked = {}

PREDICTIONS = ["Position 1", "Position 2", "Position 3", "Pole", "Fastest Lap"]

PRED_INDEX = {
    "pos1": 0,
    "pos2": 1,
    "pos3": 2,
    "pole": 3,
    "fastest_lap": 4,
}


DRIVERS = [
    "VER","HAD","RUS","ANT","LEC","HAM","NOR","PIA",
    "SAI","ALB","LAW","LIN","OCO","BEA","ALO","STR",
    "PER","BOT","BOR","HUL","COL","GAS",]

CONSTRUCTORS = [
    "Mercedes", "Red Bull Racing", "Ferrari", "McLaren",
    "Aston Martin", "Haas", "Racing Bulls","Audi",
    "Williams", "Cadillac", "Alpine"]

user_predictions = defaultdict(
    lambda: defaultdict(
        lambda: defaultdict(lambda: [None] * 5)
    )
)
class PredictionSelect(discord.ui.Select):
    def __init__(self, index, current_selection=None):
        self.index = index
        options = [
            discord.SelectOption(label=d, value=d, default=(d == current_selection))
            for d in DRIVERS
        ]
        super().__init__(
            placeholder=f"Pick driver for {PREDICTIONS[index]}",
            options=options
        )

    async def callback(self, interaction: discord.Interaction):  # <-- indented inside class
        await interaction.response.defer(ephemeral=True)
        now = get_now()

        if not predictions_open(interaction.guild.id, now, RACE_CACHE):
            self.view.disable_all()
            await interaction.edit_original_response(
                content="🔒 Predictions are closed.",
                view=self.view
            )
            return

        guild_id = interaction.guild.id
        race_number = int(RACE_CACHE.get("race_number"))
        user_id = interaction.user.id
        preds = user_predictions[guild_id][race_number][user_id]

        selected_driver = self.values[0]
        previous = preds[self.index]

        if self.index < 3:
            other_selected = {v for i, v in enumerate(preds[:3]) if v and i != self.index}
            if selected_driver in other_selected and selected_driver != previous:
                await interaction.followup.send(
                    f"❌ {selected_driver} already selected for Positions 1-3. Pick a different driver!",
                    ephemeral=True
                )
                return

            preds[self.index] = selected_driver
            save_race_predictions(
                guild_id, user_id, interaction.user.name,
                race_number, RACE_CACHE.get("race_name"), preds
            )

            taken = {v for v in preds[:3] if v}

            for child in list(self.view.children):
                if isinstance(child, PredictionSelect) and child.index < 3:
                    curr = preds[child.index]
                    new_options = []
                    for d in DRIVERS:
                        if d not in taken or d == curr:
                            new_options.append(
                                discord.SelectOption(label=d, value=d, default=(d == curr))
                            )
                    child.options = new_options

            # Fix: use edit_original_response instead of followup.edit_message
            await interaction.edit_original_response(view=self.view)
            return

        # Positions 3 and 4 (Fastest Lap / Pole) — duplicates allowed
        preds[self.index] = selected_driver
        save_race_predictions(
            guild_id, user_id, interaction.user.name,
            race_number, RACE_CACHE.get("race_name"), preds
        )
class PredictionView(discord.ui.View):
    def __init__(self, guild_id, user_id=None):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        race_number = int(RACE_CACHE.get("race_number"))

        # Add the selects
        for i in range(5):
            current = None
            if user_id and user_id in user_predictions:
                current = user_predictions[guild_id][race_number][user_id][i]
            self.add_item(PredictionSelect(i, current_selection=current))

        # Disable everything if predictions are closed
        now = get_now()
        if not predictions_open(self.guild_id, now, RACE_CACHE):
            self.disable_all()

    def disable_all(self):
        for child in self.children:
            child.disabled = True

@bot.tree.command(name="race_predict", description="Make your race predictions")
async def predict(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True)
        await interaction.followup.send(
            f"Make your predictions for the {RACE_CACHE.get("race_name")}:",
            view=PredictionView(interaction.guild.id, interaction.user.id),
            ephemeral=True)
    except Exception as e:
        print(e)

class SprintPredictionView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        
        self.sprint_winner = None
        self.sprint_pole = None

        for item in [
            SprintWinnerSelect(self),
            SprintPoleSelect(self),
            SprintSubmitButton(self),
        ]:
            self.add_item(item)

class SprintWinnerSelect(discord.ui.Select):
    def __init__(self, view2: SprintPredictionView):
        self.view2 = view2

        super().__init__(
            placeholder="Select Sprint Winner",
            options =[discord.SelectOption(label=d) for d in DRIVERS],
            min_values=1,
            max_values=1,)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        self.view2.sprint_winner = self.values[0]

class SprintPoleSelect(discord.ui.Select):
    def __init__(self, view2: SprintPredictionView):
        self.view2 = view2

        super().__init__(
            placeholder="Select Sprint Pole",
            options =[discord.SelectOption(label=d) for d in DRIVERS],
            min_values=1,
            max_values=1,)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        self.view2.sprint_pole = self.values[0]

class SprintSubmitButton(discord.ui.Button):
    def __init__(self, view2: SprintPredictionView):
        self.view2 = view2
        super().__init__(label="Submit Sprint Predictions", style=discord.ButtonStyle.green)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        #print("Sprint Submit clicked:", self.view2.sprint_winner, self.view2.sprint_pole, int(RACE_CACHE.get("race_number")), RACE_CACHE.get("race_name"), sprint_lock_time)

        now = get_now()

        if not sprint_predictions_open(
            interaction.guild.id,
            now,
            RACE_CACHE
           ):
            await interaction.followup.send(
                "Sprint predictions are closed.",
                ephemeral=True)
            return
        
        if not self.view2.sprint_winner or not self.view2.sprint_pole:
            await interaction.followup.send(
                "Please fill all sprint predictions",
                ephemeral=True)
            return
        
        user_id = interaction.user.id
        sprint_predictions[user_id] = {
            "User_ID": interaction.user.id,
            "Sprint_Winner": self.view2.sprint_winner,
            "Sprint_Pole": self.view2.sprint_pole,
        }

        save_sprint_predictions(
            interaction.guild.id,
            interaction.user.id,
            interaction.user.name,
            int(RACE_CACHE.get("race_number")),
            RACE_CACHE.get("race_name"),
            self.view2.sprint_winner,
            self.view2.sprint_pole
)


        await interaction.followup.send(
            "✅ Your sprint predictions have been recorded.",
              ephemeral=True)
        
        self.view2.stop()

@bot.tree.command(name="sprint_predict", description="Make your sprint predictions")
async def sprint_predict(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True)
        await interaction.followup.send(f"Make your sprint predictions for the {RACE_CACHE.get("race_name")} Sprint:",
                                             view=SprintPredictionView(),
                                             ephemeral=True)
    except Exception as e:
        print(e)
    
#Force Points
@bot.tree.command(name="force_points", description="Give points to a user(MODS ONLY)")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.describe(
    user="User receiving points",
    points="Number of points to give (can be negative)",
    reason="Reason for awarding points (optional)"
)

async def force_points(
        interaction: discord.Interaction,
        user: discord.Member,
        points: int,
        reason: str | None = None
):

    
    await interaction.response.defer(ephemeral=True)

    if user == interaction.client.user:
        await interaction.followup.send("❌ Cannot award points to the bot.", ephemeral=True)
        return
    
    await interaction.followup.send(f"Awarded {user.mention} {points} points", 
                                    ephemeral=True)
    
    add_points(interaction.guild.id, user.id, str(user), points, reason)
    
    if reason:
        message = f"**{user.mention} received {points} points** for *{reason}*."
    else:
        message = f"**{user.mention} received {points} points.**"   

    #Send Permanent Message
    await interaction.channel.send(message)

@force_points.error
async def force_points_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.errors.MissingPermissions):
        await interaction.response.defer(ephemeral=True)
        await interaction.followup.send(
            "You don't have permission to use this command.",
            ephemeral=True
        )

#Constructors Predict
async def constructor_autocomplete(interaction: discord.Interaction, current: str):
    try:
        # Return up to 25 matching constructors
        return [
            app_commands.Choice(name=str(cons), value=str(cons))
            for cons in CONSTRUCTORS
            if current.lower() in str(cons).lower()
        ][:25]
    except Exception as e:
        print("Autocomplete ERROR", e)
        return[]

async def constructor_prediction(interaction: discord.Interaction, constructor: str):
    await interaction.response.defer(ephemeral=True)
    now = get_now()

    # Use your existing function to check if predictions are open
    if not predictions_open(interaction.guild.id, now, RACE_CACHE):
        try:
            await interaction.followup.send(
            "⛔ Predictions are closed.",
            ephemeral=True
        )
        except Exception as e:
            print(e)

        return

    # Save prediction (overwrite if user already submitted)
    save_constructor_prediction(
        interaction.guild.id,
        interaction.user.id,
        interaction.user.name,
        int(RACE_CACHE.get("race_number")),
        RACE_CACHE.get("race_name"),
        constructor
)


    await interaction.followup.send(
        f"✅ {interaction.user.mention}, your prediction for the winning constructor is **{constructor}**.",
        ephemeral=True
    )

@bot.tree.command(name="constructor_predict", description="Predict the winning constructor")
@app_commands.describe(constructor=f"Select your most-scoring constructor")
@app_commands.autocomplete(constructor=constructor_autocomplete)
async def constructor_prediction_cmd(interaction: discord.Interaction, constructor: str):
    await constructor_prediction(interaction, constructor)

@bot.tree.command(name="prediction_lock", description="Manually Lock/Unlock predictions (MODS ONLY)")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.choices(
    prediction=[
        app_commands.Choice(name="Race", value="race"),
        app_commands.Choice(name="Sprint", value="sprint"),
    ],
    state=[
        app_commands.Choice(name="Lock", value="LOCKED"),
        app_commands.Choice(name="Unlock", value="OPEN"),
        app_commands.Choice(name="Auto", value="AUTO"),
    ]
)
async def pred_lock(
    interaction: discord.Interaction,
    prediction: app_commands.Choice[str],
    state: app_commands.Choice[str]
):
    await interaction.response.defer(ephemeral=False)
    if state.value == "AUTO":
        set_manual_lock(interaction.guild.id, prediction.value,  None)
        msg = f"⚙️ **{prediction.name} predictions set to AUTO mode.**"
    else:
        set_manual_lock(interaction.guild.id, prediction.value, state.value)
        emoji = "🔒" if state.value == "LOCKED" else "🔓"
        msg = f"{emoji} **{prediction.name} predictions manually {state.name.upper()}.**"

    await interaction.channel.send(msg)
    await interaction.followup.send("Done.", ephemeral=True)

    prediction_state_log(
        interaction.guild.id,
        str(interaction.user.id),
        str(interaction.user),
        "pred_lock",
        prediction.value,
        state.value
    )

class SeasonPredictionView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)

        self.wdc = None
        self.wcc = None

        self.add_item(WDCSelect(self))
        self.add_item(WCCSelect(self))
        self.add_item(SeasonSubmitButton(self))

class WDCSelect(discord.ui.Select):
    def __init__(self, view2: SeasonPredictionView):
        self.view2 = view2
        super().__init__(
            placeholder="Select WDC Champion",
            options=[discord.SelectOption(label=d) for d in DRIVERS],
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        self.view2.wdc = self.values[0]
        await interaction.response.defer()  # acknowledge selection

class WCCSelect(discord.ui.Select):
    def __init__(self, view2: SeasonPredictionView):
        self.view2 = view2
        super().__init__(
            placeholder="Select WCC Champion",
            options=[discord.SelectOption(label=c) for c in CONSTRUCTORS],
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        self.view2.wcc = self.values[0]
        await interaction.response.defer()

class SeasonSubmitButton(discord.ui.Button):
    def __init__(self, view2: SeasonPredictionView):
        self.view2 = view2
        super().__init__(
            label="Submit Season Predictions",
            style=discord.ButtonStyle.green
        )
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            if not is_season_open(interaction.guild.id):
                await interaction.followup.send(
                    "Season predictions are locked.",
                    ephemeral=True
                )
                return

            if not self.view2.wdc or not self.view2.wcc:
                await interaction.followup.send(
                    "Please select both WDC and WCC.",
                    ephemeral=True
                )
                return

            save_season_prediction(
                interaction.guild.id,
                interaction.user.id,
                interaction.user.name,
                wdc=self.view2.wdc,
                wcc=self.view2.wcc
            )

            await interaction.followup.send(
                "✅ Your season predictions have been recorded.",
                ephemeral=True
            )

        except Exception as e:
            print("ERROR:", e)
            raise


@bot.tree.command(name="season_predict", description="Predict the WDC & WCC")
async def season(interaction: discord.Interaction):
    try: 
        await interaction.response.defer(ephemeral=True)
        await interaction.followup.send(
        "🏆 **Season Predictions**\n"
        "Select the WDC and WCC champions below:",
        view=SeasonPredictionView(),
        ephemeral=True
    )
    except Exception as e:
        interaction.followup.send(e)

@bot.tree.command(name="season_lock", description="Lock season predictions (MODS ONLY)")
@app_commands.checks.has_permissions(manage_guild=True)
async def season_lock(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    set_season_state(interaction.guild.id, False)
    await interaction.channel.send("🔒 **Season predictions are now LOCKED.**")
    await interaction.followup.send("Done.", ephemeral=True)

    prediction_state_log(
        interaction.guild.id,
        str(interaction.user.id),
        str(interaction.user),
        "season_lock",
        prediction="season",
        state="LOCKED"
    )



@bot.tree.command(name="season_unlock", description="Unlock season predictions (MODS ONLY)")
@app_commands.checks.has_permissions(manage_guild=True)
async def season_unlock(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    set_season_state(interaction.guild.id, True)
    await interaction.channel.send("🔓 **Season predictions are now OPEN.**")
    await interaction.followup.send("Done.", ephemeral=True)

    prediction_state_log(
        interaction.guild.id,
        str(interaction.user.id),
        str(interaction.user),
        "season_unlock",
        prediction="season",
        state="UNLOCKED"
    )

@bot.tree.command(name="leaderboard", description="View the leaderboard")
async def leaderboard(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    # Top 10
    top10 = get_top_n(interaction.guild.id, 10)
    top10_text = "\n".join([f"{i+1}. {user} - {pts} pts" for i, (user, pts) in enumerate(top10)])

    # User rank
    user_rank = get_user_rank(interaction.guild.id, interaction.user.name)
    if user_rank:
        rank, total_points = user_rank
        # Highlight user if outside top 10
        if rank > 10:
            user_text = f"\n... \n**Your position: {rank} - {total_points} pts**"
        else:
            user_text = ""  # already in top 10
    else:
        user_text = "\nYou have no points yet."

    await interaction.followup.send(f"**Leaderboard**\n{top10_text}{user_text}",
                                            ephemeral=True)

GUIDE_DICTIONARY = {
    "Race Predictions": 
                    {"/race_predict": 
                    "Make your race predictions.\n\nPredict the Podium, Polesitter and Fastest Lap of the race.\n\n*Predictions will lock at the start of Qualifying.*",
                    "/constructor_predict":
                    "Predict the constructor that will score the most points in a race.\n\n*Predictions will lock at the start of Qualifying.*",
                    "/race_bold_predict":
                    "Make your bold predictions for the race.\n\nThese will be pinned before the start of Qualifying for discussion if a channel is set\n\n*Predictions will lock at the start of Qualifying*",
                    "/sprint_predict": 
                    "Make your sprint race predictions.\n\nPredict the Sprint Winner and Sprint Polesitter on Sprint Weekends.\n\n*Sprint Predictions will lock at the start of Sprint Qualifying*",},
    "Season Predictions": 
                    {"/season_predict":
                     "Predict the WDC and WCC of a season if season predictions are open.",
                     "/crazy_predict":
                     "Make your craziest predictions for the season. These can be anything from driver transfers to retirements and more!\n\n**Crazy Predictions are limited to a maximum of 5 predictions per user per season**",},
    "View Predictions":
                    {"/view_race_bold_predictions":
                     "View all bold predictions for a selected race.",
                     "/view_crazy_predictions":
                     "View all crazy predictions given by a selected user.",},
    "Leaderboard":
                    {"/leaderboard":
                     "View the leaderboard for your server.",
                     "/update_leaderboard":
                     "Update the leaderboard if points have been manually added.\n\n*Please refrain from using this if no points have been manually added since the last update.*\n\n**Leaderboard will be automatically updated ONE day after the race ends.**",},
    "MOD Commands":
                    {"*MODS ONLY*":
                     "These commands can only be used by the moderators of a server",
                     "/force_points":
                     "Manually give/deduct points to/from user.",
                     "/prediction_lock":
                     "Manually lock/unlock race/sprint predictions.",
                     "/season_lock":
                     "Lock season predictions.",
                     "/season_unlock":
                     "Unlock season predictions.",},
    "Set Channel":
                    ("/set_channel",
                     "**MODS ONLY**\n\nSet the channel in which to receive messages.\n\nThis should ideally be the channel in which the prediction competetition will be carried out"),
    "Guide":
                    ("/guide",
                     "View this guide.\n\n(Congratulations! If you're here, you already know how to use this!)"),
    "Want to help improve this bot?":
                    ("[View this bot's GitHub repository](https://github.com/TheRocketeer314/F1DiscordPredictionsBot) to open an issue or submit a pull request.\n\nThanks! -\n      TheRocketeer314")}

class GuideSelect(discord.ui.Select):
    def __init__(self):
        # One option per top-level category
        options = [
            discord.SelectOption(
                label=category,
                value=category
            )
            for category, commands in GUIDE_DICTIONARY.items()
        ]
        super().__init__(
            placeholder="Select a guide category...",
            min_values=1,
            max_values=1,
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        category = self.values[0]
        commands = GUIDE_DICTIONARY[category]

        embed = discord.Embed(
            title=category,
            color=discord.Color.red()
        )

        # Build the description listing all commands in this category
        if isinstance(commands, dict):
            desc_lines = [f"**{cmd}**\n{desc}" for cmd, desc in commands.items()]
            embed.description = "\n\n".join(desc_lines)
        elif isinstance(commands, tuple):
            cmd, desc = commands
            embed.description = f"**{cmd}**\n{desc}"
        else:
            embed.description = str(commands)

        embed.set_footer(text="Use the dropdown below to view another category.")

        await interaction.edit_original_response(embed=embed, view=self.view)

class GuideView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=180)
        self.user_id = user_id
        self.add_item(GuideSelect())

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Prevent random users from hijacking it
        return interaction.user.id == self.user_id

@bot.tree.command(name="guide", description="View the F1 Predictions guide")
async def guide(interaction: discord.Interaction):

    embed = discord.Embed(
        title="F1Rats Prediction Guide",
        description="Select a command from the dropdown below to see details.",
        color=discord.Color.red()
    )

    await interaction.response.send_message(
        embed=embed,
        view=GuideView(interaction.user.id),
        ephemeral=True
    )

@bot.tree.command(
    name="crazy_predict",
    description="Submit your absolutely unhinged prediction for the season"
)
async def crazy_predict(interaction: discord.Interaction, prediction: str):
    await interaction.response.defer(ephemeral=True)
    global SEASON
    season = SEASON
    MAX_PREDICTIONS = 5

    user = interaction.user

    current_count = count_crazy_predictions(interaction.guild.id, user.id, season)

    if current_count >= MAX_PREDICTIONS:
        await interaction.followup.send(
            f"❌ You’ve already submitted **{MAX_PREDICTIONS}** crazy predictions for **{SEASON}**.",
        )
        return

    save_crazy_prediction(
        interaction.guild.id,
        user_id=user.id,
        username=str(user),
        season=season,
        prediction=prediction,
        timestamp=get_now()
    )

    await interaction.followup.send(
        f"🔥 Prediction saved for **{season}**!\n"
        f"({current_count + 1}/{MAX_PREDICTIONS})\n"
        f"> {prediction}"
    )

    await interaction.channel.send(
        f" 🤯  **Crazy prediction for {season}**\n"
        f"By **{user.display_name}**: \n"
        f"> {prediction}"
    )

def format_timestamp(dt):
    return dt.strftime("%d %b %Y, %H:%M UTC")

def format_crazy_predictions(username, rows):
    if not rows:
        return f"❌ **{username}** has made no crazy predictions."

    lines = [f"🤯  **Crazy Predictions by {username}**\n"]

    for i, row in enumerate(rows, start=1):
        prediction = row["prediction"]
        timestamp = row["timestamp"]

        lines.append(
            f"**{i}.** _({format_timestamp(timestamp)})_\n"
            f"> {prediction}\n"
        )

    return "\n".join(lines)

@bot.tree.command(
    name="view_crazy_predictions",
    description="View a user's crazy season predictions"
)
async def crazy_predictions(interaction: discord.Interaction, user: discord.User):
    await interaction.response.defer(ephemeral=True)
    global SEASON
    season = SEASON

    rows = get_crazy_predictions(interaction.guild.id, user.id, season)
    message = format_crazy_predictions(user.display_name, rows)

    await interaction.followup.send(message)

@bot.tree.command(
    name="race_bold_predict",
    description="Submit or update your bold prediction for the next race"
)
async def bold_predict(interaction: discord.Interaction, prediction: str):
    try:
        await interaction.response.defer(ephemeral=False)  # defer immediately

        if not predictions_open(interaction.guild.id, get_now(), RACE_CACHE):
            await interaction.followup.send(
                "❌ Bold predictions are locked for this race.",
                ephemeral=True
            )
            return

        user = interaction.user
        timestamp = get_now().isoformat()

        save_bold_prediction(
            interaction.guild.id,
            user_id=user.id,
            race_number=int(RACE_CACHE.get("race_number")),
            username=str(user),
            race_name=RACE_CACHE.get("race_name"),
            prediction=prediction,
            timestamp=timestamp
        )

        await interaction.followup.send(
            f"🧨 **{user.display_name}** updated their bold prediction for the **{RACE_CACHE.get('race_name')}**:\n> {prediction}",
            ephemeral=False
        )

    except Exception as e:
        print(e)

@bot.tree.command(
    name="update_leaderboard",
    description="Manually update the leaderboard"
)
async def update_leaderboard_cmd(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)

    try:
        update_leaderboard(interaction.guild.id)
        await interaction.followup.send("✅ Leaderboard updated.")
    except Exception as e:
        await interaction.followup.send(
            f"❌ Failed to update leaderboard:\n```{e}```"
        )

@tasks.loop(minutes=60/TIME_MULTIPLE)
async def bold_predictions_publisher():
    try:
        race_number = RACE_CACHE.get("race_number")
        race_name = RACE_CACHE.get("race_name")
        lock_time = RACE_CACHE.get("lock_time")

        now = get_now()
        print(f"NOW: {now}")
        print(f"LOCK TIME: {lock_time}")

        if not race_number or not lock_time:
            return

        publish_time = lock_time - timedelta(days=2)
        if now < publish_time or now > lock_time:
            return

        # --- INIT DICTS ---
        if not hasattr(bold_predictions_publisher, "warn_messages"):
            bold_predictions_publisher.warn_messages = {}
        if not hasattr(bold_predictions_publisher, "messages"):
            bold_predictions_publisher.messages = {}

        for guild in bot.guilds:
            guild_id = guild.id
            preds = fetch_bold_predictions(guild_id, race_number = race_number)
            print(f"{guild.name} ({guild_id}) PRED COUNT:", len(preds))

            # render message
            lines = [
                f"**Bold Predictions — {race_name}**",
                "",
                f"Submissions lock in <t:{int(lock_time.timestamp())}:R>",
                ""
            ]
            if preds:
                for username, prediction in preds:
                    lines.append(f"• **{username}** — {prediction}")
            else:
                lines.append(f"No predictions for the {race_name} yet.")

            content = "\n".join(lines)

            # get channel
            channel_id = get_prediction_channel(guild_id)
            if not channel_id:
                if guild_id not in bold_predictions_publisher.warn_messages:
                    channel = guild.text_channels[0] if guild.text_channels else None
                    if channel:
                        msg = await channel.send(
                            "Prediction channel not set! Admins, use /set_channel to configure it."
                        )
                        await msg.pin()
                        bold_predictions_publisher.warn_messages[guild_id] = msg
                        print(f"Sent warning in guild {guild.name}")
                continue

            channel = guild.get_channel(channel_id)
            if not channel:
                print(f"Channel {channel_id} not found in guild {guild.name}")
                continue

            # send or edit message
            if guild_id not in bold_predictions_publisher.messages:
                msg = await channel.send(content)
                await msg.pin()
                bold_predictions_publisher.messages[guild_id] = msg
                print(f"Sent new prediction message in {guild.name}")
            else:
                await bold_predictions_publisher.messages[guild_id].edit(content=content)
                print(f"Edited existing prediction message in {guild.name}")

    except Exception as e:
        print("BOLD LOOP ERROR:", e)

def format_bold_predictions(race_name, rows):
    if not rows:
        return f"❌ No bold predictions found for **{race_name}**"
    
    message = f"🧨 **Bold Predictions for {race_name}:**\n"
    for username, prediction, in rows:
        message += f"> **{username}**: {prediction}\n"
    return message

@bot.tree.command(
    name="view_race_bold_predictions",
    description="View bold predictions for a specific race"
)
@app_commands.describe(race="Select the race")
async def view_race_bold_preds(interaction: discord.Interaction, race: str):
    await interaction.response.defer(ephemeral=True)

    # use the unified function with race_name
    rows = fetch_bold_predictions(interaction.guild.id, race_name=race)

    if not rows:
        await interaction.followup.send(f"❌ No bold predictions found for **{race}**", ephemeral=True)
        return

    message = format_bold_predictions(race, rows)
    await interaction.followup.send(message)

@view_race_bold_preds.autocomplete('race')
async def race_autocomplete(interaction: discord.Interaction, current: str):
    try:
        # Return up to 25 matching race names
        return [
            app_commands.Choice(name=str(race), value=str(race))
            for race in SEASON_CALENDER
            if current.lower() in str(race).lower()
        ][:25]
    except Exception as e:
        print("Autocomplete ERROR", e)
        return []


@bot.tree.command(name="set_channel", description="Set or update the prediction channel (MODS ONLY)")
@app_commands.checks.has_permissions(administrator=True)
async def set_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    try:
        # Attempt to defer — this gives you more time
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        # basic checks
        if interaction.guild is None:
            await interaction.followup.send("This command can only be used in a server.")
            return

        if channel.guild.id != interaction.guild.id:
            await interaction.followup.send("You can only set a channel from this server.")
            return

        old_channel_id = get_prediction_channel(interaction.guild.id)
        old_channel = interaction.guild.get_channel(old_channel_id) if old_channel_id else None

        # update DB
        set_prediction_channel(interaction.guild.id, channel.id)

        # send followup
        if old_channel:
            await interaction.followup.send(
                f"Prediction channel updated from {old_channel.mention} to {channel.mention}"
            )
        else:
            await interaction.followup.send(
                f"Prediction channel set to {channel.mention}"
            )

    except app_commands.MissingPermissions:
        # fallback response in case user lacks admin perms
        if not interaction.response.is_done():
            await interaction.response.send_message(
                "You must be an administrator to use this command.", ephemeral=True
            )
        else:
            await interaction.followup.send("You must be an administrator to use this command.")
    except Exception as e:
        # generic error handling
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(f"Error: {e}", ephemeral=True)
            else:
                await interaction.followup.send(f"Error: {e}")
        except:
            # if even that fails, just log it
            print(f"Failed to send error message: {e}")

@set_channel.error
async def set_channel_error(interaction, error):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message(
            "You must be an administrator to use this command.",
            ephemeral=True
        )

bot.run(token, log_handler=handler, log_level=logging.DEBUG)