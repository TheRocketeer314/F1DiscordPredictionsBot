import discord
from discord import app_commands
from discord.ext import commands, tasks
import os
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
import asyncio
from config import CRAZY_PRED_POINTS
from pathlib import Path
from collections import defaultdict
from FastF1_service import refresh_race_cache, season_calender
from database import (init_db,
                      safe_fetch_one,
                      save_race_predictions,
                      fetch_existing_predictions,
                      save_sprint_predictions,
                      fetch_sprint_preds,
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
                      is_bold_pred_opted_out,
                      set_bold_pred_optout,
                      fetch_bold_predictions,
                      count_crazy_predictions,
                      set_prediction_channel,
                      get_prediction_channel,
                      guild_default_lock,
                      ensure_lock_rows,
                      upsert_guild,
                      get_persistent_message,
                      save_persistent_message,
                      mark_race_scored,
                      mark_season_scored,
                      save_correct_bold_prediction,
                      get_correct_bold_predictions,
                      save_scored_crazy_prediction,
                      get_all_crazy_predictions,
                      remove_scored_crazy_prediction,
                      get_all_crazy_predictions_for_user)

from results_watcher import poll_results_loop
from champions_watcher import final_champions_loop
from get_now import get_now, TIME_MULTIPLE, SEASON
from scoring import score_race_for_guild, score_final_champions_for_guild
from keep_alive import keep_alive
import logging
import sys
from utils.git_utils import get_version, get_release_date, get_changes

sys.stdout.reconfigure(line_buffering=True)
try:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
except Exception as e:
    print("Logging couldn't initialize, error:", e)

logger = logging.getLogger(__name__)

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

leaderboard_update_time = None

if RACE_CACHE.get("lock_time") is not None:
    leaderboard_update_time = RACE_CACHE.get("lock_time") + timedelta(days=3)

leaderboard_task = None

BOLD_PRED_POINTS = int(os.getenv('BOLD_PRED_POINTS', 10))

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
            try:
                new_cache = await refresh_race_cache(now)
                if new_cache:
                    RACE_CACHE.clear()
                    RACE_CACHE.update(new_cache)

                for guild in bot.guilds:
                    try:
                        reset_locks_on_cache_refresh(guild.id)
                    except Exception:
                        logger.exception("Failed to reset locks for guild %s", guild.id)

            except Exception:
                logger.exception("Error in race_cache_watcher main loop.")

            await asyncio.sleep(60/TIME_MULTIPLE)
            continue

        try:
            await asyncio.sleep(delay/TIME_MULTIPLE)
        except asyncio.CancelledError:
            return  # task cancelled on shutdown or restart

        # Time reached
        try:
            new_cache = await refresh_race_cache(now)
            if new_cache:
                RACE_CACHE.clear()
                RACE_CACHE.update(new_cache)

            for guild in bot.guilds:
                try:
                    reset_locks_on_cache_refresh(guild.id)
                except Exception:
                    logger.exception("Failed to reset locks for guild %s", guild.id)
            
        except Exception:
            logger.exception("Error in race_cache_watcher main loop.")

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
    logger.info("Bot is ready.")
    SEASON_CALENDER = await season_calender(SEASON)

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
        logger.info("Bold loop started")

    for guild in bot.guilds:
        upsert_guild(guild.id, guild.name)
        guild_default_lock(guild.id)
        ensure_lock_rows(guild.id)

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: discord.app_commands.AppCommandError):
    try:
        logger.exception("Command error: %s", error)

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
    except Exception:
        logger.exception("Failed to send error message for command error")

@bot.event
async def on_guild_join(guild: discord.Guild):
    guild_id = guild.id
    logger.info("Joined new guild: %s, %s", guild.name, guild_id)

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
    "Haas F1 Team", "Racing Bulls", "Williams",
    "Audi", "Cadillac", "Aston Martin", "Alpine"]

@bot.tree.command(name="version", description="Show bot version information")
async def version(interaction: discord.Interaction):

    version = get_version()
    release = get_release_date()

    embed = discord.Embed(
        title="F1 Predictions Bot",
        color=discord.Color.red()
    )

    embed.add_field(name="Version", value=version, inline=True)
    embed.add_field(name="Released", value=release, inline=True)

    embed.set_footer(text="Use /whatsnew to see recent features")

    await interaction.response.send_message(embed=embed, ephemeral=True)

from utils.git_utils import get_changes, get_version

@bot.tree.command(name="whatsnew", description="Show recent bot updates")
async def whatsnew(interaction: discord.Interaction):

    version = get_version()
    features, fixes = get_changes()

    embed = discord.Embed(
        title=f"What's New in {version}",
        color=discord.Color.blue()
    )

    if features:
        embed.add_field(
            name="New Features",
            value="\n".join(f"• {f}" for f in features[:5]),
            inline=False
        )

    if fixes:
        embed.add_field(
            name="Bug Fixes",
            value="\n".join(f"• {f}" for f in fixes[:5]),
            inline=False
        )

    embed.set_footer(text="Version history generated from Git commits")

    await interaction.response.send_message(embed=embed, ephemeral=True)

# ─── Step 1: Podium Prediction View ─────────────────────────────────────────

class PodiumPredictionView(discord.ui.View):
    def __init__(self, guild_id, user_id, race_number, race_name, preds=None, closed=False):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.user_id = user_id
        self.race_number = race_number
        self.race_name = race_name
        self.preds = preds or [None, None, None]
        self.closed = closed

        for i in range(3):
            curr = self.preds[i]
            options = [
                discord.SelectOption(label=d, value=d, default=(d == curr))
                for d in DRIVERS
            ]
            select = discord.ui.Select(
                placeholder=f"Position {i+1}" if not curr else curr,
                options=options,
                row=i,
                disabled=closed
            )
            select.callback = self._make_select_callback(i)
            self.add_item(select)

        clear_btn = discord.ui.Button(
            label="Clear",
            style=discord.ButtonStyle.red,
            row=3,
            disabled=closed
        )
        clear_btn.callback = self.clear_callback
        self.add_item(clear_btn)

        next_btn = discord.ui.Button(
            label="Next ▶",
            style=discord.ButtonStyle.green,
            row=3,
            disabled=False
        )
        next_btn.callback = self.next_callback
        self.add_item(next_btn)

    def _make_select_callback(self, index):
        async def callback(interaction: discord.Interaction):
            try:
                await interaction.response.defer(ephemeral=True)

                if not predictions_open(interaction.guild.id, get_now(), RACE_CACHE):
                    for child in self.children:
                        child.disabled = True
                    await interaction.edit_original_response(content=self.get_content(), view=self)
                    return

                selected = interaction.data['values'][0]

                # Remove selected driver from other positions
                for i in range(3):
                    if i != index and self.preds[i] == selected:
                        self.preds[i] = None

                self.preds[index] = selected
                taken = {v for v in self.preds if v}

                # Mutate existing selects
                for child in self.children:
                    if isinstance(child, discord.ui.Select) and child.row < 3:
                        curr = self.preds[child.row]
                        child.options = [
                            discord.SelectOption(label=d, value=d, default=(d == curr))
                            for d in DRIVERS
                            if d not in taken or d == curr
                        ]
                        child.placeholder = f"Position {child.row + 1}" if not curr else curr

                await interaction.edit_original_response(content=await self.get_content(), view=self)

            except Exception:
                logger.exception("PodiumPredictionView select callback error")
        return callback

    async def get_content(self):
        existing, _ = await asyncio.to_thread(
            fetch_existing_predictions, self.guild_id, self.user_id, self.race_number
        )
        db_preds = [existing['pos1'], existing['pos2'], existing['pos3']] if existing else [None, None, None]
        labels = ["🥇 Position 1", "🥈 Position 2", "🥉 Position 3"]
        lines = [f"🏁 **{self.race_name} — Podium Predictions**\n"]
        for i, label in enumerate(labels):
            val = db_preds[i] or "_Not saved yet_"
            lines.append(f"{label}: **{val}**")
        if self.closed:
            lines.append("\n🔒 _Predictions are closed — view only._")
        return "\n".join(lines)

    async def clear_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            for child in self.children:
                if isinstance(child, discord.ui.Select):
                    child.options = [
                        discord.SelectOption(label=d, value=d)
                        for d in DRIVERS
                    ]
                    child.placeholder = f"Position {child.row + 1}"
            self.preds = [None, None, None]
            await interaction.edit_original_response(content=await self.get_content(), view=self)
        except Exception:
            logger.exception("PodiumPredictionView clear_callback error")

    async def next_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)

            # Only save if predictions are open
            if predictions_open(interaction.guild.id, get_now(), RACE_CACHE):
                save_race_predictions(
                    self.guild_id, self.user_id,
                    interaction.user.name,
                    self.race_number, self.race_name,
                    pos1=self.preds[0], pos2=self.preds[1], pos3=self.preds[2]
                )

            existing, _ = await asyncio.to_thread(fetch_existing_predictions, self.guild_id, self.user_id, self.race_number)
            pole = existing['pole'] if existing else None
            fl = existing['fastest_lap'] if existing else None
            constructor = existing['constructor_winner'] if existing else None

            next_view = OtherPredictionView(
                self.guild_id, self.user_id, self.race_number,
                self.race_name, self.preds, pole, fl, constructor,
                closed=not predictions_open(interaction.guild.id, get_now(), RACE_CACHE)
            )
            await interaction.followup.send(
                content=await next_view.get_content(),
                view=next_view,
                ephemeral=True
            )
        except Exception:
            logger.exception("PodiumPredictionView next_callback error")

# ─── Step 2: Pole / Fastest Lap / Constructor View ────────────────────────────

class OtherPredictionView(discord.ui.View):
    def __init__(self, guild_id, user_id, race_number, race_name,
                 podium_preds, pole=None, fastest_lap=None, constructor=None, closed=False):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.user_id = user_id
        self.race_number = race_number
        self.race_name = race_name
        self.podium_preds = podium_preds
        self.pole = pole
        self.fastest_lap = fastest_lap
        self.constructor = constructor
        self.closed = closed

        # Pole select
        pole_select = discord.ui.Select(
            placeholder="🏆 Pole Position",
            options=[
                discord.SelectOption(label=d, value=d, default=(d == pole))
                for d in DRIVERS
            ],
            row=0,
            disabled=closed
        )
        pole_select.callback = self._make_driver_callback('pole')
        self.add_item(pole_select)

        # Fastest lap select
        fl_select = discord.ui.Select(
            placeholder="⚡ Fastest Lap",
            options=[
                discord.SelectOption(label=d, value=d, default=(d == fastest_lap))
                for d in DRIVERS
            ],
            row=1,
            disabled=closed
        )
        fl_select.callback = self._make_driver_callback('fastest_lap')
        self.add_item(fl_select)

        # Constructor select
        cons_select = discord.ui.Select(
            placeholder="🏎️ Winning Constructor",
            options=[
                discord.SelectOption(label=c, value=c, default=(c == constructor))
                for c in CONSTRUCTORS
            ],
            row=2,
            disabled=closed
        )
        cons_select.callback = self._make_constructor_callback()
        self.add_item(cons_select)

        clear_btn = discord.ui.Button(
            label="Clear",
            style=discord.ButtonStyle.red,
            row=3,
            disabled=closed
        )
        clear_btn.callback = self.clear_callback
        self.add_item(clear_btn)

        next_btn = discord.ui.Button(
            label="Next ▶",
            style=discord.ButtonStyle.green,
            row=3,
            disabled=False
        )
        next_btn.callback = self.next_callback
        self.add_item(next_btn)

    def _make_driver_callback(self, field):
        async def callback(interaction: discord.Interaction):
            try:
                await interaction.response.defer(ephemeral=True)

                if not predictions_open(interaction.guild.id, get_now(), RACE_CACHE):
                    for child in self.children:
                        child.disabled = True
                    await interaction.edit_original_response(
                        content=await self.get_content(), view=self
                    )
                    return

                selected = interaction.data['values'][0]
                if field == 'pole':
                    self.pole = selected
                else:
                    self.fastest_lap = selected

                # Update placeholder of the select that was just used
                for child in self.children:
                    if isinstance(child, discord.ui.Select) and child.row == (0 if field == 'pole' else 1):
                        child.placeholder = selected
                        for opt in child.options:
                            opt.default = (opt.value == selected)

                await interaction.edit_original_response(
                    content=await self.get_content(), view=self
                )
            except Exception:
                logger.exception("OtherPredictionView driver callback error")
        return callback

    def _make_constructor_callback(self):
        async def callback(interaction: discord.Interaction):
            try:
                await interaction.response.defer(ephemeral=True)

                if not predictions_open(interaction.guild.id, get_now(), RACE_CACHE):
                    for child in self.children:
                        child.disabled = True
                    await interaction.edit_original_response(
                        content=await self.get_content(), view=self
                    )
                    return

                self.constructor = interaction.data['values'][0]

                for child in self.children:
                    if isinstance(child, discord.ui.Select) and child.row == 2:
                        child.placeholder = self.constructor
                        for opt in child.options:
                            opt.default = (opt.value == self.constructor)

                await interaction.edit_original_response(
                    content=await self.get_content(), view=self
                )
            except Exception:
                logger.exception("OtherPredictionView constructor callback error")
        return callback

    async def get_content(self):
        lines = [f"🏁 **{self.race_name} — Other Predictions**\n"]
        
        existing, _ = await asyncio.to_thread(fetch_existing_predictions, self.guild_id, self.user_id, self.race_number)
        db_pole = existing['pole'] if existing else None
        db_fl = existing['fastest_lap'] if existing else None
        db_cons = existing['constructor_winner'] if existing else None
        
        lines.append(f"🏆 Pole: **{db_pole or '_Not saved yet_'}**")
        lines.append(f"⚡ Fastest Lap: **{db_fl or '_Not saved yet_'}**")
        lines.append(f"🏎️ Constructor: **{db_cons or '_Not saved yet_'}**")
        if self.closed:
            lines.append("\n🔒 _Predictions are closed — view only._")
        return "\n".join(lines)

    async def clear_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            new_view = OtherPredictionView(
                self.guild_id, self.user_id, self.race_number, self.race_name,
                self.podium_preds, None, None, None
            )
            await interaction.edit_original_response(
                content=await new_view.get_content(), view=new_view
            )
        except Exception:
            logger.exception("OtherPredictionView clear_callback error")

    async def next_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)

            if predictions_open(interaction.guild.id, get_now(), RACE_CACHE):
                save_race_predictions(
                    self.guild_id, self.user_id, interaction.user.name,
                    self.race_number, self.race_name,
                    pole=self.pole, fastest_lap=self.fastest_lap,
                    constructor_winner=self.constructor
                )

            _, existing_bold = await asyncio.to_thread(fetch_existing_predictions, self.guild_id, self.user_id, self.race_number)
            bold = existing_bold['prediction'] if existing_bold else None

            next_view = BoldPredictionView(
                self.guild_id, self.user_id, self.race_number,
                self.race_name, bold,
                closed=not predictions_open(interaction.guild.id, get_now(), RACE_CACHE)
            )
            await interaction.followup.send(
                content=await next_view.get_content(),
                view=next_view,
                ephemeral=True
            )
        except Exception:
            logger.exception("OtherPredictionView next_callback error")

# ─── Step 3: Bold Prediction View ─────────────────────────────────────────────

class BoldPredModal(discord.ui.Modal, title="Bold Prediction"):
    prediction = discord.ui.TextInput(
        label="Your bold prediction",
        placeholder="Enter your bold prediction...",
        max_length=200,
        style=discord.TextStyle.paragraph
    )

    def __init__(self, guild_id, user_id, race_number, race_name, view):
        super().__init__()
        self.guild_id = guild_id
        self.user_id = user_id
        self.race_number = race_number
        self.race_name = race_name
        self.parent_view = view

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)

            save_bold_prediction(
                self.guild_id,
                user_id=self.user_id,
                race_number=self.race_number,
                username=str(interaction.user),
                race_name=self.race_name,
                prediction=str(self.prediction),
                timestamp=get_now().isoformat()
            )

            new_view = BoldPredictionView(
                self.guild_id, self.user_id, self.race_number,
                self.race_name, str(self.prediction)
            )
            await interaction.edit_original_response(
                content=await new_view.get_content(),
                view=new_view
            )
            await interaction.followup.send(
                "✅ All your predictions have been recorded!",
                ephemeral=True
            )

            # Public announcement
            channel_id = get_prediction_channel(self.guild_id)
            if channel_id:
                try:
                    guild = interaction.guild
                    channel = guild.get_channel(channel_id) or await bot.fetch_channel(channel_id)
                    if channel:
                        await channel.send(
                            f"🧨 **{interaction.user.display_name}** submitted their bold prediction for the **{self.race_name}**:\n"
                            f"> {str(self.prediction)}"
                        )
                except discord.NotFound:
                    logger.exception("Channel not found for bold pred announcement")

        except Exception:
            logger.exception("BoldPredModal on_submit error")


class BoldPredictionView(discord.ui.View):
    def __init__(self, guild_id, user_id, race_number, race_name, bold=None, closed=False):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.user_id = user_id
        self.race_number = race_number
        self.race_name = race_name
        self.bold = bold
        self.closed = closed

        enter_btn = discord.ui.Button(
            label="Enter Bold Prediction",
            style=discord.ButtonStyle.blurple,
            row=0,
            disabled=closed
        )
        enter_btn.callback = self.enter_callback
        self.add_item(enter_btn)

        skip_btn = discord.ui.Button(
            label="Skip",
            style=discord.ButtonStyle.grey,
            row=0,
            disabled=closed
        )
        skip_btn.callback = self.skip_callback
        self.add_item(skip_btn)

    async def get_content(self):
        lines = [f"🧨 **{self.race_name} — Bold Prediction**\n"]
        
        # Always show DB value
        _, existing_bold = await asyncio.to_thread(fetch_existing_predictions, self.guild_id, self.user_id, self.race_number)
        db_bold = existing_bold['prediction'] if existing_bold else None
        
        lines.append(f"**Saved Prediction:** {db_bold or '_Not saved yet_'}")
                
        if self.closed:
            lines.append("\n🔒 _Predictions are closed — view only._")
        else:
            lines.append("\n_Click **Enter Bold Prediction** to type your prediction. Click **Skip** to skip making/updating your prediction._")
        return "\n".join(lines)

    async def enter_callback(self, interaction: discord.Interaction):
        try:
            if not predictions_open(interaction.guild.id, get_now(), RACE_CACHE):
                await interaction.response.defer(ephemeral=True)
                new_view = BoldPredictionView(
                    self.guild_id, self.user_id, self.race_number,
                    self.race_name, self.bold, closed=True
                )
                await interaction.edit_original_response(
                    content= await new_view.get_content(), view=new_view
                )
                return

            await interaction.response.send_modal(
                BoldPredModal(
                    self.guild_id, self.user_id,
                    self.race_number, self.race_name, self
                )
            )
        except Exception:
            logger.exception("BoldPredictionView enter_callback error")

    async def skip_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            await interaction.followup.send(
                "✅ Predictions saved! Bold prediction skipped.",
                ephemeral=True
            )
        except Exception:
            logger.exception("BoldPredictionView skip_callback error")

# ─── Main Command ──────────────────────────────────────────────────────────────

@bot.tree.command(name="race_predict", description="Make all your race predictions")
async def predict(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True)

        race_number = int(RACE_CACHE.get("race_number"))
        race_name = RACE_CACHE.get("race_name")
        guild_id = interaction.guild.id
        user_id = interaction.user.id
        closed = not predictions_open(guild_id, get_now(), RACE_CACHE)

        existing, _ = await asyncio.to_thread(fetch_existing_predictions, guild_id, user_id, race_number)
        preds = [existing['pos1'], existing['pos2'], existing['pos3']] if existing else [None, None, None]

        view = PodiumPredictionView(guild_id, user_id, race_number, race_name, preds, closed=closed)
        await interaction.followup.send(
            content=await view.get_content(),
            view=view,
            ephemeral=True
        )

    except Exception:
        logger.exception("predict command error")

class SprintWinnerSelect(discord.ui.Select):
    def __init__(self, current=None):
        super().__init__(
            placeholder="Sprint Winner" if not current else current,
            options=[discord.SelectOption(label=d, value=d, default=(d == current)) for d in DRIVERS],
            row=0
        )

    async def callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            selected = self.values[0]
            self.placeholder = selected
            for opt in self.options:
                opt.default = (opt.value == selected)
            self.view.sprint_winner = selected
            await interaction.edit_original_response(view=self.view)
        except Exception:
            logger.exception("SprintWinner error")


class SprintPoleSelect(discord.ui.Select):
    def __init__(self, current=None):
        super().__init__(
            placeholder="Sprint Pole" if not current else current,
            options=[discord.SelectOption(label=d, value=d, default=(d == current)) for d in DRIVERS],
            row=1
        )

    async def callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            selected = self.values[0]
            self.placeholder = selected
            for opt in self.options:
                opt.default = (opt.value == selected)
            self.view.sprint_pole = selected
            await interaction.edit_original_response(view=self.view)
        except Exception:
            logger.exception("SprintPole error")


class SprintSubmitButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Submit Sprint Predictions", style=discord.ButtonStyle.green, row=2)

    async def callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)

            if not sprint_predictions_open(interaction.guild.id, get_now(), RACE_CACHE):
                await interaction.followup.send("⛔ Sprint predictions are closed.", ephemeral=True)
                return

            if not self.view.sprint_winner or not self.view.sprint_pole:
                await interaction.followup.send("❌ Please select both predictions first.", ephemeral=True)
                return

            save_sprint_predictions(
                interaction.guild.id,
                interaction.user.id,
                interaction.user.name,
                int(RACE_CACHE.get("race_number")),
                RACE_CACHE.get("race_name"),
                self.view.sprint_winner,
                self.view.sprint_pole
            )

            await interaction.followup.send("✅ Sprint predictions recorded!", ephemeral=True)
            self.view.stop()

        except Exception:
            logger.exception("SprintSubmit error")


class SprintPredictionView(discord.ui.View):
    def __init__(self, sprint_winner=None, sprint_pole=None, closed=False):
        super().__init__(timeout=300)
        self.sprint_winner = sprint_winner
        self.sprint_pole = sprint_pole

        self.add_item(SprintWinnerSelect(sprint_winner))
        self.add_item(SprintPoleSelect(sprint_pole))

        submit_btn = SprintSubmitButton()
        submit_btn.disabled = closed
        self.add_item(submit_btn)

        if closed:
            for child in self.children:
                if isinstance(child, discord.ui.Select):
                    child.disabled = True

@bot.tree.command(name="sprint_predict", description="Make your sprint predictions")
async def sprint_predict(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True)

        race_number = int(RACE_CACHE.get("race_number"))
        guild_id = interaction.guild.id
        user_id = interaction.user.id
        closed = not sprint_predictions_open(guild_id, get_now(), RACE_CACHE)

        existing = fetch_sprint_preds(guild_id, user_id, race_number)

        sprint_winner = existing['sprint_winner'] if existing else None
        sprint_pole = existing['sprint_pole'] if existing else None

        view = SprintPredictionView(sprint_winner, sprint_pole, closed=closed)
        await interaction.followup.send(
            f"🏎️ **{RACE_CACHE.get('race_name')} Sprint Predictions**",
            view=view,
            ephemeral=True
        )

    except Exception:
        logger.exception("Sprint prediction error")

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

    try:
        await interaction.response.defer(ephemeral=True)

        if user == interaction.client.user:
            await interaction.followup.send("❌ Cannot award points to the bot.", ephemeral=True)
            return
        
        await interaction.followup.send(f"Awarded {user.mention} {points} points", 
                                        ephemeral=True)
        
        add_points(interaction.guild.id, user.id, str(user), points, reason)
        
        if points < 0:
            script = f"**{-(points)} points deducted from {user.mention}**"
        else:
            script = f"**{user.mention} received {points} points**"
        
        if reason:
            message = f"{script} for *{reason}*."
        else:
            message = f"{script}"   

        #Send Permanent Message
        await interaction.channel.send(message)

    except Exception:
        logger.exception("Force points error")


@force_points.error
async def force_points_error(interaction: discord.Interaction, error):
    try:
        if isinstance(error, app_commands.errors.MissingPermissions):
            await interaction.response.defer(ephemeral=True)
            await interaction.followup.send(
                "You don't have permission to use this command.",
                ephemeral=True
            )
    except Exception:
        logger.exception("force_points_error error")
        return []

@bot.tree.command(name="prediction_lock", description="Manually un/lock race/sprint predictions. (MODS ONLY)")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.choices(
    pred_type=[
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
    pred_type: app_commands.Choice[str],
    state: app_commands.Choice[str]
):
    try:
        await interaction.response.defer(ephemeral=False)
        if state.value == "AUTO":
            set_manual_lock(interaction.guild.id, pred_type.value,  None)
            msg = f"⚙️ **{pred_type.name} predictions set to AUTO mode.**"
        else:
            set_manual_lock(interaction.guild.id, pred_type.value, state.value)
            emoji = "🔒" if state.value == "LOCKED" else "🔓"
            msg = f"{emoji} **{pred_type.name} predictions manually {state.name.upper()}.**"

        await interaction.channel.send(msg)
        await interaction.followup.send("Done.", ephemeral=True)

        prediction_state_log(
            interaction.guild.id,
            str(interaction.user.id),
            str(interaction.user),
            "pred_lock",
            pred_type.value,
            state.value
        )

    except Exception:
        logger.exception("Prediction lock error")

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

        except Exception:
            logger.exception("Season submit error")

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
    except Exception:
        logger.exception("Season prediction error")


@bot.tree.command(name="season_lock", description="Lock season predictions (MODS ONLY)")
@app_commands.checks.has_permissions(manage_guild=True)
async def season_lock(interaction: discord.Interaction):
    try:
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

    except Exception:
        logger.exception("Season lock error")

@bot.tree.command(name="season_unlock", description="Unlock season predictions (MODS ONLY)")
@app_commands.checks.has_permissions(manage_guild=True)
async def season_unlock(interaction: discord.Interaction):
    try:
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

    except Exception:
        logger.exception("Season lock error")

@bot.tree.command(name="leaderboard", description="View the leaderboard")
async def leaderboard(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True)
        # Top 10
        top10 = get_top_n(interaction.guild.id, 10)
        top10_text = "\n".join([
            f"{i+1}. {user} - {pts} pts          ◄" if user == interaction.user.name else f"{i+1}. {user} - {pts} pts"
            for i, (user, pts) in enumerate(top10)
        ])
        # User rank
        user_rank = get_user_rank(interaction.guild.id, interaction.user.name)
        if user_rank:
            rank, total_points = user_rank
            # Highlight user if outside top 10
            if rank > 10:
                user_text = f"\n\n**{rank}. {interaction.user.name} - {total_points} pts**"
            else:
                user_text = ""  # already in top 10
        else:
            user_text = "\nYou have no points yet."

        await interaction.followup.send(f"**Leaderboard**\n{top10_text}{user_text}",
                                                ephemeral=True)

    except Exception:
        logger.exception("Leaderboard error")

GUIDE_DICTIONARY = {
    "Race Predictions": 
                    {"/race_predict": 
                    """Make your race predictions.
                    \n\nPredict the Podium, Polesitter, Fastest Lap and Winning Constructor of the race.
                    \n\n*Note: Winning Constructor is the constructor who scored the most points.*
                    \n\nAlso make your bold prediction for the race.
                    \n\n*Predictions will lock at the start of Qualifying.*""",
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
                     "/view_correct_bold_predictions":
                     "View all the correct bold predictions by a user.",
                     "/view_crazy_predictions":
                     "View all crazy predictions given by a selected user.",
                     "/view_all_crazy_predictions":
                     "View all crazy predictions for this server (optionally filter by season).\n\nMods can also score crazy predictions with this."},
    "Leaderboard":
                    {"/leaderboard":
                     "View the leaderboard for your server.",
                     "/update_leaderboard":
                     "Update the leaderboard if points have been manually added.\n\n*Please refrain from using this if no points have been manually added since the last update.*\n\n**Leaderboard will be automatically updated ONE day after the race ends.**",},
    "MOD Commands":
                    {"*MODS ONLY*":
                     "These commands can only be used by the moderators of a server",
                     "/toggle_bold_predictions":
                     "Toggle whether to receive the list of bold predictions before every race in the set channel.",
                     "/force_points":
                     "Manually give/deduct points to/from user.",
                     "/prediction_lock":
                     "Manually lock/unlock race/sprint predictions.",
                     "/season_lock":
                     "Lock season predictions.",
                     "/season_unlock":
                     "Unlock season predictions.",
                     "/force_score_race":
                     "Force scoring of a particular race.",
                     "/force_score_season":
                     "Force scoring of the current season.",
                     "/correct_bold_predictions":
                     "Score the correct bold predictions for a race.",
                     "/score_crazy_predictions":
                     "Score a user's crazy predictions in a server (optionally filter by season).",
                     "/mass_score_crazy_predictions":
                     "Score multiple crazy predictions in a server at once (optionally filter by season)."},
    "Set Channel":
                    ("/set_channel",
                     "**MODS ONLY**\n\nSet the channel in which to receive messages.\n\nThis should ideally be the channel in which the prediction competetition will be carried out"),
    "Guide":
                    ("/guide",
                     "View this guide.\n\n(Congratulations! If you're here, you already know how to use this!)"),
    "Application Did Not Respond?":
                    ("This issue may have occurred due to temporary exhaustion of the available RAM. \n\n Please try again after a couple of minutes."),
    "Versions & Changelog":
                    {"/version":
                     "View the current version and release date.",
                     "/whatsnew":
                     "View new features and bug fixes."},
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
    
    try:
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

    except Exception:
        logger.exception("GuideSelect error")

class GuideView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=180)
        self.user_id = user_id
        self.add_item(GuideSelect())

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        try:
            # Prevent random users from hijacking it
            return interaction.user.id == self.user_id
        except Exception:
            logger.exception("GuideView error")


@bot.tree.command(name="guide", description="View the F1 Predictions guide")
async def guide(interaction: discord.Interaction):
    try:
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

    except Exception:
        logger.exception("Guide error")

@bot.tree.command(
    name="crazy_predict",
    description="Submit your absolutely unhinged prediction for the season"
)
async def crazy_predict(interaction: discord.Interaction, prediction: str):
    try:
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

    except Exception:
        logger.exception("Crazy_predict error")

class CrazyPredsPaginationView(discord.ui.View):
    def __init__(self, pages, page_data, guild_id, season, is_mod=False, current_page=0, selected_pred_id=None, selected_difficulty=None):
        super().__init__(timeout=300)
        self.pages = pages
        self.page_data = page_data
        self.guild_id = guild_id
        self.season = season
        self.is_mod = is_mod
        self.current_page = current_page
        self.total_pages = len(pages)
        self.selected_pred_id = selected_pred_id
        self.selected_difficulty = selected_difficulty

        current_preds = page_data[current_page]
        pred_options = [
            discord.SelectOption(
                label=f"{row['username']}: {row['prediction'][:50]}",
                value=str(row['id']),
                description="✅ Already scored" if row['difficulty'] else None,
                default=(str(row['id']) == str(selected_pred_id))
            )
            for row in current_preds
        ]
        pred_select = discord.ui.Select(
            placeholder="Select a prediction to score...",
            options=pred_options,
            row=0
        )
        pred_select.callback = self.pred_select_callback
        self.add_item(pred_select)

        if is_mod:
            diff_options = [
                discord.SelectOption(
                    label=f"{diff} ({pts} pts)",
                    value=diff,
                    default=(diff == selected_difficulty)
                )
                for diff, pts in CRAZY_PRED_POINTS.items()
            ]
            diff_select = discord.ui.Select(
                placeholder="Select difficulty...",
                options=diff_options,
                row=1
            )
            diff_select.callback = self.diff_select_callback
            self.add_item(diff_select)

        prev = discord.ui.Button(emoji="◀️", style=discord.ButtonStyle.grey, disabled=current_page == 0, row=2)
        prev.callback = self.prev_callback
        self.add_item(prev)

        if is_mod:
            score_btn = discord.ui.Button(
                label="Score",
                style=discord.ButtonStyle.green,
                disabled=not (selected_pred_id and selected_difficulty),
                row=2
            )
            score_btn.callback = self.score_callback
            self.add_item(score_btn)

            descore_btn = discord.ui.Button(
                label="Remove Score",
                style=discord.ButtonStyle.red,
                disabled=not selected_pred_id,
                row=2
            )
            descore_btn.callback = self.descore_callback
            self.add_item(descore_btn)

        next_ = discord.ui.Button(emoji="▶️", style=discord.ButtonStyle.grey, disabled=current_page == self.total_pages - 1, row=2)
        next_.callback = self.next_callback
        self.add_item(next_)

    def get_embed(self):
        embed = discord.Embed(
            title=f"🤯 Crazy Predictions — {self.season}",
            description=self.pages[self.current_page],
            color=discord.Color.red()
        )
        embed.set_footer(text=f"Page {self.current_page + 1} of {self.total_pages}")
        return embed

    async def pred_select_callback(self, interaction: discord.Interaction):
        try:
            new_view = CrazyPredsPaginationView(
                self.pages, self.page_data, self.guild_id, self.season,
                self.is_mod, self.current_page,
                int(interaction.data['values'][0]), self.selected_difficulty
            )
            await interaction.response.edit_message(embed=new_view.get_embed(), view=new_view)
        except Exception:
            logger.exception("pred_select_callback error")

    async def diff_select_callback(self, interaction: discord.Interaction):
        try:
            new_view = CrazyPredsPaginationView(
                self.pages, self.page_data, self.guild_id, self.season,
                self.is_mod, self.current_page,
                self.selected_pred_id, interaction.data['values'][0]
            )
            await interaction.response.edit_message(embed=new_view.get_embed(), view=new_view)
        except Exception:
            logger.exception("diff_select_callback error")

    async def score_callback(self, interaction: discord.Interaction):
        try:
            if not interaction.user.guild_permissions.manage_guild:
                await interaction.response.send_message("❌ You don't have permission to score predictions.", ephemeral=True)
                return

            pred_row = None
            for row in self.page_data[self.current_page]:
                if row['id'] == self.selected_pred_id:
                    pred_row = row
                    break

            if not pred_row:
                await interaction.response.send_message("❌ Prediction not found.", ephemeral=True)
                return

            points = CRAZY_PRED_POINTS[self.selected_difficulty]

            save_scored_crazy_prediction(
                self.guild_id,
                self.selected_pred_id,
                pred_row['user_id'],
                pred_row['username'],
                self.selected_difficulty,
                points
            )

            update_leaderboard(self.guild_id)

            rows = get_all_crazy_predictions(self.guild_id, self.season)
            pages, page_data = build_crazy_pred_pages(rows)

            new_view = CrazyPredsPaginationView(
                pages, page_data, self.guild_id, self.season,
                self.is_mod, self.current_page, None, None
            )
            await interaction.response.edit_message(embed=new_view.get_embed(), view=new_view)

            channel_id = get_prediction_channel(self.guild_id)
            if channel_id:
                channel = interaction.guild.get_channel(channel_id) or await bot.fetch_channel(channel_id)
                if channel:
                    await channel.send(
                        f"🎯 **Crazy Prediction Scored!**\n"
                        f"**{pred_row['username']}**'s prediction came true:\n"
                        f"> {pred_row['prediction']}\n"
                        f"Difficulty: **{self.selected_difficulty}** — **{points} points** awarded!"
                    )

        except Exception:
            logger.exception("score_callback error")

    async def descore_callback(self, interaction: discord.Interaction):
        try:
            if not interaction.user.guild_permissions.manage_guild:
                await interaction.response.send_message("❌ You don't have permission.", ephemeral=True)
                return

            if not self.selected_pred_id:
                await interaction.response.send_message("❌ No prediction selected.", ephemeral=True)
                return

            remove_scored_crazy_prediction(self.guild_id, self.selected_pred_id)
            update_leaderboard(self.guild_id)

            rows = get_all_crazy_predictions(self.guild_id, self.season)
            pages, page_data = build_crazy_pred_pages(rows)

            new_view = CrazyPredsPaginationView(
                pages, page_data, self.guild_id, self.season,
                self.is_mod, self.current_page, None, None
            )
            await interaction.response.edit_message(embed=new_view.get_embed(), view=new_view)

        except Exception:
            logger.exception("descore_callback error")

    async def prev_callback(self, interaction: discord.Interaction):
        try:
            new_view = CrazyPredsPaginationView(
                self.pages, self.page_data, self.guild_id, self.season,
                self.is_mod, self.current_page - 1, None, None
            )
            await interaction.response.edit_message(embed=new_view.get_embed(), view=new_view)
        except Exception:
            logger.exception("prev_callback error")

    async def next_callback(self, interaction: discord.Interaction):
        try:
            new_view = CrazyPredsPaginationView(
                self.pages, self.page_data, self.guild_id, self.season,
                self.is_mod, self.current_page + 1, None, None
            )
            await interaction.response.edit_message(embed=new_view.get_embed(), view=new_view)
        except Exception:
            logger.exception("next_callback error")


def build_crazy_pred_pages(rows):
    items_per_page = 5
    pages = []
    page_data = []
    for i in range(0, len(rows), items_per_page):
        chunk = rows[i:i + items_per_page]
        lines = []
        for j, row in enumerate(chunk, start=i+1):
            scored = row['difficulty'] is not None
            tick = "✅" if scored else "⬜"
            score_info = f" — {row['difficulty']} ({row['points']} pts)" if scored else ""
            lines.append(
                f"{tick} **{j}.** **{row['username']}** _({format_timestamp(row['timestamp'])})_{score_info}\n"
                f"> {row['prediction']}\n"
            )
        pages.append("\n\n".join(lines))
        page_data.append(chunk)
    return pages, page_data


@bot.tree.command(name="view_all_crazy_predictions", description="View all crazy predictions for this server")
@app_commands.describe(season="Season year (defaults to current season)")
async def view_all_crazy_predictions(interaction: discord.Interaction, season: int = None):
    try:
        await interaction.response.defer(ephemeral=True)

        if season is None:
            season = SEASON

        rows = get_all_crazy_predictions(interaction.guild.id, season)

        if not rows:
            await interaction.followup.send(f"❌ No crazy predictions found for **{season}**.", ephemeral=True)
            return

        is_mod = interaction.user.guild_permissions.manage_guild
        pages, page_data = build_crazy_pred_pages(rows)
        view = CrazyPredsPaginationView(pages, page_data, interaction.guild.id, season, is_mod=is_mod)

        await interaction.followup.send(embed=view.get_embed(), view=view, ephemeral=True)

    except Exception:
        logger.exception("view_all_crazy_predictions error")

class MassCrazyScoreView(discord.ui.View):
    def __init__(self, guild_id, rows, season, current_page=0, selected_pred_ids=None, selected_difficulty=None):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.rows = rows
        self.season = season
        self.current_page = current_page
        self.selected_pred_ids = selected_pred_ids or []
        self.selected_difficulty = selected_difficulty
        self.items_per_page = 25
        self.total_pages = max(1, (len(rows) + self.items_per_page - 1) // self.items_per_page)

        start = current_page * self.items_per_page
        end = start + self.items_per_page
        page_rows = rows[start:end]

        pred_options = [
            discord.SelectOption(
                label=f"{row['username']}: {row['prediction'][:50]}",
                value=str(row['id']),
                description="✅ Already scored" if row['difficulty'] else None,
                default=(row['id'] in self.selected_pred_ids)
            )
            for row in page_rows
        ]
        pred_select = discord.ui.Select(
            placeholder=f"Select predictions (page {current_page + 1}/{self.total_pages})...",
            options=pred_options,
            min_values=1,
            max_values=len(pred_options),
            row=0
        )
        pred_select.callback = self.pred_select_callback
        self.add_item(pred_select)

        diff_options = [
            discord.SelectOption(
                label=f"{diff} ({pts} pts)",
                value=diff,
                default=(diff == selected_difficulty)
            )
            for diff, pts in CRAZY_PRED_POINTS.items()
        ]
        diff_select = discord.ui.Select(
            placeholder="Select difficulty...",
            options=diff_options,
            row=1
        )
        diff_select.callback = self.diff_select_callback
        self.add_item(diff_select)

        score_btn = discord.ui.Button(
            label=f"Score Selected ({len(self.selected_pred_ids)})",
            style=discord.ButtonStyle.green,
            disabled=not (self.selected_pred_ids and self.selected_difficulty),
            row=2
        )
        score_btn.callback = self.submit_callback
        self.add_item(score_btn)

        unscore_btn = discord.ui.Button(
            label="Remove Score",
            style=discord.ButtonStyle.red,
            disabled=not self.selected_pred_ids,
            row=2
        )
        unscore_btn.callback = self.unscore_callback
        self.add_item(unscore_btn)

        prev = discord.ui.Button(
            emoji="◀️", style=discord.ButtonStyle.grey,
            disabled=current_page == 0, row=3
        )
        prev.callback = self.prev_callback
        self.add_item(prev)

        next_ = discord.ui.Button(
            emoji="▶️", style=discord.ButtonStyle.grey,
            disabled=current_page == self.total_pages - 1, row=3
        )
        next_.callback = self.next_callback
        self.add_item(next_)

    def get_content(self):
        selected_count = len(self.selected_pred_ids)
        diff_text = f" | Difficulty: **{self.selected_difficulty} ({CRAZY_PRED_POINTS[self.selected_difficulty]} pts)**" if self.selected_difficulty else ""
        return (
            f"Select predictions to score across pages — selections carry over between pages!\n"
            f"**{selected_count} prediction(s) selected**{diff_text}\n"
            f"Page {self.current_page + 1}/{self.total_pages}"
        )

    async def pred_select_callback(self, interaction: discord.Interaction):
        try:
            new_ids = [int(v) for v in interaction.data['values']]
            start = self.current_page * self.items_per_page
            end = start + self.items_per_page
            current_page_ids = {row['id'] for row in self.rows[start:end]}
            other_page_ids = [pid for pid in self.selected_pred_ids if pid not in current_page_ids]
            merged_ids = other_page_ids + new_ids
            new_view = MassCrazyScoreView(
                self.guild_id, self.rows, self.season,
                self.current_page, merged_ids, self.selected_difficulty
            )
            await interaction.response.edit_message(content=new_view.get_content(), view=new_view)
        except Exception:
            logger.exception("MassCrazyScoreView pred_select_callback error")

    async def diff_select_callback(self, interaction: discord.Interaction):
        try:
            new_view = MassCrazyScoreView(
                self.guild_id, self.rows, self.season,
                self.current_page, self.selected_pred_ids,
                interaction.data['values'][0]
            )
            await interaction.response.edit_message(content=new_view.get_content(), view=new_view)
        except Exception:
            logger.exception("MassCrazyScoreView diff_select_callback error")

    async def prev_callback(self, interaction: discord.Interaction):
        try:
            new_view = MassCrazyScoreView(
                self.guild_id, self.rows, self.season,
                self.current_page - 1, self.selected_pred_ids, self.selected_difficulty
            )
            await interaction.response.edit_message(content=new_view.get_content(), view=new_view)
        except Exception:
            logger.exception("MassCrazyScoreView prev_callback error")

    async def next_callback(self, interaction: discord.Interaction):
        try:
            new_view = MassCrazyScoreView(
                self.guild_id, self.rows, self.season,
                self.current_page + 1, self.selected_pred_ids, self.selected_difficulty
            )
            await interaction.response.edit_message(content=new_view.get_content(), view=new_view)
        except Exception:
            logger.exception("MassCrazyScoreView next_callback error")

    async def submit_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)

            if not interaction.user.guild_permissions.manage_guild:
                await interaction.followup.send("❌ You don't have permission.", ephemeral=True)
                return

            points = CRAZY_PRED_POINTS[self.selected_difficulty]
            scored_users = []

            for pred_id in self.selected_pred_ids:
                pred_row = next((r for r in self.rows if r['id'] == pred_id), None)
                if not pred_row:
                    continue
                save_scored_crazy_prediction(
                    self.guild_id, pred_id,
                    pred_row['user_id'], pred_row['username'],
                    self.selected_difficulty, points
                )
                scored_users.append(f"**{pred_row['username']}**: {pred_row['prediction']}")

            update_leaderboard(self.guild_id)

            channel_id = get_prediction_channel(self.guild_id)
            if channel_id:
                try:
                    channel = interaction.guild.get_channel(channel_id) or await bot.fetch_channel(channel_id)
                    if channel:
                        users_text = "\n".join(f"> {u}" for u in scored_users)
                        await channel.send(
                            f"🎯 **Crazy Predictions Scored!**\n"
                            f"The following predictions came true:\n"
                            f"{users_text}\n"
                            f"Difficulty: **{self.selected_difficulty}** — **{points} points** each!"
                        )
                except discord.NotFound:
                    logger.exception("Channel not found when sending mass crazy score results")

            await interaction.followup.send(
                f"✅ Scored {len(scored_users)} predictions at **{self.selected_difficulty}** difficulty ({points} pts each)!",
                ephemeral=True
            )
            self.stop()

        except Exception:
            logger.exception("MassCrazyScoreView submit_callback error")

    async def unscore_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)

            if not interaction.user.guild_permissions.manage_guild:
                await interaction.followup.send("❌ You don't have permission.", ephemeral=True)
                return

            for pred_id in self.selected_pred_ids:
                remove_scored_crazy_prediction(self.guild_id, pred_id)

            update_leaderboard(self.guild_id)

            rows = get_all_crazy_predictions(self.guild_id, self.season)
            new_view = MassCrazyScoreView(self.guild_id, rows, self.season, self.current_page, [], None)
            await interaction.edit_original_response(content=new_view.get_content(), view=new_view)
            await interaction.followup.send(
                f"✅ Removed scores for {len(self.selected_pred_ids)} predictions!",
                ephemeral=True
            )

        except Exception:
            logger.exception("MassCrazyScoreView unscore_callback error")


@bot.tree.command(name="mass_score_crazy_predictions", description="Score multiple crazy predictions at once (MODS ONLY)")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.describe(season="Season year (defaults to current season)")
async def mass_score_crazy_predictions(interaction: discord.Interaction, season: int = None):
    try:
        await interaction.response.defer(ephemeral=True)

        if season is None:
            season = SEASON

        rows = get_all_crazy_predictions(interaction.guild.id, season)
        if not rows:
            await interaction.followup.send(f"❌ No crazy predictions found for **{season}**.", ephemeral=True)
            return

        view = MassCrazyScoreView(interaction.guild.id, rows, season)
        await interaction.followup.send(content=view.get_content(), view=view, ephemeral=True)

    except Exception:
        logger.exception("mass_score_crazy_predictions error")

@mass_score_crazy_predictions.error
async def mass_score_crazy_predictions_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.",
            ephemeral=True
        )


class UserCrazyPredsView(discord.ui.View):
    def __init__(self, guild_id, rows, target_user, season, is_mod=False, selected_pred_id=None, selected_difficulty=None):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.rows = rows
        self.target_user = target_user
        self.season = season
        self.is_mod = is_mod
        self.selected_pred_id = selected_pred_id
        self.selected_difficulty = selected_difficulty

        pred_options = [
            discord.SelectOption(
                label=f"{i+1}. {row['prediction'][:70]}",
                value=str(row['id']),
                description="✅ Already scored" if row['difficulty'] else None,
                default=(str(row['id']) == str(selected_pred_id))
            )
            for i, row in enumerate(rows)
        ]
        pred_select = discord.ui.Select(
            placeholder="Select a prediction...",
            options=pred_options,
            row=0
        )
        pred_select.callback = self.pred_select_callback
        self.add_item(pred_select)

        if is_mod:
            diff_options = [
                discord.SelectOption(
                    label=f"{diff} ({pts} pts)",
                    value=diff,
                    default=(diff == selected_difficulty)
                )
                for diff, pts in CRAZY_PRED_POINTS.items()
            ]
            diff_select = discord.ui.Select(
                placeholder="Select difficulty...",
                options=diff_options,
                row=1
            )
            diff_select.callback = self.diff_select_callback
            self.add_item(diff_select)

            score_btn = discord.ui.Button(
                label="Score",
                style=discord.ButtonStyle.green,
                disabled=not (selected_pred_id and selected_difficulty),
                row=2
            )
            score_btn.callback = self.score_callback
            self.add_item(score_btn)

            unscore_btn = discord.ui.Button(
                label="Remove Score",
                style=discord.ButtonStyle.red,
                disabled=not selected_pred_id,
                row=2
            )
            unscore_btn.callback = self.unscore_callback
            self.add_item(unscore_btn)

    def get_embed(self):
        count = len(self.rows)
        total_points = sum(row['points'] for row in self.rows if row['points'])
        embed = discord.Embed(
            title=f"🤯 Crazy Predictions by {self.target_user.display_name} — {self.season}",
            description=f"{count} prediction(s) | {total_points} pts earned",
            color=discord.Color.red()
        )
        for i, row in enumerate(self.rows, start=1):
            scored = row['difficulty'] is not None
            tick = "✅" if scored else "⬜"
            score_info = f" — {row['difficulty']} ({row['points']} pts)" if scored else ""
            embed.add_field(
                name=f"{tick} {i}. {format_timestamp(row['timestamp'])}{score_info}",
                value=f"> {row['prediction']}",
                inline=False
            )
        return embed

    async def pred_select_callback(self, interaction: discord.Interaction):
        try:
            new_view = UserCrazyPredsView(
                self.guild_id, self.rows, self.target_user, self.season,
                self.is_mod, int(interaction.data['values'][0]), self.selected_difficulty
            )
            await interaction.response.edit_message(embed=new_view.get_embed(), view=new_view)
        except Exception:
            logger.exception("UserCrazyPredsView pred_select_callback error")

    async def diff_select_callback(self, interaction: discord.Interaction):
        try:
            new_view = UserCrazyPredsView(
                self.guild_id, self.rows, self.target_user, self.season,
                self.is_mod, self.selected_pred_id, interaction.data['values'][0]
            )
            await interaction.response.edit_message(embed=new_view.get_embed(), view=new_view)
        except Exception:
            logger.exception("UserCrazyPredsView diff_select_callback error")

    async def score_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)

            if not interaction.user.guild_permissions.manage_guild:
                await interaction.followup.send("❌ You don't have permission.", ephemeral=True)
                return

            pred_row = next((r for r in self.rows if r['id'] == self.selected_pred_id), None)
            if not pred_row:
                await interaction.followup.send("❌ Prediction not found.", ephemeral=True)
                return

            points = CRAZY_PRED_POINTS[self.selected_difficulty]
            save_scored_crazy_prediction(
                self.guild_id, self.selected_pred_id,
                pred_row['user_id'], pred_row['username'],
                self.selected_difficulty, points
            )
            update_leaderboard(self.guild_id)

            rows = get_all_crazy_predictions_for_user(self.guild_id, self.target_user.id, self.season)
            new_view = UserCrazyPredsView(
                self.guild_id, rows, self.target_user, self.season,
                self.is_mod, None, None
            )
            await interaction.edit_original_response(embed=new_view.get_embed(), view=new_view)

            channel_id = get_prediction_channel(self.guild_id)
            if channel_id:
                try:
                    channel = interaction.guild.get_channel(channel_id) or await bot.fetch_channel(channel_id)
                    if channel:
                        await channel.send(
                            f"🎯 **Crazy Prediction Scored!**\n"
                            f"**{pred_row['username']}**'s prediction came true:\n"
                            f"> {pred_row['prediction']}\n"
                            f"Difficulty: **{self.selected_difficulty}** — **{points} points** awarded!"
                        )
                except discord.NotFound:
                    logger.exception("Channel not found")

            await interaction.followup.send("✅ Scored!", ephemeral=True)

        except Exception:
            logger.exception("UserCrazyPredsView score_callback error")

    async def unscore_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)

            if not interaction.user.guild_permissions.manage_guild:
                await interaction.followup.send("❌ You don't have permission.", ephemeral=True)
                return

            remove_scored_crazy_prediction(self.guild_id, self.selected_pred_id)
            update_leaderboard(self.guild_id)

            rows = get_all_crazy_predictions_for_user(self.guild_id, self.target_user.id, self.season)
            new_view = UserCrazyPredsView(
                self.guild_id, rows, self.target_user, self.season,
                self.is_mod, None, None
            )
            await interaction.edit_original_response(embed=new_view.get_embed(), view=new_view)
            await interaction.followup.send("✅ Score removed!", ephemeral=True)

        except Exception:
            logger.exception("UserCrazyPredsView unscore_callback error")


@bot.tree.command(name="score_crazy_predictions", description="Score a user's crazy predictions (MODS ONLY)")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.describe(user="The user whose predictions to score", season="Season year (defaults to current season)")
async def score_crazy_predictions(interaction: discord.Interaction, user: discord.Member, season: int = None):
    try:
        await interaction.response.defer(ephemeral=True)

        if season is None:
            season = SEASON

        rows = get_all_crazy_predictions_for_user(interaction.guild.id, user.id, season)
        if not rows:
            await interaction.followup.send(
                f"❌ **{user.display_name}** has no crazy predictions for **{season}**.",
                ephemeral=True
            )
            return

        view = UserCrazyPredsView(interaction.guild.id, rows, user, season, is_mod=True)
        await interaction.followup.send(embed=view.get_embed(), view=view, ephemeral=True)

    except Exception:
        logger.exception("score_crazy_predictions error")

@score_crazy_predictions.error
async def score_crazy_predictions_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.",
            ephemeral=True
        )


def format_timestamp(dt):
    try:
        return dt.strftime("%d %b %Y, %H:%M UTC")
    except Exception:
        logger.exception("format_timestamp error")


def format_crazy_predictions(username, rows):
    try:
        if not rows:
            return f"❌ **{username}** has made no crazy predictions."

        lines = [f"🤯  **Crazy Predictions by {username}**\n"]

        for i, row in enumerate(rows, start=1):
            prediction = row["prediction"]
            timestamp = row["timestamp"]
            scored = row["difficulty"] is not None
            tick = "✅" if scored else "⬜"
            score_info = f" — {row['difficulty']} ({row['points']} pts)" if scored else ""
            lines.append(
                f"{tick} **{i}.** _({format_timestamp(timestamp)})_{score_info}\n"
                f"> {prediction}\n"
            )

        return "\n".join(lines)

    except Exception:
        logger.exception("format_crazy_predictions error")


@bot.tree.command(name="view_crazy_predictions", description="View a user's crazy season predictions")
@app_commands.describe(user="The user to view", season="Season year (defaults to current season)")
async def crazy_predictions(interaction: discord.Interaction, user: discord.Member, season: int = None):
    try:
        await interaction.response.defer(ephemeral=True)

        if season is None:
            season = SEASON

        rows = get_all_crazy_predictions_for_user(interaction.guild.id, user.id, season)

        if not rows:
            await interaction.followup.send(
                f"❌ **{user.display_name}** has made no crazy predictions for **{season}**.",
                ephemeral=True
            )
            return

        message = format_crazy_predictions(user.display_name, rows)

        if not message:
            await interaction.followup.send("❌ Error formatting predictions.", ephemeral=True)
            return

        await interaction.followup.send(message)

    except Exception:
        logger.exception("crazy_predictions error")

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

    except Exception:
        logger.exception("bold_predict error")

@bot.tree.command(
    name="update_leaderboard",
    description="Manually update the leaderboard"
)
async def update_leaderboard_cmd(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)

    try:
        update_leaderboard(interaction.guild.id)
        await interaction.followup.send("✅ Leaderboard updated.")
    except Exception:
        logger.exception("update_leaderboard error")

@bot.tree.command(name="toggle_bold_predictions", description="Toggle bold prediction messages on or off (MODS ONLY)")
@app_commands.checks.has_permissions(manage_guild=True)
async def toggle_bold_predictions(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True)
        guild_id = interaction.guild.id
        currently_opted_out = is_bold_pred_opted_out(guild_id)
        set_bold_pred_optout(guild_id, not currently_opted_out)

        if currently_opted_out:
            await interaction.followup.send("✅ Bold prediction messages are now **enabled** for this server.", ephemeral=True)
            await interaction.channel.send("✅ Bold prediction messages are now **enabled** for this server.")
        else:
            await interaction.followup.send("✅ Bold prediction messages are now **disabled** for this server.", ephemeral=True)
            await interaction.channel.send("✅ Bold prediction messages are now **disabled** for this server.")

    except Exception:
        logger.exception("toggle_bold_predictions error")

@toggle_bold_predictions.error
async def toggle_bold_predictions_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.",
            ephemeral=True
        )

@tasks.loop(minutes=max(1, 60/TIME_MULTIPLE))
async def bold_predictions_publisher():
    try:
        race_number = RACE_CACHE.get("race_number")
        race_name = RACE_CACHE.get("race_name")
        lock_time = RACE_CACHE.get("lock_time")

        now = get_now()

        if not race_number or not lock_time:
            return

        publish_time = lock_time - timedelta(days=4)
        if now < publish_time or now > lock_time:
            return

        for guild in bot.guilds:
            try:
                guild_id = guild.id

                # Check opt out
                if is_bold_pred_opted_out(guild_id):
                    continue

                preds = fetch_bold_predictions(guild_id, race_number=race_number)
                lines = [
                    f"**Bold Predictions — {race_name}**",
                    "",
                    f"Lock in your predictions before Qualifying! \nSubmissions lock <t:{int(lock_time.timestamp())}:R>",
                    "",
                    "*This message can be turned off by moderators using /toggle_bold_predictions.*",
                    ""
                ]
                if preds:
                    for username, prediction in preds:
                        lines.append(f"• **{username}** — {prediction}")
                else:
                    lines.append(f"No predictions for the {race_name} yet.")

                content = "\n".join(lines)

                channel_id = get_prediction_channel(guild_id)
                if not channel_id:
                    try:
                        first_channel = next(
                            ch for ch in guild.text_channels
                            if ch.permissions_for(guild.me).send_messages
                        )
                        existing_warning = get_persistent_message(guild_id, "no_channel_warning")
                        if not existing_warning:
                            msg = await first_channel.send(
                                "⚠️ No prediction channel set! Admins, use /set_channel to configure it."
                            )
                            await msg.pin()
                            save_persistent_message(guild_id, "no_channel_warning", first_channel.id, msg.id)
                    except StopIteration:
                        logger.warning("No accessible text channels in guild %s", guild.name)
                    continue
                
                try:
                    channel = guild.get_channel(channel_id) or await bot.fetch_channel(channel_id)
                except discord.NotFound:
                    logger.exception("Channel %s not found in guild %s", channel_id, guild.name)
                    continue

                perms = channel.permissions_for(guild.me)
                logger.debug("Bot permissions in %s: manage_messages=%s, read_messages=%s, send_messages=%s", 
                    channel.name, perms.manage_messages, perms.read_messages, perms.send_messages)

                current_key = f"bold_predictions_{race_number}"
                existing = get_persistent_message(guild_id, current_key)

                if existing:
                    # Edit existing message for this race
                    try:
                        msg = await channel.fetch_message(existing["message_id"])
                        await msg.edit(content=content)
                        logger.info("Edited bold pred message in %s", guild.name)
                    except discord.NotFound:
                       msg = await channel.send(content)
                    try:
                        await msg.pin()
                    except discord.Forbidden:
                        logger.warning("No permission to pin in %s", guild.name)
                    save_persistent_message(guild_id, current_key, channel.id, msg.id)
                else:
                    # Delete previous race's message
                    if race_number > 1:
                        prev_key = f"bold_predictions_{race_number - 1}"
                        prev = get_persistent_message(guild_id, prev_key)
                        if prev:
                            try:
                                prev_msg = await channel.fetch_message(prev["message_id"])
                                await prev_msg.unpin()
                                await prev_msg.delete()
                                logger.info("Deleted previous bold pred message in %s", guild.name)
                            except discord.NotFound:
                                pass
                            except discord.Forbidden:
                                logger.warning("No permission to delete previous bold pred message in %s", guild.name)

                    msg = await channel.send(content)
                    try:
                        await msg.pin()
                    except discord.Forbidden:
                        logger.warning("No permission to pin in %s", guild.name)
                    save_persistent_message(guild_id, current_key, channel.id, msg.id)
                    logger.info("Sent new bold pred message in %s", guild.name)
            except Exception:
                logger.exception("bold_predictions_publisher error in guild %s", guild_id)

    except Exception:
        logger.exception("bold_predictions_publisher error")

def format_bold_predictions(race_name, rows):
    try:
        if not rows:
            return f"❌ No bold predictions found for **{race_name}**"
        
        message = f"🧨 **Bold Predictions for {race_name}:**\n"
        for username, prediction, in rows:
            message += f"> **{username}**: {prediction}\n"
        return message
    except Exception:
        logger.exception("format_bold_predictions error")

@bot.tree.command(
    name="view_race_bold_predictions",
    description="View bold predictions for a specific race"
)
@app_commands.describe(race="Select the race")
async def view_race_bold_preds(interaction: discord.Interaction, race: str):
    await interaction.response.defer(ephemeral=True)
    
    try:
        # use the unified function with race_name
        rows = fetch_bold_predictions(interaction.guild.id, race_name=race)

        if not rows:
            await interaction.followup.send(f"❌ No bold predictions found for **{race}**", ephemeral=True)
            return

        message = format_bold_predictions(race, rows)
        await interaction.followup.send(message)
    except Exception:
        logger.exception("view_race_bold_preds error")

@view_race_bold_preds.autocomplete('race')
async def race_autocomplete(interaction: discord.Interaction, current: str):
    try:
        # Return up to 25 matching race names
        return [
            app_commands.Choice(name=str(race), value=str(race))
            for race in SEASON_CALENDER
            if current.lower() in str(race).lower()
        ][:25]
    except Exception:
        logger.exception("view_race_bold_preds.autocomplete error")

class CorrectBoldPredView(discord.ui.View):
    def __init__(self, guild: discord.Guild):
        super().__init__(timeout=300)
        self.guild = guild
        self.selected_users = []
        self.selected_race = None
        self.add_item(CorrectBoldPredUserSelect(self))
        self.add_item(CorrectBoldPredRaceSelect(self))
        self.add_item(CorrectBoldPredSubmit(self))

class CorrectBoldPredUserSelect(discord.ui.UserSelect):
    def __init__(self, view2):
        self.view2 = view2
        super().__init__(
            placeholder="Select users who got it correct",
            min_values=1,
            max_values=10
        )

    async def callback(self, interaction: discord.Interaction):
        try:
            self.view2.selected_users = self.values
            await interaction.response.defer()
        except Exception:
            logger.exception("CorrectBoldPredUserSelect callback error")

class CorrectBoldPredRaceSelect(discord.ui.Select):
    def __init__(self, view2):
        self.view2 = view2
        super().__init__(
            placeholder="Select the race",
            options=[
                discord.SelectOption(label=race, value=race)
                for race in SEASON_CALENDER
            ][:25]
        )

    async def callback(self, interaction: discord.Interaction):
        try:
            self.view2.selected_race = self.values[0]
            await interaction.response.defer()
        except Exception:
            logger.exception("CorrectBoldPredRaceSelect callback error")

class CorrectBoldPredSubmit(discord.ui.Button):
    global BOLD_PRED_POINTS 
    def __init__(self, view2):
        self.view2 = view2
        super().__init__(label="Submit", style=discord.ButtonStyle.green)

    async def callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)

            if not self.view2.selected_users or not self.view2.selected_race:
                await interaction.followup.send("❌ Please select both users and a race.", ephemeral=True)
                return

            guild_id = interaction.guild.id
            race_name = self.view2.selected_race

            for user in self.view2.selected_users:
                save_correct_bold_prediction(guild_id, user.id, str(user), race_name)

            update_leaderboard(guild_id)

            user_mentions = ", ".join(u.mention for u in self.view2.selected_users)
            channel_id = get_prediction_channel(guild_id)
            if channel_id:
                try:
                    channel = interaction.guild.get_channel(channel_id) or await bot.fetch_channel(channel_id)
                    await channel.send(
                        f"🎯 **Correct Bold Predictions — {race_name}**\n"
                        f"The following users got their bold prediction right:\n"
                        f"{user_mentions}\n"
                        f"Each receives **{BOLD_PRED_POINTS} points**!"
                    )
                except discord.NotFound:
                    logger.exception("Channel not found when sending bold pred results")

            await interaction.followup.send("✅ Correct bold predictions saved and leaderboard updated!", ephemeral=True)
            self.view2.stop()

        except Exception:
            logger.exception("CorrectBoldPredSubmit callback error")

@bot.tree.command(name="correct_bold_predictions", description="Mark correct bold predictions (MODS ONLY)")
@app_commands.checks.has_permissions(manage_guild=True)
async def correct_bold_predictions(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True)
        await interaction.followup.send(
            "Select the users who got their bold prediction correct and the race:",
            view=CorrectBoldPredView(interaction.guild),
            ephemeral=True
        )
    except Exception:
        logger.exception("correct_bold_predict error")

@correct_bold_predictions.error
async def correct_bold_predict_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.",
            ephemeral=True
        )

@bot.tree.command(name="view_correct_bold_predictions", description="View a user's correct bold predictions")
@app_commands.describe(user="The user to view")
async def view_correct_bold_predictions(interaction: discord.Interaction, user: discord.Member):
    try:
        await interaction.response.defer(ephemeral=True)

        rows = get_correct_bold_predictions(interaction.guild.id, user.id)
        count = len(rows)

        if not rows:
            await interaction.followup.send(
                f"❌ **{user.display_name}** has no correct bold predictions yet.",
                ephemeral=True
            )
            return

        lines = [f"🎯 **Correct Bold Predictions by {user.display_name}** ({count} total, {count * BOLD_PRED_POINTS} points)\n"]
        for i, row in enumerate(rows, start=1):
            prediction = row['prediction'] if row['prediction'] else "_Prediction not recorded_"
            lines.append(f"**{i}.** {row['race_name']}\n> {prediction}")

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    except Exception:
        logger.exception("view_correct_bold_predictions error")


@bot.tree.command(name="set_channel", description="Set or update the prediction channel (MODS ONLY)")
@app_commands.checks.has_permissions(administrator=True)
async def set_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        if interaction.guild is None:
            await interaction.followup.send("This command can only be used in a server.")
            return

        if channel.guild.id != interaction.guild.id:
            await interaction.followup.send("You can only set a channel from this server.")
            return

        old_channel_id = get_prediction_channel(interaction.guild.id)
        old_channel = interaction.guild.get_channel(old_channel_id) if old_channel_id else None

        set_prediction_channel(interaction.guild.id, channel.id)

        if old_channel:
            await interaction.followup.send(
                f"Prediction channel updated from {old_channel.mention} to {channel.mention}"
            )
        else:
            await interaction.followup.send(
                f"Prediction channel set to {channel.mention}"
            )

    except app_commands.MissingPermissions:
        await interaction.response.send_message(
            "You must be an administrator to use this command.", ephemeral=True
        )
    except Exception:
        logger.exception("Failed to set prediction channel for guild %s", interaction.guild.id)
        # Send a generic error message to user without leaking e
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "An unexpected error occurred. Please try again later.", ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "An unexpected error occurred. Please try again later."
                )
        except Exception:
            logger.exception("Failed to notify user about error in set_channel for guild %s", interaction.guild.id)

@bot.tree.command(name="force_score_race", description="Manually score a race (MODS ONLY)")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.describe(race="Select the race to score")
async def force_score_race(interaction: discord.Interaction, race: str):
    await interaction.response.defer(ephemeral=True)
    try:
        # Get race number from race name
        result = safe_fetch_one(
            "SELECT race_number FROM race_results WHERE race_name = %s",
            (race,)
        )
        if not result:
            await interaction.followup.send(f"❌ No results found for **{race}**. Results may not be saved yet.", ephemeral=True)
            return

        race_num = result['race_number']
        guild_id = interaction.guild.id

        score_race_for_guild(race_num, guild_id)
        update_leaderboard(guild_id)
        mark_race_scored(guild_id, race_num)

        await interaction.followup.send(f"✅ **The {race}** has been scored!", ephemeral=True)

    except Exception:
        logger.exception("force_score_race error")

@force_score_race.autocomplete('race')
async def force_score_race_autocomplete(interaction: discord.Interaction, current: str):
    try:
        return [
            app_commands.Choice(name=str(race), value=str(race))
            for race in SEASON_CALENDER
            if current.lower() in str(race).lower()
        ][:25]
    except Exception:
        logger.exception("force_score_race autocomplete error")
        return []

@force_score_race.error
async def force_score_race_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.",
            ephemeral=True
        )

@bot.tree.command(name="force_score_season", description="Manually score season predictions (MODS ONLY)")
@app_commands.checks.has_permissions(manage_guild=True)
async def force_score_season(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        guild_id = interaction.guild.id

        result = safe_fetch_one("SELECT wdc, wcc FROM final_champions WHERE season = %s", (SEASON,))
        if not result:
            await interaction.followup.send("❌ No final champions data found. Champions may not be saved yet.", ephemeral=True)
            return

        score_final_champions_for_guild(guild_id)
        update_leaderboard(guild_id)
        mark_season_scored(guild_id, SEASON)

        await interaction.followup.send(f"✅ **The {SEASON}** season predictions have been scored!", ephemeral=True)

    except Exception:
        logger.exception("force_score_season error")

@force_score_season.error
async def force_score_season_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.",
            ephemeral=True
        )

bot.run(token, log_handler=handler, log_level=logging.INFO)