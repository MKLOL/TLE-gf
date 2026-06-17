"""Queens slash group (Minigames cog slash mixin; see minigames.py)."""

import datetime as dt
import logging
from typing import Optional

import discord
from discord import app_commands


from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs._minigame_helpers import _SlashCtx
from tle.cogs._minigame_slash_consts import _TIMEFRAME_CHOICES, _MODE_CHOICES

logger = logging.getLogger(__name__)


class QueensSlashMixin:
    queens_slash = app_commands.Group(
        name='queens', description='LinkedIn Queens commands', guild_only=True)

    @queens_slash.command(name='show', description='Show LinkedIn Queens settings')
    async def slash_queens_show(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            await self._cmd_queens_show(_SlashCtx(interaction))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='here', description='Set the LinkedIn Queens channel')
    async def slash_queens_here(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        try:
            await self._cmd_here(_SlashCtx(interaction), QUEENS_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='register', description='Link a Discord user to a LinkedIn Queens name')
    @app_commands.describe(
        linkedin_name='LinkedIn display name',
        member='Discord member to register (mods only when not yourself)',
        anonymous='Hide the LinkedIn name in public bot output')
    async def slash_queens_register(
        self, interaction: discord.Interaction,
        linkedin_name: str,
        member: Optional[discord.Member] = None,
        anonymous: bool = False,
    ):
        await interaction.response.defer()
        ctx = _SlashCtx(interaction)
        try:
            target = self._resolve_queens_registrar_target(ctx, member)
            await self._cmd_queens_register(
                ctx, target, linkedin_name, anonymous=anonymous)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='set', description='Set a Queens LinkedIn name without verification')
    @app_commands.describe(
        member='Discord member to set',
        linkedin_name='LinkedIn display name',
        anonymous='Hide the LinkedIn name in public bot output')
    async def slash_queens_set(
        self, interaction: discord.Interaction,
        member: discord.Member,
        linkedin_name: str,
        anonymous: bool = False,
    ):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        try:
            await self._cmd_queens_set(
                _SlashCtx(interaction), member, linkedin_name,
                anonymous=anonymous)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='unregister', description='Remove a LinkedIn Queens link')
    @app_commands.describe(member='Discord member to unregister (mods only when not yourself)')
    async def slash_queens_unregister(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
    ):
        await interaction.response.defer()
        ctx = _SlashCtx(interaction)
        try:
            await self._cmd_queens_unregister(ctx, member)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='update', description='Refresh the LinkedIn Queens leaderboard')
    @app_commands.describe(yesterday='Fetch the Yesterday results tab')
    async def slash_queens_update(
        self, interaction: discord.Interaction,
        yesterday: bool = False,
    ):
        await interaction.response.defer()
        try:
            await self._cmd_queens_update(
                _SlashCtx(interaction),
                results_day='yesterday' if yesterday else 'today')
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='vs', description='Head-to-head comparison')
    @app_commands.describe(
        member1='First player', member2='Second player',
        timeframe='Time period filter', mode='Scoring mode',
        weekdays='Queens days: mon,wed, weekday, or weekend')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES, mode=_MODE_CHOICES)
    async def slash_queens_vs(
        self, interaction: discord.Interaction,
        member1: discord.Member, member2: discord.Member,
        timeframe: Optional[app_commands.Choice[str]] = None,
        mode: Optional[app_commands.Choice[str]] = None,
        weekdays: Optional[str] = None,
    ):
        await interaction.response.defer()
        try:
            await self._cmd_vs(
                _SlashCtx(interaction), QUEENS_GAME, member1, member2,
                *self._slash_choice_args(timeframe, mode),
                *self._slash_queens_weekday_args(weekdays))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='top', description='Show fastest-result winners')
    @app_commands.describe(
        timeframe='Time period filter', mode='Scoring mode',
        weekdays='Queens days: mon,wed, weekday, or weekend')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES, mode=_MODE_CHOICES)
    async def slash_queens_top(
        self, interaction: discord.Interaction,
        timeframe: Optional[app_commands.Choice[str]] = None,
        mode: Optional[app_commands.Choice[str]] = None,
        weekdays: Optional[str] = None,
    ):
        await interaction.response.defer()
        try:
            await self._cmd_top(
                _SlashCtx(interaction), QUEENS_GAME,
                *self._slash_choice_args(timeframe, mode),
                *self._slash_queens_weekday_args(weekdays))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='streak', description='Show current clean streak')
    @app_commands.describe(
        member='Player to check', timeframe='Time period filter',
        weekdays='Queens days: mon,wed, weekday, or weekend')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES)
    async def slash_queens_streak(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        timeframe: Optional[app_commands.Choice[str]] = None,
        weekdays: Optional[str] = None,
    ):
        await interaction.response.defer()
        ctx = _SlashCtx(interaction)
        if member:
            ctx.author = member
        try:
            await self._cmd_queens_streak(
                ctx, *self._slash_choice_args(timeframe),
                *self._slash_queens_weekday_args(weekdays))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='stats', description='Show personal Queens stats')
    @app_commands.describe(
        member='Player to check', timeframe='Time period filter',
        weekdays='Queens days: mon,wed, weekday, or weekend')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES)
    async def slash_queens_stats(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        timeframe: Optional[app_commands.Choice[str]] = None,
        weekdays: Optional[str] = None,
    ):
        await interaction.response.defer()
        ctx = _SlashCtx(interaction)
        if member:
            ctx.author = member
        try:
            await self._cmd_queens_stats(
                ctx, *self._slash_choice_args(timeframe),
                *self._slash_queens_weekday_args(weekdays))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='results', description='Show a Queens date leaderboard')
    @app_commands.describe(
        date='Date or puzzle number (defaults to today)',
        weekdays='Queens days: mon,wed, weekday, or weekend',
        date_filter='Rating date filter, e.g. d>=01062026 d<08062026')
    async def slash_queens_results(
        self, interaction: discord.Interaction,
        date: Optional[str] = None,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
    ):
        await interaction.response.defer()
        try:
            await self._cmd_queens_stats_date(
                _SlashCtx(interaction), date or dt.date.today().isoformat(),
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='ratings', description='Show Queens rating leaderboard')
    @app_commands.describe(
        weekdays='Queens days: mon,wed, weekday, or weekend',
        date_filter='Rating date filter, e.g. d>=01062026 d<08062026')
    async def slash_queens_ratings(
        self, interaction: discord.Interaction,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
    ):
        await interaction.response.defer()
        try:
            await self._cmd_queens_ratings(
                _SlashCtx(interaction),
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='rating', description="Show a user's Queens rating graph")
    @app_commands.describe(
        member='Player (defaults to you)',
        weekdays='Queens days: mon,wed, weekday, or weekend',
        date_filter='Rating date filter, e.g. d>=01062026 d<08062026',
        recalculate='Recalculate ratings from the filtered result set')
    async def slash_queens_rating(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
        recalculate: Optional[bool] = False,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_queens_rating(
                _SlashCtx(interaction), [target],
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter),
                recalculate=bool(recalculate))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='performance', description="Show a user's Queens performance graph")
    @app_commands.describe(
        member='Player (defaults to you)',
        weekdays='Queens days: mon,wed, weekday, or weekend',
        date_filter='Rating date filter, e.g. d>=01062026 d<08062026')
    async def slash_queens_performance(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_queens_performance(
                _SlashCtx(interaction), [target],
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='history', description="Show a user's Queens rating delta log")
    @app_commands.describe(
        member='Player (defaults to you)',
        weekdays='Queens days: mon,wed, weekday, or weekend',
        date_filter='Rating date filter, e.g. d>=01062026 d<08062026')
    async def slash_queens_history(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_queens_history(
                _SlashCtx(interaction), target,
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='add', description='Manually add a Queens result')
    @app_commands.describe(
        member='Player', date='Date or puzzle number',
        time='Time as M:SS or H:MM:SS',
        status='Status text, defaults to no hints and no mistakes')
    async def slash_queens_add(
        self, interaction: discord.Interaction,
        member: discord.Member, date: str, time: str,
        status: Optional[str] = None,
    ):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        try:
            status = status or 'No hints & no mistakes'
            await self._cmd_queens_add(
                _SlashCtx(interaction),
                f'{member.id} {date} {time} {status}')
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='remove', description='Remove a Queens result')
    @app_commands.describe(member='Player', date='Date or puzzle number')
    async def slash_queens_remove(
        self, interaction: discord.Interaction,
        member: discord.Member, date: str,
    ):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        try:
            await self._cmd_queens_remove(
                _SlashCtx(interaction), f'{member.id} {date}')
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='clear', description='Remove all Queens results for a date')
    @app_commands.describe(date='Date or puzzle number')
    async def slash_queens_clear(
        self, interaction: discord.Interaction, date: str,
    ):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        try:
            await self._cmd_queens_clear(_SlashCtx(interaction), date)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='clean', description='Remove Queens results for a date range')
    @app_commands.describe(
        start_date='Start date or puzzle number',
        end_date='End date or puzzle number (defaults to start date)')
    async def slash_queens_clean(
        self, interaction: discord.Interaction, start_date: str,
        end_date: Optional[str] = None,
    ):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        try:
            await self._cmd_queens_clean(
                _SlashCtx(interaction), start_date, end_date)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='reparse', description='Reparse all stored raw Queens messages')
    async def slash_queens_reparse(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        try:
            await self._cmd_reparse(_SlashCtx(interaction), QUEENS_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='import-start', description='Rebuild imported Queens history')
    @app_commands.describe(channel='Channel to import from')
    async def slash_queens_import_start(
        self, interaction: discord.Interaction,
        channel: Optional[discord.TextChannel] = None,
    ):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        ctx = _SlashCtx(interaction)
        try:
            original = await interaction.original_response()
            ctx.message = original
            await self._cmd_import_start(ctx, QUEENS_GAME, channel)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='import-status', description='Show Queens import status')
    async def slash_queens_import_status(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        try:
            await self._cmd_import_status(_SlashCtx(interaction), QUEENS_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='import-cancel', description='Cancel a running Queens import')
    async def slash_queens_import_cancel(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        try:
            await self._cmd_import_cancel(_SlashCtx(interaction), QUEENS_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='import-clear', description='Delete imported Queens history')
    async def slash_queens_import_clear(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        try:
            await self._cmd_import_clear(_SlashCtx(interaction), QUEENS_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash.command(name='ratings-recompute', description='Rebuild the Queens rating snapshot')
    async def slash_queens_ratings_recompute(
        self, interaction: discord.Interaction,
    ):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        try:
            await self._cmd_queens_ratings_recompute(_SlashCtx(interaction))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)
