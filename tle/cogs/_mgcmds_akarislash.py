"""Akari slash group (Minigames cog slash mixin; see minigames.py).

Shared slash helpers (error plumbing, mod checks, option adapters) live in
``_mgcmds_slashhelpers``; this module carries only the ``/akari`` commands.
"""

import logging
from typing import Optional

import discord
from discord import app_commands

from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_helpers import MinigameCogError, _SlashCtx
from tle.cogs._minigame_slash_consts import _TIMEFRAME_CHOICES, _MODE_CHOICES

logger = logging.getLogger(__name__)


class AkariSlashMixin:
    akari_slash = app_commands.Group(
        name='akari', description='Daily Akari commands', guild_only=True)
    # Nested group mirroring the ';akari import <sub>' prefix commands and
    # the /queens import subgroup (Discord caps a group at 25 children).
    akari_slash_import = app_commands.Group(
        name='import', description='Manage imported Akari history',
        parent=akari_slash)

    @akari_slash.command(name='show', description='Show Daily Akari settings')
    async def slash_akari_show(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            await self._cmd_show(_SlashCtx(interaction), AKARI_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='vs', description='Compare two to five players')
    @app_commands.describe(
        member1='First player', member2='Second player',
        member3='Optional third player', member4='Optional fourth player',
        member5='Optional fifth player',
        timeframe='Time period filter', mode='Scoring mode',
        weekdays='Days: mon,wed, weekday, or weekend')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES, mode=_MODE_CHOICES)
    async def slash_akari_vs(
        self, interaction: discord.Interaction,
        member1: discord.Member, member2: discord.Member,
        member3: Optional[discord.Member] = None,
        member4: Optional[discord.Member] = None,
        member5: Optional[discord.Member] = None,
        timeframe: Optional[app_commands.Choice[str]] = None,
        mode: Optional[app_commands.Choice[str]] = None,
        weekdays: Optional[str] = None,
    ):
        await interaction.response.defer()
        try:
            members = [
                member for member in
                (member1, member2, member3, member4, member5)
                if member is not None
            ]
            await self._cmd_vs_members(
                _SlashCtx(interaction), AKARI_GAME, members,
                *self._slash_choice_args(timeframe, mode),
                *self._slash_queens_weekday_args(weekdays))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='streak', description='Show current perfect streak')
    @app_commands.describe(
        member='Player to check', timeframe='Time period filter',
        weekdays='Days: mon,wed, weekday, or weekend')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES)
    async def slash_akari_streak(
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
            await self._cmd_streak(
                ctx, AKARI_GAME, *self._slash_choice_args(timeframe),
                *self._slash_queens_weekday_args(weekdays))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='top', description='Show outright winners leaderboard')
    @app_commands.describe(
        timeframe='Time period filter', mode='Scoring mode',
        weekdays='Days: mon,wed, weekday, or weekend',
        ties='Also count shared wins, ordered by the combined total')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES, mode=_MODE_CHOICES)
    async def slash_akari_top(
        self, interaction: discord.Interaction,
        timeframe: Optional[app_commands.Choice[str]] = None,
        mode: Optional[app_commands.Choice[str]] = None,
        weekdays: Optional[str] = None,
        ties: bool = False,
    ):
        await interaction.response.defer()
        try:
            await self._cmd_top(
                _SlashCtx(interaction), AKARI_GAME,
                *self._slash_choice_args(timeframe, mode),
                *self._slash_queens_weekday_args(weekdays),
                *(('+ties',) if ties else ()))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='stats', description='Show personal stats with graphs')
    @app_commands.describe(
        member='Player to check', timeframe='Time period filter',
        weekdays='Days: mon,wed, weekday, or weekend')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES)
    async def slash_akari_stats(
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
            await self._cmd_stats(
                ctx, AKARI_GAME, *self._slash_choice_args(timeframe),
                *self._slash_queens_weekday_args(weekdays))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='results', description='Show an Akari puzzle/date leaderboard')
    @app_commands.describe(
        selector='Puzzle number, #number, or date (defaults to today)',
        weekdays='Days: mon,wed, weekday, or weekend',
        date_filter='Date filter, e.g. d>=01062026 d<08062026',
        beta='Use the beta testing rating system',
        time_only='Ignore accuracy and rate only by completion time')
    async def slash_akari_results(
        self, interaction: discord.Interaction,
        selector: Optional[str] = None,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
        beta: bool = False,
        time_only: bool = False,
    ):
        await interaction.response.defer()
        args = [selector] if selector else []
        args += self._slash_queens_weekday_args(weekdays)
        args += str(date_filter or '').split()
        args += ['+beta'] if beta else []
        args += ['+time'] if time_only else []
        try:
            await self._cmd_akari_results(_SlashCtx(interaction), args)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='ratings', description='Show Akari rating leaderboard')
    @app_commands.describe(
        weekly='Show ratings from completed weekly contests',
        current='Show only this week\'s in-progress standings',
        weekdays='Days: mon,wed, weekday, or weekend',
        date_filter='Date filter, e.g. d>=01062026 d<08062026',
        beta='Use the beta testing rating system',
        time_only='Ignore accuracy and rate only by completion time')
    async def slash_akari_ratings(self, interaction: discord.Interaction,
                                  weekly: bool = False,
                                  current: bool = False,
                                  weekdays: Optional[str] = None,
                                  date_filter: Optional[str] = None,
                                  beta: bool = False,
                                  time_only: bool = False):
        await interaction.response.defer()
        try:
            if weekly and current:
                raise MinigameCogError(
                    'Choose either `weekly` or `current`.')
            await self._cmd_akari_ratings(
                _SlashCtx(interaction), weekly=weekly, current=current,
                beta=beta,
                time_only=time_only,
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='rating', description="Show a user's Akari rating graph")
    @app_commands.describe(
        member='Player (defaults to you)',
        decay='Include every day (with decay slopes), not only days played',
        weekdays='Days: mon,wed, weekday, or weekend',
        date_filter='Date filter, e.g. d>=01062026 d<08062026',
        recalculate='Recalculate ratings from the filtered result set',
        beta='Use the beta testing rating system',
        time_only='Ignore accuracy and rate only by completion time')
    async def slash_akari_rating(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        decay: bool = False,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
        recalculate: Optional[bool] = False,
        beta: bool = False,
        time_only: bool = False,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_akari_rating(
                _SlashCtx(interaction), [target], include_decay=decay,
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter),
                recalculate=bool(recalculate), beta=beta,
                time_only=time_only)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='performance', description="Show a user's Akari performance graph")
    @app_commands.describe(
        member='Player (defaults to you)',
        weekdays='Days: mon,wed, weekday, or weekend',
        date_filter='Date filter, e.g. d>=01062026 d<08062026',
        beta='Use the beta testing rating system',
        time_only='Ignore accuracy and rate only by completion time')
    async def slash_akari_performance(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
        beta: bool = False,
        time_only: bool = False,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_akari_performance(
                _SlashCtx(interaction), [target],
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter),
                beta=beta, time_only=time_only)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='history', description="Show a user's Akari rating delta log")
    @app_commands.describe(
        member='Player (defaults to you)',
        weekdays='Days: mon,wed, weekday, or weekend',
        date_filter='Date filter, e.g. d>=01062026 d<08062026',
        beta='Use the beta testing rating system',
        time_only='Ignore accuracy and rate only by completion time')
    async def slash_akari_history(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
        beta: bool = False,
        time_only: bool = False,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_akari_history(
                _SlashCtx(interaction), target,
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter),
                beta=beta, time_only=time_only)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(
        name='skips',
        description="Show a user's skipped Akari days")
    @app_commands.describe(member='Player (defaults to you)')
    async def slash_akari_skips(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_akari_skips(_SlashCtx(interaction), target)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='here', description='Set the Daily Akari channel')
    async def slash_akari_here(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_akari_mod(interaction):
            return
        try:
            await self._cmd_here(_SlashCtx(interaction), AKARI_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='clear', description='Clear the Daily Akari channel')
    async def slash_akari_clear(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_akari_mod(interaction):
            return
        try:
            await self._cmd_clear(_SlashCtx(interaction), AKARI_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='remove', description='Remove a user result')
    @app_commands.describe(member='Player', puzzle_id='Puzzle number')
    async def slash_akari_remove(
        self, interaction: discord.Interaction,
        member: discord.Member, puzzle_id: int,
    ):
        await interaction.response.defer()
        if not await self._slash_require_akari_mod(interaction):
            return
        try:
            await self._cmd_remove(
                _SlashCtx(interaction), AKARI_GAME, member, puzzle_id)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='add', description='Manually add a result for a user/puzzle')
    @app_commands.describe(
        member='Player', puzzle_id='Puzzle number',
        result='`perfect` or `N%` (e.g. 92%)',
        time='Time as M:SS or H:MM:SS (e.g. 1:34)')
    async def slash_akari_add(
        self, interaction: discord.Interaction,
        member: discord.Member, puzzle_id: int, result: str, time: str,
    ):
        await interaction.response.defer()
        if not await self._slash_require_akari_mod(interaction):
            return
        try:
            await self._cmd_akari_add(
                _SlashCtx(interaction), member, puzzle_id, result, time)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='delete', description='Remove all Akari results for a date/puzzle')
    @app_commands.describe(selector='Puzzle number, #number, or date')
    async def slash_akari_delete(
        self, interaction: discord.Interaction, selector: str,
    ):
        await interaction.response.defer()
        if not await self._slash_require_akari_mod(interaction):
            return
        try:
            await self._cmd_akari_delete_date(_SlashCtx(interaction), selector)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='clean', description='Remove Akari results for a date range')
    @app_commands.describe(
        start_date='Start date or puzzle number',
        end_date='End date or puzzle number (defaults to start date)')
    async def slash_akari_clean(
        self, interaction: discord.Interaction, start_date: str,
        end_date: Optional[str] = None,
    ):
        await interaction.response.defer()
        if not await self._slash_require_akari_mod(interaction):
            return
        try:
            await self._cmd_akari_clean(
                _SlashCtx(interaction), start_date, end_date)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='reparse', description='Reparse all stored raw messages')
    async def slash_akari_reparse(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_akari_mod(interaction):
            return
        try:
            await self._cmd_reparse(_SlashCtx(interaction), AKARI_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash_import.command(name='start', description='Rebuild imported history')
    @app_commands.describe(channel='Channel to import from')
    async def slash_akari_import_start(
        self, interaction: discord.Interaction,
        channel: Optional[discord.TextChannel] = None,
    ):
        await interaction.response.defer()
        if not await self._slash_require_akari_mod(interaction):
            return
        ctx = _SlashCtx(interaction)
        try:
            original = await interaction.original_response()
            ctx.message = original
            await self._cmd_import_start(ctx, AKARI_GAME, channel)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash_import.command(name='status', description='Show import status')
    async def slash_akari_import_status(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_akari_mod(interaction):
            return
        try:
            await self._cmd_import_status(_SlashCtx(interaction), AKARI_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash_import.command(name='cancel', description='Cancel a running import')
    async def slash_akari_import_cancel(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_akari_mod(interaction):
            return
        try:
            await self._cmd_import_cancel(_SlashCtx(interaction), AKARI_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash_import.command(name='clear', description='Delete imported history')
    async def slash_akari_import_clear(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_akari_mod(interaction):
            return
        try:
            await self._cmd_import_clear(_SlashCtx(interaction), AKARI_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)
