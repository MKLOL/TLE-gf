"""Tango slash group (Minigames cog slash mixin; see minigames.py)."""

import logging
from typing import Optional

import discord
from discord import app_commands


from tle.cogs._minigame_tango import TANGO_GAME
from tle.cogs._minigame_helpers import _SlashCtx
from tle.cogs._minigame_queens_cog import _queens_current_puzzle_date
from tle.cogs._minigame_slash_consts import _TIMEFRAME_CHOICES, _MODE_CHOICES

logger = logging.getLogger(__name__)


class TangoSlashMixin:
    tango_slash = app_commands.Group(
        name='tango', description='LinkedIn Tango commands', guild_only=True)
    # Nested group: Discord caps a group at 25 direct children, and this
    # group is at that limit. It also mirrors the ';tango import <sub>'
    # prefix commands.
    tango_slash_import = app_commands.Group(
        name='import', description='Manage imported Tango history',
        parent=tango_slash)

    @tango_slash.command(name='show', description='Show LinkedIn Tango settings')
    async def slash_tango_show(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            await self._cmd_queens_show(_SlashCtx(interaction), TANGO_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='here', description='Set the LinkedIn Tango channel')
    async def slash_tango_here(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            await self._cmd_here(_SlashCtx(interaction), TANGO_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='clear', description='Clear the LinkedIn Tango channel')
    async def slash_tango_channel_clear(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            await self._cmd_clear(_SlashCtx(interaction), TANGO_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='register', description='Link a Discord user to a LinkedIn Tango name')
    @app_commands.describe(
        linkedin_name='LinkedIn display name',
        member='Discord member to register (mods only when not yourself)',
        anonymous='Hide the LinkedIn name in public bot output')
    async def slash_tango_register(
        self, interaction: discord.Interaction,
        linkedin_name: str,
        member: Optional[discord.Member] = None,
        anonymous: bool = False,
    ):
        await interaction.response.defer()
        ctx = _SlashCtx(interaction)
        try:
            target = self._resolve_queens_registrar_target(ctx, TANGO_GAME, member)
            await self._cmd_queens_register(
                ctx, TANGO_GAME, target, linkedin_name, anonymous=anonymous)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='set', description='Overwrite a member Tango name')
    @app_commands.describe(
        member='Discord member to set',
        linkedin_name='LinkedIn display name',
        anonymous='Hide the LinkedIn name in public bot output')
    async def slash_tango_set(
        self, interaction: discord.Interaction,
        member: discord.Member,
        linkedin_name: str,
        anonymous: bool = False,
    ):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            await self._cmd_queens_set(
                _SlashCtx(interaction), TANGO_GAME, member, linkedin_name,
                anonymous=anonymous)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='unregister', description='Remove a LinkedIn Tango link')
    @app_commands.describe(member='Discord member to unregister (mods only when not yourself)')
    async def slash_tango_unregister(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
    ):
        await interaction.response.defer()
        ctx = _SlashCtx(interaction)
        try:
            await self._cmd_queens_unregister(ctx, TANGO_GAME, member)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='vs', description='Compare two to five players')
    @app_commands.describe(
        member1='First player', member2='Second player',
        member3='Optional third player', member4='Optional fourth player',
        member5='Optional fifth player',
        timeframe='Time period filter',
        weekdays='Tango days: mon,wed, weekday, or weekend')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES)
    async def slash_tango_vs(
        self, interaction: discord.Interaction,
        member1: discord.Member, member2: discord.Member,
        member3: Optional[discord.Member] = None,
        member4: Optional[discord.Member] = None,
        member5: Optional[discord.Member] = None,
        timeframe: Optional[app_commands.Choice[str]] = None,
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
                _SlashCtx(interaction), TANGO_GAME, members,
                *self._slash_choice_args(timeframe),
                *self._slash_queens_weekday_args(weekdays))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(
        name='top', description='Show outright fastest-result winners')
    @app_commands.describe(
        timeframe='Time period filter', mode='Scoring mode',
        weekdays='Tango days: mon,wed, weekday, or weekend',
        ties='Also count shared wins, ordered by the combined total')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES, mode=_MODE_CHOICES)
    async def slash_tango_top(
        self, interaction: discord.Interaction,
        timeframe: Optional[app_commands.Choice[str]] = None,
        mode: Optional[app_commands.Choice[str]] = None,
        weekdays: Optional[str] = None,
        ties: bool = False,
    ):
        await interaction.response.defer()
        try:
            await self._cmd_top(
                _SlashCtx(interaction), TANGO_GAME,
                *self._slash_choice_args(timeframe, mode),
                *self._slash_queens_weekday_args(weekdays),
                *(('+ties',) if ties else ()))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='streak', description='Show current clean streak')
    @app_commands.describe(
        member='Player to check', timeframe='Time period filter',
        weekdays='Tango days: mon,wed, weekday, or weekend')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES)
    async def slash_tango_streak(
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
                ctx, TANGO_GAME, *self._slash_choice_args(timeframe),
                *self._slash_queens_weekday_args(weekdays))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='stats', description='Show personal Tango stats')
    @app_commands.describe(
        member='Player to check', timeframe='Time period filter',
        weekdays='Tango days: mon,wed, weekday, or weekend')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES)
    async def slash_tango_stats(
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
                ctx, TANGO_GAME, *self._slash_choice_args(timeframe),
                *self._slash_queens_weekday_args(weekdays))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='results', description='Show a Tango date leaderboard')
    @app_commands.describe(
        date='Date or puzzle number (defaults to today)',
        weekdays='Tango days: mon,wed, weekday, or weekend',
        date_filter='Rating date filter, e.g. d>=01062026 d<08062026',
        beta='Use the beta testing rating system',
        unrated='Also show permanently unrated results')
    async def slash_tango_results(
        self, interaction: discord.Interaction,
        date: Optional[str] = None,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
        beta: bool = False,
        unrated: bool = False,
    ):
        await interaction.response.defer()
        try:
            await self._cmd_queens_stats_date(
                _SlashCtx(interaction), TANGO_GAME,
                date or _queens_current_puzzle_date().isoformat(),
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter),
                improved=beta, show_unrated=unrated)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='ratings', description='Show Tango rating leaderboard')
    @app_commands.describe(
        weekly='Preview weekly-contest ratings and this week\'s scores',
        weekdays='Tango days: mon,wed, weekday, or weekend',
        date_filter='Rating date filter, e.g. d>=01062026 d<08062026',
        beta='Use the beta testing rating system')
    async def slash_tango_ratings(
        self, interaction: discord.Interaction,
        weekly: bool = False,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
        beta: bool = False,
    ):
        await interaction.response.defer()
        try:
            await self._cmd_queens_ratings(
                _SlashCtx(interaction), TANGO_GAME,
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter),
                improved=beta, weekly=weekly)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='rating', description="Show a user's Tango rating graph")
    @app_commands.describe(
        member='Player (defaults to you)',
        weekdays='Tango days: mon,wed, weekday, or weekend',
        date_filter='Rating date filter, e.g. d>=01062026 d<08062026',
        recalculate='Recalculate ratings from the filtered result set',
        decay='Show inactivity days on the graph',
        beta='Use the beta testing rating system')
    async def slash_tango_rating(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
        recalculate: Optional[bool] = False,
        decay: bool = False,
        beta: bool = False,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_queens_rating(
                _SlashCtx(interaction), TANGO_GAME, [target],
                include_decay=bool(decay),
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter),
                recalculate=bool(recalculate), improved=beta)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='performance', description="Show a user's Tango performance graph")
    @app_commands.describe(
        member='Player (defaults to you)',
        weekdays='Tango days: mon,wed, weekday, or weekend',
        date_filter='Rating date filter, e.g. d>=01062026 d<08062026',
        beta='Use the beta testing rating system')
    async def slash_tango_performance(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
        beta: bool = False,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_queens_performance(
                _SlashCtx(interaction), TANGO_GAME, [target],
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter),
                improved=beta)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='history', description="Show a user's Tango rating delta log")
    @app_commands.describe(
        member='Player (defaults to you)',
        weekdays='Tango days: mon,wed, weekday, or weekend',
        date_filter='Rating date filter, e.g. d>=01062026 d<08062026',
        beta='Use the beta testing rating system')
    async def slash_tango_history(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        weekdays: Optional[str] = None,
        date_filter: Optional[str] = None,
        beta: bool = False,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_queens_history(
                _SlashCtx(interaction), TANGO_GAME, target,
                weekdays=self._slash_queens_weekdays(weekdays),
                date_bounds=self._slash_queens_date_bounds(date_filter),
                improved=beta)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='add', description='Manually add a Tango result')
    @app_commands.describe(
        member='Player', date='Date or puzzle number',
        time='Time as M:SS or H:MM:SS',
        status='Status text, defaults to no hints and no mistakes')
    async def slash_tango_add(
        self, interaction: discord.Interaction,
        member: discord.Member, date: str, time: str,
        status: Optional[str] = None,
    ):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            status = status or 'No hints & no mistakes'
            await self._cmd_queens_add(
                _SlashCtx(interaction), TANGO_GAME,
                f'{member.id} {date} {time} {status}')
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='remove', description='Remove a Tango result')
    @app_commands.describe(member='Player', date='Date or puzzle number')
    async def slash_tango_remove(
        self, interaction: discord.Interaction,
        member: discord.Member, date: str,
    ):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            await self._cmd_queens_remove(
                _SlashCtx(interaction), TANGO_GAME, f'{member.id} {date}')
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='delete', description='Remove all Tango results for a date')
    @app_commands.describe(date='Date or puzzle number')
    async def slash_tango_delete(
        self, interaction: discord.Interaction, date: str,
    ):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            await self._cmd_queens_clear(_SlashCtx(interaction), TANGO_GAME, date)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='clean', description='Remove Tango results for a date range')
    @app_commands.describe(
        start_date='Start date or puzzle number',
        end_date='End date or puzzle number (defaults to start date)')
    async def slash_tango_clean(
        self, interaction: discord.Interaction, start_date: str,
        end_date: Optional[str] = None,
    ):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            await self._cmd_queens_clean(
                _SlashCtx(interaction), TANGO_GAME, start_date, end_date)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='reparse', description='Reparse all stored raw Tango messages')
    async def slash_tango_reparse(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            await self._cmd_reparse(_SlashCtx(interaction), TANGO_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash_import.command(name='start', description='Rebuild imported Tango history')
    @app_commands.describe(channel='Channel to import from')
    async def slash_tango_import_start(
        self, interaction: discord.Interaction,
        channel: Optional[discord.TextChannel] = None,
    ):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        ctx = _SlashCtx(interaction)
        try:
            original = await interaction.original_response()
            ctx.message = original
            await self._cmd_import_start(ctx, TANGO_GAME, channel)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash_import.command(name='status', description='Show Tango import status')
    async def slash_tango_import_status(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            await self._cmd_import_status(_SlashCtx(interaction), TANGO_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash_import.command(name='cancel', description='Cancel a running Tango import')
    async def slash_tango_import_cancel(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            await self._cmd_import_cancel(_SlashCtx(interaction), TANGO_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash_import.command(name='clear', description='Delete imported Tango history')
    async def slash_tango_import_clear(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            await self._cmd_import_clear(_SlashCtx(interaction), TANGO_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash.command(name='ratings-recompute', description='Rebuild the Tango rating snapshot')
    async def slash_tango_ratings_recompute(
        self, interaction: discord.Interaction,
    ):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            await self._cmd_queens_ratings_recompute(_SlashCtx(interaction), TANGO_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)
