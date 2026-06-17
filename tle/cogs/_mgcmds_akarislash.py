"""Akari slash group + shared slash helpers (Minigames cog slash mixin; see minigames.py)."""

import logging
from typing import Optional

import discord
from discord import app_commands

from tle import constants
from tle.util import discord_common

from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_helpers import MinigameCogError, _SlashCtx
from tle.cogs._minigame_queens_filters import (
    _split_queens_weekday_filter, _split_queens_rating_date_filter)
from tle.cogs._minigame_slash_consts import _TIMEFRAME_CHOICES, _MODE_CHOICES

logger = logging.getLogger(__name__)


class AkariSlashMixin:
    async def _slash_handle_error(self, interaction, exc):
        if isinstance(exc, MinigameCogError):
            await self._slash_send_error(interaction, exc)
        else:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(
                interaction, 'An unexpected error occurred.')

    akari_slash = app_commands.Group(
        name='akari', description='Daily Akari commands', guild_only=True)

    def _has_mod_role(self, interaction):
        allowed = {constants.TLE_ADMIN, constants.TLE_MODERATOR}
        return any(r.name in allowed for r in interaction.user.roles)

    async def _slash_require_queens_mod(self, interaction):
        if self._has_queens_mod_access(interaction.guild.id, interaction.user):
            return True
        await self._slash_send_error(interaction, self._mod_role_error_message())
        return False

    @staticmethod
    def _slash_choice_args(*choices):
        return [choice.value for choice in choices if choice]

    @staticmethod
    def _slash_queens_weekday_args(weekdays):
        if not weekdays:
            return []
        text = str(weekdays).strip()
        if not text:
            return []
        if text.startswith('+'):
            return [text]
        return [f'+dow={text}']

    @staticmethod
    def _slash_queens_weekdays(weekdays):
        _remaining, parsed = _split_queens_weekday_filter(
            AkariSlashMixin._slash_queens_weekday_args(weekdays))
        return parsed

    @staticmethod
    def _slash_queens_date_bounds(date_filter):
        if not date_filter:
            return None
        args = str(date_filter).split()
        remaining, date_bounds = _split_queens_rating_date_filter(args)
        if remaining:
            raise MinigameCogError(
                'Use date filters like `d>=01062026 d<08062026`.')
        return date_bounds

    async def _slash_send_error(self, interaction, error):
        try:
            await interaction.followup.send(
                embed=discord_common.embed_alert(str(error)))
        except Exception:
            logger.warning('Failed to send slash error response', exc_info=True)

    @akari_slash.command(name='show', description='Show Daily Akari settings')
    async def slash_akari_show(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            await self._cmd_show(_SlashCtx(interaction), AKARI_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='vs', description='Head-to-head comparison')
    @app_commands.describe(
        member1='First player', member2='Second player',
        timeframe='Time period filter', mode='Scoring mode')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES, mode=_MODE_CHOICES)
    async def slash_akari_vs(
        self, interaction: discord.Interaction,
        member1: discord.Member, member2: discord.Member,
        timeframe: Optional[app_commands.Choice[str]] = None,
        mode: Optional[app_commands.Choice[str]] = None,
    ):
        await interaction.response.defer()
        args = []
        if timeframe:
            args.append(timeframe.value)
        if mode:
            args.append(mode.value)
        try:
            await self._cmd_vs(
                _SlashCtx(interaction), AKARI_GAME, member1, member2, *args)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='streak', description='Show current perfect streak')
    @app_commands.describe(member='Player to check', timeframe='Time period filter')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES)
    async def slash_akari_streak(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        timeframe: Optional[app_commands.Choice[str]] = None,
    ):
        await interaction.response.defer()
        ctx = _SlashCtx(interaction)
        if member:
            ctx.author = member
        args = []
        if timeframe:
            args.append(timeframe.value)
        try:
            await self._cmd_streak(ctx, AKARI_GAME, *args)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='top', description='Show winners leaderboard')
    @app_commands.describe(timeframe='Time period filter', mode='Scoring mode')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES, mode=_MODE_CHOICES)
    async def slash_akari_top(
        self, interaction: discord.Interaction,
        timeframe: Optional[app_commands.Choice[str]] = None,
        mode: Optional[app_commands.Choice[str]] = None,
    ):
        await interaction.response.defer()
        args = []
        if timeframe:
            args.append(timeframe.value)
        if mode:
            args.append(mode.value)
        try:
            await self._cmd_top(_SlashCtx(interaction), AKARI_GAME, *args)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='stats', description='Show personal stats with graphs')
    @app_commands.describe(member='Player to check', timeframe='Time period filter')
    @app_commands.choices(timeframe=_TIMEFRAME_CHOICES)
    async def slash_akari_stats(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        timeframe: Optional[app_commands.Choice[str]] = None,
    ):
        await interaction.response.defer()
        ctx = _SlashCtx(interaction)
        if member:
            ctx.author = member
        args = []
        if timeframe:
            args.append(timeframe.value)
        try:
            await self._cmd_stats(ctx, AKARI_GAME, *args)
        except MinigameCogError as e:
            await self._slash_send_error(interaction, e)

    @akari_slash.command(name='ratings', description='Show Akari rating leaderboard')
    async def slash_akari_ratings(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            await self._cmd_akari_ratings(_SlashCtx(interaction))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='rating', description="Show a user's Akari rating graph")
    @app_commands.describe(
        member='Player (defaults to you)',
        decay='Include every day (with decay slopes), not only days played')
    async def slash_akari_rating(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        decay: bool = False,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_akari_rating(
                _SlashCtx(interaction), [target], include_decay=decay)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='performance', description="Show a user's Akari performance graph")
    @app_commands.describe(member='Player (defaults to you)')
    async def slash_akari_performance(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_akari_performance(_SlashCtx(interaction), [target])
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='history', description="Show a user's Akari rating delta log")
    @app_commands.describe(member='Player (defaults to you)')
    async def slash_akari_history(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
    ):
        await interaction.response.defer()
        target = member or interaction.user
        try:
            await self._cmd_akari_history(_SlashCtx(interaction), target)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='here', description='Set the Daily Akari channel')
    async def slash_akari_here(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_here(_SlashCtx(interaction), AKARI_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='clear', description='Clear the Daily Akari channel')
    async def slash_akari_clear(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
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
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
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
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_akari_add(
                _SlashCtx(interaction), member, puzzle_id, result, time)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='reparse', description='Reparse all stored raw messages')
    async def slash_akari_reparse(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_reparse(_SlashCtx(interaction), AKARI_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='import-start', description='Rebuild imported history')
    @app_commands.describe(channel='Channel to import from')
    async def slash_akari_import_start(
        self, interaction: discord.Interaction,
        channel: Optional[discord.TextChannel] = None,
    ):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        ctx = _SlashCtx(interaction)
        try:
            original = await interaction.original_response()
            ctx.message = original
            await self._cmd_import_start(ctx, AKARI_GAME, channel)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='import-status', description='Show import status')
    async def slash_akari_import_status(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_import_status(_SlashCtx(interaction), AKARI_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='import-cancel', description='Cancel a running import')
    async def slash_akari_import_cancel(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_import_cancel(_SlashCtx(interaction), AKARI_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @akari_slash.command(name='import-clear', description='Delete imported history')
    async def slash_akari_import_clear(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if not self._has_mod_role(interaction):
            return await self._slash_send_error(
                interaction,
                f'You need the `{constants.TLE_ADMIN}` or '
                f'`{constants.TLE_MODERATOR}` role.')
        try:
            await self._cmd_import_clear(_SlashCtx(interaction), AKARI_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)
