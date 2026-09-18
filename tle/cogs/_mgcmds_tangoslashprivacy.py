"""Tango privacy slash commands split from the main slash mixin."""

from typing import Optional

import discord
from discord import app_commands

from tle.cogs._minigame_tango import TANGO_GAME
from tle.cogs._mgcmds_tangoslash import TangoSlashMixin
from tle.cogs._minigame_helpers import _SlashCtx


class TangoPrivacySlashMixin:
    tango_slash_result = app_commands.Group(
        name='result',
        description='Manage whether a Tango result affects ratings',
        parent=TangoSlashMixin.tango_slash)

    @TangoSlashMixin.tango_slash.command(
        name='opt-out',
        description='Store future Tango results unrated; mods may target')
    @app_commands.describe(
        member='Member to opt out (mods only when not yourself)')
    async def slash_tango_optout(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
    ):
        await interaction.response.defer()
        try:
            await self._cmd_queens_optout(
                _SlashCtx(interaction), TANGO_GAME, member)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @TangoSlashMixin.tango_slash.command(
        name='opt-in',
        description='Make future Tango results rated')
    async def slash_tango_optin(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            await self._cmd_queens_optin(_SlashCtx(interaction), TANGO_GAME)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash_result.command(
        name='unrate',
        description='Hide one member result and exclude it from ratings')
    @app_commands.describe(
        member='Member whose result should be unrated',
        date='Tango date or puzzle number')
    async def slash_tango_result_unrate(
        self, interaction: discord.Interaction,
        member: discord.Member,
        date: str,
    ):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            await self._cmd_queens_set_result_rating(
                _SlashCtx(interaction), TANGO_GAME, date,
                is_rated=False, member=member)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @tango_slash_result.command(
        name='rate',
        description='Restore one member result to Tango ratings')
    @app_commands.describe(
        member='Member whose result should be rated',
        date='Tango date or puzzle number')
    async def slash_tango_result_rate(
        self, interaction: discord.Interaction,
        member: discord.Member,
        date: str,
    ):
        await interaction.response.defer()
        if not await self._slash_require_tango_mod(interaction):
            return
        try:
            await self._cmd_queens_set_result_rating(
                _SlashCtx(interaction), TANGO_GAME, date,
                is_rated=True, member=member)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)
