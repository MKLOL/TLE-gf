"""Queens privacy slash commands split from the main slash mixin."""

from typing import Optional

import discord
from discord import app_commands

from tle.cogs._mgcmds_queensslash import QueensSlashMixin
from tle.cogs._minigame_helpers import _SlashCtx


class QueensPrivacySlashMixin:
    queens_slash_result = app_commands.Group(
        name='result',
        description='Manage whether a Queens result affects ratings',
        parent=QueensSlashMixin.queens_slash)

    @QueensSlashMixin.queens_slash.command(
        name='opt-out',
        description='Store future Queens results unrated; mods may target')
    @app_commands.describe(
        member='Member to opt out (mods only when not yourself)')
    async def slash_queens_optout(
        self, interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
    ):
        await interaction.response.defer()
        try:
            await self._cmd_queens_optout(
                _SlashCtx(interaction), member)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @QueensSlashMixin.queens_slash.command(
        name='opt-in',
        description='Make future Queens results rated')
    async def slash_queens_optin(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            await self._cmd_queens_optin(_SlashCtx(interaction))
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash_result.command(
        name='unrate',
        description='Hide one member result and exclude it from ratings')
    @app_commands.describe(
        member='Member whose result should be unrated',
        date='Queens date or puzzle number')
    async def slash_queens_result_unrate(
        self, interaction: discord.Interaction,
        member: discord.Member,
        date: str,
    ):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        try:
            await self._cmd_queens_set_result_rating(
                _SlashCtx(interaction), date,
                is_rated=False, member=member)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)

    @queens_slash_result.command(
        name='rate',
        description='Restore one member result to Queens ratings')
    @app_commands.describe(
        member='Member whose result should be rated',
        date='Queens date or puzzle number')
    async def slash_queens_result_rate(
        self, interaction: discord.Interaction,
        member: discord.Member,
        date: str,
    ):
        await interaction.response.defer()
        if not await self._slash_require_queens_mod(interaction):
            return
        try:
            await self._cmd_queens_set_result_rating(
                _SlashCtx(interaction), date,
                is_rated=True, member=member)
        except Exception as _slash_exc:
            await self._slash_handle_error(interaction, _slash_exc)
