"""Shared slash-command helpers (Minigames cog slash mixin; see minigames.py).

Error plumbing, mod-access checks, and option→filter-arg adapters used by both
the Akari and Queens slash groups.  Moved out of ``_mgcmds_akarislash`` so the
slash command files stay under the 500-line limit.
"""

import logging

from tle import constants
from tle.util import discord_common

from tle.cogs._minigame_helpers import MinigameCogError
from tle.cogs._minigame_queens_filters import (
    _split_queens_weekday_filter, _split_queens_rating_date_filter)

logger = logging.getLogger(__name__)


class SlashHelpersMixin:
    async def _slash_handle_error(self, interaction, exc):
        if isinstance(exc, MinigameCogError):
            await self._slash_send_error(interaction, exc)
        else:
            logger.exception('Unhandled error in slash command')
            await self._slash_send_error(
                interaction, 'An unexpected error occurred.')

    def _has_mod_role(self, interaction):
        allowed = {constants.TLE_ADMIN, constants.TLE_MODERATOR}
        return any(r.name in allowed for r in interaction.user.roles)

    async def _slash_require_queens_mod(self, interaction):
        if self._has_queens_mod_access(interaction.guild.id, interaction.user):
            return True
        await self._slash_send_error(interaction, self._mod_role_error_message())
        return False

    async def _slash_require_akari_mod(self, interaction):
        if self._has_akari_mod_access(interaction.guild.id, interaction.user):
            return True
        await self._slash_send_error(
            interaction, self._akari_mod_role_error_message())
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
            SlashHelpersMixin._slash_queens_weekday_args(weekdays))
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
