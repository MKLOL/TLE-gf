"""Tango opt-out and per-result rating prefix commands."""

from tle.cogs._mgcmds_tango import TangoCmdsMixin
from tle.cogs._minigame_tango import TANGO_GAME
from tle.cogs._minigame_helpers import (
    CaseInsensitiveMember,
    tango_mod_only,
)


class TangoPrivacyCmdsMixin:
    @TangoCmdsMixin.tango.command(
        name='optout', aliases=['opt-out'],
        brief='Store future Tango results unrated; mods may target a user',
        usage='[@user]')
    async def tango_optout(
            self, ctx, member: CaseInsensitiveMember = None):
        await self._cmd_queens_optout(ctx, TANGO_GAME, member)

    @TangoCmdsMixin.tango.command(
        name='optin', aliases=['opt-in'],
        brief='Make future Tango results rated')
    async def tango_optin(self, ctx):
        await self._cmd_queens_optin(ctx, TANGO_GAME)

    @TangoCmdsMixin.tango.command(
        name='unrate',
        brief='(Mod) Hide one Tango result and exclude it from ratings',
        usage='<@user|LinkedIn Name> <date|#>')
    @tango_mod_only()
    async def tango_unrate(self, ctx, *, args: str = None):
        await self._cmd_queens_set_result_rating(
            ctx, TANGO_GAME, args, is_rated=False)

    @TangoCmdsMixin.tango.command(
        name='rate', brief='(Mod) Restore one Tango result to ratings',
        usage='<@user|LinkedIn Name> <date|#>')
    @tango_mod_only()
    async def tango_rate(self, ctx, *, args: str = None):
        await self._cmd_queens_set_result_rating(
            ctx, TANGO_GAME, args, is_rated=True)
