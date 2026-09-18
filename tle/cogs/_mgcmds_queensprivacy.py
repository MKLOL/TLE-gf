"""Queens opt-out and per-result rating prefix commands."""

from tle.cogs._mgcmds_queens import QueensCmdsMixin
from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs._minigame_helpers import (
    CaseInsensitiveMember,
    queens_mod_only,
)


class QueensPrivacyCmdsMixin:
    @QueensCmdsMixin.queens.command(
        name='optout', aliases=['opt-out'],
        brief='Store future Queens results unrated; mods may target a user',
        usage='[@user]')
    async def queens_optout(
            self, ctx, member: CaseInsensitiveMember = None):
        await self._cmd_queens_optout(ctx, QUEENS_GAME, member)

    @QueensCmdsMixin.queens.command(
        name='optin', aliases=['opt-in'],
        brief='Make future Queens results rated')
    async def queens_optin(self, ctx):
        await self._cmd_queens_optin(ctx, QUEENS_GAME)

    @QueensCmdsMixin.queens.command(
        name='unrate',
        brief='(Mod) Hide one Queens result and exclude it from ratings',
        usage='<@user|LinkedIn Name> <date|#>')
    @queens_mod_only()
    async def queens_unrate(self, ctx, *, args: str = None):
        await self._cmd_queens_set_result_rating(
            ctx, QUEENS_GAME, args, is_rated=False)

    @QueensCmdsMixin.queens.command(
        name='rate', brief='(Mod) Restore one Queens result to ratings',
        usage='<@user|LinkedIn Name> <date|#>')
    @queens_mod_only()
    async def queens_rate(self, ctx, *, args: str = None):
        await self._cmd_queens_set_result_rating(
            ctx, QUEENS_GAME, args, is_rated=True)
