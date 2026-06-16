"""GuessGame text command group + the ;minigames parent group (Minigames cog command mixin; see minigames.py)."""

from typing import Optional

import asyncio
import datetime as dt
import json
import os
import sys
import time

import discord
from discord import app_commands
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator

from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_guessgame import GUESSGAME_GAME
from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs._minigame_helpers import (
    MinigameCogError, ChannelOrThread, CaseInsensitiveMember, queens_mod_only,
    _SlashCtx, _safe_member_name, _safe_user_name, _format_akari_ban_line,
)
from tle.cogs._minigame_tables import _AKARI_HISTORY_PER_PAGE
from tle.cogs._minigame_queens_cog import (
    _QueensAnonymousRegisterView, _QUEENS_ANONYMOUS_FLAGS,
    _QUEENS_HISTORY_PER_PAGE, _QUEENS_UPDATE_THROTTLE_SECONDS,
    _QUEENS_DAILY_UPDATE_TIME, _QUEENS_DAILY_UPDATE_TZ,
    _QUEENS_LINKEDIN_NAME_KEY, _QUEENS_UPDATE_THROTTLE_PREFIX,
    _QUEENS_STATE_PATH_KEY, _QUEENS_DEFAULT_STATE_PATH,
    _QUEENS_PLAYWRIGHT_PLATFORM, _QUEENS_STATE_MAX_BYTES,
    _QUEENS_IMPORTER_KEY, _QUEENS_BACKFILL_MAX_BYTES,
    _is_queens_anonymous_modal_request, _split_queens_anonymous_flag,
    _split_queens_connection_account_text, _queens_public_link_name,
    _parse_queens_update_args,
)
from tle.cogs._minigame_queens_filters import (
    _split_queens_weekday_filter, _split_queens_rating_date_filter,
    _format_queens_weekday_filter, _format_queens_date_filter,
)
from tle.cogs._minigame_slash_consts import (
    _TIMEFRAME_CHOICES, _MODE_CHOICES,
)

logger = __import__('logging').getLogger(__name__)


class GuessGameCmdsMixin:
    @commands.group(name='minigames', aliases=['mg'], brief='Daily puzzle minigame commands',
                    invoke_without_command=True)
    async def minigames(self, ctx):
        """Daily puzzle minigame commands."""
        await ctx.send_help(ctx.command)

    # ── GuessGame commands: ;minigames guessgame … ──────────────────────

    @minigames.group(name='guessgame', aliases=['gg'], brief='GuessThe.Game commands',
                     invoke_without_command=True)
    async def guessgame(self, ctx):
        """GuessThe.Game commands."""
        await ctx.send_help(ctx.command)

    @guessgame.command(name='here', brief='Set the GuessGame channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_here(self, ctx):
        await self._cmd_here(ctx, GUESSGAME_GAME)

    @guessgame.command(name='clear', brief='Clear the GuessGame channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_clear(self, ctx):
        await self._cmd_clear(ctx, GUESSGAME_GAME)

    @guessgame.command(name='show', brief='Show GuessGame settings')
    async def gg_show(self, ctx):
        await self._cmd_show(ctx, GUESSGAME_GAME)

    @guessgame.command(name='vs', brief='Head-to-head comparison',
                       usage='@user1 @user2 [p>=N] [p<N] [filters...]')
    async def gg_vs(self, ctx, member1: CaseInsensitiveMember, member2: CaseInsensitiveMember, *args):
        await self._cmd_vs(ctx, GUESSGAME_GAME, member1, member2, *args)

    @guessgame.command(name='results', aliases=['matchups'], brief='Show per-puzzle side-by-side results',
                       usage='@user1 @user2 [p>=N] [p<N] [filters...]')
    async def gg_results(self, ctx, member1: CaseInsensitiveMember, member2: CaseInsensitiveMember, *args):
        await self._cmd_guessgame_matchups(ctx, member1, member2, *args)

    @guessgame.command(name='streak', brief='Show current win streak',
                       usage='[@user] [filters...]')
    async def gg_streak(self, ctx, *args):
        await self._cmd_streak(ctx, GUESSGAME_GAME, *args)

    @guessgame.command(name='top', brief='Show winners leaderboard',
                       usage='[p>=N] [p<N] [filters...]')
    async def gg_top(self, ctx, *args):
        await self._cmd_top(ctx, GUESSGAME_GAME, *args)

    @guessgame.command(name='stats', brief='Show personal stats with graphs',
                       usage='[@user] [filters...]')
    async def gg_stats(self, ctx, *args):
        await self._cmd_stats(ctx, GUESSGAME_GAME, *args)

    @guessgame.command(name='remove', brief='Remove a user result for a puzzle',
                       usage='@user puzzle_id')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_remove(self, ctx, member: CaseInsensitiveMember, puzzle_id: int):
        await self._cmd_remove(ctx, GUESSGAME_GAME, member, puzzle_id)

    @guessgame.group(name='import', brief='Manage imported history',
                     invoke_without_command=True)
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import(self, ctx):
        await ctx.send_help(ctx.command)

    @gg_import.command(name='start', brief='Rebuild imported history')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import_start(self, ctx, channel: ChannelOrThread = None):
        await self._cmd_import_start(ctx, GUESSGAME_GAME, channel)

    @gg_import.command(name='status', brief='Show import status')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import_status(self, ctx):
        await self._cmd_import_status(ctx, GUESSGAME_GAME)

    @gg_import.command(name='cancel', brief='Cancel a running import')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import_cancel(self, ctx):
        await self._cmd_import_cancel(ctx, GUESSGAME_GAME)

    @gg_import.command(name='clear', brief='Delete imported history')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import_clear(self, ctx):
        await self._cmd_import_clear(ctx, GUESSGAME_GAME)

    @guessgame.command(name='reparse', brief='Reparse all stored raw messages')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_reparse(self, ctx):
        await self._cmd_reparse(ctx, GUESSGAME_GAME)
