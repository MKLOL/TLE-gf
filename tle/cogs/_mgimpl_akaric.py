"""Akari admins/delete/clean/results commands and extended filter parsing.
(Minigames cog impl mixin; see minigames.py).

These are the Akari counterparts of the Queens delegated-admin tier, bulk
date deletion, per-date results view, and the ``+dow=`` / ``d>=`` / ``d<`` /
``+recalculate`` filters.  The filter split/apply helpers themselves live in
``_minigame_queens_filters`` — they are pure functions over result rows and
are shared verbatim by both games.
"""

import datetime as dt
import logging

from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._minigame_akari import AKARI_GAME, puzzle_date_for
from tle.cogs._minigame_helpers import (
    MinigameCogError, _mg,
)
from tle.cogs._minigame_queens_filters import (
    _split_queens_weekday_filter, _split_queens_rating_date_filter,
    _split_queens_recalculate_filter,
)
from tle.cogs._minigame_tables import _maybe_parse_puzzle_selector

logger = logging.getLogger(__name__)


class ImplAkariCMixin:
    # ── Extended filter parsing (weekdays / date bounds / +recalculate) ──

    async def _extract_akari_extended_filters(self, ctx, args, *,
                                              allow_recalculate=False):
        """The base Akari filters plus the Queens-style extras.

        Returns ``(remaining, include_decay, excluded_ids, included_ids,
        include_inactive, test_decay, weekdays, date_bounds, recalculate)``.
        ``_extract_akari_filters`` keeps its original six-tuple shape for
        existing callers; this wrapper is for the commands that also accept
        ``+dow=…``, ``d>=…`` / ``d<…`` and (optionally) ``+recalculate``.
        """
        args, recalculate = _split_queens_recalculate_filter(args)
        if recalculate and not allow_recalculate:
            raise MinigameCogError(
                '`+recalculate` is only supported by `;akari rating`.')
        args, weekdays = _split_queens_weekday_filter(args)
        args, date_bounds = _split_queens_rating_date_filter(args)
        (remaining, include_decay, excluded_ids, included_ids,
         include_inactive, test_decay) = await self._extract_akari_filters(
            ctx, args)
        return (remaining, include_decay, excluded_ids, included_ids,
                include_inactive, test_decay, weekdays, date_bounds,
                recalculate)

    async def _parse_akari_rating_filter_args(self, ctx, args, *,
                                              member_required=False,
                                              allow_recalculate=False):
        """Members + extended filters, for the rating-family commands.

        Extended-filter sibling of ``_parse_akari_rating_args`` (which keeps
        its original six-tuple shape for the test suite).
        """
        (remaining, include_decay, excluded_ids, included_ids,
         include_inactive, test_decay, weekdays, date_bounds,
         recalculate) = await self._extract_akari_extended_filters(
            ctx, args, allow_recalculate=allow_recalculate)
        members = [await self._resolve_member(ctx, token) for token in remaining]
        if not members:
            if member_required:
                raise MinigameCogError('A user is required for this command.')
            members = [ctx.author]
        return (members, include_decay, excluded_ids, included_ids,
                include_inactive, test_decay, weekdays, date_bounds,
                recalculate)

    # ── Akari command admins (delegated-admin tier) ─────────────────────

    async def _cmd_akari_admins(self, ctx):
        await self._cmd_minigame_admins(
            ctx, AKARI_GAME.display_name, self._akari_admin_ids)

    async def _cmd_akari_admins_add(self, ctx, member):
        await self._cmd_minigame_admins_add(
            ctx, member, AKARI_GAME.display_name,
            self._akari_admin_ids, self._set_akari_admin_ids)

    async def _cmd_akari_admins_remove(self, ctx, member):
        await self._cmd_minigame_admins_remove(
            ctx, member, AKARI_GAME.display_name,
            self._akari_admin_ids, self._set_akari_admin_ids)

    # ── Bulk deletion (per date / date range) ───────────────────────────

    @staticmethod
    def _parse_akari_date_or_number(value):
        """Resolve a date (ISO or ``ddmmyyyy``) / ``N`` / ``#N`` selector to a date."""
        selector = _maybe_parse_puzzle_selector(str(value).strip())
        if selector is None:
            raise MinigameCogError(
                f'Could not parse `{value}` — use a puzzle number, `#number`, '
                'or a date like `2026-06-01` / `ddmmyyyy`.')
        selector_type, selector_value = selector
        if selector_type == 'day':
            return selector_value
        return puzzle_date_for(selector_value)

    async def _cmd_akari_delete_date(self, ctx, selector_arg):
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if selector_arg is None:
            raise MinigameCogError('Usage: `;akari delete <date|#number>`.')
        selector = _maybe_parse_puzzle_selector(str(selector_arg).strip())
        if selector is None:
            raise MinigameCogError(
                f'Could not parse `{selector_arg}` — use a puzzle number, '
                '`#number`, or a date like `2026-06-01` / `ddmmyyyy`.')
        selector_type, selector_value = selector
        if selector_type == 'puzzle':
            puzzle_number = selector_value
            puzzle_date = puzzle_date_for(puzzle_number)
        else:
            puzzle_date = selector_value
            puzzle_number = _mg().expected_puzzle_number(puzzle_date)
        deleted = cf_common.user_db.delete_minigame_results_for_puzzle(
            ctx.guild.id, AKARI_GAME.name, puzzle_number)
        if not deleted:
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} results found for '
                f'#{puzzle_number} {puzzle_date.isoformat()}.')
        self._recompute_akari_ratings(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {deleted} {AKARI_GAME.display_name} result(s) for '
            f'#{puzzle_number} {puzzle_date.isoformat()}.'))

    async def _cmd_akari_clean(self, ctx, start_date, end_date=None):
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if start_date is None:
            raise MinigameCogError(
                'Usage: `;akari clean START_DATE [END_DATE]`.')
        parsed_start = self._parse_akari_date_or_number(start_date)
        parsed_end = (
            self._parse_akari_date_or_number(end_date)
            if end_date is not None
            else parsed_start
        )
        if parsed_end < parsed_start:
            raise MinigameCogError(
                'Akari clean end date cannot be before start date.')

        days = (parsed_end - parsed_start).days + 1
        end_exclusive = parsed_end + dt.timedelta(days=1)
        deleted = cf_common.user_db.delete_minigame_results_for_date_range(
            ctx.guild.id, AKARI_GAME.name,
            parsed_start.isoformat(), end_exclusive.isoformat())
        if not deleted:
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} results found from '
                f'{parsed_start.isoformat()} to {parsed_end.isoformat()}.')
        self._recompute_akari_ratings(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {deleted} {AKARI_GAME.display_name} result(s) from '
            f'{parsed_start.isoformat()} to {parsed_end.isoformat()} '
            f'({days} day(s)).'))

    # ── Per-date results view (``;akari results``) ──────────────────────

    async def _cmd_akari_results(self, ctx, args, *, show_all=False):
        """Render the results table for one puzzle/date, defaulting to today.

        Thin front-end over ``_cmd_akari_stats_puzzle`` — the Akari analogue
        of ``;queens results``.
        """
        (remaining, _include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay, weekdays, date_bounds,
         _recalculate) = await self._extract_akari_extended_filters(ctx, args)
        if len(remaining) > 1:
            raise MinigameCogError(
                'Usage: `;akari results [date|#number] [+test] [+exclude=…] '
                '[+include=…] [+dow=mon,wed|weekday|weekend] '
                '[d>=date] [d<date]`.')
        # '#N', not a bare number: a 4-digit bare number parses as a year
        # (see _maybe_parse_puzzle_selector), which would break the no-arg
        # default once puzzle numbers reach 1000.
        selector = (
            remaining[0] if remaining
            else f'#{_mg().expected_puzzle_number(dt.date.today())}')
        await self._cmd_akari_stats_puzzle(
            ctx, selector, show_all=show_all,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay, weekdays=weekdays, date_bounds=date_bounds)
