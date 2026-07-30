"""Akari replay/filter helpers and per-puzzle results rendering."""

import datetime as dt

from tle.util import codeforces_common as cf_common

from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_helpers import MinigameCogError, _mg
from tle.cogs._minigame_queens_filters import (
    _filter_queens_rating_date_rows,
    _filter_queens_weekday_rows,
    _queens_filter_suffix,
)
from tle.cogs._minigame_stats import (
    plot_akari_stats,
    plot_guessgame_stats,
)
from tle.cogs._minigame_tables import _maybe_parse_puzzle_selector


class ImplAkariDMixin:
    def _akari_user_history(self, guild_id, user_id, *, include_decay=False,
                            excluded_ids=None, included_ids=None,
                            test_decay=False, weekdays=None, date_bounds=None):
        """Replay the guild's results and return one user's per-day history."""
        state, history = self._akari_user_data(
            guild_id, user_id, include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay, weekdays=weekdays, date_bounds=date_bounds)
        del state
        return history

    def _akari_user_data(self, guild_id, user_id, *, include_decay=False,
                         excluded_ids=None, included_ids=None,
                         test_decay=False, weekdays=None, date_bounds=None):
        """Return one user's replayed state and history in a single pass."""
        return self._minigame_user_data(
            guild_id, AKARI_GAME, user_id, include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds,
            extra_compute_kwargs=self._akari_extra_compute_kwargs(test_decay))

    def _akari_filtered_rating_rows(self, guild_id, *, excluded_ids=None,
                                    included_ids=None, test_decay=False,
                                    weekdays=None, date_bounds=None):
        """Return transient leaderboard states for an ad-hoc filtered replay."""
        return self._minigame_rating_rows(
            guild_id, AKARI_GAME,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds,
            extra_compute_kwargs=self._akari_extra_compute_kwargs(test_decay))

    def _akari_puzzle_change_info(self, guild_id, puzzle_number, *,
                                  excluded_ids=None, included_ids=None,
                                  test_decay=False, weekdays=None,
                                  date_bounds=None):
        """Return pre-rating, delta, and performance for one Akari puzzle."""
        return self._minigame_puzzle_change_info(
            guild_id, AKARI_GAME, puzzle_number,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds,
            extra_compute_kwargs=self._akari_extra_compute_kwargs(test_decay))

    async def _extract_akari_filters(self, ctx, args):
        """Extract base Akari flags while preserving the legacy six-tuple."""
        remaining = []
        include_decay = False
        include_inactive = False
        test_decay = False
        excluded_ids = set()
        included_ids = set()
        for arg in args:
            if arg == '+decay':
                include_decay = True
            elif arg == '+test':
                test_decay = True
            elif arg == '+inactive':
                include_inactive = True
            elif arg.startswith('+exclude=') or arg.startswith('+include='):
                positive = arg.startswith('+include=')
                payload = arg[len('+include=' if positive else '+exclude='):]
                target_set = included_ids if positive else excluded_ids
                for raw in payload.split(','):
                    name = raw.strip()
                    if not name:
                        continue
                    member = await self._resolve_member(ctx, name)
                    target_set.add(str(member.id))
            else:
                remaining.append(arg)
        return (remaining, include_decay, excluded_ids, included_ids,
                include_inactive, test_decay)

    @staticmethod
    def _filter_akari_rows(rows, *, excluded_ids=None, included_ids=None):
        """Apply include first, then exclude, to a result-row iterable."""
        if included_ids:
            rows = [r for r in rows if str(r.user_id) in included_ids]
        if excluded_ids:
            rows = [r for r in rows if str(r.user_id) not in excluded_ids]
        return rows

    async def _parse_akari_rating_args(
            self, ctx, args, *, member_required=False):
        """Return members plus the base Akari rating filters."""
        (remaining, include_decay, excluded_ids, included_ids,
         include_inactive, test_decay) = await self._extract_akari_filters(
            ctx, args)
        members = [await self._resolve_member(ctx, token) for token in remaining]
        if not members:
            if member_required:
                raise MinigameCogError('A user is required for this command.')
            members = [ctx.author]
        return (members, include_decay, excluded_ids, included_ids,
                include_inactive, test_decay)

    _STATS_PLOTTERS = {
        'akari': plot_akari_stats,
        'guessgame': plot_guessgame_stats,
    }

    async def _cmd_akari_stats_puzzle(self, ctx, selector_arg, *,
                                      show_all=False, excluded_ids=None,
                                      included_ids=None, test_decay=False,
                                      weekdays=None, date_bounds=None,
                                      sort_key_fn=None, rank_key_fn=None):
        """Render one puzzle's results with pre-puzzle rating annotations."""
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        selector = _maybe_parse_puzzle_selector(selector_arg)
        if selector is None:
            raise MinigameCogError(
                f'Expected a puzzle number or date, got `{selector_arg}`.')
        selector_type, selector_value = selector
        if selector_type == 'puzzle':
            rows = cf_common.user_db.get_minigame_results_for_guild(
                ctx.guild.id, AKARI_GAME.name,
                plo=selector_value, phi=selector_value + 1)
            title = f'{AKARI_GAME.display_name} #{selector_value} Results'
        else:
            day_start = dt.datetime.combine(
                selector_value, dt.time.min).timestamp()
            day_end = day_start + 24 * 60 * 60
            rows = cf_common.user_db.get_minigame_results_for_guild(
                ctx.guild.id, AKARI_GAME.name, dlo=day_start, dhi=day_end)
            title = (
                f'{AKARI_GAME.display_name} '
                f'{selector_value.isoformat()} Results')

        rows = self._filter_akari_rows(
            rows, excluded_ids=excluded_ids, included_ids=included_ids)
        rows = _filter_queens_weekday_rows(rows, weekdays)
        rows = _filter_queens_rating_date_rows(rows, date_bounds)
        if not rows:
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} results found for '
                f'`{selector_arg}`.')

        puzzle_numbers = {int(row.puzzle_number) for row in rows}
        puzzle_info = None
        registrants = None
        if len(puzzle_numbers) == 1:
            puzzle_info = self._akari_puzzle_change_info(
                ctx.guild.id, next(iter(puzzle_numbers)),
                excluded_ids=excluded_ids, included_ids=included_ids,
                test_decay=test_decay, weekdays=weekdays,
                date_bounds=date_bounds)
            registrants = (
                set(puzzle_info)
                if show_all
                else cf_common.user_db.get_akari_registrants(ctx.guild.id)
            )

        if test_decay:
            title += ' [test decay]'
        title += _queens_filter_suffix(
            weekdays=weekdays, date_bounds=date_bounds)
        discord_file = _mg()._get_akari_puzzle_table_image_file(
            ctx.guild, rows, title,
            puzzle_info=puzzle_info, registrants=registrants,
            sort_key_fn=sort_key_fn, rank_key_fn=rank_key_fn)
        await ctx.send(file=discord_file)
