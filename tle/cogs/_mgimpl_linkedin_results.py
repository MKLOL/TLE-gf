"""Shared weekly-result controls and Queens/Tango result commands."""

from tle.util import codeforces_common as cf_common
from tle.util.akari_weekly import annotate_weekly_standings, parse_weekly_results_args
from tle.cogs._minigame_helpers import MinigameCogError
from tle.cogs._minigame_queens_cog import _queens_current_puzzle_date
from tle.cogs._minigame_queens_filters import _split_queens_improved_filter


class ImplLinkedInResultsMixin:
    @staticmethod
    def _reject_weekly_rating_results_args(args, game, *, show_all=False):
        if '+current' in args or any(arg.startswith('+days=') for arg in args):
            debug = ' debug' if show_all else ''
            raise MinigameCogError(
                'For current weekly results, use '
                f'`;{game.name} results{debug} +weekly [+days=N]`.')

    async def _cmd_queens_results(
            self, ctx, game, args, *, show_all=False, default_date=None):
        show_unrated = any(
            str(arg).strip().casefold() == '+unrated' for arg in args)
        args = tuple(arg for arg in args
                     if str(arg).strip().casefold() != '+unrated')
        args, improved = _split_queens_improved_filter(args)
        try:
            args, weekly, first_days = parse_weekly_results_args(args)
        except ValueError as exc:
            raise MinigameCogError(str(exc)) from exc
        (remaining, _include_decay, excluded_ids, included_ids, weekdays,
         date_bounds) = await self._extract_queens_rating_filters(ctx, game, args)
        if weekly:
            if remaining:
                raise MinigameCogError(
                    '`+weekly` shows the current week and cannot be combined '
                    'with a puzzle/date selector.')
            if improved or show_unrated:
                raise MinigameCogError(
                    '`+weekly` cannot be combined with `+beta` or `+unrated`.')
            return await self._cmd_queens_current_week_results(
                ctx, game, excluded_ids=excluded_ids, included_ids=included_ids,
                weekdays=weekdays, date_bounds=date_bounds,
                first_days=first_days, show_all=show_all)
        if len(remaining) > 1:
            debug = ' debug' if show_all else ''
            raise MinigameCogError(
                f'Usage: `;{game.name} results{debug} '
                '[date|number | +weekly [+days=N]] [+unrated] [+beta] '
                '[+exclude=…] [+include=…] [+dow=…] [d>=date] [d<date]`.')
        date_arg = (remaining[0] if remaining
                    else (default_date or _queens_current_puzzle_date()).isoformat())
        kwargs = dict(
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds, improved=improved)
        if show_all:
            kwargs['show_all'] = True
        else:
            kwargs['show_unrated'] = show_unrated
        await self._cmd_queens_stats_date(ctx, game, date_arg, **kwargs)

    async def _cmd_queens_current_week_results(
            self, ctx, game, *, excluded_ids=None, included_ids=None,
            weekdays=None, date_bounds=None, show_all=False, first_days=None):
        self._require_enabled(ctx.guild.id, game)
        ratings, standings = self._queens_weekly_preview(
            ctx.guild.id, game, excluded_ids=excluded_ids,
            included_ids=included_ids, weekdays=weekdays,
            date_bounds=date_bounds, first_days=first_days)
        standings = annotate_weekly_standings(standings, ratings)
        links_by_user = self._queens_links_by_user(ctx.guild.id, game)
        if not show_all:
            banned = self._minigame_banned_user_ids(ctx.guild.id, game)
            standings = [row for row in standings
                         if row.user_id in links_by_user and row.user_id not in banned]
        await self._send_queens_weekly_scores(
            ctx, game, standings, links_by_user, first_days=first_days)
