"""LinkedIn-game add/remove/clear/clean and ratings/rating commands. (Minigames cog impl mixin; see minigames.py)."""

import datetime as dt
import logging
from collections import namedtuple

import discord

from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util.akari_rating import rank_for_rating
from tle.util.akari_weekly import (
    compute_weekly_ratings, current_week_standings,
)

from tle.cogs._minigame_queens import (
    queens_weekly_difficulty_map,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError, _mg,
    _display_rating, _display_peak, _display_games,
)
from tle.cogs._minigame_queens_filters import (
    _split_queens_weekday_filter, _split_queens_rating_date_filter, _split_queens_recalculate_filter,
    _filter_queens_rating_date_history,
    _format_queens_weekday_filter, _format_queens_date_filter, _queens_filter_suffix,
    _queens_improved_title_suffix,
    _filter_queens_contested_rating_history,
)
from tle.cogs._minigame_queens_cog import (
    _parse_linkedin_date_or_number,
    _queens_current_puzzle_date,
    _queens_puzzle_date_text,
)
from tle.cogs._mgimpl_sharedcmd import _skipped_puzzles

logger = logging.getLogger(__name__)

_QueensWeeklyRow = namedtuple(
    '_QueensWeeklyRow',
    'user_id puzzle_number puzzle_date accuracy time_seconds is_perfect',
)


class ImplQueensCmdMixin:
    async def _cmd_queens_skips(self, ctx, game, member):
        """List missing concluded puzzles since a linked user's first day."""
        self._require_enabled(ctx.guild.id, game)
        link = self._require_queens_registered_member(
            ctx.guild.id, game, member)
        self._migrate_legacy_queens_results_to_external(ctx.guild.id, game)
        rows = cf_common.user_db.get_minigame_unresolved_results_for_name(
            ctx.guild.id, game.name, link.normalized_name)
        calendar = game.linkedin
        puzzle_numbers = []
        for row in rows:
            try:
                puzzle_numbers.append(
                    calendar.number_for_date(row.puzzle_date))
            except (AttributeError, TypeError, ValueError, OverflowError):
                continue
        current_puzzle = calendar.number_for_date(
            _queens_current_puzzle_date())
        first_submission, skipped = _skipped_puzzles(
            puzzle_numbers, current_puzzle)
        await self._send_minigame_skips(
            ctx, member, game, first_submission, skipped,
            calendar.date_for_number)

    async def _cmd_queens_clear(self, ctx, game, puzzle_date):
        self._require_enabled(ctx.guild.id, game)
        if puzzle_date is None:
            raise MinigameCogError(f'Usage: `;{game.name} clear DATE/#`.')
        parsed_date = _parse_linkedin_date_or_number(game, puzzle_date)
        parsed_number = game.linkedin.number_for_date(parsed_date)
        deleted = 0
        unresolved_deleted = 0
        for puzzle_number in game.linkedin.puzzle_numbers_for_date(
                parsed_date):
            deleted += cf_common.user_db.delete_minigame_results_for_puzzle(
                ctx.guild.id, game.name, puzzle_number)
            unresolved_deleted += (
                cf_common.user_db.delete_minigame_unresolved_results_for_puzzle(
                    ctx.guild.id, game.name, puzzle_number))
        if not deleted and not unresolved_deleted:
            raise MinigameCogError(
                f'No {game.display_name} results found for '
                f'{parsed_date.isoformat()}.')
        self._recompute_minigame_ratings(ctx.guild.id, game)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {deleted} registered and {unresolved_deleted} unresolved '
            f'{game.display_name} result(s) for '
            f'#{parsed_number} {parsed_date.isoformat()}.'))

    async def _cmd_queens_clean(self, ctx, game, start_date, end_date=None):
        self._require_enabled(ctx.guild.id, game)
        if start_date is None:
            raise MinigameCogError(
                f'Usage: `;{game.name} clean START_DATE [END_DATE]`.')
        parsed_start = _parse_linkedin_date_or_number(game, start_date)
        parsed_end = (
            _parse_linkedin_date_or_number(game, end_date)
            if end_date is not None
            else parsed_start
        )
        if parsed_end < parsed_start:
            raise MinigameCogError(
                f'{game.display_name} clean end date cannot be before start '
                'date.')

        days = (parsed_end - parsed_start).days + 1
        end_exclusive = parsed_end + dt.timedelta(days=1)
        deleted = cf_common.user_db.delete_minigame_results_for_date_range(
            ctx.guild.id, game.name,
            _queens_puzzle_date_text(parsed_start),
            _queens_puzzle_date_text(end_exclusive))
        unresolved_deleted = (
            cf_common.user_db.delete_minigame_unresolved_results_for_date_range(
                ctx.guild.id, game.name,
                _queens_puzzle_date_text(parsed_start),
                _queens_puzzle_date_text(end_exclusive)))

        if not deleted and not unresolved_deleted:
            raise MinigameCogError(
                f'No {game.display_name} results found from '
                f'{parsed_start.isoformat()} to {parsed_end.isoformat()}.')
        self._recompute_minigame_ratings(ctx.guild.id, game)
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {deleted} registered and {unresolved_deleted} unresolved '
            f'{game.display_name} result(s) from '
            f'{parsed_start.isoformat()} to {parsed_end.isoformat()} '
            f'({days} day(s)).'))

    async def _cmd_queens_ratings_recompute(self, ctx, game):
        self._require_enabled(ctx.guild.id, game)
        self._sync_queens_materialized_results(ctx.guild.id, game)
        self._recompute_minigame_ratings(
            ctx.guild.id, game, sync_results=False)
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} ratings recomputed.'))

    async def _extract_queens_rating_filters(self, ctx, game, args):
        """Split LinkedIn-game rating flags, mirroring the Akari six-tuple shape.

        ``+decay`` is a real view now that these games rate inactivity like
        Akari: it threads absent days into the rating graph.  Commands with
        nothing to draw it on accept and ignore it, exactly as the Akari ones
        do.  ``+test`` stays Akari-only — the first-skip-last-place experiment
        is not wired into the LinkedIn compute kwargs.
        """
        args, weekdays = _split_queens_weekday_filter(args)
        args, date_bounds = _split_queens_rating_date_filter(args)
        (remaining, include_decay, excluded_ids, included_ids,
         _include_inactive, test_decay) = await self._extract_akari_filters(
            ctx, args)
        if test_decay:
            raise MinigameCogError(
                f'`+test` is not supported for {game.display_name} '
                f'ratings.')
        return (remaining, include_decay, excluded_ids, included_ids,
                weekdays, date_bounds)

    async def _parse_queens_rating_args(self, ctx, game, args, *,
                                        member_required=False,
                                        allow_recalculate=False):
        args, recalculate = _split_queens_recalculate_filter(args)
        if recalculate and not allow_recalculate:
            raise MinigameCogError(
                f'`+recalculate` is only supported by `;{game.name} rating`.')
        (remaining, include_decay, excluded_ids, included_ids, weekdays,
         date_bounds) = (
            await self._extract_queens_rating_filters(ctx, game, args))
        members = [await self._resolve_member(ctx, token) for token in remaining]
        if not members:
            if member_required:
                raise MinigameCogError('A user is required for this command.')
            members = [ctx.author]
        return (
            members, include_decay, excluded_ids, included_ids, weekdays,
            date_bounds, recalculate,
        )

    async def _cmd_queens_ratings(self, ctx, game, *, show_all=False,
                                  excluded_ids=None, included_ids=None,
                                  weekdays=None, date_bounds=None,
                                  improved=False, weekly=False):
        self._require_enabled(ctx.guild.id, game)
        if weekly and improved:
            raise MinigameCogError(
                '`+weekly` and `+beta` are separate testing rating '
                'systems and cannot be combined.')
        standings = []
        if weekly:
            rows, standings = self._queens_weekly_preview(
                ctx.guild.id, game,
                excluded_ids=excluded_ids, included_ids=included_ids,
                weekdays=weekdays, date_bounds=date_bounds)
        elif not improved:
            self._recompute_minigame_ratings(ctx.guild.id, game)
            rows = None
        else:
            rows = None
        if not weekly and (
                improved or excluded_ids or included_ids
                or weekdays is not None or date_bounds is not None):
            rows = self._minigame_rating_rows(
                ctx.guild.id, game,
                excluded_ids=excluded_ids, included_ids=included_ids,
                weekdays=weekdays, date_bounds=date_bounds,
                improved=improved)
        elif not weekly and rows is None:
            rows = cf_common.user_db.get_minigame_ratings(
                ctx.guild.id, game.name)
        if not rows and not standings:
            raise MinigameCogError(
                f'No {game.display_name} ratings yet.')
        links_by_user = self._queens_links_by_user(ctx.guild.id, game)
        linked_ids = set(links_by_user)
        # Banned players stay rated (forward-only ban) but are hidden from
        # the public board, like Akari's auto-opted-out banned users; the
        # debug view still shows them.
        banned_ids = self._minigame_banned_user_ids(ctx.guild.id, game)
        shown = rows if show_all else [
            row for row in rows
            if row.user_id in linked_ids and row.user_id not in banned_ids
        ]
        if weekly and not show_all:
            standings = [
                standing for standing in standings
                if standing.user_id in linked_ids
                and standing.user_id not in banned_ids
            ]
        if not shown and not standings:
            raise MinigameCogError(
                f'No registered {game.display_name} players yet. '
                f'Players register with `;{game.name} register LinkedIn Name`.')
        if show_all:
            suffix_parts = ['all']
            weekday_label = _format_queens_weekday_filter(weekdays)
            if weekday_label:
                suffix_parts.append(weekday_label)
            date_label = _format_queens_date_filter(date_bounds)
            if date_label:
                suffix_parts.append(date_label)
            title = (
                f'{game.display_name} Ratings'
                f'{_queens_improved_title_suffix(improved)}'
                f'{" [weekly preview]" if weekly else ""} '
                f'({", ".join(suffix_parts)})')
        else:
            title = (
                f'{game.display_name} Ratings'
                f'{_queens_improved_title_suffix(improved)}'
                f'{" [weekly preview]" if weekly else ""}'
                f'{_queens_filter_suffix(weekdays=weekdays, date_bounds=date_bounds)}')
        if shown:
            discord_file = _mg()._get_akari_rating_table_image_file(
                ctx.guild, shown, linked_ids,
                title=title,
                mark_registered=show_all,
                games_label='Weeks' if weekly else 'Games',
                identity_label='LinkedIn',
                identity_fn=self._queens_rating_identity_fn(links_by_user),
                name_fn=self._queens_name_fn(links_by_user))
            await ctx.send(file=discord_file)
        if weekly:
            await self._send_queens_weekly_scores(
                ctx, game, standings, links_by_user)

    def _queens_weekly_preview(self, guild_id, game, *, excluded_ids=None,
                               included_ids=None, weekdays=None,
                               date_bounds=None):
        """Build speed-based weekly ratings and this week's live scores."""
        result_rows = self._filtered_minigame_result_rows(
            guild_id, game,
            excluded_ids=excluded_ids, included_ids=included_ids,
            weekdays=weekdays, date_bounds=date_bounds)
        # LinkedIn's native competition rule is time-only. Direct share
        # messages do not preserve hint/mistake badges while pasted
        # leaderboards do, so feeding those badges into Akari's accuracy model
        # would make the same solve worth different amounts based solely on
        # its ingestion path.
        scoring_rows = [
            _QueensWeeklyRow(
                row.user_id, row.puzzle_number, row.puzzle_date,
                100, row.time_seconds, True)
            for row in result_rows
        ]
        today = _queens_current_puzzle_date()
        difficulties = queens_weekly_difficulty_map(scoring_rows)
        states = compute_weekly_ratings(
            scoring_rows, difficulties, as_of_date=today)
        rating_rows = sorted(
            states.values(),
            key=lambda state: (
                -state.rating, -state.games, int(state.user_id)))
        standings = current_week_standings(
            scoring_rows, difficulties, as_of_date=today)
        return rating_rows, standings

    async def _send_queens_weekly_scores(
            self, ctx, game, standings, links_by_user):
        if not standings:
            await ctx.send(embed=discord_common.embed_neutral(
                f'No {game.display_name} scores have been posted '
                'this week yet.'))
            return
        start = standings[0].week_start
        end = standings[0].week_end
        score_file = _mg()._get_akari_weekly_table_image_file(
            ctx.guild, standings,
            title=(
                f'{game.display_name} Weekly Scores · '
                f'{start:%b %d}–{end:%b %d} (in progress)'),
            identity_label='LinkedIn',
            identity_fn=self._queens_rating_identity_fn(links_by_user),
            name_fn=self._queens_name_fn(links_by_user),
            filename=f'{game.name}-weekly-scores.png')
        await ctx.send(file=score_file)

    async def _cmd_queens_rating(self, ctx, game, members, *,
                                 require_registered=True,
                                 include_decay=False,
                                 excluded_ids=None, included_ids=None,
                                 weekdays=None, date_bounds=None,
                                 recalculate=False, improved=False):
        """Per-user rating graph for a LinkedIn game.

        ``include_decay=True`` (the ``+decay`` arg) threads inactivity days
        into the plotted history so absent-day slopes are visible; played days
        stay the marker anchors, and solo days remain omitted either way.
        """
        self._require_enabled(ctx.guild.id, game)
        if not improved:
            self._recompute_minigame_ratings(ctx.guild.id, game)
        if require_registered:
            for member in members:
                self._require_queens_registered_member(
                    ctx.guild.id, game, member)

        replay_date_bounds = date_bounds if recalculate else None
        filtered = bool(improved or excluded_ids or included_ids or weekdays is not None
                        or replay_date_bounds is not None)
        per_member = []
        for member in members:
            if filtered:
                row, history = self._minigame_user_data(
                    ctx.guild.id, game, member.id,
                    include_decay=include_decay,
                    excluded_ids=excluded_ids, included_ids=included_ids,
                    weekdays=weekdays, date_bounds=replay_date_bounds,
                    improved=improved)
            else:
                row = cf_common.user_db.get_minigame_rating(
                    ctx.guild.id, game.name, member.id)
                history = self._minigame_user_history(
                    ctx.guild.id, game, member.id,
                    include_decay=include_decay)
            if not recalculate:
                history = _filter_queens_rating_date_history(history, date_bounds)
            if row is None:
                raise MinigameCogError(
                    f'No {game.display_name} rating for '
                    f'`{self._queens_public_user_name(ctx.guild, member.id)}` yet.')
            if not history:
                raise MinigameCogError(
                    f'`{self._queens_public_user_name(ctx.guild, member.id)}` has no rated '
                    f'{game.display_name} days to plot yet.')
            graph_history = _filter_queens_contested_rating_history(
                history, keep_decay=include_decay)
            if not graph_history:
                raise MinigameCogError(
                    f'`{self._queens_public_user_name(ctx.guild, member.id)}` has no contested '
                    f'{game.display_name} days to plot yet.')
            per_member.append((member, row, history, graph_history))

        series = [
            (graph_history, self._queens_legend_name(ctx.guild.id, game, member))
            for member, _row, _history, graph_history in per_member
        ]
        discord_file = _mg().plot_akari_rating(series)

        if len(per_member) == 1:
            member, row, history, _graph_history = per_member[0]
            display_name = self._queens_public_user_name(ctx.guild, member.id)
            rating = round(_display_rating(row, history, date_bounds))
            rank = rank_for_rating(rating)
            peak = round(_display_peak(row, history, date_bounds))
            peak_rank = rank_for_rating(peak)
            last_contest = next((h for h in reversed(history)
                                 if h.performance is not None), None)
            last_change_str = (f'{last_contest.delta:+.0f}'
                               if last_contest is not None else '—')
            last_perf_str = (
                f'{round(last_contest.performance)} '
                f'({rank_for_rating(round(last_contest.performance)).title_abbr})'
                if last_contest is not None else '—')
            embed = discord.Embed(
                title=(f'{game.display_name} rating'
                       f'{_queens_improved_title_suffix(improved)} — '
                       f'{display_name}'),
                color=rank.color_embed,
            )
            embed.add_field(name='Rating', value=f'{rating} ({rank.title_abbr})')
            embed.add_field(name='Peak', value=f'{peak} ({peak_rank.title_abbr})')
            embed.add_field(name='Games', value=str(_display_games(row, history, date_bounds)))
            embed.add_field(name='Last change', value=last_change_str)
            embed.add_field(name='Last performance', value=last_perf_str)
        else:
            _top_member, top_row, top_history, _top_graph_history = max(
                per_member, key=lambda t: _display_rating(t[1], t[2], date_bounds))
            top_rank = rank_for_rating(
                round(_display_rating(top_row, top_history, date_bounds)))

            def _rating_line(member, row, history):
                rating = round(_display_rating(row, history, date_bounds))
                return (
                    f'**{self._queens_public_user_name(ctx.guild, member.id)}**: '
                    f'{rating} ({rank_for_rating(rating).title_abbr})'
                )

            lines = [
                _rating_line(member, row, history)
                for member, row, history, _graph_history in per_member
            ]
            embed = discord.Embed(
                title=(f'{game.display_name} ratings'
                       f'{_queens_improved_title_suffix(improved)} — '
                       f'{len(per_member)} players'),
                description='\n'.join(lines),
                color=top_rank.color_embed,
            )

        discord_common.attach_image(embed, discord_file)
        await ctx.send(embed=embed, file=discord_file)
