"""Akari rating/performance/history/stats commands. (Minigames cog impl mixin; see minigames.py)."""

import datetime as dt
import logging

import discord

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.util.akari_rating import rank_for_rating

from tle.cogs._minigame_akari import (
    AKARI_GAME,
)
from tle.cogs._minigame_stats import (
    plot_akari_stats, plot_guessgame_stats,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError, _mg, _safe_member_name,
    _legend_name_for, _format_akari_history_line,
)
from tle.cogs._minigame_tables import (
    _maybe_parse_puzzle_selector,
)
from tle.cogs._minigame_tables import _AKARI_HISTORY_PER_PAGE

logger = logging.getLogger(__name__)


class ImplAkariBMixin:
    async def _cmd_akari_rating(self, ctx, members, *, require_registered=True,
                                include_decay=False, excluded_ids=None,
                                included_ids=None, test_decay=False):
        """Per-user rating graph (``;plot rating`` style).

        ``members`` is a list of one-or-more members.  With a single member
        the embed keeps the rich layout (Rating / Peak / Games / Last change /
        Last performance); with multiple members the graph plots one line per
        user and the embed switches to a compact roster.

        ``require_registered=True`` (the default, public-facing path) refuses
        to show the rating of users who haven't opted in via ``;mg akari register``.
        The ``rating debug`` subcommand passes False so admins can inspect any
        shadow-rated player.

        ``include_decay=True`` (the ``+decay`` arg) threads decay days into the
        plotted history so absent-day slopes are visible; played days remain
        the marker anchors so they still stand out.

        ``excluded_ids`` (the ``+exclude=...`` arg) recomputes both the embed
        figures and the graph as if those users never played; the persisted
        snapshot stays untouched.  ``test_decay`` (the ``+test`` arg) does the
        same under the experimental decay model.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if require_registered:
            for member in members:
                if not cf_common.user_db.is_akari_registered(
                        ctx.guild.id, member.id):
                    raise MinigameCogError(
                        f'`{_safe_member_name(member)}` has not opted in to '
                        f'{AKARI_GAME.display_name} ratings '
                        f'(`;mg akari register`).')

        filtered = bool(excluded_ids or included_ids or test_decay)
        per_member = []
        for member in members:
            if filtered:
                row, history = self._akari_user_data(
                    ctx.guild.id, member.id,
                    include_decay=include_decay,
                    excluded_ids=excluded_ids, included_ids=included_ids,
                    test_decay=test_decay)
            else:
                row = cf_common.user_db.get_akari_rating(
                    ctx.guild.id, member.id)
                history = self._akari_user_history(
                    ctx.guild.id, member.id, include_decay=include_decay)
            if row is None:
                raise MinigameCogError(
                    f'No {AKARI_GAME.display_name} rating for '
                    f'`{_safe_member_name(member)}` yet.')
            if not history:
                raise MinigameCogError(
                    f'`{_safe_member_name(member)}` has no rated '
                    f'{AKARI_GAME.display_name} days to plot yet.')
            per_member.append((member, row, history))

        series = [(history, _legend_name_for(ctx.guild, member))
                  for member, _row, history in per_member]
        discord_file = _mg().plot_akari_rating(series)

        title_suffix = ' [test decay]' if test_decay else ''
        if len(per_member) == 1:
            member, row, history = per_member[0]
            rating = round(row.rating)
            rank = rank_for_rating(rating)
            peak_rank = rank_for_rating(round(row.peak))
            # Last contest day's delta and performance (skip solo-day Nones).
            # row.last_delta on the snapshot is overwritten by daily decay steps
            # and rounds to +0 for most users — use the history to find their
            # last actual contest instead, matching how Performance is shown.
            last_contest = next((h for h in reversed(history)
                                 if h.performance is not None), None)
            last_change_str = (f'{last_contest.delta:+.0f}'
                               if last_contest is not None else '—')
            last_perf_str = (
                f'{round(last_contest.performance)} '
                f'({rank_for_rating(round(last_contest.performance)).title_abbr})'
                if last_contest is not None else '—')
            embed = discord.Embed(
                title=(f'{AKARI_GAME.display_name} rating — '
                       f'{_safe_member_name(member)}{title_suffix}'),
                color=rank.color_embed,
            )
            embed.add_field(name='Rating', value=f'{rating} ({rank.title_abbr})')
            embed.add_field(name='Peak', value=f'{round(row.peak)} ({peak_rank.title_abbr})')
            embed.add_field(name='Games', value=str(row.games))
            embed.add_field(name='Last change', value=last_change_str)
            embed.add_field(name='Last performance', value=last_perf_str)
        else:
            top_member, top_row, _ = max(per_member, key=lambda t: t[1].rating)
            top_rank = rank_for_rating(round(top_row.rating))
            del top_member  # only its row drives the embed colour
            lines = [
                f'**{_safe_member_name(member)}**: '
                f'{round(row.rating)} '
                f'({rank_for_rating(round(row.rating)).title_abbr})'
                for member, row, _ in per_member
            ]
            embed = discord.Embed(
                title=(f'{AKARI_GAME.display_name} ratings — '
                       f'{len(per_member)} players{title_suffix}'),
                description='\n'.join(lines),
                color=top_rank.color_embed,
            )

        discord_common.attach_image(embed, discord_file)
        await ctx.send(embed=embed, file=discord_file)

    async def _cmd_akari_performance(self, ctx, members, *, require_registered=True,
                                     excluded_ids=None, included_ids=None,
                                     test_decay=False):
        """Per-user performance graph.

        Performance is the rating that, given the day's field, would seed the
        player at exactly their actual rank — i.e. their "rating-equivalent
        finish" for that contest, independent of their incoming rating.  Solo
        days have no field and are dropped from the plot.

        ``members`` is a list of one-or-more members; single-member uses the
        rich embed (Last / Best / Contests), multi-member uses a compact one
        with each player's latest performance.

        ``require_registered=True`` (the default, public-facing path) refuses
        to show performance for users who haven't opted in via ``;mg akari register``.
        The ``performance debug`` subcommand passes False so admins can inspect
        any shadow-rated player.  ``excluded_ids`` runs a fresh replay without
        those users so their presence doesn't shape this player's performance.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if require_registered:
            for member in members:
                if not cf_common.user_db.is_akari_registered(
                        ctx.guild.id, member.id):
                    raise MinigameCogError(
                        f'`{_safe_member_name(member)}` has not opted in to '
                        f'{AKARI_GAME.display_name} ratings '
                        f'(`;mg akari register`).')

        filtered = bool(excluded_ids or included_ids or test_decay)
        per_member = []
        for member in members:
            if filtered:
                row, history = self._akari_user_data(
                    ctx.guild.id, member.id,
                    excluded_ids=excluded_ids, included_ids=included_ids,
                    test_decay=test_decay)
            else:
                row = cf_common.user_db.get_akari_rating(
                    ctx.guild.id, member.id)
                history = self._akari_user_history(ctx.guild.id, member.id)
            if row is None:
                raise MinigameCogError(
                    f'No {AKARI_GAME.display_name} rating for '
                    f'`{_safe_member_name(member)}` yet.')
            contest_history = [h for h in history if h.performance is not None]
            if not contest_history:
                raise MinigameCogError(
                    f'`{_safe_member_name(member)}` has no contested '
                    f'{AKARI_GAME.display_name} days to plot performance for yet.')
            per_member.append((member, row, history, contest_history))

        series = [(history, _legend_name_for(ctx.guild, member), round(row.rating))
                  for member, row, history, _ in per_member]
        discord_file = _mg().plot_akari_performance(series)

        title_suffix = ' [test decay]' if test_decay else ''
        if len(per_member) == 1:
            member, row, _history, contest_history = per_member[0]
            last_perf = contest_history[-1].performance
            last_rank = rank_for_rating(round(last_perf))
            best_perf = max(h.performance for h in contest_history)
            best_rank = rank_for_rating(round(best_perf))
            embed = discord.Embed(
                title=(f'{AKARI_GAME.display_name} performance — '
                       f'{_safe_member_name(member)}{title_suffix}'),
                color=last_rank.color_embed,
            )
            embed.add_field(name='Last performance',
                            value=f'{round(last_perf)} ({last_rank.title_abbr})')
            embed.add_field(name='Best performance',
                            value=f'{round(best_perf)} ({best_rank.title_abbr})')
            embed.add_field(name='Contests', value=str(len(contest_history)))
        else:
            # Pick the embed colour from the strongest *recent* performance.
            best_per_member = [
                (member, contest_history[-1].performance)
                for member, _row, _history, contest_history in per_member
            ]
            top_rank = rank_for_rating(round(
                max(perf for _m, perf in best_per_member)))
            lines = [
                f'**{_safe_member_name(member)}**: '
                f'last {round(contest_history[-1].performance)} '
                f'({rank_for_rating(round(contest_history[-1].performance)).title_abbr})'
                for member, _row, _history, contest_history in per_member
            ]
            embed = discord.Embed(
                title=(f'{AKARI_GAME.display_name} performance — '
                       f'{len(per_member)} players{title_suffix}'),
                description='\n'.join(lines),
                color=top_rank.color_embed,
            )

        discord_common.attach_image(embed, discord_file)
        await ctx.send(embed=embed, file=discord_file)

    async def _cmd_akari_ratings_debug(self, ctx, *, excluded_ids=None,
                                        included_ids=None,
                                        include_inactive=False,
                                        test_decay=False, weekly=False):
        """Admin view: leaderboard image including shadow-rated (unopted-in) users.

        Same image as ``;mg akari ratings`` but without the registration filter —
        so admins can see everyone's rating, with a ``✓`` marking opted-in users.
        Honours ``+exclude=...`` / ``+include=...`` / ``+inactive`` the same
        way as the public command.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if weekly:
            rows, standings = await self._akari_weekly_preview(
                ctx.guild.id, excluded_ids=excluded_ids,
                included_ids=included_ids)
        elif excluded_ids or included_ids or test_decay:
            rows = self._akari_filtered_rating_rows(
                ctx.guild.id, excluded_ids=excluded_ids,
                included_ids=included_ids, test_decay=test_decay)
        else:
            rows = cf_common.user_db.get_akari_ratings(ctx.guild.id)
        if not rows and not (weekly and standings):
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} ratings yet. They appear once '
                f'players post results.')
        shown = self._active_ranking_rows(
            rows, include_inactive=include_inactive)
        if not shown and not (weekly and standings):
            if include_inactive:
                raise MinigameCogError(
                    f'No {AKARI_GAME.display_name} players yet.')
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} players active in the last '
                f'{constants.AKARI_RANKING_MAX_INACTIVE_DAYS} days. '
                f'Use `+inactive` to include dormant players.')
        registrants = cf_common.user_db.get_akari_registrants(ctx.guild.id)
        title = ('Daily Akari Ratings (all, incl. inactive)'
                 if include_inactive else 'Daily Akari Ratings (all)')
        if test_decay and not weekly:
            title += ' [test decay]'
        if weekly:
            title += ' [weekly preview]'
        if shown:
            table_kwargs = {'games_label': 'Weeks'} if weekly else {}
            discord_file = _mg()._get_akari_rating_table_image_file(
                ctx.guild, shown, registrants,
                title=title, mark_registered=True,
                **table_kwargs)
            await ctx.send(file=discord_file)
        if weekly and standings:
            start = standings[0].week_start
            end = standings[0].week_end
            score_file = _mg()._get_akari_weekly_table_image_file(
                ctx.guild, standings,
                title=(f'Daily Akari Weekly Scores · {start:%b %d}–'
                       f'{end:%b %d} (in progress)'))
            await ctx.send(file=score_file)
        elif weekly:
            await ctx.send(embed=discord_common.embed_neutral(
                'No Daily Akari scores have been posted this week yet.'))

    async def _cmd_akari_history(self, ctx, member, *, require_registered=True,
                                 excluded_ids=None, included_ids=None,
                                 test_decay=False):
        """Per-user paginated rating delta history (``;handles updates`` style).

        One line per contest the user played, newest first.  Solo days (single
        player) are skipped — they have no field, no contest delta, and don't
        appear on the rating graph either.  Decay days never had their own
        history points to begin with; their net effect surfaces in the next
        played day's rating.  ``excluded_ids`` recomputes the history without
        those users so each delta reflects the contest minus them.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        if require_registered and not cf_common.user_db.is_akari_registered(
                ctx.guild.id, member.id):
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` has not opted in to '
                f'{AKARI_GAME.display_name} ratings (`;mg akari register`).')

        history = self._akari_user_history(
            ctx.guild.id, member.id,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay)
        contest_history = [h for h in history if h.performance is not None]
        if not contest_history:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` has no contested '
                f'{AKARI_GAME.display_name} days yet.')

        lines = [_format_akari_history_line(h) for h in reversed(contest_history)]
        title_suffix = ' [test decay]' if test_decay else ''
        title = (f'{AKARI_GAME.display_name} rating history — '
                 f'{_safe_member_name(member)} '
                 f'({len(contest_history)} contests){title_suffix}')
        pages = []
        for chunk in paginator.chunkify(lines, _AKARI_HISTORY_PER_PAGE):
            embed = discord.Embed(
                title=title,
                description='\n'.join(chunk),
                color=discord_common.random_cf_color(),
            )
            pages.append((None, embed))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id)

    _STATS_PLOTTERS = {
        'akari': plot_akari_stats,
        'guessgame': plot_guessgame_stats,
    }

    async def _cmd_akari_stats_puzzle(self, ctx, selector_arg, *,
                                       show_all=False, excluded_ids=None,
                                       included_ids=None, test_decay=False):
        """Render a per-puzzle results image annotated with pre-puzzle ratings.

        ``show_all=False`` (public path): only opted-in users get the rating
        + tier colour; everyone else stays plain.  ``show_all=True`` (the
        ``stats debug`` subcommand, mod-only) annotates every player including
        shadow-rated ones, mirroring how ``ratings debug`` reveals opt-outs.
        ``excluded_ids`` hides those users from the displayed table *and*
        runs the rating annotation without them, so deltas reflect the
        smaller field.
        """
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
            day_start = dt.datetime.combine(selector_value, dt.time.min).timestamp()
            day_end = day_start + 24 * 60 * 60
            rows = cf_common.user_db.get_minigame_results_for_guild(
                ctx.guild.id, AKARI_GAME.name, dlo=day_start, dhi=day_end)
            title = f'{AKARI_GAME.display_name} {selector_value.isoformat()} Results'

        rows = self._filter_akari_rows(
            rows, excluded_ids=excluded_ids, included_ids=included_ids)

        if not rows:
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} results found for `{selector_arg}`.')

        # Annotation requires a single puzzle worth of rows (1 puzzle/day).
        # For a multi-puzzle slice (theoretical), fall back to plain rendering.
        puzzle_numbers = {int(row.puzzle_number) for row in rows}
        puzzle_info = None
        registrants = None
        if len(puzzle_numbers) == 1:
            puzzle_info = self._akari_puzzle_change_info(
                ctx.guild.id, next(iter(puzzle_numbers)),
                excluded_ids=excluded_ids, included_ids=included_ids,
                test_decay=test_decay)
            if show_all:
                # Debug: pretend every rated player is registered for display.
                registrants = set(puzzle_info.keys())
            else:
                registrants = cf_common.user_db.get_akari_registrants(
                    ctx.guild.id)

        if test_decay:
            title += ' [test decay]'
        discord_file = _mg()._get_akari_puzzle_table_image_file(
            ctx.guild, rows, title,
            puzzle_info=puzzle_info, registrants=registrants)
        await ctx.send(file=discord_file)

