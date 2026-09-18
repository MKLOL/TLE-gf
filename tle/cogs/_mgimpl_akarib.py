"""Akari rating/performance/history/stats commands. (Minigames cog impl mixin; see minigames.py)."""

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
from tle.cogs._minigame_helpers import (
    MinigameCogError, _mg, _safe_member_name,
    _legend_name_for, _format_akari_history_line,
    _display_rating, _display_peak, _display_games,
)
from tle.cogs._minigame_queens_filters import (
    _filter_queens_rating_date_history, _queens_filter_suffix,
    _queens_improved_title_suffix,
)
from tle.cogs._minigame_tables import _AKARI_HISTORY_PER_PAGE

logger = logging.getLogger(__name__)


class ImplAkariBMixin:
    async def _cmd_akari_rating(self, ctx, members, *, require_registered=True,
                                include_decay=False, excluded_ids=None,
                                included_ids=None, test_decay=False,
                                weekdays=None, date_bounds=None,
                                recalculate=False, beta=False,
                                time_only=False):
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
        self._validate_akari_beta(
            beta, include_decay=include_decay, test_decay=test_decay,
            time_only=time_only)
        if require_registered:
            for member in members:
                if not cf_common.user_db.is_akari_registered(
                        ctx.guild.id, member.id):
                    raise MinigameCogError(
                        f'`{_safe_member_name(member)}` has not opted in to '
                        f'{AKARI_GAME.display_name} ratings '
                        f'(`;mg akari register`).')
                # Banned players are hidden from public views (forward-only
                # ban); the mod-only debug variants skip this gate.
                if cf_common.user_db.is_akari_banned(ctx.guild.id, member.id):
                    raise MinigameCogError(
                        f'`{_safe_member_name(member)}` is banned from '
                        f'{AKARI_GAME.display_name}.')

        # Mirrors the Queens semantics: date bounds display-filter the stored
        # history by default; ``+recalculate`` replays on only the filtered
        # rows.  Weekday filters always force a fresh (ad-hoc) replay.
        replay_date_bounds = date_bounds if recalculate else None
        filtered = bool(excluded_ids or included_ids or test_decay or beta
                        or time_only
                        or weekdays is not None
                        or replay_date_bounds is not None)
        per_member = []
        for member in members:
            if filtered:
                row, history = self._akari_user_data(
                    ctx.guild.id, member.id,
                    include_decay=include_decay,
                    excluded_ids=excluded_ids, included_ids=included_ids,
                    test_decay=test_decay, weekdays=weekdays,
                    date_bounds=replay_date_bounds, beta=beta,
                    time_only=time_only)
            else:
                row = cf_common.user_db.get_akari_rating(
                    ctx.guild.id, member.id)
                history = self._akari_user_history(
                    ctx.guild.id, member.id, include_decay=include_decay)
            if not recalculate:
                history = _filter_queens_rating_date_history(
                    history, date_bounds)
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

        title_suffix = _queens_improved_title_suffix(beta)
        title_suffix += ' [time only]' if time_only else ''
        title_suffix += ' [test decay]' if test_decay else ''
        title_suffix += _queens_filter_suffix(
            weekdays=weekdays, date_bounds=date_bounds)
        if len(per_member) == 1:
            member, row, history = per_member[0]
            rating = round(_display_rating(row, history, date_bounds))
            rank = rank_for_rating(rating)
            peak = round(_display_peak(row, history, date_bounds))
            peak_rank = rank_for_rating(peak)
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
            embed.add_field(name='Peak', value=f'{peak} ({peak_rank.title_abbr})')
            embed.add_field(name='Games',
                            value=str(_display_games(row, history, date_bounds)))
            embed.add_field(name='Last change', value=last_change_str)
            embed.add_field(name='Last performance', value=last_perf_str)
        else:
            _top_member, top_row, top_history = max(
                per_member, key=lambda t: _display_rating(t[1], t[2], date_bounds))
            top_rank = rank_for_rating(
                round(_display_rating(top_row, top_history, date_bounds)))
            lines = [
                f'**{_safe_member_name(member)}**: '
                f'{round(_display_rating(row, history, date_bounds))} '
                f'({rank_for_rating(round(_display_rating(row, history, date_bounds))).title_abbr})'
                for member, row, history in per_member
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
                                     test_decay=False, weekdays=None,
                                     date_bounds=None, beta=False,
                                     time_only=False):
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
        self._validate_akari_beta(
            beta, test_decay=test_decay, time_only=time_only)
        if require_registered:
            for member in members:
                if not cf_common.user_db.is_akari_registered(
                        ctx.guild.id, member.id):
                    raise MinigameCogError(
                        f'`{_safe_member_name(member)}` has not opted in to '
                        f'{AKARI_GAME.display_name} ratings '
                        f'(`;mg akari register`).')
                # Banned players are hidden from public views (forward-only
                # ban); the mod-only debug variants skip this gate.
                if cf_common.user_db.is_akari_banned(ctx.guild.id, member.id):
                    raise MinigameCogError(
                        f'`{_safe_member_name(member)}` is banned from '
                        f'{AKARI_GAME.display_name}.')

        filtered = bool(excluded_ids or included_ids or test_decay or beta
                        or time_only
                        or weekdays is not None)
        per_member = []
        for member in members:
            if filtered:
                row, history = self._akari_user_data(
                    ctx.guild.id, member.id,
                    excluded_ids=excluded_ids, included_ids=included_ids,
                    test_decay=test_decay, weekdays=weekdays, beta=beta,
                    time_only=time_only)
            else:
                row = cf_common.user_db.get_akari_rating(
                    ctx.guild.id, member.id)
                history = self._akari_user_history(ctx.guild.id, member.id)
            history = _filter_queens_rating_date_history(history, date_bounds)
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

        series = [
            (
                history,
                _legend_name_for(ctx.guild, member),
                round(history[-1].rating if date_bounds is not None
                      else row.rating),
            )
            for member, row, history, _ in per_member
        ]
        discord_file = _mg().plot_akari_performance(series)

        title_suffix = _queens_improved_title_suffix(beta)
        title_suffix += ' [time only]' if time_only else ''
        title_suffix += ' [test decay]' if test_decay else ''
        title_suffix += _queens_filter_suffix(
            weekdays=weekdays, date_bounds=date_bounds)
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
                                        test_decay=False, weekly=False,
                                        weekdays=None, date_bounds=None,
                                        beta=False, time_only=False,
                                        current=False):
        """Admin view: leaderboard image including shadow-rated (unopted-in) users.

        Same image as ``;mg akari ratings`` but without the registration filter —
        so admins can see everyone's rating, with a ``✓`` marking opted-in users.
        Honours ``+exclude=...`` / ``+include=...`` / ``+inactive`` the same
        way as the public command.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        self._validate_akari_beta(
            beta, test_decay=test_decay, weekly=weekly or current,
            time_only=time_only)
        if weekly:
            return await self._cmd_akari_completed_weekly_ratings(
                ctx, excluded_ids=excluded_ids, included_ids=included_ids,
                include_inactive=include_inactive, weekdays=weekdays,
                date_bounds=date_bounds, show_all=True)
        if current:
            return await self._cmd_akari_current_week_ratings(
                ctx, excluded_ids=excluded_ids, included_ids=included_ids,
                weekdays=weekdays, date_bounds=date_bounds, show_all=True)
        filtered = bool(excluded_ids or included_ids or test_decay or beta
                        or time_only
                        or weekdays is not None or date_bounds is not None)
        if filtered:
            rows = self._akari_filtered_rating_rows(
                ctx.guild.id, excluded_ids=excluded_ids,
                included_ids=included_ids, test_decay=test_decay,
                weekdays=weekdays, date_bounds=date_bounds, beta=beta,
                time_only=time_only)
        else:
            rows = cf_common.user_db.get_akari_ratings(ctx.guild.id)
        if not rows:
            raise MinigameCogError(
                f'No {AKARI_GAME.display_name} ratings yet. They appear once '
                f'players post results.')
        shown = self._active_ranking_rows(
            rows, include_inactive=include_inactive)
        if not shown:
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
        if test_decay:
            title += ' [test decay]'
        title += _queens_improved_title_suffix(beta)
        title += ' [time only]' if time_only else ''
        title += _queens_filter_suffix(
            weekdays=weekdays, date_bounds=date_bounds)
        discord_file = _mg()._get_akari_rating_table_image_file(
            ctx.guild, shown, registrants,
            title=title, mark_registered=True)
        await ctx.send(file=discord_file)

    async def _cmd_akari_history(self, ctx, member, *, require_registered=True,
                                 excluded_ids=None, included_ids=None,
                                 test_decay=False, weekdays=None,
                                 date_bounds=None, beta=False,
                                 time_only=False):
        """Per-user paginated rating delta history (``;handles updates`` style).

        One line per contest the user played, newest first.  Solo days (single
        player) are skipped — they have no field, no contest delta, and don't
        appear on the rating graph either.  Decay days never had their own
        history points to begin with; their net effect surfaces in the next
        played day's rating.  ``excluded_ids`` recomputes the history without
        those users so each delta reflects the contest minus them.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        self._validate_akari_beta(
            beta, test_decay=test_decay, time_only=time_only)
        if require_registered:
            if not cf_common.user_db.is_akari_registered(
                    ctx.guild.id, member.id):
                raise MinigameCogError(
                    f'`{_safe_member_name(member)}` has not opted in to '
                    f'{AKARI_GAME.display_name} ratings (`;mg akari register`).')
            if cf_common.user_db.is_akari_banned(ctx.guild.id, member.id):
                raise MinigameCogError(
                    f'`{_safe_member_name(member)}` is banned from '
                    f'{AKARI_GAME.display_name}.')

        history = self._akari_user_history(
            ctx.guild.id, member.id,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay, weekdays=weekdays, date_bounds=date_bounds,
            beta=beta, time_only=time_only)
        contest_history = [h for h in history if h.performance is not None]
        if not contest_history:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` has no contested '
                f'{AKARI_GAME.display_name} days yet.')

        lines = [_format_akari_history_line(h) for h in reversed(contest_history)]
        title_suffix = _queens_improved_title_suffix(beta)
        title_suffix += ' [time only]' if time_only else ''
        title_suffix += ' [test decay]' if test_decay else ''
        title_suffix += _queens_filter_suffix(
            weekdays=weekdays, date_bounds=date_bounds)
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
