"""Akari add/ratings and rating-replay helpers. (Minigames cog impl mixin; see minigames.py)."""

import datetime as dt
import logging
import time


from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util.akari_difficulty import fetch_akari_difficulties
from tle.util.akari_weekly import (
    compute_weekly_ratings, current_week_standings, week_start,
)

from tle.cogs._minigame_common import (
    format_duration,
)
from tle.cogs._minigame_akari import (
    AKARI_GAME, puzzle_date_for, rank_akari_time_participants,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError, _mg, _safe_member_name,
)
from tle.cogs._minigame_queens_filters import (
    _filter_queens_weekday_rows, _filter_queens_rating_date_rows,
    _queens_filter_suffix, _queens_improved_title_suffix,
)

logger = logging.getLogger(__name__)


class ImplAkariAMixin:
    async def _cmd_akari_add(self, ctx, member, puzzle_number, result_text,
                             time_text, *, display_time_text=None):
        """Mod-only: manually insert an Akari result for a (user, puzzle) pair.

        For backfilling missed posts or posts that landed in the wrong channel.
        The row goes into the live result table keyed on the command/interaction
        message id, so deleting the originating message removes the row (the
        same path the normal ingestion uses for edits/deletes).
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)

        # ── Parse result ───────────────────────────────────────────────
        cleaned = result_text.strip().lower().lstrip('\U0001f31f').strip()
        if cleaned in ('perfect', '\U0001f31f'):
            is_perfect, accuracy = True, 100
        else:
            cleaned = cleaned.rstrip('%').strip()
            try:
                n = int(cleaned)
            except ValueError:
                raise MinigameCogError(
                    f'Could not parse result `{result_text}` \N{EM DASH} '
                    f'expected `perfect` or `N%`.')
            if not 0 <= n <= 100:
                raise MinigameCogError(
                    f'Accuracy must be between 0 and 100, got `{n}`.')
            is_perfect = n == 100
            accuracy = n

        # ── Parse time (mirrors _minigame_akari._parse_time) ──────────
        try:
            parts = [int(p) for p in time_text.split(':')]
        except ValueError:
            raise MinigameCogError(f'Could not parse time `{time_text}`.')
        if len(parts) == 2:
            time_seconds = parts[0] * 60 + parts[1]
        elif len(parts) == 3:
            time_seconds = parts[0] * 3600 + parts[1] * 60 + parts[2]
        else:
            raise MinigameCogError(
                f'Time `{time_text}` must be `M:SS` or `H:MM:SS`.')
        if time_seconds < 0:
            raise MinigameCogError(f'Time must be non-negative.')

        # ── Validate puzzle number ─────────────────────────────────────
        today_puzzle = _mg().expected_puzzle_number(dt.date.today())
        if puzzle_number < 1 or puzzle_number > today_puzzle + constants.AKARI_MAX_PUZZLE_LOOKAHEAD:
            raise MinigameCogError(
                f'Puzzle number `{puzzle_number}` is out of range '
                f'(today\'s puzzle is `{today_puzzle}`).')
        puzzle_date = puzzle_date_for(puzzle_number)

        existing = cf_common.user_db.get_minigame_result_for_user_puzzle(
            ctx.guild.id, AKARI_GAME.name, member.id, puzzle_number)
        if existing is not None:
            raise MinigameCogError(
                f'`{_safe_member_name(member)}` already has a result for '
                f'puzzle `{puzzle_number}`. Use `;mg akari remove` first.')

        result_label = 'perfect' if is_perfect else f'{accuracy}%'
        raw_content = (
            f'Daily Akari {puzzle_number}\n'
            f'{puzzle_date.isoformat()}\n'
            f'\U0001f3af {result_label} \U0001f553 {time_text}\n'
            f'[manually added by {ctx.author}]'
        )
        cf_common.user_db.save_minigame_result(
            ctx.message.id, ctx.guild.id, AKARI_GAME.name, ctx.channel.id,
            member.id, puzzle_number, puzzle_date.isoformat(),
            accuracy, time_seconds, is_perfect, raw_content)

        self._recompute_akari_ratings(ctx.guild.id)

        shown_time = display_time_text or format_duration(time_seconds)
        await ctx.send(embed=discord_common.embed_success(
            f'Added {AKARI_GAME.display_name} result for '
            f'`{_safe_member_name(member)}` on puzzle `{puzzle_number}` '
            f'({puzzle_date.isoformat()}): **{result_label}** in '
            f'**{shown_time}**.'))

    async def _cmd_akari_giveup(self, ctx, selector):
        """Record the invoking user's deliberate 0% result."""
        if selector is None:
            raise MinigameCogError(
                'Usage: `;akari giveup <date|#number>`.')
        if cf_common.user_db.is_akari_banned(ctx.guild.id, ctx.author.id):
            raise MinigameCogError(
                f'You are banned from posting {AKARI_GAME.display_name} '
                'results. Ask a moderator to lift the ban.')

        puzzle_date = self._parse_akari_date_or_number(selector)
        puzzle_number = _mg().expected_puzzle_number(puzzle_date)
        await self._cmd_akari_add(
            ctx, ctx.author, puzzle_number, '0%', '67:67:67',
            display_time_text='67:67:67')

    async def _cmd_akari_ratings(self, ctx, *, excluded_ids=None,
                                  included_ids=None, include_inactive=False,
                                  test_decay=False, weekly=False,
                                  weekdays=None, date_bounds=None, beta=False,
                                  time_only=False, current=False):
        """Guild leaderboard — registered, recently-active players only.

        ``excluded_ids`` / ``included_ids`` run an ad-hoc replay with the
        chosen filter applied and render the result, leaving the persisted
        snapshot untouched so the cache stays canonical.  ``test_decay``
        (the ``+test`` arg) also forces the ad-hoc replay, under the
        experimental decay model.

        ``include_inactive=True`` (the ``+inactive`` arg) skips the
        ``AKARI_RANKING_MAX_INACTIVE_DAYS`` cutoff so dormant players
        reappear on the board.  Garbage future puzzle numbers are still
        filtered out — they're never a real player, just a stale row.
        """
        self._require_enabled(ctx.guild.id, AKARI_GAME)
        self._validate_akari_beta(
            beta, test_decay=test_decay, weekly=weekly or current,
            time_only=time_only)
        if weekly:
            return await self._cmd_akari_completed_weekly_ratings(
                ctx, excluded_ids=excluded_ids, included_ids=included_ids,
                include_inactive=include_inactive, weekdays=weekdays,
                date_bounds=date_bounds)
        if current:
            return await self._cmd_akari_current_week_ratings(
                ctx, excluded_ids=excluded_ids, included_ids=included_ids,
                weekdays=weekdays, date_bounds=date_bounds)
        registrants = cf_common.user_db.get_akari_registrants(ctx.guild.id)
        # Banned players stay rated (forward-only ban) but are hidden from
        # public boards at display time, like Queens'; debug shows them.
        banned_ids = self._akari_banned_user_ids(ctx.guild.id)
        visible = registrants - banned_ids
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
        registered = [r for r in rows if r.user_id in visible]
        if not registered:
            raise MinigameCogError(
                f'No registered {AKARI_GAME.display_name} players yet. '
                f'Players opt in with `;mg akari register`.')
        shown = self._active_ranking_rows(
            registered, include_inactive=include_inactive)
        if not shown:
            if include_inactive:
                raise MinigameCogError(
                    f'No registered {AKARI_GAME.display_name} players yet.')
            raise MinigameCogError(
                f'No registered {AKARI_GAME.display_name} players active in '
                f'the last {constants.AKARI_RANKING_MAX_INACTIVE_DAYS} days. '
                f'Use `+inactive` to include dormant players.')
        # All shown users are registered, so the ✓ marker is redundant noise.
        title = ('Daily Akari Ratings (incl. inactive)'
                 if include_inactive else 'Daily Akari Ratings')
        if test_decay:
            title += ' [test decay]'
        title += _queens_improved_title_suffix(beta)
        title += ' [time only]' if time_only else ''
        title += _queens_filter_suffix(
            weekdays=weekdays, date_bounds=date_bounds)
        discord_file = _mg()._get_akari_rating_table_image_file(
            ctx.guild, shown, registrants, title=title,
            mark_registered=False)
        await ctx.send(file=discord_file)

    @staticmethod
    def _akari_banned_user_ids(guild_id):
        return {str(row.user_id)
                for row in cf_common.user_db.get_akari_bans(guild_id)}

    @staticmethod
    async def _send_akari_weekly_scores(ctx, standings):
        """Send the provisional current-week scores table (or an empty notice).

        Shared by the public and ``debug`` ratings commands so both render the
        in-progress week identically.
        """
        if not standings:
            await ctx.send(embed=discord_common.embed_neutral(
                'No Daily Akari scores have been posted this week yet.'))
            return
        start = standings[0].week_start
        end = standings[0].week_end
        score_title = (
            f'Daily Akari Current Weekly Ratings · {start:%b %d}–{end:%b %d} '
            f'(in progress)')
        score_file = _mg()._get_akari_weekly_table_image_file(
            ctx.guild, standings, title=score_title)
        await ctx.send(file=score_file)

    async def _akari_weekly_preview(self, guild_id, *, excluded_ids=None,
                                    included_ids=None, weekdays=None,
                                    date_bounds=None, as_of_date=None,
                                    standings_date=None):
        """Build weekly ratings plus provisional current-week standings."""
        result_rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, AKARI_GAME.name)
        result_rows = self._filter_akari_rows(
            result_rows, excluded_ids=excluded_ids,
            included_ids=included_ids)
        result_rows = _filter_queens_weekday_rows(result_rows, weekdays)
        result_rows = _filter_queens_rating_date_rows(result_rows, date_bounds)
        today = as_of_date or dt.date.today()
        standings_date = standings_date or today
        current_puzzle = _mg().expected_puzzle_number(today)
        wanted = set()
        for row in result_rows:
            try:
                row_date = dt.date.fromisoformat(str(row.puzzle_date))
            except ValueError:
                continue
            monday_number = int(row.puzzle_number) - row_date.weekday()
            wanted.update(range(monday_number, monday_number + 7))
        current_monday = week_start(today)
        monday_number = _mg().expected_puzzle_number(current_monday)
        wanted.update(range(monday_number, current_puzzle + 1))
        difficulties = await self._akari_difficulty_map(wanted)
        states = compute_weekly_ratings(
            result_rows, difficulties, as_of_date=today)
        rating_rows = sorted(
            states.values(), key=lambda s: (-s.rating, -s.games, int(s.user_id)))
        standings = current_week_standings(
            result_rows, difficulties, as_of_date=standings_date)
        return rating_rows, standings

    @staticmethod
    async def _akari_difficulty_map(puzzle_numbers):
        """Read cached difficulties and best-effort fetch any missing values."""
        puzzle_numbers = {int(number) for number in puzzle_numbers if int(number) > 0}
        cached = cf_common.user_db.get_akari_puzzle_difficulties(puzzle_numbers)
        missing = puzzle_numbers - set(cached)
        if not missing:
            return cached
        try:
            fetched = await fetch_akari_difficulties(missing)
        except Exception:
            logger.warning('Could not refresh Daily Akari difficulties',
                           exc_info=True)
            return cached
        valid = {number: difficulty for number, difficulty in fetched.items()
                 if 1 <= int(difficulty) <= 5}
        if valid:
            cf_common.user_db.upsert_akari_puzzle_difficulties(
                valid, time.time())
            cached.update(valid)
        return cached

    @staticmethod
    def _akari_test_decay_kwargs(test_decay):
        """Extra ``compute_ratings`` kwargs for the experimental ``+test`` decay.

        First missed day = virtual last-place loss (engine flag); later
        misses = flat, non-ramping pull, achieved by pinning ``decay_max``
        to ``decay_base`` so the streak scaling in ``_decay_rate`` vanishes.
        """
        if not test_decay:
            return {}
        return {
            'first_skip_last_place': True,
            'decay_max': constants.AKARI_DECAY_BASE,
        }

    def _akari_extra_compute_kwargs(
            self, test_decay=False, *, beta=False, time_only=False):
        """Akari overrides for the generic minigame replay helpers.

        Pins ``current_puzzle_number``/``max_puzzle`` through the
        monkeypatch-sensitive ``_mg().expected_puzzle_number`` (rather than
        ``AKARI_GAME.rating.current_puzzle_number_fn``, which resolves to the
        unpatched module function) and folds in the ``+test`` decay kwargs.
        """
        current_puzzle = _mg().expected_puzzle_number(dt.date.today())
        kwargs = {
            'current_puzzle_number': current_puzzle,
            'max_puzzle': current_puzzle + constants.AKARI_MAX_PUZZLE_LOOKAHEAD,
            **self._akari_test_decay_kwargs(test_decay),
        }
        if time_only:
            key = 'time_only' if beta else 'rank_fn'
            kwargs[key] = True if beta else rank_akari_time_participants
        return kwargs
