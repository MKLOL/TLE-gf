"""Akari add/ratings and rating-replay helpers. (Minigames cog impl mixin; see minigames.py)."""

import datetime as dt
import logging
import time


from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util.minigame_rating import compute_ratings
from tle.util.akari_difficulty import fetch_akari_difficulties
from tle.util.akari_weekly import (
    compute_weekly_ratings, current_week_standings, week_start,
)

from tle.cogs._minigame_common import (
    format_duration,
)
from tle.cogs._minigame_akari import (
    AKARI_GAME, puzzle_date_for,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError, _mg, _safe_member_name,
)
from tle.cogs._minigame_tables import (
    _PuzzlePlayerInfo,
)

logger = logging.getLogger(__name__)


class ImplAkariAMixin:
    async def _cmd_akari_add(self, ctx, member, puzzle_number, result_text, time_text):
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

        await ctx.send(embed=discord_common.embed_success(
            f'Added {AKARI_GAME.display_name} result for '
            f'`{_safe_member_name(member)}` on puzzle `{puzzle_number}` '
            f'({puzzle_date.isoformat()}): **{result_label}** in '
            f'**{format_duration(time_seconds)}**.'))

    async def _cmd_akari_ratings(self, ctx, *, excluded_ids=None,
                                  included_ids=None, include_inactive=False,
                                  test_decay=False, weekly=False):
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
        if weekly:
            rows, standings = await self._akari_weekly_preview(
                ctx.guild.id,
                excluded_ids=excluded_ids,
                included_ids=included_ids,
            )
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
        registrants = cf_common.user_db.get_akari_registrants(ctx.guild.id)
        registered = [r for r in rows if r.user_id in registrants]
        if not registered and not (weekly and standings):
            raise MinigameCogError(
                f'No registered {AKARI_GAME.display_name} players yet. '
                f'Players opt in with `;mg akari register`.')
        shown = self._active_ranking_rows(
            registered, include_inactive=include_inactive)
        if not shown and not (weekly and standings):
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
        if test_decay and not weekly:
            title += ' [test decay]'
        if weekly:
            title += ' [weekly preview]'
        if shown:
            table_kwargs = {'games_label': 'Weeks'} if weekly else {}
            discord_file = _mg()._get_akari_rating_table_image_file(
                ctx.guild, shown, registrants, title=title,
                mark_registered=False,
                **table_kwargs)
            await ctx.send(file=discord_file)
        if weekly:
            if not standings:
                await ctx.send(embed=discord_common.embed_neutral(
                    'No Daily Akari scores have been posted this week yet.'))
                return
            start = standings[0].week_start
            end = standings[0].week_end
            score_title = (
                f'Daily Akari Weekly Scores · {start:%b %d}–{end:%b %d} '
                f'(in progress)')
            score_file = _mg()._get_akari_weekly_table_image_file(
                ctx.guild, standings, title=score_title)
            await ctx.send(file=score_file)

    async def _akari_weekly_preview(self, guild_id, *, excluded_ids=None,
                                    included_ids=None):
        """Build weekly ratings plus provisional current-week standings."""
        result_rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, AKARI_GAME.name)
        result_rows = self._filter_akari_rows(
            result_rows, excluded_ids=excluded_ids,
            included_ids=included_ids)
        today = dt.date.today()
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
            result_rows, difficulties, as_of_date=today)
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
        if fetched:
            cf_common.user_db.upsert_akari_puzzle_difficulties(
                fetched, time.time())
            cached.update(fetched)
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

    def _akari_user_history(self, guild_id, user_id, *, include_decay=False,
                            excluded_ids=None, included_ids=None,
                            test_decay=False):
        """Replay the guild's results and return one user's per-day history.

        Shared by the rating and performance graphs — the replay is the same;
        each caller picks the field it needs off the :class:`HistoryPoint`s.
        ``include_decay=True`` additionally emits one entry per absent puzzle
        day for the rating graph's ``+decay`` mode.  ``excluded_ids`` and
        ``included_ids`` (sets of stringified user IDs) compose the include /
        exclude filter before the replay so the queried user's history
        reflects only the surviving field.  ``test_decay=True`` replays under
        the experimental decay model (see ``_akari_test_decay_kwargs``).
        """
        state, history = self._akari_user_data(
            guild_id, user_id, include_decay=include_decay,
            excluded_ids=excluded_ids, included_ids=included_ids,
            test_decay=test_decay)
        del state  # this helper returns history only; callers needing both use _akari_user_data
        return history

    def _akari_user_data(self, guild_id, user_id, *, include_decay=False,
                          excluded_ids=None, included_ids=None,
                          test_decay=False):
        """One replay, two artefacts: ``(RatingState, [HistoryPoint])`` for one user.

        Used by rating / performance commands that show both an embed (needs
        the snapshot-shaped state) and a graph (needs the history).  Saves a
        second replay versus calling ``_akari_user_history`` separately.
        Returns ``(None, [])`` when the user has no rated days.
        """
        result_rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, AKARI_GAME.name)
        result_rows = self._filter_akari_rows(
            result_rows, excluded_ids=excluded_ids, included_ids=included_ids)
        current_puzzle = _mg().expected_puzzle_number(dt.date.today())
        max_puzzle = current_puzzle + constants.AKARI_MAX_PUZZLE_LOOKAHEAD
        histories = {}
        states = compute_ratings(
            result_rows, max_puzzle=max_puzzle, histories=histories,
            include_decay_in_history=include_decay,
            current_puzzle_number=current_puzzle,
            **self._akari_test_decay_kwargs(test_decay))
        key = str(user_id)
        return states.get(key), histories.get(key, [])

    def _akari_filtered_rating_rows(self, guild_id, *, excluded_ids=None,
                                     included_ids=None, test_decay=False):
        """Fresh leaderboard states with some users excluded/included — bypasses cache.

        Used by ``;mg akari ratings +exclude=...`` / ``+include=...`` so the
        persisted snapshot (the canonical rating store) stays untouched while
        we render an ad-hoc view.  Returns the same
        ``rating DESC, games DESC, user_id ASC`` order ``get_akari_ratings``
        produces, so the rest of the rendering path doesn't care which source
        it got.
        """
        rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, AKARI_GAME.name)
        rows = self._filter_akari_rows(
            rows, excluded_ids=excluded_ids, included_ids=included_ids)
        current_puzzle = _mg().expected_puzzle_number(dt.date.today())
        max_puzzle = current_puzzle + constants.AKARI_MAX_PUZZLE_LOOKAHEAD
        states = compute_ratings(
            rows, max_puzzle=max_puzzle,
            current_puzzle_number=current_puzzle,
            **self._akari_test_decay_kwargs(test_decay))
        return sorted(
            states.values(),
            key=lambda s: (-s.rating, -s.games, int(s.user_id)),
        )

    def _akari_puzzle_change_info(self, guild_id, puzzle_number,
                                   *, excluded_ids=None, included_ids=None,
                                   test_decay=False):
        """Map ``user_id -> _PuzzlePlayerInfo(pre_rating, delta)`` for puzzle N.

        Replays the full guild history once and pulls each user's HistoryPoint
        for the target puzzle; the pre-contest rating is the post-contest one
        minus the day's delta (so first-timers get the seed value, 1200).
        Used by ``;mg akari stats <puzzle>`` to colour each row by the
        player's pre-puzzle tier (post-puzzle would be circular) and to fill
        the Δ column with the day's signed change.  ``excluded_ids`` /
        ``included_ids`` apply the same include / exclude filter as the rest
        of the command surface so the surfaced pre-rating and delta reflect
        the chosen field.
        """
        result_rows = cf_common.user_db.get_minigame_results_for_guild(
            guild_id, AKARI_GAME.name)
        result_rows = self._filter_akari_rows(
            result_rows, excluded_ids=excluded_ids, included_ids=included_ids)
        current_puzzle = _mg().expected_puzzle_number(dt.date.today())
        max_puzzle = current_puzzle + constants.AKARI_MAX_PUZZLE_LOOKAHEAD
        histories = {}
        compute_ratings(
            result_rows, max_puzzle=max_puzzle, histories=histories,
            current_puzzle_number=current_puzzle,
            **self._akari_test_decay_kwargs(test_decay))
        info = {}
        for user_id, points in histories.items():
            for point in points:
                if point.puzzle_number == puzzle_number:
                    info[user_id] = _PuzzlePlayerInfo(
                        pre_rating=point.rating - point.delta,
                        delta=point.delta,
                    )
                    break
        return info

    async def _extract_akari_filters(self, ctx, args):
        """Pull akari-wide filter flags out of ``args``.

        Recognised flags:

        - ``+decay``: include decay days in history/graph output
        - ``+test``: preview the experimental decay model (first missed day
          costs a virtual last-place finish; later misses use a flat,
          non-ramping pull).  Forces a fresh replay — the persisted rating
          snapshot is never touched, so this is a safe what-if view.
        - ``+inactive``: keep players whose last puzzle is older than
          ``AKARI_RANKING_MAX_INACTIVE_DAYS`` (default behaviour hides them
          from the ratings leaderboard).  Only meaningful for commands that
          surface an active-only leaderboard; harmless elsewhere.
        - ``+exclude=user1,user2,...``: pretend the listed users never played;
          they drop out of result tables, leaderboards, and every other user's
          rating calculation
        - ``+include=user1,user2,...``: the inverse — *only* the listed users
          count; everyone else is dropped before the replay.  When both flags
          are supplied they compose: the universe shrinks to the include set
          first, then the exclude set is removed from it.

        Each comma-separated name is resolved via the usual case-insensitive
        member converter, so mentions / display names / raw IDs all work.

        Returns ``(remaining_args, include_decay, excluded_ids, included_ids,
        include_inactive, test_decay)``.  Unknown flags pass through in
        ``remaining_args``; the caller decides whether they're a member, a
        puzzle selector, or an error.
        """
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
        """Apply ``+include`` and ``+exclude`` filters to a result-row iterable.

        Include narrows first, exclude trims; composition is the natural
        intersection of the two sets minus the excluded ones.  Both arguments
        accept ``None`` / empty set for "no filter", and the function returns
        the input untouched in that case.
        """
        if included_ids:
            rows = [r for r in rows if str(r.user_id) in included_ids]
        if excluded_ids:
            rows = [r for r in rows if str(r.user_id) not in excluded_ids]
        return rows

    async def _parse_akari_rating_args(self, ctx, args, *, member_required=False):
        """Pull ``+decay`` / ``+test`` / ``+inactive`` / ``+exclude=`` /
        ``+include=`` and zero-or-more members out of the args.

        Returns ``(members, include_decay, excluded_ids, included_ids,
        include_inactive, test_decay)``.  Every remaining token is resolved
        via the case-insensitive member converter, so the rating / performance
        graphs can plot multiple users at once (``;mg akari rating @alice @bob``).
        An empty list falls back to ``[ctx.author]`` unless
        ``member_required=True`` (the ``debug`` subcommands), which then
        errors with a usage hint.
        """
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

