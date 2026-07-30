"""Experimental margin-aware multiplayer Elo replay for LinkedIn Queens.

The canonical Queens ladder remains Codeforces-style.  This module powers only
the opt-in ``+improved`` views and deliberately uses a calmer, time-sensitive
model:

* every opponent contributes a bounded fraction of one daily result;
* close times behave almost like ties instead of full wins/losses;
* the field is averaged, so a 20-player day is not 19 independent games;
* the averaged residual is naturally bounded by the K-factor and preserves a
  zero-sum round without post-hoc clipping;
* player-facing rating points use a wider scale so sustained skill differences
  span the existing minigame rank tiers.

Displayed performance uses the same soft time bracket as rating changes.  A
neutral self-comparison keeps the best and worst performance finite, while a
single extreme time can affect every other player by only ``1 / field_size``.
"""

import math
from dataclasses import dataclass

from tle.util.akari_rating import HistoryPoint, RatingState


_START_RATING = 1200.0
# Rating scales have arbitrary units.  The original beta's sound latent model
# occupied only half of the rank bands used by Queens/Akari, so expose two
# player-facing points per original beta point.  Scaling the expectation curve,
# K, and performance search together preserves every probability and ordering.
_RATING_POINT_SCALE = 2.0
# Traditional Elo's base-10 scale, widened into player-facing rating points.
_ELO_SCALE = _RATING_POINT_SCALE * 400.0 / math.log(10.0)
# Adding a few seconds before taking logs stops a one-second gap on a very fast
# Monday from looking like an enormous percentage difference.
_TIME_OFFSET_SECONDS = 4
_TIME_MARGIN_WIDTH = 0.35
# This is a bound on one pair's *evidence*, not on a player's rating change.
# It activates only beyond a 16.4x adjusted-time ratio and prevents malformed
# or repeated extreme margins from producing numerical 0/1 separation.
_TIME_MARGIN_LOGIT_LIMIT = 8.0
_RATING_K = _RATING_POINT_SCALE * 72.0
_PERFORMANCE_SEARCH_MARGIN = _RATING_POINT_SCALE * 800.0
_PERFORMANCE_SEARCH_ITERS = 60


@dataclass(frozen=True)
class _Player:
    rating: float = _START_RATING
    games: int = 0
    peak: float = _START_RATING
    last_delta: float = 0.0
    skip_streak: int = 0
    last_puzzle: int = 0


@dataclass(frozen=True)
class _RoundUpdate:
    delta: float
    performance: float


def _sigmoid(value):
    """Numerically stable logistic function."""
    if value >= 0:
        exp_neg = math.exp(-value)
        return 1.0 / (1.0 + exp_neg)
    exp_pos = math.exp(value)
    return exp_pos / (1.0 + exp_pos)


def _time_log(time_seconds):
    """Return the softened log-time used by the daily performance bracket."""
    try:
        if isinstance(time_seconds, int):
            seconds = time_seconds
        else:
            seconds = float(time_seconds)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            f'Queens time must be numeric, got {time_seconds!r}.') from exc
    if (
            isinstance(seconds, float) and not math.isfinite(seconds)
            or seconds < 0):
        raise ValueError(
            f'Queens time must be finite and non-negative, '
            f'got {time_seconds!r}.')
    # Keep integer inputs as integers so even an unexpectedly huge legacy value
    # can be logged without overflowing an intermediate float conversion.
    return math.log(seconds + _TIME_OFFSET_SECONDS)


def _result_time_seconds(time_seconds):
    """Validate one stored result time, returning its integral seconds."""
    try:
        seconds = int(time_seconds)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            f'Queens result time must be an integer, got {time_seconds!r}.'
        ) from exc
    if seconds <= 0:
        raise ValueError(
            f'Queens result time must be positive, got {time_seconds!r}.')
    # Reject fractional floats rather than silently truncating them.  SQLite
    # rows use integers, but this also protects imported/test row-like objects.
    if isinstance(time_seconds, float) and time_seconds != seconds:
        raise ValueError(
            f'Queens result time must be an integer, got {time_seconds!r}.')
    _time_log(seconds)
    return seconds


def _soft_time_score(time_self, time_other):
    """Soft result for ``time_self`` against ``time_other``.

    Lower is better.  Equal times return exactly 0.5; increasingly large gaps
    approach a full 1/0 result without ever making performance infinite.
    """
    return _soft_time_score_from_logs(
        _time_log(time_self), _time_log(time_other))


def _soft_time_score_from_logs(log_self, log_other):
    """Return bounded pair evidence from two already-transformed times."""
    logit = (log_other - log_self) / _TIME_MARGIN_WIDTH
    logit = max(
        -_TIME_MARGIN_LOGIT_LIMIT,
        min(_TIME_MARGIN_LOGIT_LIMIT, logit),
    )
    return _sigmoid(logit)


def _elo_expected(rating, opponent_rating):
    return _sigmoid((float(rating) - float(opponent_rating)) / _ELO_SCALE)


def _field_expected(performance, field_ratings):
    return sum(
        _elo_expected(performance, rating) for rating in field_ratings
    ) / len(field_ratings)


def _performance_rating(field_ratings, target_score):
    """Invert the common field expectation for a finite performance rating."""
    lo = min(field_ratings) - _PERFORMANCE_SEARCH_MARGIN
    hi = max(field_ratings) + _PERFORMANCE_SEARCH_MARGIN
    span = _PERFORMANCE_SEARCH_MARGIN

    # The neutral self-result keeps target_score strictly inside (0, 1), but
    # expand defensively for unusually large fields or rating spreads.
    while _field_expected(lo, field_ratings) > target_score:
        span *= 2.0
        lo -= span
    span = _PERFORMANCE_SEARCH_MARGIN
    while _field_expected(hi, field_ratings) < target_score:
        span *= 2.0
        hi += span

    for _ in range(_PERFORMANCE_SEARCH_ITERS):
        mid = (lo + hi) / 2.0
        if _field_expected(mid, field_ratings) < target_score:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def _compute_round(ratings, times):
    """Return naturally bounded, zero-sum updates for one multiplayer day."""
    users = sorted(ratings)
    if set(users) != set(times):
        raise ValueError('Queens round ratings and times must have the same users.')
    if len(users) < 2:
        return {
            user: _RoundUpdate(delta=0.0, performance=float(ratings[user]))
            for user in users
        }

    time_logs = {user: _time_log(times[user]) for user in users}
    actual = {
        user: sum(
            _soft_time_score_from_logs(
                time_logs[user], time_logs[opponent])
            for opponent in users
        ) / len(users)
        for user in users
    }
    return _compute_round_from_actual(ratings, actual)


def _compute_round_from_actual(ratings, actual):
    """Convert complementary field scores into beta deltas/performance."""
    users = sorted(ratings)
    if set(users) != set(actual):
        raise ValueError(
            'Beta round ratings and actual scores must have the same users.')
    if len(users) < 2:
        return {
            user: _RoundUpdate(delta=0.0, performance=float(ratings[user]))
            for user in users
        }

    field_ratings = [float(ratings[user]) for user in users]
    expected = {}
    for user in users:
        # The pair-score average includes a neutral self-comparison. It
        # cancels out of the residual and keeps extreme performances finite.
        expected[user] = _field_expected(ratings[user], field_ratings)

    raw_deltas = {
        user: _RATING_K * (actual[user] - expected[user])
        for user in users
    }

    return {
        user: _RoundUpdate(
            delta=raw_deltas[user],
            performance=_performance_rating(field_ratings, actual[user]),
        )
        for user in users
    }


def _compute_pair_round(ratings, rows, pair_score_fn):
    """Run a beta round using a game-specific complementary pair score."""
    users = sorted(ratings)
    if set(users) != set(rows):
        raise ValueError('Beta round ratings and rows must have the same users.')
    actual = {
        user: sum(
            0.5 if opponent == user
            else pair_score_fn(rows[user], rows[opponent])
            for opponent in users
        ) / len(users)
        for user in users
    }
    return _compute_round_from_actual(ratings, actual)


def _row_order_key(row):
    """Stable first-submission key used for defensive per-user/day deduping."""
    message_id = getattr(row, 'message_id', None)
    try:
        message_key = (0, int(message_id))
    except (TypeError, ValueError):
        message_key = (1, '' if message_id is None else str(message_id))
    time_seconds = getattr(row, 'time_seconds', None)
    try:
        time_key = (0, int(time_seconds))
    except (TypeError, ValueError, OverflowError):
        time_key = (1, repr(time_seconds))
    return (
        message_key,
        str(getattr(row, 'puzzle_date', '')),
        time_key,
        -int(bool(getattr(row, 'is_perfect', False))),
        -int(getattr(row, 'accuracy', 0)),
        str(getattr(row, 'raw_content', '')),
    )


def _history_point(puzzle_number, row, rating, delta, performance):
    return HistoryPoint(
        puzzle_number=puzzle_number,
        puzzle_date=getattr(row, 'puzzle_date', None),
        rating=rating,
        delta=delta,
        performance=performance,
        is_perfect=bool(getattr(row, 'is_perfect', False)),
        accuracy=int(getattr(row, 'accuracy', 0)),
        time_seconds=int(getattr(row, 'time_seconds', 0)),
    )


def compute_queens_improved_ratings(
        rows, *, max_puzzle=None, histories=None,
        include_decay_in_history=False, current_puzzle_number=None,
        rank_fn=None, pair_score_fn=None, **_ignored):
    """Replay Queens results with the experimental soft-bracket Elo model.

    The return and history shapes match :func:`compute_ratings`, so every
    existing ``+improved`` table and graph can use this engine without storing
    a second rating snapshot.  Queens inactivity never changes visible skill;
    ``include_decay_in_history`` and ``rank_fn`` are accepted only for shared
    engine compatibility.
    """
    del include_decay_in_history, rank_fn

    by_puzzle = {}
    for row in rows:
        puzzle_number = int(row.puzzle_number)
        if puzzle_number < 1:
            continue
        if max_puzzle is not None and puzzle_number > max_puzzle:
            continue
        by_puzzle.setdefault(puzzle_number, []).append(row)

    players = {}
    for puzzle_number in sorted(by_puzzle):
        day_rows = {}
        for row in sorted(by_puzzle[puzzle_number], key=_row_order_key):
            day_rows.setdefault(str(row.user_id), row)
        valid_day_rows = {}
        for user_id, row in day_rows.items():
            try:
                _result_time_seconds(row.time_seconds)
            except ValueError:
                # A malformed locked first result must not become a zero-second
                # win, seed a ghost player, or break every +improved command.
                # Do this after first-attempt deduplication so a later share
                # cannot replace the quarantined first one.
                continue
            valid_day_rows[user_id] = row
        day_rows = valid_day_rows
        active_ids = sorted(day_rows)

        for user_id in active_ids:
            players.setdefault(
                user_id, _Player(last_puzzle=puzzle_number))

        if len(active_ids) < 2:
            for user_id in active_ids:
                old = players[user_id]
                players[user_id] = _Player(
                    rating=old.rating,
                    games=old.games,
                    peak=old.peak,
                    last_delta=0.0,
                    skip_streak=0,
                    last_puzzle=puzzle_number,
                )
                if histories is not None:
                    histories.setdefault(user_id, []).append(_history_point(
                        puzzle_number, day_rows[user_id], old.rating, 0.0, None))
            continue

        before = {
            user_id: players[user_id].rating for user_id in active_ids
        }
        times = {
            user_id: _result_time_seconds(day_rows[user_id].time_seconds)
            for user_id in active_ids
        }
        updates = (
            _compute_round(before, times)
            if pair_score_fn is None
            else _compute_pair_round(before, day_rows, pair_score_fn)
        )

        for user_id in active_ids:
            old = players[user_id]
            update = updates[user_id]
            new_rating = old.rating + update.delta
            players[user_id] = _Player(
                rating=new_rating,
                games=old.games + 1,
                peak=max(old.peak, new_rating),
                last_delta=update.delta,
                skip_streak=0,
                last_puzzle=puzzle_number,
            )
            if histories is not None:
                histories.setdefault(user_id, []).append(_history_point(
                    puzzle_number,
                    day_rows[user_id],
                    new_rating,
                    update.delta,
                    update.performance,
                ))

        day_concluded = (
            current_puzzle_number is None
            or puzzle_number < current_puzzle_number
        )
        if day_concluded:
            for user_id in sorted(players):
                if user_id in day_rows:
                    continue
                old = players[user_id]
                players[user_id] = _Player(
                    rating=old.rating,
                    games=old.games,
                    peak=old.peak,
                    last_delta=0.0,
                    skip_streak=old.skip_streak + 1,
                    last_puzzle=old.last_puzzle,
                )

    return {
        user_id: RatingState(
            user_id=user_id,
            rating=player.rating,
            games=player.games,
            peak=player.peak,
            last_delta=player.last_delta,
            skip_streak=player.skip_streak,
            last_puzzle=player.last_puzzle,
        )
        for user_id, player in sorted(players.items())
    }
