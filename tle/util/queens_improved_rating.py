"""Experimental margin-aware multiplayer Elo replay for LinkedIn Queens.

The canonical Queens ladder remains Codeforces-style.  This module powers only
the opt-in ``+beta`` views and deliberately uses a bounded hybrid model:

* every opponent contributes a bounded fraction of one daily result;
* 75% of each pair score comes from the time margin and 25% from the hard
  faster/slower result, so close wins still matter;
* the field is averaged, so a 20-player day is not 19 independent games;
* a proper log-loss/Brier blend smoothly reduces one surprising day's
  leverage without a post-hoc delta cap;
* complementary pair updates preserve an exactly zero-sum round;
* player-facing rating points use a wider scale so sustained skill differences
  span the existing minigame rank tiers.

Displayed performance uniquely inverts the common field expectation from the
mean hybrid result. This keeps result order monotone even though the robust
update loss itself can be non-convex. A neutral self-comparison keeps the best
and worst performance finite, while a single extreme time can affect every
other player by only ``1 / field_size``.
"""

import math
from dataclasses import dataclass

from tle.util.akari_rating import HistoryPoint, RatingState
from tle.util._beta_rating_performance import (
    _BRIER_BLEND,
    _ELO_SCALE,
    _RATING_POINT_SCALE,
    _elo_expected,
    _field_expected,
    _performance_rating,
    _proper_residual,
    _sigmoid,
)


_START_RATING = 1200.0
# Rating scales have arbitrary units.  The original beta's sound latent model
# occupied only half of the rank bands used by Queens/Akari, so expose two
# player-facing points per original beta point.  Scaling the expectation curve,
# K, and performance search together preserves every probability and ordering.
_TIME_MARGIN_WIDTH = 0.35
_HEAD_TO_HEAD_WEIGHT = 0.25
# This is a bound on one pair's *evidence*, not on a player's rating change.
# It activates only beyond a 16.4x raw-time ratio and prevents malformed
# or repeated extreme margins from producing numerical 0/1 separation.
_TIME_MARGIN_LOGIT_LIMIT = 8.0
_RATING_K = _RATING_POINT_SCALE * 62.0


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


def _time_log(time_seconds):
    """Return the raw log-time used by the daily performance bracket."""
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
            or seconds <= 0):
        raise ValueError(
            f'Queens time must be finite and positive, '
            f'got {time_seconds!r}.')
    # Keep integer inputs as integers so even an unexpectedly huge legacy value
    # can be logged without overflowing an intermediate float conversion.
    return math.log(seconds)


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
    if not isinstance(time_seconds, str) and time_seconds != seconds:
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


def _hard_time_score(time_self, time_other):
    """Return the strict faster/slower result, with exact ties neutral."""
    if time_self == time_other:
        return 0.5
    return float(time_self < time_other)


def _blend_pair_score(margin_score, head_to_head_score):
    """Blend continuous margin evidence with one bounded hard result."""
    return (
        (1.0 - _HEAD_TO_HEAD_WEIGHT) * margin_score
        + _HEAD_TO_HEAD_WEIGHT * head_to_head_score
    )


def _hybrid_time_score(time_self, time_other):
    """Return the 75% time-margin, 25% head-to-head pair score."""
    self_seconds = _result_time_seconds(time_self)
    other_seconds = _result_time_seconds(time_other)
    return _blend_pair_score(
        _soft_time_score(self_seconds, other_seconds),
        _hard_time_score(self_seconds, other_seconds),
    )


def _compute_round(ratings, times, *, compute_performance=True):
    """Return naturally bounded, zero-sum updates for one multiplayer day."""
    users = sorted(ratings)
    if set(users) != set(times):
        raise ValueError('Queens round ratings and times must have the same users.')
    if len(users) < 2:
        return {
            user: _RoundUpdate(delta=0.0, performance=float(ratings[user]))
            for user in users
        }

    normalized_times = {
        user: _result_time_seconds(times[user]) for user in users
    }
    time_logs = {
        user: _time_log(normalized_times[user]) for user in users
    }
    return _compute_round_from_pair_score(
        ratings,
        lambda user, opponent: _blend_pair_score(
            _soft_time_score_from_logs(
                time_logs[user], time_logs[opponent]),
            _hard_time_score(
                normalized_times[user], normalized_times[opponent]),
        ),
        compute_performance=compute_performance,
    )


def _compute_round_from_pair_score(
        ratings, pair_score_fn, *, compute_performance=True,
        performance_pair_score_fn=None):
    """Convert update scores and optional display scores into one beta round."""
    users = sorted(ratings)
    if len(users) < 2:
        return {
            user: _RoundUpdate(delta=0.0, performance=float(ratings[user]))
            for user in users
        }

    field_ratings = [float(ratings[user]) for user in users]
    scores_by_user = {}
    residuals_by_user = {}
    for user in users:
        scores = []
        residuals = []
        for opponent in users:
            update_score = (
                0.5 if opponent == user
                else float(pair_score_fn(user, opponent))
            )
            if (not math.isfinite(update_score)
                    or not 0 <= update_score <= 1):
                raise ValueError(
                    f'Beta pair score must be in [0, 1], '
                    f'got {update_score}.')
            expected = _elo_expected(ratings[user], ratings[opponent])
            residuals.append(_proper_residual(update_score, expected))
            performance_score = update_score
            if (compute_performance
                    and performance_pair_score_fn is not None
                    and opponent != user):
                performance_score = float(
                    performance_pair_score_fn(user, opponent))
                if (not math.isfinite(performance_score)
                        or not 0 <= performance_score <= 1):
                    raise ValueError(
                        'Beta performance pair score must be in [0, 1], '
                        f'got {performance_score}.')
            scores.append(performance_score)
        scores_by_user[user] = scores
        residuals_by_user[user] = residuals

    return {
        user: _RoundUpdate(
            delta=_RATING_K * sum(residuals_by_user[user]) / len(users),
            performance=(
                _performance_rating(
                    field_ratings,
                    sum(scores_by_user[user]) / len(users),
                )
                if compute_performance else None
            ),
        )
        for user in users
    }


def _compute_pair_round(
        ratings, rows, pair_score_fn, *, compute_performance=True,
        performance_pair_score_fn=None):
    """Run a beta round using a game-specific complementary pair score."""
    users = sorted(ratings)
    if set(users) != set(rows):
        raise ValueError('Beta round ratings and rows must have the same users.')
    return _compute_round_from_pair_score(
        ratings,
        lambda user, opponent: pair_score_fn(
            rows[user], rows[opponent]),
        compute_performance=compute_performance,
        performance_pair_score_fn=(
            None if performance_pair_score_fn is None
            else lambda user, opponent: performance_pair_score_fn(
                rows[user], rows[opponent])
        ),
    )


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
    raw_accuracy = getattr(row, 'accuracy', 0)
    try:
        accuracy_key = (0, -int(raw_accuracy))
    except (TypeError, ValueError, OverflowError):
        accuracy_key = (1, repr(raw_accuracy))
    return (
        message_key,
        str(getattr(row, 'puzzle_date', '')),
        time_key,
        -int(bool(getattr(row, 'is_perfect', False))),
        accuracy_key,
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
        rank_fn=None, pair_score_fn=None, row_validator_fn=None,
        performance_pair_score_fn=None, performance_puzzles=None, **_ignored):
    """Replay Queens results with the experimental hybrid-bracket Elo model.

    The return and history shapes match :func:`compute_ratings`, so every
    existing ``+beta`` table and graph can use this engine without storing
    a second rating snapshot.  Queens inactivity never changes visible skill;
    ``include_decay_in_history`` and ``rank_fn`` are accepted only for shared
    engine compatibility. A custom ``performance_pair_score_fn`` can decouple
    event-performance ordering from rating evidence, but requires a custom
    ``pair_score_fn`` and never affects deltas.
    """
    del include_decay_in_history, rank_fn
    if performance_pair_score_fn is not None and pair_score_fn is None:
        raise ValueError(
            'A performance pair score requires a rating pair score.')
    if performance_puzzles is not None:
        performance_puzzles = {
            int(puzzle_number) for puzzle_number in performance_puzzles
        }

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
                if row_validator_fn is not None:
                    row_validator_fn(row)
            except ValueError:
                # A malformed locked first result must not become a zero-second
                # win, seed a ghost player, or break every +beta command.
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
        compute_performance = (
            histories is not None
            and (
                performance_puzzles is None
                or puzzle_number in performance_puzzles
            )
        )
        updates = (
            _compute_round(
                before, times,
                compute_performance=compute_performance)
            if pair_score_fn is None
            else _compute_pair_round(
                before, day_rows, pair_score_fn,
                compute_performance=compute_performance,
                performance_pair_score_fn=performance_pair_score_fn)
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
