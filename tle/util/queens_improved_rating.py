"""Experimental margin-aware multiplayer Elo replay for LinkedIn Queens.

The canonical Queens ladder remains Codeforces-style.  This module powers only
the opt-in ``+beta`` views and deliberately uses a bounded hybrid model:

* every opponent contributes a bounded fraction of one daily result;
* 85% of each pair score comes from the time margin and 15% from the hard
  faster/slower result, so close wins still matter;
* the field is averaged, so a 20-player day is not 19 independent games;
* a proper log-loss/Brier blend smoothly reduces one surprising day's
  leverage without a post-hoc delta cap;
* complementary pair evidence is zero-sum before a small field correction;
* each rated participant then contributes 0.25 points of deflation to offset
  rating parked by short-lived accounts and, for Akari, redistributed decay;
* each game's player-facing point scale is calibrated independently while the
  shared latent proper-score model remains identical.

Displayed performance uniquely inverts the common field expectation from the
mean hybrid result. This keeps result order monotone even though the robust
update loss itself can be non-convex. A neutral self-comparison keeps the best
and worst performance finite, while a single extreme time can affect every
other player by only ``1 / field_size``.
"""

import math
from dataclasses import dataclass

from tle.util.akari_rating import HistoryPoint, RatingState, _decay_rate
from tle.util._beta_rating_performance import (
    _BRIER_BLEND,
    _ELO_SCALE,
    _RATING_POINT_SCALE,
    _elo_expected,
    _field_expected,
    _performance_rating,
    _proper_residual,
)
from tle.util._beta_rating_time import (
    _HEAD_TO_HEAD_WEIGHT,
    _TIME_MARGIN_LOGIT_LIMIT,
    _TIME_MARGIN_WIDTH,
    _blend_pair_score,
    _hard_time_score,
    _hybrid_time_score,
    _result_time_seconds,
    _soft_time_score,
    _soft_time_score_from_logs,
    _time_log,
)


_START_RATING = 1200.0
# Rating scales have arbitrary units. Queens keeps the established 2x display
# coordinate; Akari supplies its independently calibrated coordinate through
# ``rating_point_scale``. Within a raw contest, scaling the expectation curve,
# K, performance search, and ratings together preserves every probability,
# normalized update, and ordering. The fixed field policy stays in display
# points and is intentionally separate from that coordinate transformation.
_BASE_RATING_K = 62.0
_RATING_K = _RATING_POINT_SCALE * _BASE_RATING_K
# The pairwise model is naturally zero-sum, but the visible active pool is not:
# short-lived players can leave below the starting rating, while Akari's decay
# moves points from inactive players to active ones. Apply only the lightweight
# field-wide part of the Codeforces correction. There is deliberately no
# strongest-player correction in the beta ladder.
_FIELD_DEFLATION = 0.25


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


def _apply_field_correction(updates):
    """Center one rated field, then remove 0.25 points per participant."""
    if len(updates) < 2:
        return updates
    shift = (
        -sum(update.delta for update in updates.values()) / len(updates)
        - _FIELD_DEFLATION
    )
    return {
        user: _RoundUpdate(
            delta=update.delta + shift,
            performance=update.performance,
        )
        for user, update in updates.items()
    }


def _compute_round(
        ratings, times, *, compute_performance=True,
        rating_point_scale=_RATING_POINT_SCALE):
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
        rating_point_scale=rating_point_scale,
    )


def _compute_round_from_pair_score(
        ratings, pair_score_fn, *, compute_performance=True,
        performance_pair_score_fn=None,
        rating_point_scale=_RATING_POINT_SCALE):
    """Convert update scores and optional display scores into one beta round."""
    users = sorted(ratings)
    if len(users) < 2:
        return {
            user: _RoundUpdate(delta=0.0, performance=float(ratings[user]))
            for user in users
        }

    rating_point_scale = float(rating_point_scale)
    if not math.isfinite(rating_point_scale) or rating_point_scale <= 0:
        raise ValueError('Beta rating point scale must be finite and positive.')
    rating_k = rating_point_scale * _BASE_RATING_K
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
            expected = _elo_expected(
                ratings[user], ratings[opponent],
                point_scale=rating_point_scale)
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
            delta=rating_k * sum(residuals_by_user[user]) / len(users),
            performance=(
                _performance_rating(
                    field_ratings,
                    sum(scores_by_user[user]) / len(users),
                    point_scale=rating_point_scale,
                )
                if compute_performance else None
            ),
        )
        for user in users
    }


def _compute_pair_round(
        ratings, rows, pair_score_fn, *, compute_performance=True,
        performance_pair_score_fn=None,
        rating_point_scale=_RATING_POINT_SCALE):
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
        rating_point_scale=rating_point_scale,
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
        rank_fn=None, start_rating=None, decay_base=None, decay_max=None,
        decay_grace=None, pair_score_fn=None, row_validator_fn=None,
        performance_pair_score_fn=None, performance_puzzles=None,
        rating_point_scale=_RATING_POINT_SCALE, **_ignored):
    """Replay Queens results with the experimental hybrid-bracket Elo model.

    The return and history shapes match :func:`compute_ratings`, so every
    existing ``+beta`` table and graph can use this engine without storing
    a second rating snapshot. Inactivity decay is off unless the caller passes
    decay parameters; both Queens and Akari now supply their own, so the beta
    ladder decays exactly like the canonical one. When enabled, above-start
    absentees decay toward ``start_rating`` on concluded active days and their
    lost points are split equally among that day's valid participants. Each
    rated update is centered and then reduced by 0.25 points per participant;
    the stronger-participant Codeforces correction is not used. A custom
    ``performance_pair_score_fn`` can decouple event-performance ordering from
    rating evidence, but requires a custom ``pair_score_fn`` and never affects
    deltas.
    """
    del rank_fn
    rating_point_scale = float(rating_point_scale)
    if not math.isfinite(rating_point_scale) or rating_point_scale <= 0:
        raise ValueError('Beta rating point scale must be finite and positive.')
    if start_rating is None:
        start_rating = float(_START_RATING)
    if decay_base is None:
        decay_base = 0.0
    if decay_max is None:
        decay_max = 0.0
    if decay_grace is None:
        decay_grace = 0
    decay_enabled = decay_base > 0 and decay_max > 0
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

        # A fully malformed day supplies neither rating evidence nor anyone to
        # receive a zero-sum decay transfer, so quarantine it completely.
        if not active_ids:
            continue

        for user_id in active_ids:
            players.setdefault(
                user_id,
                _Player(
                    rating=start_rating,
                    peak=start_rating,
                    last_puzzle=puzzle_number,
                ),
            )

        rated_day = len(active_ids) >= 2
        if rated_day:
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
                    compute_performance=compute_performance,
                    rating_point_scale=rating_point_scale)
                if pair_score_fn is None
                else _compute_pair_round(
                    before, day_rows, pair_score_fn,
                    compute_performance=compute_performance,
                    performance_pair_score_fn=performance_pair_score_fn,
                    rating_point_scale=rating_point_scale)
            )
            updates = _apply_field_correction(updates)
        else:
            updates = {
                user_id: _RoundUpdate(delta=0.0, performance=None)
                for user_id in active_ids
            }

        day_concluded = (
            current_puzzle_number is None
            or puzzle_number < current_puzzle_number
        )
        absent_changes = {}
        decay_pool = 0.0
        if day_concluded and decay_enabled:
            for user_id in sorted(players):
                if user_id in day_rows:
                    continue
                old = players[user_id]
                skip_streak = old.skip_streak + 1
                raw_delta = (start_rating - old.rating) * _decay_rate(
                    skip_streak, decay_base, decay_max, decay_grace)
                delta = min(0.0, raw_delta)
                absent_changes[user_id] = (skip_streak, delta)
                decay_pool -= delta

        transfer_share = decay_pool / len(active_ids) if decay_pool > 0 else 0.0

        for user_id in active_ids:
            old = players[user_id]
            update = updates[user_id]
            combined_delta = update.delta + transfer_share
            new_rating = old.rating + combined_delta
            players[user_id] = _Player(
                rating=new_rating,
                games=old.games + int(rated_day),
                peak=max(old.peak, new_rating),
                last_delta=combined_delta,
                skip_streak=0,
                last_puzzle=puzzle_number,
            )
            if histories is not None:
                histories.setdefault(user_id, []).append(_history_point(
                    puzzle_number,
                    day_rows[user_id],
                    new_rating,
                    combined_delta,
                    update.performance,
                ))

        puzzle_date = getattr(day_rows[active_ids[0]], 'puzzle_date', None)
        for user_id, (skip_streak, delta) in absent_changes.items():
            old = players[user_id]
            new_rating = old.rating + delta
            players[user_id] = _Player(
                rating=new_rating,
                games=old.games,
                peak=old.peak,
                last_delta=delta,
                skip_streak=skip_streak,
                last_puzzle=old.last_puzzle,
            )
            if histories is not None and include_decay_in_history:
                histories.setdefault(user_id, []).append(HistoryPoint(
                    puzzle_number=puzzle_number,
                    puzzle_date=puzzle_date,
                    rating=new_rating,
                    delta=delta,
                    performance=None,
                    is_perfect=False,
                    accuracy=0,
                    time_seconds=0,
                    is_decay=True,
                ))

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
