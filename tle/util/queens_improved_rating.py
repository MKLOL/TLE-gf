"""Experimental Glicko-2 rating replay for LinkedIn Queens.

Every multiplayer puzzle day is one Glicko-2 rating period.  A player's
standing becomes one simultaneous result against every other participant:
win, draw, or loss according to rank.  Solo days remain visible in history but
carry no rating information.

The implementation follows Mark Glickman's March 2022 Glicko-2 specification.
Only the origin of the public scale is translated from 1500 to 1200; the
173.7178 scale factor and all update equations are unchanged.
"""

import math
from dataclasses import dataclass

from tle.util.akari_rating import HistoryPoint, RatingState


_START_RATING = 1200.0
_INITIAL_RD = 100.0
_INITIAL_VOLATILITY = 0.06
_SCALE = 173.7178
_TAU = 0.5
_EPSILON = 0.000001


@dataclass(frozen=True)
class _Player:
    rating: float = _START_RATING
    rd: float = _INITIAL_RD
    volatility: float = _INITIAL_VOLATILITY
    games: int = 0
    peak: float = _START_RATING
    last_delta: float = 0.0
    skip_streak: int = 0
    last_puzzle: int = 0


@dataclass(frozen=True)
class _GlickoUpdate:
    rating: float
    rd: float
    volatility: float
    performance: float


def _g(phi):
    return 1.0 / math.sqrt(1.0 + 3.0 * phi * phi / (math.pi * math.pi))


def _expected(mu, opponent_mu, opponent_phi):
    exponent = -_g(opponent_phi) * (mu - opponent_mu)
    # Ordinary Queens ratings stay far from overflow, but this keeps malformed
    # imported histories from making a deterministic replay fail.
    if exponent >= 0:
        z = math.exp(-exponent)
        return z / (1.0 + z)
    z = math.exp(exponent)
    return 1.0 / (1.0 + z)


def _new_volatility(phi, volatility, delta, variance):
    """Glicko-2 Step 5 (the stable Illinois-algorithm revision)."""
    a = math.log(volatility * volatility)

    def objective(x):
        exp_x = math.exp(x)
        numerator = exp_x * (delta * delta - phi * phi - variance - exp_x)
        denominator = 2.0 * (phi * phi + variance + exp_x) ** 2
        return numerator / denominator - (x - a) / (_TAU * _TAU)

    point_a = a
    if delta * delta > phi * phi + variance:
        point_b = math.log(delta * delta - phi * phi - variance)
    else:
        k = 1
        point_b = a - k * _TAU
        while objective(point_b) < 0:
            k += 1
            point_b = a - k * _TAU

    value_a = objective(point_a)
    value_b = objective(point_b)
    while abs(point_b - point_a) > _EPSILON:
        point_c = (
            point_a
            + (point_a - point_b) * value_a / (value_b - value_a)
        )
        value_c = objective(point_c)
        if value_c * value_b <= 0:
            point_a, value_a = point_b, value_b
        else:
            value_a /= 2.0
        point_b, value_b = point_c, value_c

    return math.exp(point_a / 2.0)


def _rate_player(rating, rd, volatility, opponents):
    """Update one player from simultaneous ``(rating, RD, score)`` results."""
    mu = (float(rating) - _START_RATING) / _SCALE
    phi = float(rd) / _SCALE
    terms = []
    for opponent_rating, opponent_rd, score in opponents:
        opponent_mu = (float(opponent_rating) - _START_RATING) / _SCALE
        opponent_phi = float(opponent_rd) / _SCALE
        weight = _g(opponent_phi)
        expectation = _expected(mu, opponent_mu, opponent_phi)
        terms.append((weight, expectation, float(score)))

    information = sum(
        weight * weight * expectation * (1.0 - expectation)
        for weight, expectation, _ in terms
    )
    variance = 1.0 / information
    residual = sum(
        weight * (score - expectation)
        for weight, expectation, score in terms
    )
    delta = variance * residual
    new_volatility = _new_volatility(phi, volatility, delta, variance)
    pre_period_phi = math.sqrt(phi * phi + new_volatility * new_volatility)
    new_phi = 1.0 / math.sqrt(
        1.0 / (pre_period_phi * pre_period_phi) + 1.0 / variance
    )
    new_mu = mu + new_phi * new_phi * residual

    return _GlickoUpdate(
        rating=_START_RATING + _SCALE * new_mu,
        rd=_SCALE * new_phi,
        volatility=new_volatility,
        # Glickman defines Delta as the estimated improvement from the
        # pre-period rating to the performance rating based only on results.
        performance=float(rating) + _SCALE * delta,
    )


def _inactive_rd(rd, volatility):
    """Glicko-2 Step 6 for a player absent from one rating period."""
    phi = float(rd) / _SCALE
    return _SCALE * math.sqrt(phi * phi + volatility * volatility)


def _row_order_key(row):
    """Stable first-submission key used for defensive per-user/day deduping."""
    message_id = getattr(row, 'message_id', None)
    try:
        message_key = (0, int(message_id))
    except (TypeError, ValueError):
        message_key = (1, '' if message_id is None else str(message_id))
    return (
        message_key,
        str(getattr(row, 'puzzle_date', '')),
        int(getattr(row, 'time_seconds', 0)),
        -int(bool(getattr(row, 'is_perfect', False))),
        -int(getattr(row, 'accuracy', 0)),
        str(getattr(row, 'raw_content', '')),
    )


def _rank_by_time(rows):
    ordered = sorted(
        rows,
        key=lambda row: (
            int(getattr(row, 'time_seconds', 0)),
            str(row.user_id),
        ),
    )
    ranks = {}
    previous_time = None
    rank = 0
    for index, row in enumerate(ordered):
        time_seconds = int(getattr(row, 'time_seconds', 0))
        if previous_time is None or time_seconds != previous_time:
            rank = index + 1
            previous_time = time_seconds
        ranks[str(row.user_id)] = rank
    return ranks


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
        rank_fn=None, **_ignored):
    """Replay Queens results with the experimental Glicko-2 model.

    The return and history shapes intentionally match ``compute_ratings`` so
    existing Queens tables and plots can select this engine without a separate
    persistence format.  ``include_decay_in_history`` is accepted for that
    interface but produces no extra points: inactivity changes uncertainty,
    never the visible rating.
    """
    del include_decay_in_history
    rank_fn = rank_fn or _rank_by_time

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
        active_ids = sorted(day_rows)

        for user_id in active_ids:
            if user_id not in players:
                players[user_id] = _Player(last_puzzle=puzzle_number)

        if len(active_ids) < 2:
            for user_id in active_ids:
                old = players[user_id]
                players[user_id] = _Player(
                    rating=old.rating,
                    rd=old.rd,
                    volatility=old.volatility,
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

        raw_ranks = rank_fn([day_rows[user_id] for user_id in active_ids])
        ranks = {str(user_id): int(rank) for user_id, rank in raw_ranks.items()}
        missing = [user_id for user_id in active_ids if user_id not in ranks]
        if missing:
            raise ValueError(f'Rank function omitted participants: {missing!r}')

        before = {user_id: players[user_id] for user_id in active_ids}
        updates = {}
        for user_id in active_ids:
            opponents = []
            for opponent_id in active_ids:
                if opponent_id == user_id:
                    continue
                if ranks[user_id] < ranks[opponent_id]:
                    score = 1.0
                elif ranks[user_id] == ranks[opponent_id]:
                    score = 0.5
                else:
                    score = 0.0
                opponent = before[opponent_id]
                opponents.append((opponent.rating, opponent.rd, score))
            player = before[user_id]
            updates[user_id] = _rate_player(
                player.rating, player.rd, player.volatility, opponents)

        for user_id in active_ids:
            old = before[user_id]
            update = updates[user_id]
            rating_delta = update.rating - old.rating
            players[user_id] = _Player(
                rating=update.rating,
                rd=update.rd,
                volatility=update.volatility,
                games=old.games + 1,
                peak=max(old.peak, update.rating),
                last_delta=rating_delta,
                skip_streak=0,
                last_puzzle=puzzle_number,
            )
            if histories is not None:
                histories.setdefault(user_id, []).append(_history_point(
                    puzzle_number,
                    day_rows[user_id],
                    update.rating,
                    rating_delta,
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
                    rd=_inactive_rd(old.rd, old.volatility),
                    volatility=old.volatility,
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
