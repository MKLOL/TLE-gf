"""Weekly Daily Akari scoring and rating replay.

One Monday-Sunday week is one rating contest.  Each puzzle contributes a
difficulty-weighted score in [0, 1-ish], and the seven scores are summed before
the existing Codeforces-style multiplayer rating round is applied.

The tuning constants are intentionally gathered at the top of this module so
the preview model can be adjusted without touching the replay machinery.
"""

import datetime as dt
import math
from collections import namedtuple

from tle import constants
from tle.util.akari_rating import RatingState, compute_round


# Rating movement for one weekly contest.  Daily ratings use 0.25; a larger
# value is appropriate when seven puzzles are collapsed into one contest.
WEEKLY_RATING_DAMPING = 0.75

# Relative-time curve.  At 2x the best perfect time, the speed component is
# exp(-0.7), making the final perfect score about 0.75.
TIME_DECAY = 0.7

# Imperfect results occupy [0, 0.5), leaving every perfect result above every
# imperfect result.  Raising accuracy to this power separates near-perfect
# runs while preserving accuracy as the primary ordering criterion.
IMPERFECT_SCORE_CEILING = 0.5
ACCURACY_EXPONENT = 4.0

# Difficulty 3 is neutral.  Moving four difficulty levels doubles/halves the
# weight, producing approximately .707, .841, 1, 1.189, 1.414 for levels 1-5.
DIFFICULTY_EXPONENT_STEP = 0.25
DEFAULT_DIFFICULTY = 3


WeeklyStanding = namedtuple(
    'WeeklyStanding',
    'user_id score days_played perfects total_time week_start week_end',
)


def week_start(day):
    """Return the Monday containing ``day``."""
    if not isinstance(day, dt.date):
        day = dt.date.fromisoformat(str(day))
    return day - dt.timedelta(days=day.weekday())


def difficulty_weight(difficulty):
    """Raw weight for Daily Akari's integer difficulty level (1 through 5)."""
    try:
        level = int(difficulty)
    except (TypeError, ValueError):
        level = DEFAULT_DIFFICULTY
    level = min(5, max(1, level))
    return 2.0 ** ((level - DEFAULT_DIFFICULTY) * DIFFICULTY_EXPONENT_STEP)


def _time_factor(time_seconds, best_time):
    if best_time is None or best_time <= 0:
        return 1.0
    ratio = max(1.0, float(time_seconds) / float(best_time))
    return math.exp(-TIME_DECAY * (ratio - 1.0))


def result_performance(row, *, best_perfect_time=None,
                       best_time_for_accuracy=None):
    """Return a result's configurable 0..1 relative performance.

    Perfect runs use the best perfect time as their reference and remain in
    [0.5, 1].  Imperfect runs use accuracy first.  Time only moves a result
    inside the narrow band between its accuracy and the next-lower percentage,
    so a faster 98% can never overtake any 99% result.
    """
    time_seconds = max(0, int(getattr(row, 'time_seconds', 0)))
    if bool(getattr(row, 'is_perfect', False)):
        speed = _time_factor(time_seconds, best_perfect_time)
        return IMPERFECT_SCORE_CEILING + (1.0 - IMPERFECT_SCORE_CEILING) * speed

    accuracy = min(99, max(0, int(getattr(row, 'accuracy', 0))))
    upper = IMPERFECT_SCORE_CEILING * (accuracy / 100.0) ** ACCURACY_EXPONENT
    if accuracy == 0:
        return 0.0
    lower = (IMPERFECT_SCORE_CEILING
             * ((accuracy - 1) / 100.0) ** ACCURACY_EXPONENT)
    speed = _time_factor(time_seconds, best_time_for_accuracy)
    return lower + (upper - lower) * speed


def _week_puzzle_numbers(rows, start):
    """Infer all seven sequential puzzle numbers from any row in the week."""
    anchor = rows[0]
    anchor_date = dt.date.fromisoformat(str(anchor.puzzle_date))
    monday_number = int(anchor.puzzle_number) - (anchor_date - start).days
    return [monday_number + offset for offset in range(7)]


def score_week(rows, difficulties=None):
    """Score one week's rows and return standings sorted strongest first."""
    rows = list(rows)
    if not rows:
        return []
    difficulties = difficulties or {}
    start = week_start(rows[0].puzzle_date)
    end = start + dt.timedelta(days=6)

    # First submitted result per player/day is rating-eligible, matching the
    # database replay contract used by the existing daily engine.
    by_puzzle = {}
    for row in sorted(rows, key=lambda r: (int(r.puzzle_number), str(r.user_id))):
        day = by_puzzle.setdefault(int(row.puzzle_number), {})
        day.setdefault(str(row.user_id), row)

    puzzle_numbers = _week_puzzle_numbers(rows, start)
    raw_weights = {
        number: difficulty_weight(difficulties.get(number, DEFAULT_DIFFICULTY))
        for number in puzzle_numbers
    }
    # Keep every week on the same seven-point scale while letting hard days
    # matter more within that week.
    normalizer = 7.0 / sum(raw_weights.values())
    weights = {number: value * normalizer
               for number, value in raw_weights.items()}

    totals = {}
    days = {}
    perfects = {}
    total_times = {}
    for number, day_rows in by_puzzle.items():
        values = list(day_rows.values())
        perfect_times = [int(r.time_seconds) for r in values if r.is_perfect]
        best_perfect = min(perfect_times) if perfect_times else None
        best_by_accuracy = {}
        for row in values:
            if row.is_perfect:
                continue
            accuracy = min(99, max(0, int(row.accuracy)))
            best_by_accuracy[accuracy] = min(
                int(row.time_seconds),
                best_by_accuracy.get(accuracy, int(row.time_seconds)),
            )
        weight = weights.get(number, 1.0)
        for user_id, row in day_rows.items():
            performance = result_performance(
                row,
                best_perfect_time=best_perfect,
                best_time_for_accuracy=best_by_accuracy.get(
                    min(99, max(0, int(row.accuracy)))),
            )
            totals[user_id] = totals.get(user_id, 0.0) + weight * performance
            days[user_id] = days.get(user_id, 0) + 1
            perfects[user_id] = perfects.get(user_id, 0) + int(bool(row.is_perfect))
            total_times[user_id] = total_times.get(user_id, 0) + int(row.time_seconds)

    standings = [
        WeeklyStanding(user_id, score, days[user_id], perfects[user_id],
                       total_times[user_id], start, end)
        for user_id, score in totals.items()
    ]
    return sorted(
        standings,
        key=lambda s: (-s.score, -s.days_played, -s.perfects,
                       s.total_time, s.user_id),
    )


def rank_week(standings):
    """Competition ranks based only on summed score; exact totals tie."""
    ranks = {}
    previous_score = None
    current_rank = 0
    for index, standing in enumerate(standings):
        if previous_score is None or not math.isclose(
                standing.score, previous_score, rel_tol=0.0, abs_tol=1e-12):
            current_rank = index + 1
            previous_score = standing.score
        ranks[str(standing.user_id)] = current_rank
    return ranks


def _group_rows_by_week(rows):
    grouped = {}
    for row in rows:
        try:
            start = week_start(row.puzzle_date)
        except (TypeError, ValueError):
            continue
        grouped.setdefault(start, []).append(row)
    return grouped


def current_week_standings(rows, difficulties=None, *, as_of_date=None):
    """Return provisional standings for the week containing ``as_of_date``."""
    as_of_date = as_of_date or dt.date.today()
    current = week_start(as_of_date)
    grouped = _group_rows_by_week(rows)
    return score_week(grouped.get(current, []), difficulties)


def compute_weekly_ratings(rows, difficulties=None, *, as_of_date=None,
                           start_rating=None, damping=WEEKLY_RATING_DAMPING):
    """Replay completed Monday-Sunday weeks into Codeforces-style ratings."""
    rows = list(rows)
    difficulties = difficulties or {}
    as_of_date = as_of_date or dt.date.today()
    if start_rating is None:
        start_rating = float(constants.AKARI_START_RATING)

    ratings = {}
    games = {}
    peaks = {}
    last_delta = {}
    last_puzzle = {}
    grouped = _group_rows_by_week(rows)
    for start in sorted(grouped):
        if start + dt.timedelta(days=6) >= as_of_date:
            continue  # current/future week is provisional, never rated
        standings = score_week(grouped[start], difficulties)
        for standing in standings:
            user_id = str(standing.user_id)
            if user_id not in ratings:
                ratings[user_id] = start_rating
                games[user_id] = 0
                peaks[user_id] = start_rating
                last_delta[user_id] = 0.0
            user_rows = [r for r in grouped[start] if str(r.user_id) == user_id]
            last_puzzle[user_id] = max(int(r.puzzle_number) for r in user_rows)

        if len(standings) < 2:
            continue
        participants = [str(s.user_id) for s in standings]
        round_ratings = {user_id: ratings[user_id] for user_id in participants}
        deltas = compute_round(
            round_ratings, rank_week(standings), damping=damping)
        for user_id, delta in deltas.items():
            ratings[user_id] += delta
            games[user_id] += 1
            last_delta[user_id] = delta
            peaks[user_id] = max(peaks[user_id], ratings[user_id])

    return {
        user_id: RatingState(
            user_id=user_id,
            rating=rating,
            games=games[user_id],
            peak=peaks[user_id],
            last_delta=last_delta[user_id],
            skip_streak=0,
            last_puzzle=last_puzzle[user_id],
        )
        for user_id, rating in ratings.items()
    }
