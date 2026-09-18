"""Weekly recap aggregation for Daily Akari and LinkedIn Queens.

The recap answers "how did the server's week go": who won each day, who led
the week, which results stood out, and how the weekly rating contest moved.
Everything here is pure data so it can be tested without matplotlib or
Discord; rendering lives in ``_weekly_recap_render``.

A week is Monday-Sunday, matching ``akari_weekly``'s rating contest, so a
completed week's recap and its rating round always describe the same span.
"""

import datetime as dt
import math
import statistics
from collections import namedtuple

from tle import constants
from tle.util.akari_rating import compute_ratings
from tle.util.akari_weekly import compute_weekly_ratings, week_start
from tle.cogs._minigame_common import (
    compute_top_breakdown,
    default_is_eligible_winner,
    normalize_puzzle_date,
    result_sort_key,
    winning_rows_per_puzzle,
)


DayResult = namedtuple(
    'DayResult', 'date winner_ids best_row participants tied')

Superlative = namedtuple('Superlative', 'label user_id value detail')

PaceEntry = namedtuple('PaceEntry', 'user_id pace days seconds')

RatingChange = namedtuple('RatingChange', 'user_id old new delta')

PersonalWeek = namedtuple(
    'PersonalWeek',
    'user_id days_played perfects solo_wins tied_wins best_row rank')

WeeklyRecap = namedtuple(
    'WeeklyRecap',
    'display_name week_start week_end today in_progress days leaders '
    'superlatives paces ratings daily_ratings personal player_count '
    'result_count')


def week_bounds(day):
    """Return the (Monday, Sunday) pair containing ``day``."""
    start = week_start(day)
    return start, start + dt.timedelta(days=6)


def rows_in_range(rows, start, end):
    """Keep rows whose puzzle date falls in the inclusive ``start``-``end`` span."""
    kept = []
    for row in rows:
        try:
            day = normalize_puzzle_date(row.puzzle_date)
        except (TypeError, ValueError):
            continue
        if start <= day <= end:
            kept.append(row)
    return kept


def _rows_by_date(rows):
    grouped = {}
    for row in rows:
        try:
            day = normalize_puzzle_date(row.puzzle_date)
        except (TypeError, ValueError):
            continue
        grouped.setdefault(day, []).append(row)
    return grouped


def _best_row_per_user(rows, best_result_sort_key_fn):
    """One row per user — their own best attempt at a puzzle."""
    best = {}
    for row in rows:
        user_id = str(row.user_id)
        prev = best.get(user_id)
        if prev is None or (best_result_sort_key_fn(row)
                            > best_result_sort_key_fn(prev)):
            best[user_id] = row
    return best


def _day_results(week_rows, start, end, top_kwargs, best_result_sort_key_fn):
    """Resolve the winner (or tied winners) of each day in the week."""
    winners_by_date = {}
    for winning_rows in winning_rows_per_puzzle(week_rows, **top_kwargs).values():
        day = normalize_puzzle_date(winning_rows[0].puzzle_date)
        winners_by_date[day] = winning_rows

    by_date = _rows_by_date(week_rows)
    days = []
    day = start
    while day <= end:
        played = by_date.get(day, [])
        winning_rows = winners_by_date.get(day, [])
        # Ordered so the render can name tied winners deterministically.
        winner_ids = sorted(
            {str(row.user_id) for row in winning_rows}, key=int)
        days.append(DayResult(
            date=day,
            winner_ids=winner_ids,
            best_row=winning_rows[0] if winning_rows else None,
            participants=len(_best_row_per_user(
                played, best_result_sort_key_fn)),
            tied=len(winning_rows) > 1,
        ))
        day += dt.timedelta(days=1)
    return days


def _best_per_user_day(rows, best_result_sort_key_fn):
    """Each player's own best row for each day they played."""
    per_user_day = {}
    for row in rows:
        key = (str(row.user_id), normalize_puzzle_date(row.puzzle_date))
        prev = per_user_day.get(key)
        if prev is None or (best_result_sort_key_fn(row)
                            > best_result_sort_key_fn(prev)):
            per_user_day[key] = row
    return per_user_day


def _average_seconds(rows, best_result_sort_key_fn):
    """Mean of each player's best time per day, keyed by user."""
    per_user_day = _best_per_user_day(rows, best_result_sort_key_fn)
    totals = {}
    for (user_id, _day), row in per_user_day.items():
        seconds, count = totals.get(user_id, (0, 0))
        totals[user_id] = (seconds + int(row.time_seconds), count + 1)
    return {user_id: (seconds / count, count)
            for user_id, (seconds, count) in totals.items() if count}


def _superlatives(week_rows, previous_rows, top_kwargs,
                  best_result_sort_key_fn, active_days):
    """Standout results of the week, skipping any that nobody qualifies for."""
    is_eligible = top_kwargs.get('is_eligible') or default_is_eligible_winner
    entries = []

    eligible = [row for row in week_rows if is_eligible(row)]
    if eligible:
        fastest = min(eligible, key=lambda row: int(row.time_seconds))
        entries.append(Superlative(
            'Fastest solve', str(fastest.user_id), int(fastest.time_seconds),
            normalize_puzzle_date(fastest.puzzle_date).strftime('%a')))

    # Averages must be built from comparable results only. An abandoned Akari
    # grid can carry an arbitrary time, which would otherwise show up as an
    # "improvement" of several hours.
    threshold = _average_threshold(active_days)
    averages = _average_seconds(eligible, best_result_sort_key_fn)
    qualified = {user_id: value for user_id, (value, count) in averages.items()
                 if count >= threshold}

    previous = _average_seconds(
        [row for row in previous_rows if is_eligible(row)],
        best_result_sort_key_fn)
    gains = {}
    for user_id, average in qualified.items():
        before = previous.get(user_id)
        if before and before[1] >= threshold:
            gains[user_id] = before[0] - average
    improved = {user_id: gain for user_id, gain in gains.items() if gain > 0}
    if improved:
        best = max(improved.items(), key=lambda item: (item[1], -int(item[0])))
        entries.append(Superlative(
            'Most improved', best[0], int(round(best[1])),
            'vs last week'))

    return entries


def _average_threshold(active_days):
    """Days a player must have played before a comparison means anything."""
    return max(1, (active_days + 1) // 2)


def pace_leaderboard(week_rows, best_result_sort_key_fn, min_days=None,
                     is_eligible=None):
    """Rank players by speed relative to each day's field.

    Raw average time is dominated by *which* days a player showed up for: the
    median solve inside a single week routinely swings by a factor of ten, so
    someone who only plays the easy days posts a better average without being
    faster. Each result is therefore scored against that day's median, and a
    player's week is the geometric mean of those ratios. Returned ``pace`` is
    a speed multiple: 2.0 means twice the day's median pace.

    Two choices matter here. Ratios are multiplicative, so a day twice as slow
    and a day twice as fast must cancel to 1.0 — the geometric mean does that
    and an arithmetic mean does not. And the reference is the day's median
    rather than its winning time, which on a three-second day would be a
    single noisy observation rescaling everyone.

    Only players present on every scored day are ranked. There is no shrinkage
    for a short week, so without that rule dropping the days that went badly
    would be the cheapest way to climb this board. ``min_days`` defaults to
    every scored day; a day with a field of one is not scored at all, which
    also keeps an in-progress week from demanding days nobody has played yet.

    ``is_eligible`` optionally restricts which results are comparable at all.
    The recap does not pass it, so every posted result counts in both games: a
    slow day is still part of the week, and discarding it would reward giving
    up. The trade-off is that an abandoned Akari grid keeps whatever time it
    was posted with, so a fast wrong answer scores as a fast day.
    """
    if is_eligible is not None:
        week_rows = [row for row in week_rows if is_eligible(row)]
    per_user_day = _best_per_user_day(week_rows, best_result_sort_key_fn)
    by_day = {}
    for (_user_id, day), row in per_user_day.items():
        by_day.setdefault(day, []).append(max(1, int(row.time_seconds)))
    # A field of one carries no information about pace, so it is not scored.
    medians = {day: statistics.median(times)
               for day, times in by_day.items() if len(times) > 1}

    logs, totals = {}, {}
    for (user_id, day), row in per_user_day.items():
        median = medians.get(day)
        if not median:
            continue
        seconds = max(1, int(row.time_seconds))
        logs.setdefault(user_id, []).append(math.log(seconds / median))
        total, count = totals.get(user_id, (0, 0))
        totals[user_id] = (total + seconds, count + 1)

    required = len(medians) if min_days is None else min_days
    entries = []
    for user_id, values in logs.items():
        days = len(values)
        if not days or days < required:
            continue
        index = math.exp(sum(values) / days)
        total, count = totals[user_id]
        entries.append(PaceEntry(user_id=user_id, pace=1 / index, days=days,
                                 seconds=total / count))
    return sorted(entries,
                  key=lambda entry: (-entry.pace, -entry.days,
                                     int(entry.user_id)))


def rating_changes(rows, difficulties, start, end, today=None):
    """Weekly-contest rating movement produced by the week ending ``end``.

    Ratings are diffed either side of the week rather than read from
    ``last_delta``, which only advances for weeks that had enough players to
    rate and would otherwise report a stale earlier week's movement.

    A week that has not finished yet is never rated: ``compute_weekly_ratings``
    decides that from ``as_of_date`` alone, so asking it about a future Sunday
    would invent a rating round out of a half-played week.
    """
    if today is not None and end >= today:
        return []
    before = compute_weekly_ratings(rows, difficulties, as_of_date=start)
    after = compute_weekly_ratings(
        rows, difficulties, as_of_date=end + dt.timedelta(days=1))
    start_rating = float(constants.AKARI_START_RATING)
    changes = []
    for user_id, state in after.items():
        prior = before.get(user_id)
        # A player rated for the first time this week has no prior entry; they
        # moved from the shared starting rating rather than from nothing.
        prior_rating = prior.rating if prior else start_rating
        prior_games = prior.games if prior else 0
        if state.games == prior_games:
            continue  # Played, but the week was not rated for them.
        changes.append(RatingChange(
            user_id=user_id, old=prior_rating, new=state.rating,
            delta=state.rating - prior_rating))
    return sorted(changes, key=lambda item: (-item.delta, int(item.user_id)))


def daily_rating_changes(rows, start, end, compute_kwargs=None):
    """Movement in the day-by-day rating ladder over the week.

    The daily ladder (``;<game> ratings``) and the weekly contest ladder
    (``;<game> ratings +weekly``) are separate rating systems, so the recap
    reports both rather than implying one number. Deltas come from the
    replay's own per-day history, which already folds in inactivity decay for
    days the player skipped.
    """
    histories = {}
    compute_ratings(rows, histories=histories,
                    include_decay_in_history=True, **(compute_kwargs or {}))
    changes = []
    for user_id, points in histories.items():
        window = [point for point in points
                  if start <= normalize_puzzle_date(point.puzzle_date) <= end]
        # Decay-only entries are not this week's story; the panel reports the
        # movement of players who actually turned up.
        if not any(not point.is_decay for point in window):
            continue
        delta = sum(point.delta for point in window)
        latest = window[-1].rating
        changes.append(RatingChange(
            user_id=str(user_id), old=latest - delta, new=latest, delta=delta))
    return sorted(changes, key=lambda item: (-item.delta, int(item.user_id)))


def top_and_bottom(changes, count=3):
    """Biggest movers each way, never repeating anyone in a short list."""
    best = list(changes[:count])
    worst = [change for change in changes[-count:] if change not in best]
    return best, worst


def _personal_week(user_id, week_rows, days, leaders, best_result_sort_key_fn):
    user_id = str(user_id)
    mine = [row for row in week_rows if str(row.user_id) == user_id]
    if not mine:
        return None
    by_day = _rows_by_date(mine)
    perfect_days = sum(
        1 for rows in by_day.values()
        if any(getattr(row, 'is_perfect', False) for row in rows))
    solo = sum(1 for day in days
               if day.winner_ids == [user_id] and not day.tied)
    tied = sum(1 for day in days if day.tied and user_id in day.winner_ids)
    best = max(mine, key=best_result_sort_key_fn)
    # Rank against the board the recap actually renders, so a player is never
    # told they placed on a list they are absent from.
    rank = next((index + 1 for index, entry in enumerate(leaders)
                 if entry[0] == user_id), None)
    return PersonalWeek(
        user_id=user_id, days_played=len(by_day), perfects=perfect_days,
        solo_wins=solo, tied_wins=tied, best_row=best, rank=rank)


def build_week_recap(rows, *, display_name, anchor_date, today,
                     difficulties=None, viewer_id=None, top_kwargs=None,
                     best_result_sort_key_fn=None, rating_rows=None,
                     daily_ratings=None):
    """Assemble the full recap for the Monday-Sunday week holding ``anchor_date``.

    ``rating_rows`` defaults to ``rows`` but lets Queens feed the time-only
    projection its weekly contest is scored on, while the day-by-day and
    leaderboard sections keep using the raw results.
    """
    top_kwargs = dict(top_kwargs or {})
    if best_result_sort_key_fn is None:
        best_result_sort_key_fn = (
            top_kwargs.get('best_result_sort_key_fn') or result_sort_key)
    start, end = week_bounds(anchor_date)
    week_rows = rows_in_range(rows, start, end)
    previous_rows = rows_in_range(
        rows, start - dt.timedelta(days=7), start - dt.timedelta(days=1))

    days = _day_results(
        week_rows, start, end, top_kwargs, best_result_sort_key_fn)
    active_days = sum(1 for day in days if day.participants)
    leaders = compute_top_breakdown(week_rows, **top_kwargs)
    superlatives = _superlatives(
        week_rows, previous_rows, top_kwargs, best_result_sort_key_fn,
        max(1, active_days))
    # Pace counts every posted result, in both games: a slow day is part of
    # the week, and dropping it would reward giving up.
    paces = pace_leaderboard(week_rows, best_result_sort_key_fn)
    ratings = rating_changes(
        rows if rating_rows is None else rating_rows,
        difficulties or {}, start, end, today=today)
    personal = (
        _personal_week(viewer_id, week_rows, days, leaders,
                       best_result_sort_key_fn)
        if viewer_id is not None else None)

    return WeeklyRecap(
        display_name=display_name,
        week_start=start,
        week_end=end,
        today=today,
        in_progress=start <= today <= end,
        days=days,
        leaders=leaders,
        superlatives=superlatives,
        paces=paces,
        ratings=ratings,
        daily_ratings=list(daily_ratings or []),
        personal=personal,
        player_count=len({str(row.user_id) for row in week_rows}),
        result_count=len(week_rows),
    )
