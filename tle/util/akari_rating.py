"""Shared Codeforces-style multiplayer rating engine for minigames.

Each minigame day (one ``puzzle_number`` per day) is treated as a single
Codeforces contest: every player who submitted that day is ranked against the
others, and ratings move toward each player's expected-versus-actual rank using
the exact Codeforces rating formula (logistic win-probabilities, expected-rank
"seed", geometric-mean target rank, binary-searched needed rating, and the two
CF anti-inflation corrections).  The final per-contest change is then scaled by
a small ``damping`` factor so the rating is less volatile than real CF —
appropriate for daily games where a year of play is hundreds of "contests".

This module is pure: no database, no discord, no wall-clock.  Given the same
result rows it always returns the same ratings, regardless of row order, which
keeps it trivially unit-testable and lets callers replay the full history on
every change.

The default tuning is the original Akari tuning:
:data:`tle.constants.AKARI_START_RATING` (1200), Akari damping, and Akari decay.
Callers can override those values per game through ``compute_ratings`` kwargs
(wired from ``GameDef.rating`` in the minigame cog).  Ratings are kept as
``float`` throughout the replay (and stored as ``REAL``); callers round only for
display.  At a quarter-strength damping, rounding every daily delta to an
integer would floor most of them to zero and ratings would never move.

Inactive players above the default rating decay back toward it, with the pull
growing the longer they stay away (see :func:`compute_ratings`).  The points
they lose are pooled and redistributed equally to the day's active players —
the rating ladder is zero-sum within each puzzle day, so coasters' rating
funds the regulars instead of vanishing.  Sub-default absentees freeze rather
than drift up: the engine refuses to create rating ex nihilo.
"""

import math
from collections import namedtuple

from tle import constants
# Rating bands (display/graph tiers) live in akari_ranks to keep this module
# under the 500-line limit. Re-exported so akari_rating.<name> still resolves.
from tle.util.akari_ranks import _AkariRank, AKARI_RANKS, rank_for_rating


# Codeforces logistic scale: a 400-point gap ⇒ ~10x odds.
_RATING_SCALE = 400.0
# Bounds and iteration count for the needed-rating binary search.  25 bisections
# over [1, 8000] resolve to < 3e-4 — far finer than any rating difference, while
# keeping a full-history replay cheap enough to run on every result change.
_SEARCH_LO = 1.0
_SEARCH_HI = 8000.0
_SEARCH_ITERS = 25


# Per-user result of a full replay.  ``rating``/``peak``/``last_delta`` are floats
# (round for display); ``games`` counts only *rated* days (days with >= 2 players);
# ``skip_streak`` is the number of consecutive recent days the user missed (drives
# decay); ``last_puzzle`` is the last day they actually played.  The last two
# default to 0 so callers that only care about the rating can omit them.
RatingState = namedtuple(
    'RatingState',
    'user_id rating games peak last_delta skip_streak last_puzzle',
    defaults=(0, 0),
)


# One point on a user's rating history.  By default, emitted only for days the
# user actually played — decay days between plays modify ``rating`` but don't
# produce their own entry; their net effect shows up in the next played day's
# ``rating``.  Pass ``include_decay_in_history=True`` to also get one point per
# absent day, with ``is_decay=True`` and ``delta`` set to that day's decay
# amount.
# ``performance`` is the Codeforces-style per-contest performance: ``2*need -
# rating``, where ``need`` is the geometric-mean target ``compute_round``
# binary-searches for.  The substitution comes from the assumption that ``need``
# is the arithmetic midpoint between the player's current rating and their
# performance — i.e. ``need = (rating + performance) / 2``, rearranged.  This
# matches what CF's ``correct_rating_changes`` recovers from public deltas, and
# stays bounded for clean wins (the rank-exact definition asymptotes at 1).
# ``None`` for solo days and for decay days (no field → no contest).
HistoryPoint = namedtuple(
    'HistoryPoint',
    'puzzle_number puzzle_date rating delta performance '
    'is_perfect accuracy time_seconds is_decay',
    defaults=(False,),
)


def _result_sort_key(row):
    """Ranking key for one day's result — smaller is better.

    Mirrors the display ordering in ``minigames.py`` (``_sort_akari_puzzle_results``):
    perfect beats imperfect, then higher accuracy, then faster time.  The
    message-id tiebreak used for *display* is deliberately omitted so that two
    genuinely identical performances tie (share a rank), as a real contest would.
    """
    return (
        -int(bool(row.is_perfect)),
        -int(getattr(row, 'accuracy', 0)),
        int(getattr(row, 'time_seconds', 0)),
    )


def rank_participants(rows):
    """Assign standard competition ranks to one day's participants.

    Returns ``{user_id (str): rank}`` where rank is 1-based and tied players
    (identical :func:`_result_sort_key`) share the lower rank, e.g. places
    ``A, B, C`` with ``B == C`` get ranks ``1, 2, 2``.
    """
    ordered = sorted(rows, key=_result_sort_key)
    ranks = {}
    current_rank = 0
    prev_key = None
    for index, row in enumerate(ordered):
        key = _result_sort_key(row)
        if prev_key is None or key != prev_key:
            current_rank = index + 1  # standard competition ("1224") ranking
            prev_key = key
        ranks[str(row.user_id)] = current_rank
    return ranks


def _pow10(rating):
    """``10 ** (rating / 400)`` — precomputed per player so the seed sums below
    contain no ``pow`` calls (P(b beats a) = x_b / (x_a + x_b) where x = 10^(R/400))."""
    return 10.0 ** (rating / _RATING_SCALE)


def _expected_seed(x_self, pow_others):
    """Codeforces "seed": the expected rank of a player whose ``_pow10`` is ``x_self``.

    seed = 1 + Σ P(other ranks above me) = 1 + Σ x_other / (x_self + x_other).
    ``pow_others`` is the list of the *other* players' ``_pow10`` values.
    Monotonically decreasing in the player's rating.
    """
    seed = 1.0
    for x_other in pow_others:
        seed += x_other / (x_self + x_other)
    return seed


def _needed_rating(pow_others, target_seed):
    """Binary-search the rating whose :func:`_expected_seed` equals ``target_seed``.

    The seed decreases as rating rises, so when the seed at ``mid`` is below the
    target we have overshot and search lower.
    """
    lo, hi = _SEARCH_LO, _SEARCH_HI
    for _ in range(_SEARCH_ITERS):
        mid = (lo + hi) / 2.0
        if _expected_seed(_pow10(mid), pow_others) < target_seed:
            hi = mid
        else:
            lo = mid
    return (lo + hi) / 2.0


def compute_round(ratings, ranks, damping=None, needs=None):
    """Run one Codeforces rating round and return ``{user_id: delta}``.

    ``ratings``: ``{user_id: float}`` pre-contest ratings of the participants.
    ``ranks``:   ``{user_id: int}`` actual ranks (1-based, ties share a rank).
    Returned deltas are already damped; add them to ``ratings`` to advance.

    ``needs`` (optional): if a dict is passed in, it is populated with each
    user's geometric-mean target rating — the rating that would seed them at
    ``sqrt(actual_rank * expected_rank)`` — which Codeforces uses as the
    "performance" of that contest.  We compute this value here anyway to derive
    the delta, so capturing it costs nothing.

    Users are processed in a stable sorted order so floating-point summation is
    deterministic regardless of dict insertion order.
    """
    if damping is None:
        damping = constants.AKARI_RATING_DAMPING

    users = sorted(ratings)
    n = len(users)
    if n < 2:
        return {user: 0.0 for user in users}

    pows = {user: _pow10(ratings[user]) for user in users}
    deltas = {}
    for user in users:
        pow_others = [pows[other] for other in users if other != user]
        seed = _expected_seed(pows[user], pow_others)
        mid_rank = math.sqrt(ranks[user] * seed)
        need = _needed_rating(pow_others, mid_rank)
        if needs is not None:
            needs[user] = need
        deltas[user] = (need - ratings[user]) / 2.0

    # CF correction 1: shift everyone so the field loses a tiny, fixed amount of
    # rating (total change becomes exactly -n), counteracting inflation.
    total = sum(deltas.values())
    inc = -total / n - 1.0
    for user in users:
        deltas[user] += inc

    # CF correction 2: clamp inflation among the strongest participants.  Take
    # the top s by pre-contest rating; if their deltas sum positive, deflate the
    # whole field (capped at 10) to absorb it.
    by_rating = sorted(users, key=lambda u: ratings[u], reverse=True)
    s = round(min(n, 4 * round(math.sqrt(n))))
    if s > 0:
        top_sum = sum(deltas[user] for user in by_rating[:s])
        inc = min(max(-top_sum / s, -10.0), 0.0)
        for user in users:
            deltas[user] += inc

    return {user: damping * deltas[user] for user in users}


def _last_place_decay_delta(day_ratings, day_ranks, user_id, rating, damping):
    """Damped delta the absent ``user_id`` would get for a last-place finish.

    Inserts the absentee (alone — multiple absentees are independent
    hypotheticals) into the day's real field at strictly-last rank and runs
    the normal Codeforces round.  The real players' actual deltas are *not*
    affected; this round exists only to read off the absentee's loss.
    Clamped to ≤ 0 defensively — the CF corrections make a strictly-last
    finish lose for any rating, so the clamp should never bind.
    """
    hyp_ratings = dict(day_ratings)
    hyp_ratings[user_id] = rating
    hyp_ranks = dict(day_ranks)
    hyp_ranks[user_id] = len(day_ratings) + 1
    deltas = compute_round(hyp_ratings, hyp_ranks, damping=damping)
    return min(0.0, deltas[user_id])


def _decay_rate(skip_streak, decay_base, decay_max, decay_grace):
    """Fraction of the gap-to-default to close on a skipped day.

    The rate grows linearly with the streak — absence bites harder the longer
    it lasts — up to a ceiling. E.g. base 0.002 / max 0.05 / grace 0 ⇒ 0.2%
    of the gap on the first absent day, rising to a 5% cap. A non-zero
    ``decay_grace`` reintroduces the classic "first N days free" window
    (the rate is forced to 0 for streaks within it).
    """
    effective_streak = max(0, skip_streak - decay_grace)
    return min(decay_max, decay_base * effective_streak)


def compute_ratings(rows, start_rating=None, damping=None,
                    decay_base=None, decay_max=None, decay_grace=None,
                    max_puzzle=None, histories=None,
                    include_decay_in_history=False,
                    current_puzzle_number=None, rank_fn=None,
                    first_skip_last_place=False):
    """Replay every minigame day in order and return ``{user_id: RatingState}``.

    ``rows`` is any iterable of result rows, each exposing ``user_id``,
    ``puzzle_number``, ``is_perfect``, ``accuracy`` and ``time_seconds``.  Pass
    the DB's first-submission-per-(user, puzzle) rows
    (``get_minigame_results_for_guild``) so a player's locked-in first result is
    what counts — resubmitting a better time can't farm rating.

    Days are processed by ascending ``puzzle_number``.  Rows whose puzzle number
    is non-positive or greater than ``max_puzzle`` (when given) are dropped as bad
    data.  A newly seen player is seeded at ``start_rating`` (default 1200).  Days
    with fewer than two players are not a contest and change nothing (but still
    seed any newcomer).

    **Inactivity decay:** for every day present in the data, each previously seen
    player who did *not* submit that day has their consecutive-skip streak bumped
    and their above-default rating pulled toward ``start_rating`` by
    :func:`_decay_rate` of the remaining gap — stronger the longer they stay
    away.  Sub-default absentees freeze (no drift up) so the engine never
    creates rating ex nihilo.  The lost points are pooled and split equally
    among the day's active players, conserving total guild rating across the
    day.  Playing resets the streak.
    The "days" counted are days the guild was active (puzzles in the data), so
    decay advances as others keep playing, not by wall-clock; it is therefore a
    pure, deterministic function of the result rows.

    **History capture:** pass an empty dict as ``histories`` to receive
    ``{user_id: [HistoryPoint, ...]}`` covering every day each user actually
    played.  Decay days don't produce their own entries — their effect surfaces
    in the next played day's rating, so the line a caller plots through the
    points already accounts for any intervening inactivity.

    Pass ``include_decay_in_history=True`` to additionally emit one point per
    absent puzzle day for every user already seen on a prior day, with
    ``is_decay=True``, ``delta`` set to that day's decay (zero during grace)
    and ``rating`` set to the post-decay value.  A caller can then plot a
    fully day-resolved trajectory — playing days are still distinguishable
    via ``is_decay=False``.

    ``current_puzzle_number`` (optional): the puzzle that is "in progress" on
    the server's wall clock — i.e. ``expected_puzzle_number(date.today())``.
    Absence decay (and its history points / skip-streak bumps) is gated on
    ``puzzle_number < current_puzzle_number``, so the still-open day does not
    punish anyone who hasn't posted yet.  Contest math among players who *did*
    post on the current day still runs.  Leave as ``None`` (default) to treat
    every puzzle day in the data as concluded — useful in tests where there
    is no "today".

    ``rank_fn`` optionally replaces the default Akari-style
    perfect/accuracy/time ranking with another minigame's per-day ranker.

    ``first_skip_last_place`` (experimental, the ``+test`` command flag):
    an above-default player's *first* absent day costs what a virtual
    last-place finish against that day's real field would have cost
    (:func:`_last_place_decay_delta`), never dropping past ``start_rating``.
    Requires at least two real players that day — solo days, sub-default
    absentees, and every later day of the same streak fall through to the
    percentage rule unchanged (callers wanting a flat non-ramping rate pin
    ``decay_max`` to ``decay_base``).  The lost points feed the same
    zero-sum transfer pool.
    """
    if rank_fn is None:
        rank_fn = rank_participants
    if start_rating is None:
        start_rating = float(constants.AKARI_START_RATING)
    if damping is None:
        damping = constants.AKARI_RATING_DAMPING
    if decay_base is None:
        decay_base = constants.AKARI_DECAY_BASE
    if decay_max is None:
        decay_max = constants.AKARI_DECAY_MAX
    if decay_grace is None:
        decay_grace = constants.AKARI_DECAY_GRACE

    by_puzzle = {}
    for row in rows:
        number = int(row.puzzle_number)
        # Drop garbage puzzle numbers: non-positive, or far beyond today's real
        # puzzle (e.g. a troll posting "Daily Akari 9999999999").
        if number < 1 or (max_puzzle is not None and number > max_puzzle):
            continue
        by_puzzle.setdefault(number, []).append(row)

    ratings = {}       # user_id -> float
    games = {}         # user_id -> int (rated days only)
    peak = {}          # user_id -> float
    last_delta = {}    # user_id -> float (last change, contest or decay)
    skip_streak = {}   # user_id -> int (consecutive recent days missed)
    last_puzzle = {}   # user_id -> int (last day actually played)

    for puzzle_number in sorted(by_puzzle):
        # One row per user per day; the DB already guarantees this, but dedupe
        # defensively (keep the first occurrence) so the algorithm is robust.
        day_rows = {}
        puzzle_date_for_day = None
        for row in by_puzzle[puzzle_number]:
            day_rows.setdefault(str(row.user_id), row)
            if puzzle_date_for_day is None:
                puzzle_date_for_day = getattr(row, 'puzzle_date', None)

        for user_id in sorted(day_rows):
            if user_id not in ratings:
                ratings[user_id] = start_rating
                games[user_id] = 0
                peak[user_id] = start_rating
                last_delta[user_id] = 0.0
                skip_streak[user_id] = 0
                last_puzzle[user_id] = puzzle_number

        # Contest among the day's players (needs at least two to be a contest).
        # day_ratings/ranks stay None on solo days; the first-skip last-place
        # decay below keys off that to fall back to the percentage rule.
        performances = {}
        day_ratings = None
        ranks = None
        if len(day_rows) >= 2:
            day_ratings = {user_id: ratings[user_id] for user_id in day_rows}
            ranks = rank_fn(day_rows.values())
            # When the caller wants history, harvest the geometric-mean "need"
            # values that compute_round computes anyway, then convert to the
            # CF-style performance ``2*need - rating``: assuming ``need`` is the
            # arithmetic midpoint between the user's rating and their performance
            # (the same implicit assumption CF's UI makes), this is bounded by
            # the field's spread and matches what ``;plot perf`` displays.
            round_needs = {} if histories is not None else None
            deltas = compute_round(
                day_ratings, ranks, damping=damping, needs=round_needs)
            for user_id, delta in deltas.items():
                ratings[user_id] += delta
                games[user_id] += 1
                last_delta[user_id] = delta
                if ratings[user_id] > peak[user_id]:
                    peak[user_id] = ratings[user_id]
            if round_needs is not None:
                for user_id, need in round_needs.items():
                    performances[user_id] = 2.0 * need - day_ratings[user_id]
        else:
            # Solo days produce no contest delta but still record a history point
            # so a lone early result shows up on the user's graph.
            deltas = {user_id: 0.0 for user_id in day_rows}

        # Everyone who showed up resets their skip streak and records the day.
        for user_id in day_rows:
            skip_streak[user_id] = 0
            last_puzzle[user_id] = puzzle_number

        # Suppress decay for the puzzle that is still "in progress" on the
        # server's clock: that day has not concluded for absent players yet, so
        # bumping their skip-streak / rating now would penalise them prematurely.
        day_concluded = (current_puzzle_number is None
                         or puzzle_number < current_puzzle_number)

        # Absence-decay loop.  Two design choices, both serving the zero-sum
        # invariant the guild has chosen for the rating ladder:
        #   1. Only above-default ratings actually move — sub-default absentees
        #      freeze.  Drifting low ratings back up to 1200 would create
        #      rating ex nihilo; freezing is the honest answer to "no signal,
        #      no change".
        #   2. The lost rating is pooled and redistributed to today's active
        #      players (below), so the day's total guild rating is conserved.
        # Frozen absentees still get a HistoryPoint (delta=0) so the +decay
        # graph stays a continuous line, not a dotted one.
        absent_records = []  # (user_id, delta) for every absent user
        decay_pool = 0.0
        if day_concluded:
            for user_id in ratings:
                if user_id in day_rows:
                    continue
                skip_streak[user_id] += 1
                if (first_skip_last_place
                        and skip_streak[user_id] == 1
                        and ratings[user_id] > start_rating
                        and day_ratings is not None):
                    delta = _last_place_decay_delta(
                        day_ratings, ranks, user_id, ratings[user_id], damping)
                    # Decay pulls toward the default but never past it.
                    delta = max(delta, start_rating - ratings[user_id])
                else:
                    raw = (start_rating - ratings[user_id]) * _decay_rate(
                        skip_streak[user_id], decay_base, decay_max, decay_grace)
                    delta = min(0.0, raw)  # clamp out the sub-default drift-up
                ratings[user_id] += delta
                last_delta[user_id] = delta
                absent_records.append((user_id, delta))
                decay_pool -= delta  # delta ≤ 0 ⇒ pool grows positive

        # Zero-sum transfer: today's pool funds today's participants equally.
        # A solo active player banks the whole thing; a 5-active field shares
        # it.  No active players means no payout (and no leak — the pool only
        # forms on a day with at least one row, which is the only kind of day
        # this loop iterates).
        transfer_share = 0.0
        if decay_pool > 0 and day_rows:
            transfer_share = decay_pool / len(day_rows)
            for user_id in day_rows:
                ratings[user_id] += transfer_share
                last_delta[user_id] = deltas[user_id] + transfer_share
                if ratings[user_id] > peak[user_id]:
                    peak[user_id] = ratings[user_id]

        if histories is not None:
            for user_id, row in day_rows.items():
                histories.setdefault(user_id, []).append(HistoryPoint(
                    puzzle_number=puzzle_number,
                    puzzle_date=getattr(row, 'puzzle_date', None),
                    rating=ratings[user_id],
                    delta=deltas.get(user_id, 0.0) + transfer_share,
                    performance=performances.get(user_id),
                    is_perfect=bool(row.is_perfect),
                    accuracy=int(getattr(row, 'accuracy', 0)),
                    time_seconds=int(getattr(row, 'time_seconds', 0)),
                ))

        if histories is not None and include_decay_in_history:
            for user_id, delta in absent_records:
                histories.setdefault(user_id, []).append(HistoryPoint(
                    puzzle_number=puzzle_number,
                    puzzle_date=puzzle_date_for_day,
                    rating=ratings[user_id],
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
            rating=ratings[user_id],
            games=games[user_id],
            peak=peak[user_id],
            last_delta=last_delta[user_id],
            skip_streak=skip_streak[user_id],
            last_puzzle=last_puzzle[user_id],
        )
        for user_id in ratings
    }
