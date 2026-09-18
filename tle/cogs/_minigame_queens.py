"""LinkedIn Queens definition for the minigames system.

All parsing and scoring is the shared LinkedIn machinery in
``_minigame_linkedin``; this module only pins the Queens header word, the
calendar anchor, and the rating knobs.  The historical ``queens_*`` names are
kept as aliases because tests and the cog-side helpers import them.
"""

import datetime as dt

from tle import constants
from tle.cogs._minigame_common import GameDef, RatingDef
from tle.cogs._minigame_linkedin import (  # noqa: F401  (re-exported aliases)
    LINKEDIN_WEEKDAY_DIFFICULTIES as QUEENS_WEEKDAY_DIFFICULTIES,
    LinkedInDef, _LINKEDIN_TIME_ZONE, linkedin_current_puzzle_date,
    LinkedInLeaderboardEntry as QueensLeaderboardEntry,
    _SHARE_TIME_RE, _TIME_RE,
    linkedin_best_result_sort_key as queens_best_result_sort_key,
    linkedin_result_group_key as queens_result_group_key,
    linkedin_status_flags as queens_status_flags,
    linkedin_time_score_matchup as queens_time_score_matchup,
    linkedin_weekly_difficulty_map as queens_weekly_difficulty_map,
    linkedin_winner_result_sort_key as queens_winner_result_sort_key,
    make_share_parser,
    normalize_linkedin_name as normalize_queens_name,
    parse_linkedin_leaderboard as parse_queens_leaderboard,
    parse_linkedin_time as parse_queens_time,
    rank_linkedin_participants as rank_queens_participants,
    share_detect_re, share_header_re,
)


_SHARE_HEADER_RE = share_header_re('Queens')
_DETECT_RE = share_detect_re('Queens')
_QUEENS_ANCHOR_DATE = dt.date(2026, 6, 8)
_QUEENS_ANCHOR_NUMBER = 769
# Kept for importers; the canonical zone lives in ``_minigame_common``.
_QUEENS_TIME_ZONE = _LINKEDIN_TIME_ZONE

QUEENS_LINKEDIN = LinkedInDef(
    anchor_date=_QUEENS_ANCHOR_DATE,
    anchor_number=_QUEENS_ANCHOR_NUMBER,
    admins_key='queens_admin_user_ids',
    legacy_ordinal_numbers=True,
)


def _queens_date_for_puzzle_number(puzzle_number):
    return QUEENS_LINKEDIN.date_for_number(puzzle_number)


def _queens_puzzle_number_for_date(puzzle_date):
    """Inverse of :func:`_queens_date_for_puzzle_number`."""
    return QUEENS_LINKEDIN.number_for_date(puzzle_date)


# Shared with every LinkedIn game; tests monkeypatch this name in the modules
# that import it, so it stays a module-level function here.
_queens_current_puzzle_date = linkedin_current_puzzle_date


def current_puzzle_number():
    """The Queens puzzle that is in progress on LinkedIn's clock.

    Inactivity decay is gated on this: today's puzzle has not concluded for
    players who simply haven't posted yet, so it must not cost them rating.
    Routed through this module's ``_queens_current_puzzle_date`` so tests can
    pin "today".
    """
    return _queens_puzzle_number_for_date(_queens_current_puzzle_date())


parse_queens_message = make_share_parser(_SHARE_HEADER_RE, QUEENS_LINKEDIN)


QUEENS_GAME = GameDef(
    name='queens',
    display_name='LinkedIn Queens',
    feature_flag='queens',
    linkedin=QUEENS_LINKEDIN,
    parse=parse_queens_message,
    detect=_DETECT_RE,
    score_matchup=queens_time_score_matchup,
    is_eligible_winner=lambda _row: True,
    best_result_sort_key=queens_best_result_sort_key,
    winner_result_sort_key=queens_winner_result_sort_key,
    result_group_key=queens_result_group_key,
    rating=RatingDef(
        rank_fn=rank_queens_participants,
        decay_base=constants.QUEENS_DECAY_BASE,
        decay_max=constants.QUEENS_DECAY_MAX,
        decay_grace=constants.QUEENS_DECAY_GRACE,
        current_puzzle_number_fn=current_puzzle_number,
    ),
)
