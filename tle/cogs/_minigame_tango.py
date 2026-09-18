"""LinkedIn Tango definition for the minigames system.

Tango is LinkedIn's binary-grid puzzle.  It shares Queens' share format,
leaderboard format, Pacific rollover, and time-only scoring, so everything
here is the shared LinkedIn machinery pinned to the ``Tango`` header word
and Tango's own calendar anchor.  The LinkedIn player link is shared with
Queens: one ``;queens register`` / ``;tango register`` resolves both games.
"""

import datetime as dt

from tle import constants
from tle.cogs._minigame_common import GameDef, RatingDef
from tle.cogs._minigame_linkedin import (
    LinkedInDef, linkedin_current_puzzle_date,
    linkedin_best_result_sort_key,
    linkedin_result_group_key,
    linkedin_time_score_matchup,
    linkedin_winner_result_sort_key,
    make_share_parser,
    rank_linkedin_participants,
    share_detect_re, share_header_re,
)


_SHARE_HEADER_RE = share_header_re('Tango')
_DETECT_RE = share_detect_re('Tango')
# Tango #697 was published on 2026-09-04.
_TANGO_ANCHOR_DATE = dt.date(2026, 9, 4)
_TANGO_ANCHOR_NUMBER = 697

TANGO_LINKEDIN = LinkedInDef(
    anchor_date=_TANGO_ANCHOR_DATE,
    anchor_number=_TANGO_ANCHOR_NUMBER,
    admins_key='tango_admin_user_ids',
)


def _tango_date_for_puzzle_number(puzzle_number):
    return TANGO_LINKEDIN.date_for_number(puzzle_number)


def _tango_puzzle_number_for_date(puzzle_date):
    return TANGO_LINKEDIN.number_for_date(puzzle_date)


# Same Pacific rollover as Queens; a module-level name so tests can pin it.
_tango_current_puzzle_date = linkedin_current_puzzle_date


def current_puzzle_number():
    """The Tango puzzle in progress on LinkedIn's clock (gates decay)."""
    return _tango_puzzle_number_for_date(_tango_current_puzzle_date())


parse_tango_message = make_share_parser(_SHARE_HEADER_RE, TANGO_LINKEDIN)


TANGO_GAME = GameDef(
    name='tango',
    display_name='LinkedIn Tango',
    feature_flag='tango',
    linkedin=TANGO_LINKEDIN,
    parse=parse_tango_message,
    detect=_DETECT_RE,
    score_matchup=linkedin_time_score_matchup,
    is_eligible_winner=lambda _row: True,
    best_result_sort_key=linkedin_best_result_sort_key,
    winner_result_sort_key=linkedin_winner_result_sort_key,
    result_group_key=linkedin_result_group_key,
    rating=RatingDef(
        rank_fn=rank_linkedin_participants,
        decay_base=constants.TANGO_DECAY_BASE,
        decay_max=constants.TANGO_DECAY_MAX,
        decay_grace=constants.TANGO_DECAY_GRACE,
        current_puzzle_number_fn=current_puzzle_number,
    ),
)
