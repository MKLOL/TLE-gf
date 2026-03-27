"""GuessThe.Game game definition for the minigames system."""

import re

from tle.cogs._minigame_common import ParsedResult, GameDef

_GREEN = '\U0001f7e9'   # 🟩
_YELLOW = '\U0001f7e8'  # 🟨
_RED = '\U0001f7e5'     # 🟥
_WHITE = '\u2b1c'       # ⬜
_BLACK = '\u2b1b'       # ⬛

_URL_RE = re.compile(r'GuessThe\.Game', re.IGNORECASE)
_NUMBER_RE = re.compile(r'(?<!<)#(\d+)')
_SQUARES_RE = re.compile(
    r'\U0001f3ae\s*'  # 🎮
    r'((?:[\U0001f7e9\U0001f7e8\U0001f7e5\u2b1c\u2b1b]\s*)+)'
)


def _find_position(squares, target):
    """Return 1-based position of first occurrence, or 0 if absent."""
    for i, s in enumerate(squares):
        if s == target:
            return i + 1
    return 0


def _parse_squares(text):
    """Extract ordered list of square characters from text."""
    return [ch for ch in text if ch in (_GREEN, _YELLOW, _RED, _WHITE, _BLACK)]


def parse_guessgame_message(content):
    """Parse GuessThe.Game result message(s).  Returns list of ``ParsedResult``.

    A single message may contain multiple game results.
    """
    if not _URL_RE.search(content):
        return []

    results = []
    lines = content.split('\n')

    for i, line in enumerate(lines):
        squares_match = _SQUARES_RE.search(line)
        if not squares_match:
            continue

        # Look backwards (up to 4 lines) for #NUMBER
        puzzle_number = None
        for j in range(i - 1, max(i - 5, -1), -1):
            num_match = _NUMBER_RE.search(lines[j])
            if num_match:
                puzzle_number = int(num_match.group(1))
                break
        # Also check the same line
        if puzzle_number is None:
            num_match = _NUMBER_RE.search(line)
            if num_match:
                puzzle_number = int(num_match.group(1))

        if puzzle_number is None:
            continue

        squares = _parse_squares(squares_match.group(1))
        if not squares:
            continue

        green_pos = _find_position(squares, _GREEN)
        yellow_pos = _find_position(squares, _YELLOW)

        # Map to generic columns so result_sort_key works correctly:
        #   accuracy   = 7 - green_position  (higher = better; 0 = no green)
        #   time_seconds = yellow_position   (lower = better; 7 = no yellow)
        #   is_perfect = green on first guess
        accuracy = (7 - green_pos) if green_pos > 0 else 0
        time_seconds = yellow_pos if yellow_pos > 0 else 7
        is_perfect = green_pos == 1

        results.append(ParsedResult(
            puzzle_number=puzzle_number,
            accuracy=accuracy,
            time_seconds=time_seconds,
            is_perfect=is_perfect,
        ))

    return results


def guessgame_score_matchup(row1, row2):
    """GuessGame scoring: earlier green wins; tiebreak by earlier yellow."""
    a1, a2 = row1.accuracy, row2.accuracy  # higher = earlier green
    if a1 != a2:
        return (1.0, 0.0) if a1 > a2 else (0.0, 1.0)
    t1, t2 = row1.time_seconds, row2.time_seconds  # lower = earlier yellow
    if t1 != t2:
        return (1.0, 0.0) if t1 < t2 else (0.0, 1.0)
    return 0.5, 0.5


def guessgame_is_eligible_winner(row):
    """Any result with a green square is eligible for the leaderboard."""
    return row.accuracy > 0


GUESSGAME_GAME = GameDef(
    name='guessgame',
    display_name='GuessThe.Game',
    feature_flag='guessgame',
    parse=parse_guessgame_message,
    score_matchup=guessgame_score_matchup,
    is_eligible_winner=guessgame_is_eligible_winner,
    missing_is_loss=True,
)
