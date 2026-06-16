"""Codeforces API data classes, ranks, and error types.

Split out of ``codeforces_api.py`` to keep each module under the project's
line limit. Everything here is pure data / pure helpers with no module-global
mutable state, so it is safe to import from anywhere. ``codeforces_api.py``
re-exports every public name defined here, so callers should keep importing
from ``tle.util.codeforces_api``.
"""
from collections import defaultdict
from typing import Any, Dict, Iterable, List, NamedTuple, Optional

from discord.ext import commands

# ruff: noqa: N815

CONTEST_BASE_URL = 'https://codeforces.com/contest/'
CONTESTS_BASE_URL = 'https://codeforces.com/contests/'
GYM_BASE_URL = 'https://codeforces.com/gym/'
PROFILE_BASE_URL = 'https://codeforces.com/profile/'
ACMSGURU_BASE_URL = 'https://codeforces.com/problemsets/acmsguru/'
GYM_ID_THRESHOLD = 100000
DEFAULT_RATING = 800


class Rank(NamedTuple):
    """Codeforces rank."""
    low: Optional[int]
    high: Optional[int]
    title: str
    title_abbr: Optional[str]
    color_graph: Optional[str]
    color_embed: Optional[int]

RATED_RANKS = (
    Rank(-10 ** 9, 1200, 'Newbie', 'N', '#CCCCCC', 0x808080),
    Rank(1200, 1400, 'Pupil', 'P', '#77FF77', 0x008000),
    Rank(1400, 1600, 'Specialist', 'S', '#77DDBB', 0x03a89e),
    Rank(1600, 1900, 'Expert', 'E', '#AAAAFF', 0x0000ff),
    Rank(1900, 2100, 'Candidate Master', 'CM', '#FF88FF', 0xaa00aa),
    Rank(2100, 2300, 'Master', 'M', '#FFCC88', 0xff8c00),
    Rank(2300, 2400, 'International Master', 'IM', '#FFBB55', 0xf57500),
    Rank(2400, 2600, 'Grandmaster', 'GM', '#FF7777', 0xff3030),
    Rank(2600, 3000, 'International Grandmaster', 'IGM', '#FF3333', 0xff0000),
    Rank(3000, 4000, 'Legendary Grandmaster', 'LGM', '#AA0000', 0xcc0000),
    Rank(4000, 10 ** 9, 'Tourist', 'T', '#330000', 0x000000)
)
UNRATED_RANK = Rank(None, None, 'Unrated', None, None, None)


def rating2rank(rating: Optional[int]) -> Rank:
    """Returns the rank corresponding to the given rating."""
    if rating is None:
        return UNRATED_RANK
    for rank in RATED_RANKS:
        assert rank.low is not None and rank.high is not None
        if rank.low <= rating < rank.high:
            return rank
    raise ValueError(f'Rating {rating} outside range of known ranks.')


# Data classes

class User(NamedTuple):
    """Codeforces user."""
    handle: str
    firstName: Optional[str]
    lastName: Optional[str]
    country: Optional[str]
    city: Optional[str]
    organization: Optional[str]
    contribution: int
    rating: Optional[int]
    maxRating: Optional[int]
    lastOnlineTimeSeconds: int
    registrationTimeSeconds: int
    friendOfCount: int
    titlePhoto: str

    @property
    def effective_rating(self) -> int:
        """Returns the effective rating of the user."""
        return self.rating if self.rating is not None else DEFAULT_RATING

    @property
    def rank(self) -> Rank:
        """Returns the rank corresponding to the user's rating."""
        return rating2rank(self.rating)

    @property
    def url(self) -> str:
        """Returns the URL of the user's profile."""
        return f'{PROFILE_BASE_URL}{self.handle}'


class RatingChange(NamedTuple):
    """Codeforces rating change."""
    contestId: int
    contestName: str
    handle: str
    rank: int
    ratingUpdateTimeSeconds: int
    oldRating: int
    newRating: int

class Contest(NamedTuple):
    """Codeforces contest."""
    id: int
    name: str
    startTimeSeconds: Optional[int]
    durationSeconds: Optional[int]
    type: str
    phase: str
    preparedBy: Optional[str]

    PHASES = 'BEFORE CODING PENDING_SYSTEM_TEST SYSTEM_TEST FINISHED'.split()

    @property
    def end_time(self) -> Optional[int]:
        """Returns the end time of the contest."""
        if self.startTimeSeconds is None or self.durationSeconds is None:
            return None
        return self.startTimeSeconds + self.durationSeconds

    @property
    def url(self) -> str:
        """Returns the URL of the contest."""
        if self.id < GYM_ID_THRESHOLD:
            return f'{CONTEST_BASE_URL}{self.id}'
        return f'{GYM_BASE_URL}{self.id}'

    @property
    def register_url(self) -> str:
        """Returns the URL to register for the contest."""
        return f'{CONTESTS_BASE_URL}{self.id}'

    def matches(self, markers: Iterable[str]) -> bool:
        """Returns whether the contest matches any of the given markers."""
        def filter_and_normalize(s: str) -> str:
            return ''.join(x for x in s.lower() if x.isalnum())
        return any(filter_and_normalize(marker) in filter_and_normalize(self.name) for marker in markers)

class Member(NamedTuple):
    """Codeforces party member."""
    handle: str

class Party(NamedTuple):
    """Codeforces party."""
    contestId: Optional[int]
    members: List[Member]
    participantType: str
    teamId: Optional[int]
    teamName: Optional[str]
    ghost: bool
    room: Optional[int]
    startTimeSeconds: Optional[int]

    PARTICIPANT_TYPES = ('CONTESTANT', 'PRACTICE', 'VIRTUAL', 'MANAGER', 'OUT_OF_COMPETITION')

class Problem(NamedTuple):
    """Codeforces problem."""
    contestId: Optional[int]
    problemsetName: Optional[str]
    index: str
    name: str
    type: str
    points: Optional[float]
    rating: Optional[int]
    tags: List[str]

    @property
    def contest_identifier(self) -> str:
        """Returns a string identifying the contest."""
        return f'{self.contestId}{self.index}'

    @property
    def url(self) -> str:
        """Returns the URL of the problem."""
        if self.contestId is None:
            assert self.problemsetName == 'acmsguru', f'Unknown problemset {self.problemsetName}'
            return f'{ACMSGURU_BASE_URL}problem/99999/{self.index}'
        base = CONTEST_BASE_URL if self.contestId < GYM_ID_THRESHOLD else GYM_BASE_URL
        return f'{base}{self.contestId}/problem/{self.index}'

    def has_metadata(self) -> bool:
        """Returns whether the problem has metadata."""
        return self.contestId is not None and self.rating is not None

    def _matching_tags_dict(self, match_tags: Iterable[str]) -> Dict[str, List[str]]:
        """Returns a dict with matching tags."""
        tags = defaultdict(list)
        for match_tag in match_tags:
            for tag in self.tags:
                if match_tag in tag:
                    tags[match_tag].append(tag)
        return dict(tags)

    def matches_all_tags(self, match_tags: Iterable[str]) -> bool:
        """Returns whether the problem matches all of the given tags."""
        match_tags = set(match_tags)
        return len(self._matching_tags_dict(match_tags)) == len(match_tags)

    def matches_any_tag(self, match_tags: Iterable[str]) -> bool:
        """Returns whether the problem matches any of the given tags."""
        match_tags = set(match_tags)
        return len(self._matching_tags_dict(match_tags)) > 0

    def get_matched_tags(self, match_tags: Iterable[str]) -> List[str]:
        """Returns a list of tags that match any of the given tags."""
        return [
            tag for tags in self._matching_tags_dict(match_tags).values()
            for tag in tags
        ]

class ProblemStatistics(NamedTuple):
    """Codeforces problem statistics."""
    contestId: Optional[int]
    index: str
    solvedCount: int

class Submission(NamedTuple):
    """Codeforces submission for a problem."""
    id: int
    contestId: Optional[int]
    problem: Problem
    author: Party
    programmingLanguage: str
    verdict: Optional[str]
    creationTimeSeconds: int
    relativeTimeSeconds: int

class RanklistRow(NamedTuple):
    """Codeforces ranklist row."""
    party: Party
    rank: int
    points: float
    penalty: int
    problemResults: List['ProblemResult']

class ProblemResult(NamedTuple):
    """Codeforces problem result."""
    points: float
    penalty: Optional[int]
    rejectedAttemptCount: int
    type: str
    bestSubmissionTimeSeconds: Optional[int]

def make_from_dict(namedtuple_cls, dict_):
    """Creates a namedtuple from a subset of values in a dict."""
    field_vals = [dict_.get(field) for field in namedtuple_cls._fields]
    return namedtuple_cls._make(field_vals)


# Error classes

class CodeforcesApiError(commands.CommandError):
    """Base class for all API related errors."""

    def __init__(self, message: Optional[str] = None):
        super().__init__(message or 'Codeforces API error. There is nothing you or the Admins of the Discord server can do to fix it. We need to wait until Mike does his job.')


class TrueApiError(CodeforcesApiError):
    """An error originating from a valid response of the API."""

    def __init__(self, comment: str, message: Optional[str] = None):
        super().__init__(message)
        self.comment = comment


class ClientError(CodeforcesApiError):
    """An error caused by a request to the API failing."""

    def __init__(self):
        super().__init__('Error connecting to Codeforces API')


class HandleNotFoundError(TrueApiError):
    """An error caused by a handle not being found on Codeforces."""

    def __init__(self, comment: str, handle: str):
        super().__init__(comment, f'Handle `{handle}` not found on Codeforces')
        self.handle = handle


class HandleInvalidError(TrueApiError):
    """An error caused by a handle not being valid on Codeforces."""

    def __init__(self, comment: str, handle: str):
        super().__init__(comment, f'`{handle}` is not a valid Codeforces handle')
        self.handle = handle


class CallLimitExceededError(TrueApiError):
    """An error caused by the call limit being exceeded."""

    def __init__(self, comment: str):
        super().__init__(comment, 'Codeforces API call limit exceeded')


class ContestNotFoundError(TrueApiError):
    """An error caused by a contest not being found on Codeforces."""

    def __init__(self, comment: str, contest_id: Any):
        super().__init__(
            comment, f'Contest with ID `{contest_id}` not found on Codeforces'
        )


class RatingChangesUnavailableError(TrueApiError):
    """An error caused by rating changes being unavailable for a contest."""

    def __init__(self, comment: str, contest_id: Any):
        super().__init__(
            comment, f'Rating changes unavailable for contest with ID `{contest_id}`'
        )
