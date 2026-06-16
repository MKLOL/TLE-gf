"""Shared constants, error types and helpers for the cache subsystem.

Split out of the former single-file ``cache_system2`` module so each cache
class can live in its own sub-500-line module. Everything here is re-exported
from the package ``__init__`` for backwards compatibility.
"""
from discord.ext import commands

_CONTESTS_PER_BATCH_IN_CACHE_UPDATES = 100
CONTEST_BLACKLIST = {1308, 1309, 1431, 1432}
_DIV_TAGS = ['div1', 'div2', 'div3', 'div4', 'edu']


def _is_blacklisted(contest):
    return contest.id in CONTEST_BLACKLIST


class CacheError(commands.CommandError):
    pass


class ContestCacheError(CacheError):
    pass


class ContestNotFound(ContestCacheError):
    def __init__(self, contest_id):
        super().__init__(f'Contest with id `{contest_id}` not found')
        self.contest_id = contest_id


class ProblemsetCacheError(CacheError):
    pass


class ProblemsetNotCached(ProblemsetCacheError):
    def __init__(self, contest_id):
        super().__init__(f'Problemset for contest with id {contest_id} not cached.')


class RanklistCacheError(CacheError):
    pass


class RanklistNotMonitored(RanklistCacheError):
    def __init__(self, contest):
        super().__init__(f'The ranklist for `{contest.name}` is not being monitored')
        self.contest = contest
