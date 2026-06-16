"""Pure helpers and constants for the lockout (Round) cog.

Split out of ``lockout.py`` to keep each module under the line limit.
"""
from functools import cmp_to_key
from collections import namedtuple

from discord.ext import commands

MAX_ROUND_USERS = 5
LOWER_RATING = 800
UPPER_RATING = 3500
MATCH_DURATION = [5, 600]
MAX_PROBLEMS = 6
MAX_ALTS = 5
ROUNDS_PER_PAGE = 5
AUTO_UPDATE_TIME = 30
RECENT_SUBS_LIMIT = 50
PROBLEM_STATUS_UNSOLVED = 10**18
PROBLEM_STATUS_TESTING = -1
_PAGINATE_WAIT_TIME = 5 * 60


def _calc_round_score(users, status, times):
    def comp(a, b):
        if a[0] > b[0]:
            return -1
        if a[0] < b[0]:
            return 1
        if a[1] == b[1]:
            return 0
        return -1 if a[1] < b[1] else 1

    ranks = [[status[i], times[i], users[i]] for i in range(len(status))]
    ranks.sort(key=cmp_to_key(comp))
    res = []

    for user in ranks:
        User = namedtuple("User", "id points rank")
        # user points rank
        res.append(User(user[2], user[0], [[x[0], x[1]] for x in ranks].index([user[0], user[1]]) + 1))
    return res


class RoundCogError(commands.CommandError):
    pass
