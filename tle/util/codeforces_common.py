import functools
import json
import logging
import re
import time
from collections import defaultdict
import itertools
from discord.ext import commands
import discord

from tle import constants
from tle.util import cache_system2
from tle.util import codeforces_api as cf
from tle.util import db
from tle.util import events
# Formatting/parsing helpers live in cf_format to keep this module under the
# 500-line limit. Re-exported here so existing cf_common.<name> uses still work.
from tle.util.cf_format import (
    FilterError,
    ParamParseError,
    days_ago,
    filter_flags,
    get_start_and_end_of_month,
    negate_flags,
    parse_date,
    parse_daterange,
    parse_rating,
    parse_tags,
    pretty_time_format,
    time_format,
)

logger = logging.getLogger(__name__)

# Connection to database
user_db = None

# Cache system
cache2 = None

# Event system
event_sys = events.EventSystem()

_contest_id_to_writers_map = None

_initialize_done = False

active_groups = defaultdict(set)


async def initialize(nodb):
    global cache2
    global user_db
    global event_sys
    global _contest_id_to_writers_map
    global _initialize_done

    if _initialize_done:
        # This happens if the bot loses connection to Discord and on_ready is triggered again
        # when it reconnects.
        return

    init_start = time.time()
    logger.info('Initialization started')

    t = time.time()
    await cf.initialize()
    logger.info(f'cf.initialize() completed in {time.time()-t:.2f}s')

    t = time.time()
    if nodb:
        user_db = db.DummyUserDbConn()
    else:
        user_db = db.UserDbConn(constants.USER_DB_FILE_PATH)
    logger.info(f'UserDbConn created in {time.time()-t:.2f}s')

    t = time.time()
    cache_db = db.CacheDbConn(constants.CACHE_DB_FILE_PATH)
    logger.info(f'CacheDbConn created in {time.time()-t:.2f}s')

    cache2 = cache_system2.CacheSystem(cache_db)
    await cache2.run()

    t = time.time()
    try:
        with open(constants.CONTEST_WRITERS_JSON_FILE_PATH) as f:
            data = json.load(f)
        _contest_id_to_writers_map = {contest['id']: [s.lower() for s in contest['writers']] for contest in data}
        logger.info(f'Contest writers loaded from JSON file in {time.time()-t:.2f}s')
    except FileNotFoundError:
        logger.warning('JSON file containing contest writers not found')

    _initialize_done = True
    logger.info(f'Initialization completed in {time.time()-init_start:.2f}s')


# algmyr's guard idea:
def user_guard(*, group, get_exception=None):
    active = active_groups[group]

    def guard(fun):
        @functools.wraps(fun)
        async def f(self, ctx, *args, **kwargs):
            user = ctx.message.author.id
            if user in active:
                logger.info(f'{user} repeatedly calls {group} group')
                if get_exception is not None:
                    raise get_exception()
                return
            active.add(user)
            try:
                await fun(self, ctx, *args, **kwargs)
            finally:
                active.remove(user)

        return f

    return guard


def is_contest_writer(contest_id, handle):
    if _contest_id_to_writers_map is None:
        return False
    writers = _contest_id_to_writers_map.get(contest_id)
    return writers and handle.lower() in writers


_NONSTANDARD_CONTEST_INDICATORS = [
    'wild', 'fools', 'unrated', 'surprise', 'unknown', 'friday', 'q#', 'testing',
    'marathon', 'kotlin', 'onsite', 'experimental', 'abbyy', 'icpc']


def is_nonstandard_contest(contest):
    return any(string in contest.name.lower() for string in _NONSTANDARD_CONTEST_INDICATORS)

def is_nonstandard_problem(problem):
    return (is_nonstandard_contest(cache2.contest_cache.get_contest(problem.contestId)) or
            problem.matches_all_tags(['*special']))


async def get_visited_contests(handles : [str]):
    """ Returns a set of contest ids of contests that any of the given handles
        has at least one non-CE submission.
    """
    user_submissions = [await cf.user.status(handle=handle) for handle in handles]
    problem_to_contests = cache2.problemset_cache.problem_to_contests

    contest_ids = []
    for sub in itertools.chain.from_iterable(user_submissions):
        if sub.verdict == 'COMPILATION_ERROR':
            continue
        try:
            contest = cache2.contest_cache.get_contest(sub.problem.contestId)
            problem_id = (sub.problem.name, contest.startTimeSeconds)
            contest_ids += problem_to_contests[problem_id]
        except cache_system2.ContestNotFound:
            pass
    return set(contest_ids)

# These are special rated-for-all contests which have a combined ranklist for onsite and online
# participants. The onsite participants have their submissions marked as out of competition. Just
# Codeforces things.
_RATED_FOR_ONSITE_CONTEST_IDS = [
    86,   # Yandex.Algorithm 2011 Round 2 https://codeforces.com/contest/86
    173,  # Croc Champ 2012 - Round 1 https://codeforces.com/contest/173
    335,  # MemSQL start[c]up Round 2 - online version https://codeforces.com/contest/335
]


def is_rated_for_onsite_contest(contest):
    return contest.id in _RATED_FOR_ONSITE_CONTEST_IDS


class ResolveHandleError(commands.CommandError):
    pass


class HandleCountOutOfBoundsError(ResolveHandleError):
    def __init__(self, mincnt, maxcnt, detail=None):
        message = f'Number of handles must be between {mincnt} and {maxcnt}'
        if detail:
            message += f' ({detail})'
        super().__init__(message)


class FindMemberFailedError(ResolveHandleError):
    def __init__(self, member):
        super().__init__(f'Unable to convert `{member}` to a server member')


class HandleNotRegisteredError(ResolveHandleError):
    def __init__(self, member):
        super().__init__(f'Codeforces handle for {member.mention} not found in database. '
                          'Use ;handle identify <cfhandle> (where <cfhandle> needs to be replaced with your codeforces handle, e.g. ;handle identify tourist) to add yourself to the database')


class HandleIsVjudgeError(ResolveHandleError):
    HANDLES = ('vjudge1 vjudge2 vjudge3 vjudge4 vjudge5 '
               'luogu_bot1 luogu_bot2 luogu_bot3 luogu_bot4 luogu_bot5').split()

    def __init__(self, handle):
        super().__init__(f"`{handle}`? I'm not doing that!\n\n(╯°□°）╯︵ ┻━┻")


async def resolve_handles(ctx, converter, handles, *, mincnt=1, maxcnt=5, default_to_all_server=False):
    """Convert an iterable of strings to CF handles.

    Resolution rules:
      !<digits>   — Discord user by ID (internal, used for self-lookup)
      !<name>     — Discord user by name via converter (user-facing, e.g. ;versus !user)
      <@id>       — Discord mention
      -c<handle>  — Force raw Codeforces handle (skip Discord lookup)
      plain text  — Try Discord username → display name → raw CF handle
    """
    # Order-preserving: ;teamrate pairs multipliers with the handles it
    # passed in, and a set would hand them back in hash-seed order.
    handles = list(dict.fromkeys(handles))
    if default_to_all_server and not handles:
        handles.append('+server')
    if '+server' in handles:
        handles.remove('+server')
        guild_handles = sorted({handle for discord_id, handle
                                in user_db.get_handles_for_guild(ctx.guild.id)})
        handles.extend(handle for handle in guild_handles if handle not in handles)
    if len(handles) < mincnt or (maxcnt and maxcnt < len(handles)):
        raise HandleCountOutOfBoundsError(mincnt, maxcnt)
    resolved_handles = []
    for handle in handles:
        if handle.startswith('-c'):
            # -c prefix: force raw CF handle
            handle = handle[2:]
        elif handle.startswith('!'):
            # ! prefix: Discord user lookup
            member_identifier = handle[1:]
            if member_identifier.isdigit():
                # Numeric — direct ID lookup (guaranteed unique)
                member = ctx.guild.get_member(int(member_identifier))
                if member is None:
                    raise FindMemberFailedError(member_identifier)
            else:
                # Non-numeric — name-based lookup via converter (backward compat)
                if member_identifier.endswith('#0'):
                    member_identifier = member_identifier[:-2]
                try:
                    member = await converter.convert(ctx, member_identifier)
                except commands.errors.CommandError:
                    raise FindMemberFailedError(member_identifier)
            handle = user_db.get_handle(member.id, ctx.guild.id)
            if handle is None:
                raise HandleNotRegisteredError(member)
        elif (mention_match := re.match(r'<@!?(\d+)>', handle)):
            # Discord mention — extract user ID
            member_id = int(mention_match.group(1))
            member = ctx.guild.get_member(member_id)
            if member is None:
                raise FindMemberFailedError(handle)
            handle = user_db.get_handle(member.id, ctx.guild.id)
            if handle is None:
                raise HandleNotRegisteredError(member)
        else:
            # Plain text: try Discord username → display name → CF handle
            resolved_member = _resolve_member_by_name(ctx.guild, handle)
            if resolved_member is not None:
                cf_handle = user_db.get_handle(resolved_member.id, ctx.guild.id)
                if cf_handle is not None:
                    handle = cf_handle
        if handle in HandleIsVjudgeError.HANDLES:
            raise HandleIsVjudgeError(handle)
        resolved_handles.append(handle)
    return _dedupe_handles(resolved_handles, mincnt, maxcnt)


def _dedupe_handles(handles, mincnt, maxcnt):
    """Collapse spellings of one handle — Codeforces handles are case-insensitive.

    ``;versus tfg Tfg tFg`` is one person three times, not three people; taken
    literally it slips past ``maxcnt`` and then compares a user with
    themselves under every spelling. Each survivor is spelled the way the CF
    user cache has it. Order is kept; the count check is repeated on the
    result because the pre-resolution count only saw raw tokens.
    """
    canonical = user_db.canonical_handles(handles)
    unique, seen = [], set()
    for handle in handles:
        handle = canonical.get(handle.lower(), handle)
        key = handle.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(handle)
    if len(unique) < mincnt:
        raise HandleCountOutOfBoundsError(
            mincnt, maxcnt,
            f'{len(handles) - len(unique)} of them were the same handle spelled differently')
    return unique


def _resolve_member_by_name(guild, name):
    """Find a guild member by username first, then display name (case-insensitive)."""
    lowered = name.lower()
    for m in guild.members:
        if m.name.lower() == lowered:
            return m
    for m in guild.members:
        if m.display_name.lower() == lowered:
            return m
    return None

def members_to_handles(members: [discord.Member], guild_id):
    handles = []
    for member in members:
        handle = user_db.get_handle(member.id, guild_id)
        if handle is None:
            raise HandleNotRegisteredError(member)
        handles.append(handle)
    return handles

def fix_urls(user: cf.User):
    if user.titlePhoto.startswith('//'):
        user = user._replace(titlePhoto = 'https:' + user.titlePhoto)
    return user


class SubFilter:
    def __init__(self, rated=True):
        self.team = False
        self.rated = rated
        self.dlo, self.dhi = 0, 10**10
        self.rlo, self.rhi = 500, 3800
        self.types = []
        self.tags = []
        self.bantags = []
        self.contests = []
        self.indices = []
        self.only_rated = False
        self.rated_contest_ids_by_handle = {}  # handle (lowercase) -> set of contest IDs

    def parse(self, args):
        args = list(set(args))
        rest = []
        self.dlo, self.dhi = parse_daterange(args)
        for arg in args:
            if arg == '+team':
                self.team = True
            elif arg == '+contest':
                self.types.append('CONTESTANT')
            elif arg == '+rated':
                self.types.append('CONTESTANT')
                self.only_rated = True
            elif arg =='+outof':
                self.types.append('OUT_OF_COMPETITION')
            elif arg == '+virtual':
                self.types.append('VIRTUAL')
            elif arg == '+practice':
                self.types.append('PRACTICE')
            elif arg[0:2] == 'c+':
                self.contests.append(arg[2:])
            elif arg[0:2] == 'i+':
                self.indices.append(arg[2:])
            elif arg[0] == '+':
                if len(arg) == 1:
                    raise ParamParseError('Problem tag cannot be empty.')
                self.tags.append(arg[1:])
            elif arg[0] == '~':
                if len(arg) == 1:
                    raise ParamParseError('Problem tag cannot be empty.')
                self.bantags.append(arg[1:])
            elif arg[0:2] == 'd<': # these are still here to prevent them from staying in rest (they're handled above the if's though)
                pass
            elif arg[0:3] == 'd>=':
                pass
            elif arg[0:3] in ['r<=', 'r>=']:
                if len(arg) < 4:
                    raise ParamParseError(f'{arg} is an invalid rating argument')
                try:
                    val = int(arg[3:])
                except ValueError:
                    raise ParamParseError(f'{arg} is an invalid rating argument')
                if arg[1] == '>':
                    self.rlo = max(self.rlo, val)
                else:
                    self.rhi = min(self.rhi, val)
                self.rated = True
            else:
                rest.append(arg)

        self.types = self.types or ['CONTESTANT', 'OUT_OF_COMPETITION', 'VIRTUAL', 'PRACTICE']
        return rest

    @staticmethod
    def filter_solved(submissions):
        """Filters and keeps only solved submissions. If a problem is solved multiple times the first
        accepted submission is kept. The unique id for a problem is (problem name, contest start time).
        """
        submissions.sort(key=lambda sub: sub.creationTimeSeconds)
        problems = set()
        solved_subs = []

        for submission in submissions:
            problem = submission.problem
            contest = cache2.contest_cache.contest_by_id.get(problem.contestId, None)
            if submission.verdict == 'OK':
                # Assume (name, contest start time) is a unique identifier for problems
                problem_key = (problem.name, contest.startTimeSeconds if contest else 0)
                if problem_key not in problems:
                    solved_subs.append(submission)
                    problems.add(problem_key)
        return solved_subs

    def filter_subs(self, submissions):
        submissions = SubFilter.filter_solved(submissions)
        filtered_subs = []
        for submission in submissions:
            problem = submission.problem
            contest = cache2.contest_cache.contest_by_id.get(problem.contestId, None)
            type_ok = submission.author.participantType in self.types
            date_ok = self.dlo <= submission.creationTimeSeconds < self.dhi
            tag_ok = problem.matches_all_tags(self.tags)
            bantag_ok = not problem.matches_any_tag(self.bantags)
            index_ok = not self.indices or any(index.lower() == problem.index.lower() for index in self.indices)
            contest_ok = not self.contests or (contest and contest.matches(self.contests))
            team_ok = self.team or len(submission.author.members) == 1
            if self.rated:
                problem_ok = contest and contest.id < cf.GYM_ID_THRESHOLD and not is_nonstandard_problem(problem)
                rating_ok = problem.rating and self.rlo <= problem.rating <= self.rhi
            else:
                # acmsguru and gym allowed
                problem_ok = (not contest or contest.id >= cf.GYM_ID_THRESHOLD
                              or not is_nonstandard_problem(problem))
                rating_ok = True
            # +rated: skip contests that weren't rated for this user
            if self.only_rated and self.rated_contest_ids_by_handle:
                handle = submission.author.members[0].handle.lower() if submission.author.members else ''
                rated_ok2 = problem.contestId in self.rated_contest_ids_by_handle.get(handle, set())
            else:
                rated_ok2 = True
            if type_ok and date_ok and rating_ok and tag_ok and bantag_ok and team_ok and problem_ok and contest_ok and index_ok and rated_ok2:
                filtered_subs.append(submission)
        return filtered_subs

    def filter_rating_changes(self, rating_changes):
        rating_changes = [change for change in rating_changes
                    if self.dlo <= change.ratingUpdateTimeSeconds < self.dhi]
        return rating_changes
