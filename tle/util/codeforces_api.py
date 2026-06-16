import asyncio
from collections import deque
import functools
import hashlib
import itertools
import logging
import os
import random
import string
import time
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
import aiohttp

from tle.util import codeforces_common as cf_common
# Data classes, ranks, and error types live in a sibling module to keep this
# file under the line limit. Re-export them so callers keep importing from
# ``tle.util.codeforces_api``.
from tle.util._cf_api_types import (  # noqa: F401
    ACMSGURU_BASE_URL,
    CONTEST_BASE_URL,
    CONTESTS_BASE_URL,
    DEFAULT_RATING,
    GYM_BASE_URL,
    GYM_ID_THRESHOLD,
    PROFILE_BASE_URL,
    RATED_RANKS,
    UNRATED_RANK,
    CallLimitExceededError,
    ClientError,
    CodeforcesApiError,
    Contest,
    ContestNotFoundError,
    HandleInvalidError,
    HandleNotFoundError,
    Member,
    Party,
    Problem,
    ProblemResult,
    ProblemStatistics,
    Rank,
    RanklistRow,
    RatingChange,
    RatingChangesUnavailableError,
    Submission,
    TrueApiError,
    User,
    make_from_dict,
    rating2rank,
)

# ruff: noqa: N815

API_BASE_URL = 'https://codeforces.com/api/'

logger = logging.getLogger(__name__)


# Codeforces API query methods

_session: aiohttp.ClientSession = None

_CF_API_KEY = os.environ.get('CF_API_KEY')
_CF_API_SECRET = os.environ.get('CF_API_SECRET')
_SIG_RAND_CHARS = string.ascii_lowercase + string.digits

async def initialize() -> None:
    """Initialization for the Codeforces API module."""
    global _session
    _session = aiohttp.ClientSession()
    if _CF_API_KEY and _CF_API_SECRET:
        logger.info('CF API credentials loaded; requests will be signed.')
    else:
        logger.warning('CF_API_KEY/CF_API_SECRET not set; requests will be unsigned '
                       'and may be rejected by Codeforces.')


def _bool_to_str(value: bool) -> str:
    if isinstance(value, bool):
        return 'true' if value else 'false'
    raise TypeError(f'Expected bool, got {value} of type {type(value)}')


def _scrub_for_log(params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not params:
        return params
    return {k: ('***' if k in ('apiKey', 'apiSig') else v) for k, v in params.items()}


def _sign_params(method: str, params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Adds apiKey/time/apiSig per https://codeforces.com/apiHelp.

    Signing string: <rand>/<method>?k1=v1&k2=v2...&kN=vN#<secret>
    where pairs are sorted lexicographically by (name, value). apiSig is
    <rand> concatenated with the SHA-512 hex digest of that string.
    """
    if not (_CF_API_KEY and _CF_API_SECRET):
        return params
    signed = {str(k): str(v) for k, v in (params or {}).items()}
    signed['apiKey'] = _CF_API_KEY
    signed['time'] = str(int(time.time()))
    rand = ''.join(random.choices(_SIG_RAND_CHARS, k=6))
    query = '&'.join(f'{k}={v}' for k, v in sorted(signed.items()))
    digest = hashlib.sha512(f'{rand}/{method}?{query}#{_CF_API_SECRET}'.encode()).hexdigest()
    signed['apiSig'] = rand + digest
    return signed


# Shared rate-limit state across ALL @cf_ratelimit wrappers. CF allows
# ~1 rps in aggregate; giving each wrapper its own deque would let
# _query_api and _query_api_anonymous_get burn through 2 rps together.
_CF_RATELIMIT_TRIES = 3
_CF_RATELIMIT_PER_SECOND = 1
_cf_ratelimit_last = deque([0.0] * _CF_RATELIMIT_PER_SECOND)


def cf_ratelimit(f):
    @functools.wraps(f)
    async def wrapped(*args, **kwargs):
        for i in itertools.count():
            now = time.time()

            # Next valid slot is 1s after the `per_second`th last request
            next_valid = max(now, 1 + _cf_ratelimit_last[0])
            _cf_ratelimit_last.append(next_valid)
            _cf_ratelimit_last.popleft()

            # Delay as needed
            delay = next_valid - now
            if delay > 0:
                await asyncio.sleep(delay)

            try:
                return await f(*args, **kwargs)
            except (ClientError, CallLimitExceededError) as e:
                logger.info(f'Try {i+1}/{_CF_RATELIMIT_TRIES} at query failed.')
                logger.info(repr(e))
                if i < _CF_RATELIMIT_TRIES - 1:
                    logger.info('Retrying...')
                else:
                    logger.info('Aborting.')
                    raise e
        raise AssertionError('Unreachable')
    return wrapped


@cf_ratelimit
async def _query_api(path: str, data: Any=None):
    url = API_BASE_URL + path
    signed_data = _sign_params(path, data)
    try:
        logger.info(f'Querying CF API at {url} with {_scrub_for_log(signed_data)}')
        # Explicitly state encoding (though aiohttp accepts gzip by default)
        headers = {'Accept-Encoding': 'gzip'}
        async with _session.post(url, data=signed_data, headers=headers) as resp:
            try:
                respjson = await resp.json()
            except aiohttp.ContentTypeError:
                logger.warning(f'CF API did not respond with JSON, status {resp.status}.')
                raise CodeforcesApiError
            if resp.status == 200:
                return respjson['result']
            comment = f'HTTP Error {resp.status}, {respjson.get("comment")}'
    except aiohttp.ClientError as e:
        logger.error(f'Request to CF API encountered error: {e!r}')
        raise ClientError from e
    logger.warning(f'Query to CF API failed: {comment}')
    if 'limit exceeded' in comment:
        raise CallLimitExceededError(comment)
    raise TrueApiError(comment)


@cf_ratelimit
async def _query_api_anonymous_get(path: str, params: Optional[Dict[str, Any]] = None):
    """Anonymous GET request — no signing. Required by CF for
    contest.standings on public regular contests as of May 2026.
    """
    url = API_BASE_URL + path
    try:
        logger.info(f'GET CF API at {url} with {_scrub_for_log(params)}')
        headers = {'Accept-Encoding': 'gzip'}
        async with _session.get(url, params=params, headers=headers) as resp:
            try:
                respjson = await resp.json()
            except aiohttp.ContentTypeError:
                logger.warning(f'CF API did not respond with JSON, status {resp.status}.')
                raise CodeforcesApiError
            if resp.status == 200:
                return respjson['result']
            comment = f'HTTP Error {resp.status}, {respjson.get("comment")}'
    except aiohttp.ClientError as e:
        logger.error(f'Request to CF API encountered error: {e!r}')
        raise ClientError from e
    logger.warning(f'Query to CF API failed: {comment}')
    if 'limit exceeded' in comment:
        raise CallLimitExceededError(comment)
    raise TrueApiError(comment)


class contest:
    @staticmethod
    async def list(*, gym: Optional[bool] = None) -> List[Contest]:
        """Returns a list of contests."""
        params = {}
        if gym is not None:
            params['gym'] = _bool_to_str(gym)
        resp = await _query_api('contest.list', params)
        return [make_from_dict(Contest, contest_dict) for contest_dict in resp]

    @staticmethod
    async def ratingChanges(*, contest_id: Any) -> List[RatingChange]:
        """Returns a list of rating changes for a contest."""
        params = {'contestId': contest_id}
        try:
            resp = await _query_api('contest.ratingChanges', params)
        except TrueApiError as e:
            if 'not found' in e.comment:
                raise ContestNotFoundError(e.comment, contest_id)
            if 'Rating changes are unavailable' in e.comment:
                raise RatingChangesUnavailableError(e.comment, contest_id)
            raise
        return [make_from_dict(RatingChange, change_dict) for change_dict in resp]

    @staticmethod
    async def standings(
        *,
        contest_id: Any,
        handles: Optional[List[str]] = None,
    ) -> Tuple[Contest, List[Problem], List[RanklistRow]]:
        """Fetch standings for a public regular contest.

        Sent as an anonymous GET with only `contestId`, per the CF
        restriction in effect since May 2026. The endpoint returns
        CONTESTANT-participation rows only — VIRTUAL/PRACTICE/OUT_OF_COMPETITION
        rows are not available to ordinary callers. `handles` filters
        the returned rows client-side.
        """
        params = {'contestId': contest_id}
        try:
            resp = await _query_api_anonymous_get('contest.standings', params)
        except TrueApiError as e:
            if 'not found' in e.comment:
                raise ContestNotFoundError(e.comment, contest_id)
            raise
        contest_ = make_from_dict(Contest, resp['contest'])
        problems = [make_from_dict(Problem, problem_dict) for problem_dict in resp['problems']]
        for row in resp['rows']:
            row['party']['members'] = [make_from_dict(Member, member)
                                       for member in row['party']['members']]
            row['party'] = make_from_dict(Party, row['party'])
            row['problemResults'] = [make_from_dict(ProblemResult, problem_result)
                                     for problem_result in row['problemResults']]
        ranklist = [make_from_dict(RanklistRow, row_dict) for row_dict in resp['rows']]
        if handles:
            wanted = set(handles)
            ranklist = [r for r in ranklist
                        if r.party.members and r.party.members[0].handle in wanted]
        return contest_, problems, ranklist


class problemset:
    @staticmethod
    async def problems(
        *, tags=None, problemset_name=None
    ) -> Tuple[List[Problem], List[ProblemStatistics]]:
        """Returns a list of problems."""
        params = {}
        if tags is not None:
            params['tags'] = ';'.join(tags)
        if problemset_name is not None:
            params['problemsetName'] = problemset_name
        resp = await _query_api('problemset.problems', params)
        problems = [make_from_dict(Problem, problem_dict) for problem_dict in resp['problems']]
        problemstats = [make_from_dict(ProblemStatistics, problemstat_dict) for problemstat_dict in
                        resp['problemStatistics']]
        return problems, problemstats

def user_info_chunkify(handles: Iterable[str]) -> Iterator[List[str]]:
    """Yields chunks of handles that can be queried with user.info."""
    # Querying user.info using POST requests is limited to 10000 handles or 2**16
    # bytes, so requests might need to be split into chunks
    SIZE_LIMIT = 2**16
    HANDLE_LIMIT = 10000
    chunk = []
    size = 0
    for handle in handles:
        if size + len(handle) > SIZE_LIMIT or len(chunk) == HANDLE_LIMIT:
            yield chunk
            chunk = []
            size = 0
        chunk.append(handle)
        size += len(handle) + 1
    if chunk:
        yield chunk

class user:
    @staticmethod
    async def info(*, handles: Sequence[str]) -> List[User]:
        """Returns a list of user info."""
        chunks = list(user_info_chunkify(handles))
        if len(chunks) > 1:
            logger.warning(f'cf.info request with {len(handles)} handles,'
            f'will be chunkified into {len(chunks)} requests.')

        result = []
        count = 0
        for chunk in chunks:
            params = {'handles': ';'.join(chunk)}
            try:
                resp = await _query_api('user.info', params)
            except TrueApiError as e:
                if 'not found' in e.comment:
                    # Comment format is "handles: User with handle ***** not found"
                    handle = e.comment.partition('not found')[0].split()[-1]
                    raise HandleNotFoundError(e.comment, handle)
                raise
            result += [make_from_dict(User, user_dict) for user_dict in resp]
            count += len(chunk)
        logger.info(f"user.info was called for {count} entries and {len(result)} User objects could be created.")
        return [cf_common.fix_urls(user) for user in result]

    @staticmethod
    def correct_rating_changes(*, resp):
        adaptO = [1400, 900, 550, 300, 150, 50]
        adaptN = [900, 550, 300, 150, 50, 0]
        for r in resp:
            if (len(r) > 0):
                if (r[0].newRating <= 1200):
                    for ind in range(0,(min(6, len(r)))):
                        r[ind] = RatingChange(r[ind].contestId, r[ind].contestName, r[ind].handle, r[ind].rank, r[ind].ratingUpdateTimeSeconds, r[ind].oldRating+adaptO[ind], r[ind].newRating+adaptN[ind])
                else:
                    r[0] = RatingChange(r[0].contestId, r[0].contestName, r[0].handle, r[0].rank, r[0].ratingUpdateTimeSeconds, r[0].oldRating+1500, r[0].newRating)
        for r in resp:
            oldPerf = 0
            for ind in range(0,len(r)):
                r[ind] = RatingChange(r[ind].contestId, r[ind].contestName, r[ind].handle, r[ind].rank, r[ind].ratingUpdateTimeSeconds, oldPerf, r[ind].oldRating + 4*(r[ind].newRating-r[ind].oldRating))
                oldPerf = r[ind].oldRating + 4*(r[ind].newRating-r[ind].oldRating)
        return resp


    @staticmethod
    async def rating(*, handle: str):
        """Returns a list of rating changes for a user."""
        params = {'handle': handle}
        try:
            resp = await _query_api('user.rating', params)
        except TrueApiError as e:
            if 'not found' in e.comment:
                raise HandleNotFoundError(e.comment, handle)
            if 'should contain' in e.comment:
                raise HandleInvalidError(e.comment, handle)
            raise
        return [make_from_dict(RatingChange, ratingchange_dict) for ratingchange_dict in resp]

    @staticmethod
    async def ratedList(*, activeOnly: bool = None) -> List[User]:
        """Returns a list of rated users."""
        params = {}
        if activeOnly is not None:
            params['activeOnly'] = _bool_to_str(activeOnly)
        resp = await _query_api('user.ratedList', params)
        return [make_from_dict(User, user_dict) for user_dict in resp]

    @staticmethod
    async def status(
        *, handle: str, from_: Optional[int] = None, count: Optional[int] = None
    ) -> List[Submission]:
        """Returns a list of submissions for a user."""
        params: Dict[str, Any] = {'handle': handle}
        if from_ is not None:
            params['from'] = from_
        if count is not None:
            params['count'] = count
        try:
            resp = await _query_api('user.status', params)
        except TrueApiError as e:
            if 'not found' in e.comment:
                raise HandleNotFoundError(e.comment, handle)
            if 'should contain' in e.comment:
                raise HandleInvalidError(e.comment, handle)
            raise
        for submission in resp:
            submission['problem'] = make_from_dict(Problem, submission['problem'])
            submission['author']['members'] = [make_from_dict(Member, member)
                                               for member in submission['author']['members']]
            submission['author'] = make_from_dict(Party, submission['author'])
        return [make_from_dict(Submission, submission_dict) for submission_dict in resp]


async def _resolve_redirect(handle: str) -> Optional[str]:
    url = PROFILE_BASE_URL + handle
    async with _session.head(url) as r:
        if r.status == 200:
            return handle
        if r.status == 301 or r.status == 302:
            redirected = r.headers.get('Location')
            if '/profile/' not in redirected:
                # Ended up not on profile page, probably invalid handle
                return None
            return redirected.split('/profile/')[-1]
        raise CodeforcesApiError(
            f'Something went wrong trying to redirect {url}')

async def _resolve_handle_to_new_user(
    handle: str,
) -> Optional[User]:
    new_handle = await _resolve_redirect(handle)
    if new_handle is None:
        return None
    cf_user, = await user.info(handles=[new_handle])
    return cf_user


async def _resolve_handles(handles: Iterable[str]) -> Dict[str, Optional[User]]:
    chunks = user_info_chunkify(handles)

    resolved_handles: Dict[str, Optional[User]] = {}

    for handle_chunk in chunks:
        while handle_chunk:
            try:
                cf_users = await user.info(handles=handle_chunk)

                # CF API changed. We now get the new username from API
                # If handle and cf_user.handle differ then the user used magic and needs fixing!
                for handle, cf_user in zip(handle_chunk, cf_users):
                    if handle != cf_user.handle:
                        resolved_handles[handle] = cf_user
                break
            except HandleNotFoundError as e:
                # Not sure if we still need this! Magic users should not run into it. 
                # Will leave it for now.
                # >> Handle resolution failed, fix the reported handle.
                resolved_handles[e.handle] = await _resolve_handle_to_new_user(e.handle)
                handle_chunk.remove(e.handle)
    return resolved_handles


async def resolve_redirects(handles: Iterable[str]) -> Dict[str, Optional[User]]:
    """Returns a mapping of handles to their resolved CF users."""
    return await _resolve_handles(handles)
