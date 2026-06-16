import asyncio
import logging
import time
from collections import defaultdict

from tle.util import codeforces_common as cf_common
from tle.util import codeforces_api as cf
from tle.util import tasks
from tle.util.cache_system2._common import (
    _DIV_TAGS, ContestNotFound, ProblemsetNotCached)


class ProblemCache:
    _RELOAD_INTERVAL = 6 * 60 * 60

    def __init__(self, cache_master):
        self.cache_master = cache_master

        self.problems = []
        self.problem_by_name = {}
        self.problems_last_cache = 0

        self.reload_lock = asyncio.Lock()
        self.reload_exception = None

        self.logger = logging.getLogger(self.__class__.__name__)

    async def run(self):
        await self._try_disk()
        self._update_task.start()

    async def reload_now(self):
        """Force a reload. If currently reloading it will wait until done."""
        reloading = self.reload_lock.locked()
        if reloading:
            # Wait until reload complete.
            # To wait until lock is free, await acquire then release immediately.
            async with self.reload_lock:
                pass
        else:
            await self._update_task.manual_trigger()

        if self.reload_exception:
            raise self.reload_exception

    async def _try_disk(self):
        async with self.reload_lock:
            problems = self.cache_master.conn.fetch_problems()
            if not problems:
                self.logger.info('Problem cache on disk is empty.')
                return
            self.problems = problems
            self.problem_by_name = {problem.name: problem for problem in problems}
            self.logger.info(f'{len(self.problems)} problems fetched from disk')

    @tasks.task_spec(name='ProblemCacheUpdate',
                     waiter=tasks.Waiter.fixed_delay(_RELOAD_INTERVAL))
    async def _update_task(self, _):
        async with self.reload_lock:
            await self._reload_problems()
        self.reload_exception = None

    @_update_task.exception_handler()
    async def _update_task_exception_handler(self, ex):
        self.reload_exception = ex

    async def _reload_problems(self):
        problems, _ = await cf.problemset.problems()
        await self._update(problems)

    async def _update(self, problems):
        self.logger.info(f'{len(problems)} problems fetched from API')
        contest_map = {problem.contestId: self.cache_master.contest_cache.contest_by_id.get(problem.contestId)
                       for problem in problems}

        def keep(problem):
            return (contest_map[problem.contestId] and
                    problem.has_metadata())

        filtered_problems = list(filter(keep, problems))
        problem_by_name = {
            problem.name: problem  # This will discard some valid problems
            for problem in filtered_problems
        }
        self.logger.info(f'Keeping {len(problem_by_name)} problems')

        self.problems = list(problem_by_name.values())
        self.problem_by_name = problem_by_name
        self.problems_last_cache = time.time()

        for problem in self.problems:
            problem_contest = self.cache_master.contest_cache.contest_by_id.get(problem.contestId)

            divisions = [div_tag for div_tag in _DIV_TAGS if problem_contest.matches([div_tag])]

            for division in divisions:
                problem.tags.append(division)

        rc = self.cache_master.conn.cache_problems(self.problems)
        self.logger.info(f'{rc} problems stored in database')


class ProblemsetCache:
    _MONITOR_PERIOD_SINCE_CONTEST_END = 14 * 24 * 60 * 60
    _RELOAD_DELAY = 60 * 60

    def __init__(self, cache_master):
        self.problems = []
        # problem -> list of contests in which it appears
        self.problem_to_contests = defaultdict(list)
        self.cache_master = cache_master
        self.update_lock = asyncio.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)

    async def run(self):
        if self.cache_master.conn.problemset_empty():
            self.logger.warning('Problemset cache on disk is empty. This must be populated '
                                'manually before use.')
        self._update_task.start()

    async def update_for_contest(self, contest_id):
        """Update problemset for a particular contest. Intended for manual trigger."""
        async with self.update_lock:
            contest = self.cache_master.contest_cache.get_contest(contest_id)
            problemset, _ = await self._fetch_problemsets([contest], force_fetch=True)
            self.cache_master.conn.clear_problemset(contest_id)
            self._save_problems(problemset)
            return len(problemset)

    async def update_for_all(self):
        """Update problemsets for all finished contests. Intended for manual trigger."""
        async with self.update_lock:
            contests = self.cache_master.contest_cache.contests_by_phase['FINISHED']
            problemsets, _ = await self._fetch_problemsets(contests, force_fetch=True)
            self.cache_master.conn.clear_problemset()
            self._save_problems(problemsets)
            return len(problemsets)

    @tasks.task_spec(name='ProblemsetCacheUpdate',
                     waiter=tasks.Waiter.fixed_delay(_RELOAD_DELAY))
    async def _update_task(self, _):
        async with self.update_lock:
            contests = self.cache_master.contest_cache.contests_by_phase['FINISHED']
            new_problems, updated_problems = await self._fetch_problemsets(contests)
            self._save_problems(new_problems + updated_problems)
            self._update_from_disk()
            self.logger.info(f'{len(new_problems)} new problems saved and {len(updated_problems)} '
                             'saved problems updated.')

    async def _fetch_problemsets(self, contests, *, force_fetch=False):
        # We assume it is possible for problems in the same contest to get assigned rating at
        # different times.
        new_contest_ids = []
        contests_to_refetch = []  # List of (id, set of saved rated problem indices) pairs.
        if force_fetch:
            new_contest_ids = [contest.id for contest in contests]
        else:
            now = time.time()
            for contest in contests:
                if now > contest.end_time + self._MONITOR_PERIOD_SINCE_CONTEST_END:
                    # Contest too old, we do not want to check it.
                    continue
                problemset = self.cache_master.conn.fetch_problemset(contest.id)
                if not problemset:
                    new_contest_ids.append(contest.id)
                    continue
                rated_problem_idx = {prob.index for prob in problemset if prob.rating is not None}
                if len(rated_problem_idx) < len(problemset):
                    contests_to_refetch.append((contest.id, rated_problem_idx))

        new_problems, updated_problems = [], []
        for contest_id in new_contest_ids:
            new_problems += await self._fetch_for_contest(contest_id)
        for contest_id, rated_problem_idx in contests_to_refetch:
            updated_problems += [prob for prob in await self._fetch_for_contest(contest_id)
                                 if prob.rating is not None and prob.index not in rated_problem_idx]

        return new_problems, updated_problems

    async def _fetch_for_contest(self, contest_id):
        try:
            contest, problemset, _ = await cf.contest.standings(contest_id=contest_id)


            divisions = [div_tag for div_tag in _DIV_TAGS if contest.matches([div_tag])]

            for problem in problemset:
                for division in divisions:
                    problem.tags.append(division)


        except cf.CodeforcesApiError as er:
            self.logger.warning(f'Problemset fetch failed for contest {contest_id}. {er!r}')
            problemset = []

        return problemset

    def _save_problems(self, problems):
        rc = self.cache_master.conn.cache_problemset(problems)
        self.logger.info(f'Saved {rc} problems to database.')

    def get_problemset(self, contest_id):
        problemset = self.cache_master.conn.fetch_problemset(contest_id)
        if not problemset:
            raise ProblemsetNotCached(contest_id)
        return problemset

    def _update_from_disk(self):
        self.problems = self.cache_master.conn.fetch_problems2()
        self.problem_to_contests = defaultdict(list)
        for problem in self.problems:
            try:
                contest = cf_common.cache2.contest_cache.get_contest(problem.contestId)
                problem_id = (problem.name, contest.startTimeSeconds)
                self.problem_to_contests[problem_id].append(contest.id)
            except ContestNotFound:
                pass
