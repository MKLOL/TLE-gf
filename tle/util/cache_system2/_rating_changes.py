import asyncio
import logging
import time

from tle.util import codeforces_common as cf_common
from tle.util import codeforces_api as cf
from tle.util import events
from tle.util import tasks
from tle.util import paginator
from tle.util.cache_system2._common import (
    _CONTESTS_PER_BATCH_IN_CACHE_UPDATES, _is_blacklisted)


class RatingChangesCache:
    _RATED_DELAY = 36 * 60 * 60
    _RELOAD_DELAY = 10 * 60

    def __init__(self, cache_master):
        self.cache_master = cache_master
        self.monitored_contests = []
        self.handle_rating_cache = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    async def run(self):
        await self._refresh_handle_cache()
        if not self.handle_rating_cache:
            self.logger.warning('Rating changes cache on disk is empty. This must be populated '
                                'manually before use.')
        self._update_task.start()

    async def fetch_contest(self, contest_id):
        """Fetch rating changes for a particular contest. Intended for manual trigger."""
        contest = self.cache_master.contest_cache.contest_by_id[contest_id]
        changes = await self._fetch([contest])
        self.cache_master.conn.clear_rating_changes(contest_id=contest_id)
        await self._save_changes(changes)
        return len(changes)

    async def fetch_all_contests(self):
        """Fetch rating changes for all contests. Intended for manual trigger."""
        self.cache_master.conn.clear_rating_changes()
        return await self.fetch_missing_contests()

    async def fetch_missing_contests(self):
        """Fetch rating changes for contests which are not saved in database. Intended for
        manual trigger."""
        contests = self.cache_master.contest_cache.contests_by_phase['FINISHED']
        contests = [
            contest for contest in contests if not self.has_rating_changes_saved(contest.id)]
        total_changes = 0
        for contests_chunk in paginator.chunkify(contests, _CONTESTS_PER_BATCH_IN_CACHE_UPDATES):
            contests_chunk = await self._fetch(contests_chunk)
            await self._save_changes(contests_chunk)
            total_changes += len(contests_chunk)
        return total_changes

    def is_newly_finished_without_rating_changes(self, contest):
        now = time.time()
        return (contest.phase == 'FINISHED' and
                now - contest.end_time < self._RATED_DELAY and
                not self.has_rating_changes_saved(contest.id))

    @tasks.task_spec(name='RatingChangesCacheUpdate',
                     waiter=tasks.Waiter.for_event(events.ContestListRefresh))
    async def _update_task(self, _):
        # Some notes:
        # A hack phase is tagged as FINISHED with empty list of rating changes. After the hack
        # phase, the phase changes to systest then again FINISHED. Since we cannot differentiate
        # between the two FINISHED phases, we are forced to fetch during both.
        # A contest also has empty list if it is unrated. We assume that is the case if
        # _RATED_DELAY time has passed since the contest end.

        to_monitor = [
            contest for contest in
            self.cache_master.contest_cache.contests_by_phase['FINISHED']
            if self.is_newly_finished_without_rating_changes(contest)
               and not _is_blacklisted(contest)
        ]

        cur_ids = {contest.id for contest in self.monitored_contests}
        new_ids = {contest.id for contest in to_monitor}
        if new_ids != cur_ids:
            await self._monitor_task.stop()
            if to_monitor:
                self.monitored_contests = to_monitor
                self._monitor_task.start()
            else:
                self.monitored_contests = []

    @tasks.task_spec(name='RatingChangesCacheUpdate.MonitorNewlyFinishedContests',
                     waiter=tasks.Waiter.fixed_delay(_RELOAD_DELAY))
    async def _monitor_task(self, _):
        self.monitored_contests = [
            contest for contest in self.monitored_contests
            if self.is_newly_finished_without_rating_changes(contest)
               and not _is_blacklisted(contest)
        ]

        if not self.monitored_contests:
            self.logger.info('Rated changes fetched for contests that were being monitored.')
            await self._monitor_task.stop()
            return

        contest_changes_pairs = await self._fetch(self.monitored_contests)
        # Sort by the rating update time of the first change in the list of changes, assuming
        # every change in the list has the same time.
        contest_changes_pairs.sort(key=lambda pair: pair[1][0].ratingUpdateTimeSeconds)
        await self._save_changes(contest_changes_pairs)
        for contest, changes in contest_changes_pairs:
            cf_common.event_sys.dispatch(events.RatingChangesUpdate, contest=contest,
                                         rating_changes=changes)

    async def _fetch(self, contests):
        all_changes = []
        for contest in contests:
            try:
                changes = await cf.contest.ratingChanges(contest_id=contest.id)
                self.logger.info(f'{len(changes)} rating changes fetched for contest {contest.id}')
                if changes:
                    all_changes.append((contest, changes))
            except cf.CodeforcesApiError as er:
                self.logger.warning(f'Fetch rating changes failed for contest {contest.id}, ignoring. {er!r}')
                pass
        return all_changes

    async def _save_changes(self, contest_changes_pairs):
        flattened = [change for _, changes in contest_changes_pairs for change in changes]
        if not flattened:
            return
        rc = self.cache_master.conn.save_rating_changes(flattened)
        self.logger.info(f'Saved {rc} changes to database.')
        await self._refresh_handle_cache()

    async def _refresh_handle_cache(self):
        t0 = time.time()
        loop = asyncio.get_event_loop()
        handle_rating_cache = await loop.run_in_executor(
            None, self.cache_master.conn.get_handle_rating_mapping)
        self.handle_rating_cache = handle_rating_cache
        elapsed = time.time() - t0
        self.logger.info(f'Ratings for {len(handle_rating_cache)} handles cached in {elapsed:.2f}s')

    def get_users_with_more_than_n_contests(self, time_cutoff, n):
        return self.cache_master.conn.get_users_with_more_than_n_contests(time_cutoff, n)

    def get_rating_changes_for_contest(self, contest_id):
        return self.cache_master.conn.get_rating_changes_for_contest(contest_id)

    def has_rating_changes_saved(self, contest_id):
        return self.cache_master.conn.has_rating_changes_saved(contest_id)

    def get_rating_changes_for_handle(self, handle):
        return self.cache_master.conn.get_rating_changes_for_handle(handle)

    def get_current_rating(self, handle, default_if_absent=False):
        return self.handle_rating_cache.get(handle,
                                            cf.DEFAULT_RATING if default_if_absent else None)

    async def get_all_ratings_before_timestamp(self, timestamp):
        res = self.cache_master.conn.get_all_ratings_before_timestamp(timestamp)
        return {ratingchange.handle: ratingchange for ratingchange in res}

    def get_all_ratings(self):
        return list(self.handle_rating_cache.values())
