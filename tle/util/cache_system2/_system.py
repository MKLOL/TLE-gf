import logging
import time
from aiocache import cached

from tle.util import codeforces_api as cf
from tle.util.cache_system2._contest import ContestCache
from tle.util.cache_system2._problems import ProblemCache, ProblemsetCache
from tle.util.cache_system2._rating_changes import RatingChangesCache
from tle.util.cache_system2._ranklist import RanklistCache

logger = logging.getLogger(__name__)


class CacheSystem:
    def __init__(self, conn):
        self.conn = conn
        self.contest_cache = ContestCache(self)
        self.problem_cache = ProblemCache(self)
        self.rating_changes_cache = RatingChangesCache(self)
        self.ranklist_cache = RanklistCache(self)
        self.problemset_cache = ProblemsetCache(self)

    async def run(self):
        run_start = time.time()
        logger.info('CacheSystem.run() started')

        t = time.time()
        await self.rating_changes_cache.run()
        logger.info(f'rating_changes_cache.run() completed in {time.time()-t:.2f}s')

        t = time.time()
        await self.ranklist_cache.run()
        logger.info(f'ranklist_cache.run() completed in {time.time()-t:.2f}s')

        t = time.time()
        await self.contest_cache.run()
        logger.info(f'contest_cache.run() completed in {time.time()-t:.2f}s')

        t = time.time()
        await self.problem_cache.run()
        logger.info(f'problem_cache.run() completed in {time.time()-t:.2f}s')

        t = time.time()
        await self.problemset_cache.run()
        logger.info(f'problemset_cache.run() completed in {time.time()-t:.2f}s')

        logger.info(f'CacheSystem.run() completed in {time.time()-run_start:.2f}s')

    @staticmethod
    @cached(ttl=30 * 60)
    async def getUsersEffectiveRating(*, activeOnly=None):
        """ Returns a dictionary mapping user handle to his effective rating for all the users.
        """
        ratedList = await cf.user.ratedList(activeOnly=activeOnly)
        users_effective_rating_dict = {user.handle: user.effective_rating
                                       for user in ratedList}
        return users_effective_rating_dict
