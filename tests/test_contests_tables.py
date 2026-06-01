import sys
import types

from tle import util as tle_util
from tle.util import cache_system2
from tle.util import events, tasks

ranklist_stub = types.ModuleType('tle.util.ranklist')
ranklist_stub.RanklistError = type('RanklistError', (Exception,), {})
ranklist_stub.HandleNotPresentError = type('HandleNotPresentError', (Exception,), {})
ranklist_stub.Ranklist = object
sys.modules['tle.util.ranklist'] = ranklist_stub
tle_util.ranklist = ranklist_stub

if not hasattr(tasks.Waiter, 'for_event'):
    tasks.Waiter.for_event = staticmethod(lambda _event: tasks.Waiter())
for event_name in ('ContestListRefresh', 'RatingChangesUpdate'):
    if not hasattr(events, event_name):
        setattr(events, event_name, type(event_name, (), {}))
if not hasattr(cache_system2, 'CacheError'):
    cache_system2.CacheError = type('CacheError', (Exception,), {})
if not hasattr(cache_system2, 'RanklistNotMonitored'):
    cache_system2.RanklistNotMonitored = type('RanklistNotMonitored', (Exception,), {})

from tle.cogs.contests import Contests
from tle.util import table


class TestProblemRatingsTablePages:
    def test_problemratings_pages_split_by_embed_limit(self):
        indices = [f'P{i:03d}' for i in range(500)]
        official = [1200 + i for i in range(500)]
        predicted = [1300 + i for i in range(500)]

        pages = Contests._format_problemratings_table_pages(
            indices, official, predicted, from_cache=False)

        assert len(pages) > 1
        assert all(len(page) <= table.DISCORD_EMBED_DESCRIPTION_LIMIT for page in pages)
        assert all('Official' in page and 'Predicted' in page for page in pages)

    def test_problemratings_pages_truncate_extreme_index(self):
        pages = Contests._format_problemratings_table_pages(
            ['A' * 5000], [1200], [1300], from_cache=True)

        assert len(pages) == 1
        assert len(pages[0]) <= table.DISCORD_EMBED_DESCRIPTION_LIMIT
        assert '...' in pages[0]
