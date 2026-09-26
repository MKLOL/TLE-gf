"""canonical_handles — one scan for a whole batch of handles."""
import sqlite3

from tle.util.db.handle_db import HandleDbMixin
from tle.util.db.user_db_conn import namedtuple_factory


class _Db(HandleDbMixin):
    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = namedtuple_factory
        self._create_handle_tables()
        for handle in ('tourist', 'Petr', 'TFG'):
            self.conn.execute(
                'INSERT INTO cf_user_cache (handle, rating, maxRating) VALUES (?, 0, 0)',
                (handle,))
        self.conn.commit()


def test_maps_every_spelling_to_the_cached_casing():
    db = _Db()
    assert db.canonical_handles(['tfg', 'Tfg', 'PETR']) == {'tfg': 'TFG', 'petr': 'Petr'}


def test_unknown_handles_are_simply_absent():
    db = _Db()
    assert db.canonical_handles(['nobody']) == {}
    assert db.canonical_handles([]) == {}


def test_large_batches_are_chunked():
    db = _Db()
    handles = [f'h{i}' for i in range(1200)] + ['tourist']
    assert db.canonical_handles(handles) == {'tourist': 'tourist'}
