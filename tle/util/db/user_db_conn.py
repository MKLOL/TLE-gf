import logging
import sqlite3
import unicodedata
from enum import IntEnum
from collections import namedtuple

from discord.ext import commands

from tle.util.db.starboard_db import (
    StarboardDbMixin,
    snowflake_to_unix_sql, DISCORD_EPOCH_MS, SNOWFLAKE_TIMESTAMP_DIVISOR, _NO_TIME_BOUND,
)
from tle.util.db.minigame_db import MinigameDbMixin
from tle.util.db.migration_db import MigrationDbMixin
from tle.util.db.handle_db import HandleDbMixin
from tle.util.db.challenge_db import ChallengeDbMixin
from tle.util.db.duel_db import DuelDbMixin
from tle.util.db.training_db import TrainingDbMixin
from tle.util.db.vc_db import VcDbMixin
from tle.util.db.lockout_db import LockoutDbMixin
from tle.util.db.rpoll_db import RpollDbMixin
from tle.util.db.complaint_db import ComplaintDbMixin
from tle.util.db.greatday_db import GreatdayDbMixin
from tle.util.db.kvs_db import KvsDbMixin
from tle.util.db.misc_db import MiscDbMixin
from tle.util.db.betting_wallet_db import BettingWalletDbMixin
from tle.util.db.betting_market_db import BettingMarketDbMixin
from tle.util.db.betting_wager_db import BettingWagerDbMixin

logger = logging.getLogger(__name__)

_DEFAULT_VC_RATING = 1500


def _bet_pick_base(pick):
    return pick[4:] if isinstance(pick, str) and pick.startswith('not_') else pick


def _bet_pick_wins(pick, result):
    base = _bet_pick_base(pick)
    return base != result if isinstance(pick, str) and pick.startswith('not_') \
        else base == result


def _bet_not_odds(base_odds):
    if base_odds is None or base_odds <= 1:
        return None
    probability = 1.0 / base_odds
    if probability >= 1:
        return None
    return 1.0 / (1.0 - probability)


def _bet_odds_for_pick(odds_map, pick):
    base = _bet_pick_base(pick)
    odds = odds_map.get(base)
    return _bet_not_odds(odds) if pick != base else odds


def _bet_norm_team(name):
    if not name:
        return ''
    decomposed = unicodedata.normalize('NFKD', str(name))
    stripped = ''.join(c for c in decomposed if not unicodedata.combining(c))
    return ''.join(c for c in stripped.lower() if c.isalnum())


def bet_fixture_key(sport_key, home_team, away_team, commence_time):
    teams = sorted((_bet_norm_team(home_team), _bet_norm_team(away_team)))
    day = int(float(commence_time) // 86400)
    return f'{sport_key}:{day}:{teams[0]}:{teams[1]}'


class Gitgud(IntEnum):
    GOTGUD = 0
    GITGUD = 1
    NOGUD = 2
    FORCED_NOGUD = 3

class Training(IntEnum):
    NOTSTARTED = 0
    ACTIVE = 1
    COMPLETED = 2

class TrainingProblemStatus(IntEnum):
    SOLVED = 0
    SOLVED_TOO_SLOW = 1
    ACTIVE = 2
    SKIPPED = 3
    INVALIDATED = 4

class Duel(IntEnum):
    PENDING = 0
    DECLINED = 1
    WITHDRAWN = 2
    EXPIRED = 3
    ONGOING = 4
    COMPLETE = 5
    INVALID = 6

class Winner(IntEnum):
    DRAW = 0
    CHALLENGER = 1
    CHALLENGEE = 2

class DuelType(IntEnum):
    UNOFFICIAL = 0
    OFFICIAL = 1
    ADJUNOFFICIAL = 2
    ADJOFFICIAL = 3

class RatedVC(IntEnum):
    ONGOING = 0
    FINISHED = 1


class UserDbError(commands.CommandError):
    pass


class DatabaseDisabledError(UserDbError):
    pass


class DummyUserDbConn:
    def __getattribute__(self, item):
        raise DatabaseDisabledError


class UniqueConstraintFailed(UserDbError):
    pass


def namedtuple_factory(cursor, row):
    """Returns sqlite rows as named tuples."""
    fields = [col[0] if col[0].isidentifier() else f'col_{i}'
              for i, col in enumerate(cursor.description)]
    Row = namedtuple("Row", fields)
    return Row(*row)


class UserDbConn(HandleDbMixin, ChallengeDbMixin, DuelDbMixin, TrainingDbMixin,
                 VcDbMixin, LockoutDbMixin, RpollDbMixin, ComplaintDbMixin,
                 GreatdayDbMixin, KvsDbMixin, MiscDbMixin,
                 BettingWalletDbMixin, BettingMarketDbMixin, BettingWagerDbMixin,
                 MinigameDbMixin, StarboardDbMixin, MigrationDbMixin):
    def __init__(self, dbfile):
        logger.info(f'Opening user database: {dbfile}')
        self.conn = sqlite3.connect(dbfile)
        self.conn.row_factory = namedtuple_factory
        self.create_tables()
        logger.info('Base tables created/verified')

        from tle.util.db.user_db_upgrades import registry
        registry.ensure_version_table(self.conn)
        current = registry.get_current_version(self.conn)
        if current is None:
            # No version stamped yet. Check if this is a truly fresh DB or a
            # pre-upgrade existing DB by looking for legacy table data.
            has_legacy = self.conn.execute(
                'SELECT 1 FROM starboard LIMIT 1'
            ).fetchone() is not None
            if has_legacy:
                # Pre-upgrade DB with existing data — start from baseline so migrations run
                logger.info('Pre-upgrade database detected (has legacy starboard data), '
                            'starting from 1.0.0 so migrations run')
                registry.set_version(self.conn, '1.0.0')
                registry.run(self.conn)
            else:
                # Truly fresh DB — tables already latest schema, stamp version
                logger.info(f'Fresh database detected, stamping version to {registry.latest_version}')
                registry.set_version(self.conn, registry.latest_version)
        else:
            # Existing DB — run pending upgrades
            logger.info(f'Existing database at version {current}, checking for upgrades...')
            registry.run(self.conn)
        logger.info('User database initialization complete')

    def create_tables(self):
        # Each domain mixin owns its own CREATE TABLE / INDEX statements via a
        # ``_create_<domain>_tables`` method. ``CREATE TABLE IF NOT EXISTS`` is
        # order-independent, so the call order here does not matter.
        self._create_handle_tables()
        self._create_duel_tables()
        self._create_challenge_tables()
        self._create_misc_tables()
        self._create_starboard_tables()
        self._create_minigame_tables()
        self._create_complaint_tables()
        self._create_greatday_tables()
        self._create_betting_tables()
        self._create_vc_tables()
        self._create_training_tables()
        self._create_lockout_tables()
        self._create_kvs_tables()
        self._create_rpoll_tables()
        self._create_migration_tables()

    # Helper functions.

    def _insert_one(self, table: str, columns, values: tuple):
        n = len(values)
        query = '''
            INSERT OR REPLACE INTO {} ({}) VALUES ({})
        '''.format(table, ', '.join(columns), ', '.join(['?'] * n))
        rc = self.conn.execute(query, values).rowcount
        self.conn.commit()
        return rc

    def _insert_many(self, table: str, columns, values: list):
        n = len(columns)
        query = '''
            INSERT OR REPLACE INTO {} ({}) VALUES ({})
        '''.format(table, ', '.join(columns), ', '.join(['?'] * n))
        rc = self.conn.executemany(query, values).rowcount
        self.conn.commit()
        return rc

    def _fetchone(self, query: str, params=(), row_factory=None):
        original = self.conn.row_factory
        self.conn.row_factory = row_factory
        res = self.conn.execute(query, params).fetchone()
        self.conn.row_factory = original
        return res

    def _fetchall(self, query: str, params=(), row_factory=None):
        original = self.conn.row_factory
        self.conn.row_factory = row_factory
        res = self.conn.execute(query, params).fetchall()
        self.conn.row_factory = original
        return res

    def close(self):
        self.conn.close()
