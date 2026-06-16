"""user.db upgrades 1.30.0 – 1.37.0 (generic minigame tables, betting).

Part of the upgrade chain split out of ``user_db_upgrades`` to stay under the
500-line limit. Importing this module registers its upgrades on the shared
``registry``.
"""
import logging

from tle.util.db._user_db_upgrade_registry import registry
from tle.util.db.user_db_conn import bet_fixture_key

logger = logging.getLogger(__name__)


@registry.register('1.30.0', 'Generic minigame player links and ratings')
def upgrade_1_30_0(db):
    """Create generic minigame identity and rating tables.

    Akari used a dedicated ``akari_rating`` snapshot because it was the only
    rated minigame.  Queens needs the same cache shape, so ratings are now
    keyed by game.  Existing Akari rows are copied into the generic table while
    the old table remains available for compatibility during the transition.
    """
    logger.info('1.30.0: Creating generic minigame link/rating tables')
    db.execute('''
        CREATE TABLE IF NOT EXISTS minigame_player_link (
            guild_id        TEXT NOT NULL,
            game            TEXT NOT NULL,
            user_id         TEXT NOT NULL,
            external_name   TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            external_url    TEXT,
            linked_at       REAL NOT NULL,
            linked_by       TEXT NOT NULL,
            PRIMARY KEY (guild_id, game, user_id),
            UNIQUE (guild_id, game, normalized_name)
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_minigame_player_link_lookup
            ON minigame_player_link (guild_id, game, normalized_name)
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS minigame_rating (
            guild_id    TEXT NOT NULL,
            game        TEXT NOT NULL,
            user_id     TEXT NOT NULL,
            rating      REAL NOT NULL,
            games       INTEGER NOT NULL DEFAULT 0,
            peak        REAL NOT NULL,
            last_delta  REAL NOT NULL DEFAULT 0,
            skip_streak INTEGER NOT NULL DEFAULT 0,
            last_puzzle INTEGER NOT NULL DEFAULT 0,
            updated_at  REAL NOT NULL,
            PRIMARY KEY (guild_id, game, user_id)
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_minigame_rating_guild
            ON minigame_rating (guild_id, game, rating DESC)
    ''')
    try:
        copied = db.execute('''
            INSERT OR IGNORE INTO minigame_rating
                (guild_id, game, user_id, rating, games, peak, last_delta,
                 skip_streak, last_puzzle, updated_at)
            SELECT guild_id, 'akari', user_id, rating, games, peak, last_delta,
                   skip_streak, last_puzzle, updated_at
            FROM akari_rating
        ''').rowcount
        logger.info('1.30.0: Copied %s Akari rating row(s)', copied)
    except Exception as e:
        logger.debug('1.30.0: akari_rating copy skipped (%s)', e)
    db.commit()
    logger.info('1.30.0: Upgrade complete')


@registry.register('1.31.0', 'Generic minigame bans')
def upgrade_1_31_0(db):
    """Create a game-keyed ban table for manual minigame workflows."""
    logger.info('1.31.0: Creating generic minigame ban table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS minigame_ban (
            guild_id   TEXT NOT NULL,
            game       TEXT NOT NULL,
            user_id    TEXT NOT NULL,
            banned_at  REAL NOT NULL,
            banned_by  TEXT NOT NULL,
            reason     TEXT,
            PRIMARY KEY (guild_id, game, user_id)
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_minigame_ban_guild
            ON minigame_ban (guild_id, game, banned_at DESC)
    ''')
    db.commit()
    logger.info('1.31.0: Upgrade complete')


@registry.register('1.32.0', 'Unresolved minigame import results')
def upgrade_1_32_0(db):
    """Create storage for imported external results before Discord linking."""
    logger.info('1.32.0: Creating unresolved minigame result table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS minigame_unresolved_result (
            guild_id        TEXT NOT NULL,
            game            TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            external_name   TEXT NOT NULL,
            channel_id      TEXT NOT NULL,
            puzzle_number   INTEGER NOT NULL,
            puzzle_date     TEXT NOT NULL,
            accuracy        INTEGER NOT NULL,
            time_seconds    INTEGER NOT NULL,
            is_perfect      INTEGER NOT NULL DEFAULT 0,
            raw_content     TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (guild_id, game, normalized_name, puzzle_number)
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_minigame_unresolved_result_puzzle
            ON minigame_unresolved_result (guild_id, game, puzzle_number)
    ''')
    db.commit()
    logger.info('1.32.0: Upgrade complete')


@registry.register('1.33.0', 'Soccer odds-betting minigame')
def upgrade_1_33_0(db):
    """Create the betting tables.

    A *market* is one match, with the 1X2 odds (home / draw / away) frozen
    from The Odds API at open time; everyone bets at those locked odds. A
    *wager* is just one user's pick + stake — odds and payout are NOT stored
    because they are derivable: a wager's odds = the market's frozen
    odds_<pick>, and its payout = round(stake × odds) when pick == result, else
    0. A *wallet* is a per-guild points balance; stakes are escrowed (deducted
    at placement) and winnings credited at settlement, so the wallet balance is
    the source of truth for net worth.
    """
    logger.info('1.33.0: Creating betting tables')
    db.execute('''
        CREATE TABLE IF NOT EXISTS bet_wallet (
            guild_id    TEXT NOT NULL,
            user_id     TEXT NOT NULL,
            balance     INTEGER NOT NULL,
            last_daily  TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS bet_market (
            market_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id       TEXT NOT NULL,
            channel_id     TEXT NOT NULL,
            message_id     TEXT,
            thread_id      TEXT,
            thread_intro_id TEXT,
            event_id       TEXT NOT NULL,
            sport_key      TEXT NOT NULL,
            home_team      TEXT NOT NULL,
            away_team      TEXT NOT NULL,
            commence_time  REAL NOT NULL,
            odds_home      REAL NOT NULL,
            odds_draw      REAL NOT NULL,
            odds_away      REAL NOT NULL,
            status         TEXT NOT NULL DEFAULT 'open',
            bets_closed    INTEGER NOT NULL DEFAULT 0,
            result         TEXT,
            result_home    INTEGER,
            result_away    INTEGER,
            created_by     TEXT NOT NULL,
            created_at     REAL NOT NULL,
            settled_at     REAL
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_bet_market_active
            ON bet_market (guild_id, channel_id, status)
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_bet_market_pending
            ON bet_market (status, commence_time)
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_bet_market_thread
            ON bet_market (guild_id, thread_id, status)
    ''')
    db.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bet_market_open_event
            ON bet_market (guild_id, event_id)
            WHERE status = 'open'
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS bet_wager (
            market_id   INTEGER NOT NULL,
            user_id     TEXT NOT NULL,
            pick        TEXT NOT NULL,
            stake       INTEGER NOT NULL,
            placed_at   REAL NOT NULL,
            PRIMARY KEY (market_id, user_id)
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS bet_wallet_txn (
            txn_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id      TEXT NOT NULL,
            user_id       TEXT NOT NULL,
            actor_id      TEXT,
            action        TEXT NOT NULL,
            amount        INTEGER NOT NULL,
            balance_after INTEGER NOT NULL,
            market_id     INTEGER,
            note          TEXT,
            created_at    REAL NOT NULL
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_bet_wallet_txn_user
            ON bet_wallet_txn (guild_id, user_id, created_at DESC)
    ''')
    db.commit()
    logger.info('1.33.0: betting tables created')


@registry.register('1.34.0', 'Betting wallet transaction audit log')
def upgrade_1_34_0(db):
    """Create an append-only wallet transaction table for betting audit."""
    logger.info('1.34.0: Creating betting wallet transaction audit table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS bet_wallet_txn (
            txn_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id      TEXT NOT NULL,
            user_id       TEXT NOT NULL,
            actor_id      TEXT,
            action        TEXT NOT NULL,
            amount        INTEGER NOT NULL,
            balance_after INTEGER NOT NULL,
            market_id     INTEGER,
            note          TEXT,
            created_at    REAL NOT NULL
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_bet_wallet_txn_user
            ON bet_wallet_txn (guild_id, user_id, created_at DESC)
    ''')
    db.commit()
    logger.info('1.34.0: betting wallet transaction audit table created')


@registry.register('1.35.0', 'Betting fixture-level duplicate guard')
def upgrade_1_35_0(db):
    """Add canonical fixture keys and enforce one open market per fixture.

    Provider event ids can change. The bot still stores the provider id, but
    open-market uniqueness must be based on the fixture itself.
    """
    logger.info('1.35.0: Adding betting fixture duplicate guard')
    try:
        db.execute('ALTER TABLE bet_market ADD COLUMN fixture_key TEXT')
        logger.info('1.35.0: Added bet_market.fixture_key')
    except Exception as e:
        logger.debug('1.35.0: fixture_key already exists or unavailable: %s', e)

    rows = db.execute(
        'SELECT market_id, sport_key, home_team, away_team, commence_time '
        'FROM bet_market WHERE fixture_key IS NULL OR fixture_key = ""'
    ).fetchall()
    for row in rows:
        db.execute(
            'UPDATE bet_market SET fixture_key = ? WHERE market_id = ?',
            (bet_fixture_key(
                row.sport_key, row.home_team, row.away_team,
                row.commence_time),
             row.market_id)
        )

    duplicates = db.execute('''
        SELECT guild_id, fixture_key, COUNT(*) AS cnt
        FROM bet_market
        WHERE status = 'open'
        GROUP BY guild_id, fixture_key
        HAVING cnt > 1
    ''').fetchall()
    if duplicates:
        details = ', '.join(
            f'{row.guild_id}:{row.fixture_key} x{row.cnt}' for row in duplicates)
        raise RuntimeError(
            'Cannot add betting fixture uniqueness; duplicate open markets exist: '
            + details)

    db.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bet_market_open_fixture
            ON bet_market (guild_id, fixture_key)
            WHERE status = 'open'
    ''')
    db.commit()
    logger.info('1.35.0: betting fixture duplicate guard created')


@registry.register('1.36.0', 'Betting thread intro message tracking')
def upgrade_1_36_0(db):
    """Track the first message inside each betting thread for in-place edits."""
    logger.info('1.36.0: Adding betting thread intro message tracking')
    try:
        db.execute('ALTER TABLE bet_market ADD COLUMN thread_intro_id TEXT')
        logger.info('1.36.0: Added bet_market.thread_intro_id')
    except Exception as e:
        logger.debug('1.36.0: thread_intro_id already exists or unavailable: %s', e)
    db.commit()


@registry.register('1.37.0', 'Betting multi-pick wagers')
def upgrade_1_37_0(db):
    """Allow one user to hold separate wagers on multiple picks in a market.

    Existing rows migrate exactly as-is: each old one-pick wager becomes the
    row for that specific pick under the new (market_id, user_id, pick) key.
    """
    logger.info('1.37.0: Migrating betting wagers to per-pick keys')

    def _col_name(row):
        return getattr(row, 'name', row[1])

    def _col_pk(row):
        return getattr(row, 'pk', row[5])

    def _pk_cols(cols):
        return [_col_name(row) for row in sorted(
            (row for row in cols if _col_pk(row)), key=_col_pk)]

    def _create_bet_wager_table():
        db.execute('''
            CREATE TABLE IF NOT EXISTS bet_wager (
                market_id   INTEGER NOT NULL,
                user_id     TEXT NOT NULL,
                pick        TEXT NOT NULL,
                stake       INTEGER NOT NULL,
                placed_at   REAL NOT NULL,
                PRIMARY KEY (market_id, user_id, pick)
            )
        ''')

    def _copy_from_old_table():
        db.execute('''
            INSERT OR REPLACE INTO bet_wager
                (market_id, user_id, pick, stake, placed_at)
            SELECT market_id, user_id, pick, stake, placed_at
            FROM bet_wager_old_137
        ''')

    def _old_table_exists():
        return bool(db.execute('PRAGMA table_info(bet_wager_old_137)').fetchall())

    started_transaction = not db.in_transaction
    if started_transaction:
        db.execute('BEGIN IMMEDIATE')
    try:
        old_exists = _old_table_exists()
        cols = db.execute('PRAGMA table_info(bet_wager)').fetchall()
        if not cols:
            _create_bet_wager_table()
            if old_exists:
                _copy_from_old_table()
                db.execute('DROP TABLE bet_wager_old_137')
                logger.info('1.37.0: recovered betting wagers from old table')
            else:
                logger.info('1.37.0: betting wager table created')
            db.commit()
            return

        pk_cols = _pk_cols(cols)
        if pk_cols == ['market_id', 'user_id', 'pick']:
            if old_exists:
                _copy_from_old_table()
                db.execute('DROP TABLE bet_wager_old_137')
                logger.info('1.37.0: recovered betting wagers from old table')
            else:
                logger.info('1.37.0: betting wager table already uses per-pick keys')
            db.commit()
            return

        if pk_cols != ['market_id', 'user_id']:
            raise RuntimeError(
                'Cannot migrate bet_wager primary key; unexpected key columns: '
                + ', '.join(pk_cols))
        if old_exists:
            raise RuntimeError(
                'Cannot migrate bet_wager primary key; recovery table already exists')

        db.execute('ALTER TABLE bet_wager RENAME TO bet_wager_old_137')
        _create_bet_wager_table()
        _copy_from_old_table()
        db.execute('DROP TABLE bet_wager_old_137')
        db.commit()
        logger.info('1.37.0: betting wagers migrated to per-pick keys')
    except Exception:
        if started_transaction:
            db.rollback()
        raise
