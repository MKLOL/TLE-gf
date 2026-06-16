"""user.db upgrades 1.17.0 – 1.29.0 (complaints, Great Day, Akari ratings/bans).

Part of the upgrade chain split out of ``user_db_upgrades`` to stay under the
500-line limit. Importing this module registers its upgrades on the shared
``registry``.
"""
import logging

from tle.util.db._user_db_upgrade_registry import registry

logger = logging.getLogger(__name__)


@registry.register('1.17.0', 'Complaint table')
def upgrade_1_17_0(db):
    logger.info('1.17.0: Creating complaint table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS complaint (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id    TEXT NOT NULL,
            user_id     TEXT NOT NULL,
            text        TEXT NOT NULL,
            created_at  REAL NOT NULL,
            active      INTEGER NOT NULL DEFAULT 1
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_complaint_guild
            ON complaint (guild_id, created_at DESC)
    ''')
    db.commit()
    logger.info('1.17.0: Complaint table created')


@registry.register('1.18.0', 'Great Day signup table')
def upgrade_1_18_0(db):
    logger.info('1.18.0: Creating greatday_signup table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS greatday_signup (
            guild_id    TEXT NOT NULL,
            user_id     TEXT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    ''')
    db.commit()
    logger.info('1.18.0: greatday_signup table created')


@registry.register('1.19.0', 'Add active column to complaint table for soft deletes')
def upgrade_1_19_0(db):
    logger.info('1.19.0: Adding active column to complaint table')
    try:
        db.execute('ALTER TABLE complaint ADD COLUMN active INTEGER NOT NULL DEFAULT 1')
    except Exception:
        pass
    db.commit()
    logger.info('1.19.0: active column added to complaint table')


@registry.register('1.20.0', 'CF virtual contest rank cache')
def upgrade_1_20_0(db):
    logger.info('1.20.0: Creating cfvc_cache table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS cfvc_cache (
            handle       TEXT NOT NULL,
            contest_id   INTEGER NOT NULL,
            rank         INTEGER NOT NULL,
            PRIMARY KEY (handle, contest_id)
        )
    ''')
    db.commit()
    logger.info('1.20.0: cfvc_cache table created')


@registry.register('1.21.0', 'Great Day ban table')
def upgrade_1_21_0(db):
    logger.info('1.21.0: Creating greatday_ban table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS greatday_ban (
            guild_id    TEXT NOT NULL,
            user_id     TEXT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    ''')
    db.commit()
    logger.info('1.21.0: greatday_ban table created')


@registry.register('1.22.0', 'Add message_link column to complaint table')
def upgrade_1_22_0(db):
    logger.info('1.22.0: Adding message_link column to complaint table')
    try:
        db.execute('ALTER TABLE complaint ADD COLUMN message_link TEXT')
    except Exception:
        pass
    db.commit()
    logger.info('1.22.0: message_link column added to complaint table')


@registry.register('1.23.0', 'Great Day pick history table')
def upgrade_1_23_0(db):
    logger.info('1.23.0: Creating greatday_pick table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS greatday_pick (
            guild_id    TEXT NOT NULL,
            user_id     TEXT NOT NULL,
            message_id  TEXT NOT NULL,
            picked_at   REAL NOT NULL,
            PRIMARY KEY (guild_id, user_id, message_id)
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_greatday_pick_user
            ON greatday_pick (guild_id, user_id)
    ''')
    db.commit()
    logger.info('1.23.0: greatday_pick table created')


@registry.register('1.24.0', 'Akari ratings')
def upgrade_1_24_0(db):
    logger.info('1.24.0: Creating minigame_registrant and akari_rating tables')
    # Who has opted in via `;mg akari register`. Rating is computed for everyone with
    # results regardless; this table only records the opt-in flag.
    db.execute('''
        CREATE TABLE IF NOT EXISTS minigame_registrant (
            guild_id      TEXT NOT NULL,
            user_id       TEXT NOT NULL,
            registered_at REAL NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    ''')
    # Rebuildable snapshot of the Codeforces-style rating replay. Ratings are
    # stored as REAL (float) and rounded only for display; this table is a cache
    # of a pure function of the minigame result tables, not a source of truth.
    db.execute('''
        CREATE TABLE IF NOT EXISTS akari_rating (
            guild_id   TEXT NOT NULL,
            user_id    TEXT NOT NULL,
            rating     REAL NOT NULL,
            games      INTEGER NOT NULL DEFAULT 0,
            peak       REAL NOT NULL,
            last_delta REAL NOT NULL DEFAULT 0,
            updated_at REAL NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_akari_rating_guild
            ON akari_rating (guild_id, rating DESC)
    ''')
    db.commit()
    logger.info('1.24.0: minigame_registrant and akari_rating tables created')


@registry.register('1.25.0', 'Akari rating decay columns')
def upgrade_1_25_0(db):
    logger.info('1.25.0: Adding skip_streak/last_puzzle columns to akari_rating')
    for column in ('skip_streak', 'last_puzzle'):
        try:
            db.execute(
                f'ALTER TABLE akari_rating ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0')
            logger.info('1.25.0: Added %s column', column)
        except Exception as e:
            logger.debug('1.25.0: %s column already exists or error: %s', column, e)
    db.commit()
    logger.info('1.25.0: Upgrade complete')


@registry.register('1.26.0', 'Rename minigame_registrant to akari_registrant')
def upgrade_1_26_0(db):
    """Make the registrant table akari-specific.

    Registration was only ever consumed by akari rating displays, so the
    ``minigame_`` prefix was a misnomer.  Rename the table and lift the data
    over verbatim — same shape, same PK, just an honest name.  If guessgame
    ever gets ratings, it will get its own table.
    """
    logger.info('1.26.0: Renaming minigame_registrant to akari_registrant')
    db.execute('''
        CREATE TABLE IF NOT EXISTS akari_registrant (
            guild_id      TEXT NOT NULL,
            user_id       TEXT NOT NULL,
            registered_at REAL NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    ''')
    # Idempotent copy in case this runs twice or rows were already moved.
    try:
        moved = db.execute(
            '''INSERT OR IGNORE INTO akari_registrant
               (guild_id, user_id, registered_at)
               SELECT guild_id, user_id, registered_at FROM minigame_registrant
            '''
        ).rowcount
        logger.info('1.26.0: Copied %s registrant row(s)', moved)
        db.execute('DROP TABLE minigame_registrant')
    except Exception as e:
        # Fresh DB created via user_db_conn.py never had minigame_registrant;
        # there's nothing to copy or drop.
        logger.debug('1.26.0: legacy table absent (%s) — fresh DB path', e)
    db.commit()
    logger.info('1.26.0: Upgrade complete')


@registry.register('1.27.0', 'Akari ingestion banlist')
def upgrade_1_27_0(db):
    """Create the akari_ban table.

    Banned users' Akari messages are silently dropped at ingest time.  The
    table is forward-only (existing rows are not affected); banning is purely
    a save-time filter.
    """
    logger.info('1.27.0: Creating akari_ban table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS akari_ban (
            guild_id   TEXT NOT NULL,
            user_id    TEXT NOT NULL,
            banned_at  REAL NOT NULL,
            banned_by  TEXT NOT NULL,
            reason     TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
    ''')
    db.commit()
    logger.info('1.27.0: Upgrade complete')


@registry.register('1.28.0', 'Akari explicit opt-out registry')
def upgrade_1_28_0(db):
    """Create the akari_optout table.

    Registration flipped from explicit opt-in (row in akari_registrant) to
    default opt-in for any user with results.  ``akari_optout`` records the
    only signal that still matters: users who explicitly *unregistered*.  Once
    a user appears here they stay invisible until they ``;mg akari register``,
    which lifts the opt-out.  The older akari_registrant table is preserved
    but is no longer read or written.
    """
    logger.info('1.28.0: Creating akari_optout table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS akari_optout (
            guild_id     TEXT NOT NULL,
            user_id      TEXT NOT NULL,
            opted_out_at REAL NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    ''')
    db.commit()
    logger.info('1.28.0: Upgrade complete')


@registry.register('1.29.0', 'Per-user starboard default emoji')
def upgrade_1_29_0(db):
    """Create the user_starboard_default table.

    Stores each user's preferred emoji for ``;starboard`` leaderboard commands
    that default to a single emoji (leaderboard / rank / star-givers /
    narcissus / top).  Per-guild keyed because custom emojis are guild-scoped.
    Resolution order: explicit arg > this table > ``constants._DEFAULT_STAR``.
    """
    logger.info('1.29.0: Creating user_starboard_default table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS user_starboard_default (
            guild_id TEXT NOT NULL,
            user_id  TEXT NOT NULL,
            emoji    TEXT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    ''')
    db.commit()
    logger.info('1.29.0: Upgrade complete')
