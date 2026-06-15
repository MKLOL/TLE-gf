"""
Database upgrade functions for user.db.
Register upgrades in version order; they run automatically on startup.
"""
import logging

from tle.util.db.upgrades import UpgradeRegistry

logger = logging.getLogger(__name__)

registry = UpgradeRegistry(version_table='db_version')


@registry.register('1.0.0', 'Baseline — no-op for existing schemas')
def upgrade_1_0_0(db):
    logger.info('1.0.0: Baseline upgrade (no-op)')
    pass


@registry.register('1.1.0', 'Multi-emoji starboard')
def upgrade_1_1_0(db):
    logger.info('1.1.0: Creating multi-emoji starboard v1 tables')
    # Create new v1 tables
    db.execute('''
        CREATE TABLE IF NOT EXISTS starboard_config_v1 (
            guild_id    TEXT PRIMARY KEY,
            channel_id  TEXT
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS starboard_emoji_v1 (
            guild_id    TEXT,
            emoji       TEXT,
            threshold   INTEGER NOT NULL DEFAULT 3,
            color       INTEGER NOT NULL DEFAULT 16755216,
            PRIMARY KEY (guild_id, emoji)
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS starboard_message_v1 (
            original_msg_id     TEXT,
            starboard_msg_id    TEXT,
            guild_id            TEXT,
            emoji               TEXT,
            PRIMARY KEY (original_msg_id, emoji)
        )
    ''')

    # Migrate data from old starboard table → starboard_config_v1 + starboard_emoji_v1
    default_star = '\N{WHITE MEDIUM STAR}'
    default_threshold = 3
    default_color = 0xffaa10

    logger.info('1.1.0: Migrating old starboard config data...')
    try:
        rows = db.execute('SELECT guild_id, channel_id FROM starboard').fetchall()
        logger.info(f'1.1.0: Found {len(rows)} old starboard config rows to migrate')
        for row in rows:
            logger.debug(f'1.1.0: Migrating starboard config for guild {row.guild_id} '
                         f'(channel={row.channel_id})')
            db.execute(
                'INSERT OR IGNORE INTO starboard_config_v1 (guild_id, channel_id) VALUES (?, ?)',
                (row.guild_id, row.channel_id)
            )
            db.execute(
                'INSERT OR IGNORE INTO starboard_emoji_v1 (guild_id, emoji, threshold, color) VALUES (?, ?, ?, ?)',
                (row.guild_id, default_star, default_threshold, default_color)
            )
        logger.info(f'1.1.0: Successfully migrated {len(rows)} starboard config entries')
    except Exception as e:
        logger.warning(f'1.1.0: Could not migrate old starboard config: {e}', exc_info=True)

    # Migrate old starboard_message → starboard_message_v1
    logger.info('1.1.0: Migrating old starboard message data...')
    try:
        rows = db.execute('SELECT original_msg_id, starboard_msg_id, guild_id FROM starboard_message').fetchall()
        logger.info(f'1.1.0: Found {len(rows)} old starboard message rows to migrate')
        for row in rows:
            logger.debug(f'1.1.0: Migrating starboard message {row.original_msg_id} '
                         f'(starboard_msg={row.starboard_msg_id}, guild={row.guild_id})')
            db.execute(
                'INSERT OR IGNORE INTO starboard_message_v1 (original_msg_id, starboard_msg_id, guild_id, emoji) '
                'VALUES (?, ?, ?, ?)',
                (row.original_msg_id, row.starboard_msg_id, row.guild_id, default_star)
            )
        logger.info(f'1.1.0: Successfully migrated {len(rows)} starboard message entries')
    except Exception as e:
        logger.warning(f'1.1.0: Could not migrate old starboard messages: {e}', exc_info=True)

    db.commit()


@registry.register('1.2.0', 'Guild config system')
def upgrade_1_2_0(db):
    logger.info('1.2.0: Creating guild_config table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS guild_config (
            guild_id    TEXT,
            key         TEXT,
            value       TEXT,
            PRIMARY KEY (guild_id, key)
        )
    ''')
    db.commit()
    logger.info('1.2.0: guild_config table created')


@registry.register('1.3.0', 'Star count, author tracking, and reactor tracking for leaderboards')
def upgrade_1_3_0(db):
    logger.info('1.3.0: Adding author_id and star_count columns to starboard_message_v1')
    # Add author_id column
    try:
        db.execute('ALTER TABLE starboard_message_v1 ADD COLUMN author_id TEXT')
        logger.info('1.3.0: Added author_id column')
    except Exception as e:
        logger.debug(f'1.3.0: author_id column already exists or error: {e}')

    # Add star_count column
    try:
        db.execute('ALTER TABLE starboard_message_v1 ADD COLUMN star_count INTEGER DEFAULT 0')
        logger.info('1.3.0: Added star_count column')
    except Exception as e:
        logger.debug(f'1.3.0: star_count column already exists or error: {e}')

    # Add channel_id column for backfill efficiency
    try:
        db.execute('ALTER TABLE starboard_message_v1 ADD COLUMN channel_id TEXT')
        logger.info('1.3.0: Added channel_id column')
    except Exception as e:
        logger.debug(f'1.3.0: channel_id column already exists or error: {e}')

    # Create starboard_reactors table — tracks which users reacted
    db.execute('''
        CREATE TABLE IF NOT EXISTS starboard_reactors (
            original_msg_id TEXT,
            emoji           TEXT,
            user_id         TEXT,
            PRIMARY KEY (original_msg_id, emoji, user_id)
        )
    ''')
    logger.info('1.3.0: Created starboard_reactors table')

    db.commit()
    logger.info('1.3.0: Upgrade complete')


@registry.register('1.4.0', 'Per-emoji starboard channels')
def upgrade_1_4_0(db):
    logger.info('1.4.0: Migrating channel_id from starboard_config_v1 into starboard_emoji_v1')

    # Add channel_id column to starboard_emoji_v1
    try:
        db.execute('ALTER TABLE starboard_emoji_v1 ADD COLUMN channel_id TEXT')
        logger.info('1.4.0: Added channel_id column to starboard_emoji_v1')
    except Exception as e:
        logger.debug(f'1.4.0: channel_id column already exists or error: {e}')

    # Copy channel_id from starboard_config_v1 into each emoji row
    try:
        rows = db.execute('SELECT guild_id, channel_id FROM starboard_config_v1').fetchall()
        logger.info(f'1.4.0: Found {len(rows)} guilds in starboard_config_v1 to migrate')
        migrated = 0
        for row in rows:
            if row.channel_id:
                rc = db.execute(
                    'UPDATE starboard_emoji_v1 SET channel_id = ? WHERE guild_id = ? AND channel_id IS NULL',
                    (row.channel_id, row.guild_id)
                ).rowcount
                migrated += rc
                logger.debug(f'1.4.0: Guild {row.guild_id}: set channel_id={row.channel_id} '
                             f'on {rc} emoji rows')
        logger.info(f'1.4.0: Migrated channel_id for {migrated} emoji rows across {len(rows)} guilds')
    except Exception as e:
        logger.warning(f'1.4.0: Could not migrate channel_id from starboard_config_v1: {e}',
                       exc_info=True)

    db.commit()
    logger.info('1.4.0: Upgrade complete')


@registry.register('1.5.0', 'Rating-weighted polls')
def upgrade_1_5_0(db):
    logger.info('1.5.0: Creating rpoll tables')
    db.execute('''
        CREATE TABLE IF NOT EXISTS rpoll (
            poll_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id    TEXT NOT NULL,
            channel_id  TEXT NOT NULL,
            message_id  TEXT,
            question    TEXT NOT NULL,
            created_by  TEXT NOT NULL,
            created_at  REAL NOT NULL
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS rpoll_option (
            poll_id       INTEGER NOT NULL,
            option_index  INTEGER NOT NULL,
            label         TEXT NOT NULL,
            PRIMARY KEY (poll_id, option_index),
            FOREIGN KEY (poll_id) REFERENCES rpoll(poll_id)
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS rpoll_vote (
            poll_id       INTEGER NOT NULL,
            user_id       TEXT NOT NULL,
            option_index  INTEGER NOT NULL,
            rating        INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (poll_id, user_id, option_index),
            FOREIGN KEY (poll_id) REFERENCES rpoll(poll_id)
        )
    ''')
    db.commit()
    logger.info('1.5.0: rpoll tables created')


@registry.register('1.6.0', 'General key-value store')
def upgrade_1_6_0(db):
    logger.info('1.6.0: Creating kvs table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS kvs (
            key     TEXT PRIMARY KEY,
            value   TEXT NOT NULL
        )
    ''')
    db.commit()
    logger.info('1.6.0: kvs table created')


@registry.register('1.7.0', 'Starboard emoji aliases')
def upgrade_1_7_0(db):
    logger.info('1.7.0: Creating starboard_alias table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS starboard_alias (
            guild_id    TEXT,
            alias_emoji TEXT,
            main_emoji  TEXT,
            PRIMARY KEY (guild_id, alias_emoji)
        )
    ''')
    db.commit()
    logger.info('1.7.0: starboard_alias table created')


@registry.register('1.8.0', 'Anonymous rpoll option')
def upgrade_1_8_0(db):
    logger.info('1.8.0: Adding anonymous column to rpoll table')
    try:
        db.execute('ALTER TABLE rpoll ADD COLUMN anonymous INTEGER NOT NULL DEFAULT 0')
        logger.info('1.8.0: Added anonymous column')
    except Exception as e:
        logger.debug(f'1.8.0: anonymous column already exists or error: {e}')
    db.commit()
    logger.info('1.8.0: Upgrade complete')


@registry.register('1.9.0', 'Poll expiration')
def upgrade_1_9_0(db):
    logger.info('1.9.0: Adding expiration columns to rpoll table')
    try:
        db.execute('ALTER TABLE rpoll ADD COLUMN expires_at REAL DEFAULT 0')
        logger.info('1.9.0: Added expires_at column')
    except Exception as e:
        logger.debug(f'1.9.0: expires_at column already exists or error: {e}')
    try:
        db.execute('ALTER TABLE rpoll ADD COLUMN closed INTEGER NOT NULL DEFAULT 0')
        logger.info('1.9.0: Added closed column')
    except Exception as e:
        logger.debug(f'1.9.0: closed column already exists or error: {e}')

    import time
    now = time.time()
    # Backfill expires_at for all existing polls
    db.execute('UPDATE rpoll SET expires_at = created_at + 86400 WHERE expires_at = 0')
    # Close polls that have already expired or were never posted
    db.execute(
        'UPDATE rpoll SET closed = 1 WHERE closed = 0 AND '
        '(expires_at <= ? OR message_id IS NULL)',
        (now,)
    )
    db.commit()
    logger.info('1.9.0: Backfilled expiration data for existing polls')


@registry.register('1.10.0', 'Poll scoring formula')
def upgrade_1_10_0(db):
    logger.info('1.10.0: Adding formula column to rpoll table')
    try:
        db.execute("ALTER TABLE rpoll ADD COLUMN formula TEXT NOT NULL DEFAULT 'sum'")
        logger.info('1.10.0: Added formula column')
    except Exception as e:
        logger.debug(f'1.10.0: formula column already exists or error: {e}')
    db.commit()
    logger.info('1.10.0: Upgrade complete')


@registry.register('1.11.0', 'Starboard migration tables')
def upgrade_1_11_0(db):
    logger.info('1.11.0: Creating starboard migration tables')
    db.execute('''
        CREATE TABLE IF NOT EXISTS starboard_migration (
            guild_id            TEXT PRIMARY KEY,
            old_channel_id      TEXT NOT NULL,
            new_channel_id      TEXT NOT NULL,
            emojis              TEXT NOT NULL,
            status              TEXT NOT NULL DEFAULT 'crawling',
            last_crawled_msg_id TEXT,
            crawl_total         INTEGER DEFAULT 0,
            crawl_done          INTEGER DEFAULT 0,
            crawl_failed        INTEGER DEFAULT 0,
            post_total          INTEGER DEFAULT 0,
            post_done           INTEGER DEFAULT 0,
            started_at          REAL NOT NULL
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS starboard_migration_entry (
            guild_id            TEXT NOT NULL,
            original_msg_id     TEXT NOT NULL,
            emoji               TEXT NOT NULL,
            old_bot_msg_id      TEXT NOT NULL,
            old_channel_id      TEXT NOT NULL,
            source_channel_id   TEXT,
            author_id           TEXT,
            star_count          INTEGER DEFAULT 0,
            new_starboard_msg_id TEXT,
            crawl_status        TEXT NOT NULL DEFAULT 'pending',
            embed_fallback      TEXT,
            PRIMARY KEY (original_msg_id, emoji)
        )
    ''')
    db.commit()
    logger.info('1.11.0: Starboard migration tables created')


@registry.register('1.12.0', 'Migration alias map')
def upgrade_1_12_0(db):
    logger.info('1.12.0: Adding alias_map column to starboard_migration')
    try:
        db.execute('ALTER TABLE starboard_migration ADD COLUMN alias_map TEXT')
        logger.info('1.12.0: Added alias_map column')
    except Exception as e:
        logger.debug(f'1.12.0: alias_map column already exists or error: {e}')
    db.commit()
    logger.info('1.12.0: Upgrade complete')


@registry.register('1.13.0', 'Migration retry: last_error column')
def upgrade_1_13_0(db):
    logger.info('1.13.0: Adding last_error column to starboard_migration_entry')
    try:
        db.execute('ALTER TABLE starboard_migration_entry ADD COLUMN last_error TEXT')
        logger.info('1.13.0: Added last_error column')
    except Exception as e:
        logger.debug(f'1.13.0: last_error column already exists or error: {e}')
    db.commit()
    logger.info('1.13.0: Upgrade complete')


@registry.register('1.14.0', 'Minigame tables')
def upgrade_1_14_0(db):
    logger.info('1.14.0: Creating minigame tables')
    db.execute('''
        CREATE TABLE IF NOT EXISTS minigame_config (
            guild_id   TEXT NOT NULL,
            game       TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            PRIMARY KEY (guild_id, game)
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS minigame_result (
            message_id     TEXT NOT NULL,
            guild_id       TEXT NOT NULL,
            game           TEXT NOT NULL,
            channel_id     TEXT NOT NULL,
            user_id        TEXT NOT NULL,
            puzzle_number  INTEGER NOT NULL,
            puzzle_date    TEXT NOT NULL,
            accuracy       INTEGER NOT NULL,
            time_seconds   INTEGER NOT NULL,
            is_perfect     INTEGER NOT NULL DEFAULT 0,
            raw_content    TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (message_id, game, puzzle_number)
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_minigame_result_lookup
            ON minigame_result (guild_id, game, user_id, puzzle_number)
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_minigame_result_date
            ON minigame_result (guild_id, game, puzzle_date)
    ''')
    db.commit()
    logger.info('1.14.0: Minigame tables created')


@registry.register('1.15.0', 'Minigame import table')
def upgrade_1_15_0(db):
    logger.info('1.15.0: Creating minigame import table')
    db.execute('''
        CREATE TABLE IF NOT EXISTS minigame_import_result (
            message_id     TEXT NOT NULL,
            guild_id       TEXT NOT NULL,
            game           TEXT NOT NULL,
            channel_id     TEXT NOT NULL,
            user_id        TEXT NOT NULL,
            puzzle_number  INTEGER NOT NULL,
            puzzle_date    TEXT NOT NULL,
            accuracy       INTEGER NOT NULL,
            time_seconds   INTEGER NOT NULL,
            is_perfect     INTEGER NOT NULL DEFAULT 0,
            raw_content    TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (message_id, game, puzzle_number)
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_minigame_import_result_lookup
            ON minigame_import_result (guild_id, game, user_id, puzzle_number)
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_minigame_import_result_date
            ON minigame_import_result (guild_id, game, puzzle_date)
    ''')
    db.commit()
    logger.info('1.15.0: Minigame import table created')


@registry.register('1.16.0', 'Minigame raw message table')
def upgrade_1_16_0(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS minigame_raw_message (
            message_id  TEXT NOT NULL PRIMARY KEY,
            guild_id    TEXT NOT NULL,
            channel_id  TEXT NOT NULL,
            user_id     TEXT NOT NULL,
            created_at  TEXT NOT NULL,
            raw_content TEXT NOT NULL
        )
    ''')
    db.execute('''
        CREATE INDEX IF NOT EXISTS idx_minigame_raw_message_guild
            ON minigame_raw_message (guild_id)
    ''')
    db.commit()
    logger.info('1.16.0: Minigame raw message table created')


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
    db.commit()
    logger.info('1.33.0: betting tables created')
