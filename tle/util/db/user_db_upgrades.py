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


@registry.register('1.14.0', 'Daily Akari tables')
def upgrade_1_14_0(db):
    logger.info('1.14.0: Creating Daily Akari tables')
    db.execute('''
        CREATE TABLE IF NOT EXISTS dailyakari_config (
            guild_id    TEXT PRIMARY KEY,
            channel_id  TEXT NOT NULL
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS dailyakari_result (
            message_id     TEXT PRIMARY KEY,
            guild_id       TEXT NOT NULL,
            channel_id     TEXT NOT NULL,
            user_id        TEXT NOT NULL,
            puzzle_number  INTEGER NOT NULL,
            puzzle_date    TEXT NOT NULL,
            accuracy       INTEGER NOT NULL,
            time_seconds   INTEGER NOT NULL,
            is_perfect     INTEGER NOT NULL DEFAULT 0
        )
    ''')
    db.commit()
    logger.info('1.14.0: Daily Akari tables created')


@registry.register('1.15.0', 'Daily Akari import tables')
def upgrade_1_15_0(db):
    logger.info('1.15.0: Creating Daily Akari import tables')
    db.execute('''
        CREATE TABLE IF NOT EXISTS dailyakari_import_result (
            message_id     TEXT PRIMARY KEY,
            guild_id       TEXT NOT NULL,
            channel_id     TEXT NOT NULL,
            user_id        TEXT NOT NULL,
            puzzle_number  INTEGER NOT NULL,
            puzzle_date    TEXT NOT NULL,
            accuracy       INTEGER NOT NULL,
            time_seconds   INTEGER NOT NULL,
            is_perfect     INTEGER NOT NULL DEFAULT 0
        )
    ''')
    db.commit()
    logger.info('1.15.0: Daily Akari import tables created')
