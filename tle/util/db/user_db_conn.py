import logging
import sqlite3
import time
import unicodedata
from enum import IntEnum
from collections import defaultdict, namedtuple

from discord.ext import commands

from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util.db.starboard_db import (
    StarboardDbMixin,
    snowflake_to_unix_sql, DISCORD_EPOCH_MS, SNOWFLAKE_TIMESTAMP_DIVISOR, _NO_TIME_BOUND,
)
from tle.util.db.minigame_db import MinigameDbMixin
from tle.util.db.migration_db import MigrationDbMixin

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


class UserDbConn(MinigameDbMixin, StarboardDbMixin, MigrationDbMixin):
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
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS user_handle ('
            'user_id     TEXT,'
            'guild_id    TEXT,'
            'handle      TEXT,'
            'active      INTEGER,'
            'PRIMARY KEY (user_id, guild_id)'
            ')'
        )
        self.conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS ix_user_handle_guild_handle '
                          'ON user_handle (guild_id, handle)')
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS cf_user_cache ('
            'handle              TEXT PRIMARY KEY,'
            'first_name          TEXT,'
            'last_name           TEXT,'
            'country             TEXT,'
            'city                TEXT,'
            'organization        TEXT,'
            'contribution        INTEGER,'
            'rating              INTEGER,'
            'maxRating           INTEGER,'
            'last_online_time    INTEGER,'
            'registration_time   INTEGER,'
            'friend_of_count     INTEGER,'
            'title_photo         TEXT'
            ')'
        )
        # TODO: Make duel tables guild-aware.
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS duelist(
                "user_id"	INTEGER PRIMARY KEY NOT NULL,
                "rating"	INTEGER NOT NULL,
                "guild_id"  TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS duel(
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "challenger"	INTEGER NOT NULL,
                "challengee"	INTEGER NOT NULL,
                "issue_time"	REAL NOT NULL,
                "start_time"	REAL,
                "finish_time"	REAL,
                "problem_name"	TEXT,
                "contest_id"	INTEGER,
                "p_index"	INTEGER,
                "status"	INTEGER,
                "winner"	INTEGER,
                "type"		INTEGER,
                "guild_id"  TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS duel_settings (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "challenge" (
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "user_id"	TEXT NOT NULL,
                "issue_time"	REAL NOT NULL,
                "finish_time"	REAL,
                "problem_name"	TEXT NOT NULL,
                "contest_id"	INTEGER NOT NULL,
                "p_index"	INTEGER NOT NULL,
                "rating_delta"	INTEGER NOT NULL,
                "status"	INTEGER NOT NULL
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "user_challenge" (
                "user_id"	TEXT,
                "active_challenge_id"	INTEGER,
                "issue_time"	REAL,
                "score"	INTEGER NOT NULL,
                "num_completed"	INTEGER NOT NULL,
                "num_skipped"	INTEGER NOT NULL,
                PRIMARY KEY("user_id")
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS reminder (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT,
                role_id TEXT,
                before TEXT
            )
        ''')
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS starboard ('
            'guild_id     TEXT PRIMARY KEY,'
            'channel_id   TEXT'
            ')'
        )
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS starboard_message ('
            'original_msg_id    TEXT PRIMARY KEY,'
            'starboard_msg_id   TEXT,'
            'guild_id           TEXT'
            ')'
        )
        # Multi-emoji starboard v1 tables
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_config_v1 (
                guild_id    TEXT PRIMARY KEY,
                channel_id  TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_emoji_v1 (
                guild_id    TEXT,
                emoji       TEXT,
                threshold   INTEGER NOT NULL DEFAULT 3,
                color       INTEGER NOT NULL DEFAULT 16755216,
                channel_id  TEXT,
                PRIMARY KEY (guild_id, emoji)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_message_v1 (
                original_msg_id     TEXT,
                starboard_msg_id    TEXT,
                guild_id            TEXT,
                emoji               TEXT,
                author_id           TEXT,
                star_count          INTEGER DEFAULT 0,
                channel_id          TEXT,
                PRIMARY KEY (original_msg_id, emoji)
            )
        ''')
        # Starboard reactors — tracks which users reacted with which emoji
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_reactors (
                original_msg_id TEXT,
                emoji           TEXT,
                user_id         TEXT,
                PRIMARY KEY (original_msg_id, emoji, user_id)
            )
        ''')
        # Starboard emoji aliases — alias emojis that count toward the main emoji
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS starboard_alias (
                guild_id    TEXT,
                alias_emoji TEXT,
                main_emoji  TEXT,
                PRIMARY KEY (guild_id, alias_emoji)
            )
        ''')
        # Per-user default emoji for starboard leaderboard commands
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS user_starboard_default (
                guild_id TEXT NOT NULL,
                user_id  TEXT NOT NULL,
                emoji    TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        # Guild config table
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS guild_config (
                guild_id    TEXT,
                key         TEXT,
                value       TEXT,
                PRIMARY KEY (guild_id, key)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_config (
                guild_id   TEXT NOT NULL,
                game       TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                PRIMARY KEY (guild_id, game)
            )
        ''')
        self.conn.execute('''
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
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_minigame_result_lookup
                ON minigame_result (guild_id, game, user_id, puzzle_number)
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_minigame_result_date
                ON minigame_result (guild_id, game, puzzle_date)
        ''')
        self.conn.execute('''
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
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_minigame_import_result_lookup
                ON minigame_import_result (guild_id, game, user_id, puzzle_number)
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_minigame_import_result_date
                ON minigame_import_result (guild_id, game, puzzle_date)
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS minigame_raw_message (
                message_id  TEXT NOT NULL PRIMARY KEY,
                guild_id    TEXT NOT NULL,
                channel_id  TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                raw_content TEXT NOT NULL
            )
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_minigame_raw_message_guild
                ON minigame_raw_message (guild_id)
        ''')
        self.conn.execute('''
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
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_minigame_player_link_lookup
                ON minigame_player_link (guild_id, game, normalized_name)
        ''')
        self.conn.execute('''
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
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_minigame_unresolved_result_puzzle
                ON minigame_unresolved_result (guild_id, game, puzzle_number)
        ''')
        self.conn.execute('''
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
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_minigame_ban_guild
                ON minigame_ban (guild_id, game, banned_at DESC)
        ''')
        self.conn.execute('''
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
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_minigame_rating_guild
                ON minigame_rating (guild_id, game, rating DESC)
        ''')
        # Akari ratings (registrants + rebuildable rating snapshot).
        # Registration is akari-specific — guessgame doesn't have a rating
        # system, so the opt-in roster has no reason to be game-keyed.
        # Default opt-in: anyone with an Akari result is visible in rating
        # displays unless they appear in ``akari_optout``.  ``akari_registrant``
        # is legacy from the explicit-opt-in era — nothing reads or writes it
        # now; kept for the existing rows / future audit.
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS akari_registrant (
                guild_id      TEXT NOT NULL,
                user_id       TEXT NOT NULL,
                registered_at REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS akari_optout (
                guild_id     TEXT NOT NULL,
                user_id      TEXT NOT NULL,
                opted_out_at REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        # Akari ingestion banlist. Banned users' messages (live + edits + import
        # + reparse) are silently dropped — no raw store, no result row.
        # Forward-only: existing data is untouched, banning just stops the bleed.
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS akari_ban (
                guild_id   TEXT NOT NULL,
                user_id    TEXT NOT NULL,
                banned_at  REAL NOT NULL,
                banned_by  TEXT NOT NULL,
                reason     TEXT,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS akari_rating (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                rating      REAL NOT NULL,
                games       INTEGER NOT NULL DEFAULT 0,
                peak        REAL NOT NULL,
                last_delta  REAL NOT NULL DEFAULT 0,
                skip_streak INTEGER NOT NULL DEFAULT 0,
                last_puzzle INTEGER NOT NULL DEFAULT 0,
                updated_at  REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_akari_rating_guild
                ON akari_rating (guild_id, rating DESC)
        ''')
        # Complaints
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS complaint (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id     TEXT NOT NULL,
                user_id      TEXT NOT NULL,
                text         TEXT NOT NULL,
                created_at   REAL NOT NULL,
                message_link TEXT
            )
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_complaint_guild
                ON complaint (guild_id, created_at DESC)
        ''')
        # Great Day signups
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS greatday_signup (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        # Great Day pick history (one row per (guild, user, message))
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS greatday_pick (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                message_id  TEXT NOT NULL,
                picked_at   REAL NOT NULL,
                PRIMARY KEY (guild_id, user_id, message_id)
            )
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_greatday_pick_user
                ON greatday_pick (guild_id, user_id)
        ''')
        # Soccer betting minigame (see tle/cogs/betting.py). Kept in sync with
        # betting upgrades — fresh DBs get the tables here, existing DBs via
        # migrations.
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS bet_wallet (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                balance     INTEGER NOT NULL,
                last_daily  TEXT,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS bet_market (
                market_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id       TEXT NOT NULL,
                channel_id     TEXT NOT NULL,
                message_id     TEXT,
                thread_id      TEXT,
                event_id       TEXT NOT NULL,
                fixture_key    TEXT NOT NULL,
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
        bet_market_cols = [
            row.name for row in self.conn.execute('PRAGMA table_info(bet_market)')
        ]
        if 'fixture_key' not in bet_market_cols:
            self.conn.execute('ALTER TABLE bet_market ADD COLUMN fixture_key TEXT')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_bet_market_active
                ON bet_market (guild_id, channel_id, status)
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_bet_market_pending
                ON bet_market (status, commence_time)
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_bet_market_thread
                ON bet_market (guild_id, thread_id, status)
        ''')
        self.conn.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS idx_bet_market_open_event
                ON bet_market (guild_id, event_id)
                WHERE status = 'open'
        ''')
        missing_fixture_key = self.conn.execute(
            'SELECT 1 FROM bet_market '
            'WHERE fixture_key IS NULL OR fixture_key = "" LIMIT 1'
        ).fetchone()
        if missing_fixture_key is None:
            self.conn.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bet_market_open_fixture
                    ON bet_market (guild_id, fixture_key)
                    WHERE status = 'open'
            ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS bet_wager (
                market_id   INTEGER NOT NULL,
                user_id     TEXT NOT NULL,
                pick        TEXT NOT NULL,
                stake       INTEGER NOT NULL,
                placed_at   REAL NOT NULL,
                PRIMARY KEY (market_id, user_id)
            )
        ''')
        self.conn.execute('''
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
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_bet_wallet_txn_user
                ON bet_wallet_txn (guild_id, user_id, created_at DESC)
        ''')
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS rankup ('
            'guild_id     TEXT PRIMARY KEY,'
            'channel_id   TEXT'
            ')'
        )
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS auto_role_update ('
            'guild_id     TEXT PRIMARY KEY'
            ')'
        )

        # Rated VCs stuff:
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "rated_vcs" (
                "id"	         INTEGER PRIMARY KEY AUTOINCREMENT,
                "contest_id"     INTEGER NOT NULL,
                "start_time"     REAL,
                "finish_time"    REAL,
                "status"         INTEGER,
                "guild_id"       TEXT
            )
        ''')

        # TODO: Do we need to explicitly specify the fk constraint or just depend on the middleware?
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS "rated_vc_users" (
                "vc_id"	         INTEGER,
                "user_id"        TEXT NOT NULL,
                "rating"         INTEGER,

                CONSTRAINT fk_vc
                    FOREIGN KEY (vc_id)
                    REFERENCES rated_vcs(id),

                PRIMARY KEY(vc_id, user_id)
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS rated_vc_settings (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS training_settings (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS trainings (
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "user_id" TEXT,
                "score" INTEGER,
                "lives" INTEGER,
                "time_left"     REAL,
                "mode"  INTEGER NOT NULL,
                "status" INTEGER NOT NULL
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS training_problems (
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "training_id"   INTEGER NOT NULL,
                "issue_time"	REAL NOT NULL,
                "finish_time"	REAL,
                "problem_name"	TEXT NOT NULL,
                "contest_id"	INTEGER NOT NULL,
                "p_index"	INTEGER NOT NULL,
                "rating"	INTEGER NOT NULL,
                "status"	INTEGER NOT NULL
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS round_settings (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS lockout_ongoing_rounds (
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "guild" TEXT,
                "users" TEXT,
                "rating" TEXT,
                "points" TEXT,
                "time" INT,
                "problems" TEXT,
                "status" TEXT,
                "duration" INTEGER,
                "repeat" INTEGER,
                "times" TEXT
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS lockout_finished_rounds(
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "guild" TEXT,
                "users" TEXT,
                "rating" TEXT,
                "points" TEXT,
                "time" INT,
                "problems" TEXT,
                "status" TEXT,
                "duration" INTEGER,
                "repeat" INTEGER,
                "times" TEXT,
                "end_time" INT
            )
            ''')

        # General key-value store
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS kvs (
                key     TEXT PRIMARY KEY,
                value   TEXT NOT NULL
            )
        ''')

        # Rating-weighted polls
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS rpoll (
                poll_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    TEXT NOT NULL,
                channel_id  TEXT NOT NULL,
                message_id  TEXT,
                question    TEXT NOT NULL,
                created_by  TEXT NOT NULL,
                created_at  REAL NOT NULL,
                anonymous   INTEGER NOT NULL DEFAULT 0,
                expires_at  REAL NOT NULL DEFAULT 0,
                closed      INTEGER NOT NULL DEFAULT 0,
                formula     TEXT NOT NULL DEFAULT 'sum'
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS rpoll_option (
                poll_id       INTEGER NOT NULL,
                option_index  INTEGER NOT NULL,
                label         TEXT NOT NULL,
                PRIMARY KEY (poll_id, option_index),
                FOREIGN KEY (poll_id) REFERENCES rpoll(poll_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS rpoll_vote (
                poll_id       INTEGER NOT NULL,
                user_id       TEXT NOT NULL,
                option_index  INTEGER NOT NULL,
                rating        INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (poll_id, user_id, option_index),
                FOREIGN KEY (poll_id) REFERENCES rpoll(poll_id)
            )
        ''')

        # Starboard migration tables
        self.conn.execute('''
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
                started_at          REAL NOT NULL,
                alias_map           TEXT
            )
        ''')
        self.conn.execute('''
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
                last_error          TEXT,
                PRIMARY KEY (original_msg_id, emoji)
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS cfvc_cache (
                handle       TEXT NOT NULL,
                contest_id   INTEGER NOT NULL,
                rank         INTEGER NOT NULL,
                PRIMARY KEY (handle, contest_id)
            )
        ''')

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

    def new_challenge(self, user_id, issue_time, prob, delta):
        query1 = '''
            INSERT INTO challenge
            (user_id, issue_time, problem_name, contest_id, p_index, rating_delta, status)
            VALUES
            (?, ?, ?, ?, ?, ?, 1)
        '''
        query2 = '''
            INSERT OR IGNORE INTO user_challenge (user_id, score, num_completed, num_skipped)
            VALUES (?, 0, 0, 0)
        '''
        query3 = '''
            UPDATE user_challenge SET active_challenge_id = ?, issue_time = ?
            WHERE user_id = ? AND active_challenge_id IS NULL
        '''
        cur = self.conn.cursor()
        cur.execute(query1, (user_id, issue_time, prob.name, prob.contestId, prob.index, delta))
        last_id, rc = cur.lastrowid, cur.rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        cur.execute(query2, (user_id,))
        cur.execute(query3, (last_id, issue_time, user_id))
        if cur.rowcount != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return 1

    def check_challenge(self, user_id):
        query1 = '''
            SELECT active_challenge_id, issue_time FROM user_challenge
            WHERE user_id = ?
        '''
        res = self.conn.execute(query1, (user_id,)).fetchone()
        if res is None: return None
        c_id, issue_time = res
        query2 = '''
            SELECT problem_name, contest_id, p_index, rating_delta FROM challenge
            WHERE id = ?
        '''
        res = self.conn.execute(query2, (c_id,)).fetchone()
        if res is None: return None
        return c_id, issue_time, res[0], res[1], res[2], res[3]

    def get_gudgitters_last(self, timestamp):
        query = '''
            SELECT user_id, rating_delta FROM challenge WHERE finish_time >= ? ORDER BY user_id
        '''
        return self.conn.execute(query, (timestamp,)).fetchall()

    def get_gudgitters_timerange(self, timestampStart, timestampEnd):
        query = '''
            SELECT user_id, rating_delta, issue_time FROM challenge WHERE finish_time >= ? AND finish_time <= ? ORDER BY user_id
        '''
        return self.conn.execute(query, (timestampStart,timestampEnd)).fetchall()

    def get_gudgitters(self):
        query = '''
            SELECT user_id, score FROM user_challenge
        '''
        return self.conn.execute(query).fetchall()

    def get_gudgitter_score(self, user_id):
        query = '''
            SELECT score FROM user_challenge WHERE user_id = ?
        '''
        row = self.conn.execute(query, (str(user_id),)).fetchone()
        return row[0] if row is not None else 0

    def get_gudgitters_timerange_for_user(self, user_id, timestamp_start, timestamp_end):
        query = '''
            SELECT rating_delta, issue_time
            FROM challenge
            WHERE user_id = ? AND finish_time >= ? AND finish_time <= ?
            ORDER BY issue_time
        '''
        return self.conn.execute(query, (str(user_id), timestamp_start, timestamp_end)).fetchall()

    def howgud(self, user_id):
        query = '''
            SELECT rating_delta FROM challenge WHERE user_id = ? AND finish_time IS NOT NULL
        '''
        return self.conn.execute(query, (user_id,)).fetchall()

    def get_noguds(self, user_id):
        query = ('SELECT problem_name '
                 'FROM challenge '
                 f'WHERE user_id = ? AND status = {Gitgud.NOGUD}')
        return {name for name, in self.conn.execute(query, (user_id,)).fetchall()}

    def gitlog(self, user_id):
        query = f'''
            SELECT issue_time, finish_time, problem_name, contest_id, p_index, rating_delta, status
            FROM challenge WHERE user_id = ? AND status != {Gitgud.FORCED_NOGUD} ORDER BY issue_time DESC
        '''
        return self.conn.execute(query, (user_id,)).fetchall()

    def complete_challenge(self, user_id, challenge_id, finish_time, delta):
        query1 = f'''
            UPDATE challenge SET finish_time = ?, status = {Gitgud.GOTGUD}
            WHERE id = ? AND status = {Gitgud.GITGUD}
        '''
        query2 = '''
            UPDATE user_challenge SET score = score + ?, num_completed = num_completed + 1,
            active_challenge_id = NULL, issue_time = NULL
            WHERE user_id = ? AND active_challenge_id = ?
        '''
        rc = self.conn.execute(query1, (finish_time, challenge_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        rc = self.conn.execute(query2, (delta, user_id, challenge_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return 1

    def skip_challenge(self, user_id, challenge_id, status):
        query1 = '''
            UPDATE user_challenge SET active_challenge_id = NULL, issue_time = NULL
            WHERE user_id = ? AND active_challenge_id = ?
        '''
        query2 = f'''
            UPDATE challenge SET status = ? WHERE id = ? AND status = {Gitgud.GITGUD}
        '''
        rc = self.conn.execute(query1, (user_id, challenge_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        rc = self.conn.execute(query2, (status, challenge_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return 1

    def cache_cf_user(self, user):
        query = ('INSERT OR REPLACE INTO cf_user_cache '
                 '(handle, first_name, last_name, country, city, organization, contribution, '
                 '    rating, maxRating, last_online_time, registration_time, friend_of_count, title_photo) '
                 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
        with self.conn:
            return self.conn.execute(query, user).rowcount

    def fetch_cf_user(self, handle):
        query = ('SELECT handle, first_name, last_name, country, city, organization, contribution, '
                 '    rating, maxRating, last_online_time, registration_time, friend_of_count, title_photo '
                 'FROM cf_user_cache '
                 'WHERE UPPER(handle) = UPPER(?)')
        user = self.conn.execute(query, (handle,)).fetchone()
        return cf_common.fix_urls(cf.User._make(user)) if user else None

    def set_handle(self, user_id, guild_id, handle):
        query = ('SELECT user_id '
                 'FROM user_handle '
                 'WHERE guild_id = ? AND handle = ?')
        existing = self.conn.execute(query, (guild_id, handle)).fetchone()
        if existing and int(existing[0]) != user_id:
            raise UniqueConstraintFailed

        query = ('INSERT OR REPLACE INTO user_handle '
                 '(user_id, guild_id, handle, active) '
                 'VALUES (?, ?, ?, 1)')
        with self.conn:
            return self.conn.execute(query, (user_id, guild_id, handle)).rowcount

    def set_inactive(self, guild_id_user_id_pairs):
        query = ('UPDATE user_handle '
                 'SET active = 0 '
                 'WHERE guild_id = ? AND user_id = ?')
        with self.conn:
            return self.conn.executemany(query, guild_id_user_id_pairs).rowcount

    def get_handle(self, user_id, guild_id):
        query = ('SELECT handle '
                 'FROM user_handle '
                 'WHERE user_id = ? AND guild_id = ?')
        res = self.conn.execute(query, (user_id, guild_id)).fetchone()
        return res[0] if res else None

    def get_user_id(self, handle, guild_id):
        query = ('SELECT user_id '
                 'FROM user_handle '
                 'WHERE UPPER(handle) = UPPER(?) AND guild_id = ?')
        res = self.conn.execute(query, (handle, guild_id)).fetchone()
        return int(res[0]) if res else None

    def remove_handle(self, handle, guild_id):
        query = ('DELETE FROM user_handle '
                 'WHERE UPPER(handle) = UPPER(?) AND guild_id = ?')
        with self.conn:
            return self.conn.execute(query, (handle, guild_id)).rowcount

    def get_handles_for_guild(self, guild_id):
        query = ('SELECT user_id, handle '
                 'FROM user_handle '
                 'WHERE guild_id = ? AND active = 1')
        res = self.conn.execute(query, (guild_id,)).fetchall()
        return [(int(user_id), handle) for user_id, handle in res]

    def get_cf_users_for_guild(self, guild_id):
        query = ('SELECT u.user_id, c.handle, c.first_name, c.last_name, c.country, c.city, '
                 '    c.organization, c.contribution, c.rating, c.maxRating, c.last_online_time, '
                 '    c.registration_time, c.friend_of_count, c.title_photo '
                 'FROM user_handle AS u '
                 'LEFT JOIN cf_user_cache AS c '
                 'ON u.handle = c.handle '
                 'WHERE u.guild_id = ? AND u.active = 1')
        res = self.conn.execute(query, (guild_id,)).fetchall()
        return [(int(t[0]), cf.User._make(t[1:])) for t in res]

    def get_reminder_settings(self, guild_id):
        query = '''
            SELECT channel_id, role_id, before
            FROM reminder
            WHERE guild_id = ?
        '''
        return self.conn.execute(query, (guild_id,)).fetchone()

    def set_reminder_settings(self, guild_id, channel_id, role_id, before):
        query = '''
            INSERT OR REPLACE INTO reminder (guild_id, channel_id, role_id, before)
            VALUES (?, ?, ?, ?)
        '''
        self.conn.execute(query, (guild_id, channel_id, role_id, before))
        self.conn.commit()

    def clear_reminder_settings(self, guild_id):
        query = '''DELETE FROM reminder WHERE guild_id = ?'''
        self.conn.execute(query, (guild_id,))
        self.conn.commit()

    def set_duel_channel(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO duel_settings '
                 ' (guild_id, channel_id) VALUES (?, ?)'
                 )
        with self.conn:
            self.conn.execute(query, (guild_id, channel_id))

    def get_duel_channel(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM duel_settings '
                 'WHERE guild_id = ?')
        channel_id = self.conn.execute(query, (guild_id,)).fetchone()
        return int(channel_id[0]) if channel_id else None

    def check_duel_challenge(self, userid, guild_id):
        query = f'''
            SELECT id FROM duel
            WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND (status == {Duel.ONGOING} OR status == {Duel.PENDING})
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()

    def check_duel_accept(self, challengee, guild_id):
        query = f'''
            SELECT id, challenger, problem_name FROM duel
            WHERE challengee = ? AND guild_id = ? AND status == {Duel.PENDING}
        '''
        return self.conn.execute(query, (challengee,guild_id)).fetchone()

    def check_duel_decline(self, challengee, guild_id):
        query = f'''
            SELECT id, challenger FROM duel
            WHERE challengee = ? AND guild_id = ? AND status == {Duel.PENDING}
        '''
        return self.conn.execute(query, (challengee,guild_id)).fetchone()

    def check_duel_withdraw(self, challenger, guild_id):
        query = f'''
            SELECT id, challengee FROM duel
            WHERE challenger = ? AND guild_id = ? AND status == {Duel.PENDING}
        '''
        return self.conn.execute(query, (challenger,guild_id)).fetchone()

    def check_duel_draw(self, userid, guild_id):
        query = f'''
            SELECT id, challenger, challengee, start_time, type FROM duel
            WHERE (challenger = ? OR challengee = ?) AND guild_id = ? AND status == {Duel.ONGOING}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()

    def check_duel_giveup(self, userid, guild_id):
        query = f'''
            SELECT id, challenger, challengee, start_time, problem_name, contest_id, p_index, type FROM duel
            WHERE (challenger = ? OR challengee = ?) AND guild_id = ? AND status == {Duel.ONGOING}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()


    def check_duel_complete(self, userid, guild_id):
        query = f'''
            SELECT id, challenger, challengee, start_time, problem_name, contest_id, p_index, type FROM duel
            WHERE (challenger = ? OR challengee = ?) AND guild_id = ? AND status == {Duel.ONGOING}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()

    def create_duel(self, challenger, challengee, issue_time, prob, dtype, guild_id):
        query = f'''
            INSERT INTO duel (challenger, challengee, issue_time, problem_name, contest_id, p_index, status, type, guild_id) VALUES (?, ?, ?, ?, ?, ?, {Duel.PENDING}, ?, ?)
        '''
        duelid = self.conn.execute(query, (challenger, challengee, issue_time, prob.name, prob.contestId, prob.index, dtype, guild_id)).lastrowid
        self.conn.commit()
        return duelid

    def cancel_duel(self, duelid, guild_id, status):
        query = f'''
            UPDATE duel SET status = ? WHERE id = ? AND guild_id = ? AND status = {Duel.PENDING}
        '''
        rc = self.conn.execute(query, (status, duelid, guild_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return rc

    def invalidate_duel(self, duelid, guild_id):
        query = f'''
            UPDATE duel SET status = {Duel.INVALID} WHERE id = ? AND guild_id = ? AND status = {Duel.ONGOING}
        '''
        rc = self.conn.execute(query, (duelid,guild_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return rc

    def start_duel(self, duelid, guild_id, start_time):
        query = f'''
            UPDATE duel SET start_time = ?, status = {Duel.ONGOING}
            WHERE id = ? AND guild_id = ? AND status = {Duel.PENDING}
        '''
        rc = self.conn.execute(query, (start_time, duelid, guild_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return rc

    def complete_duel(self, duelid, guild_id, winner, finish_time, winner_id = -1, loser_id = -1, delta = 0, dtype = DuelType.OFFICIAL):
        query = f'''
            UPDATE duel SET status = {Duel.COMPLETE}, finish_time = ?, winner = ? WHERE id = ? AND guild_id = ? AND status = {Duel.ONGOING}
        '''
        rc = self.conn.execute(query, (finish_time, winner, duelid, guild_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0

        if dtype == DuelType.OFFICIAL or dtype == DuelType.ADJOFFICIAL:
            self.update_duel_rating(winner_id, guild_id, +delta)
            self.update_duel_rating(loser_id, guild_id, -delta)

        self.conn.commit()
        return 1

    def update_duel_rating(self, userid, guild_id, delta):
        query = '''
            UPDATE duelist SET rating = rating + ? WHERE user_id = ? AND guild_id = ?
        '''
        rc = self.conn.execute(query, (delta, userid, guild_id)).rowcount
        self.conn.commit()
        return rc

    def get_duel_wins(self, userid, guild_id):
        query = f'''
            SELECT start_time, finish_time, problem_name, challenger, challengee FROM duel
            WHERE ((challenger = ? AND winner == {Winner.CHALLENGER}) OR (challengee = ? AND winner == {Winner.CHALLENGEE})) AND status = {Duel.COMPLETE} AND guild_id = ?
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchall()

    def get_duels(self, userid, guild_id):
        query = f'''
            SELECT id, start_time, finish_time, problem_name, challenger, challengee, winner FROM duel WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND status == {Duel.COMPLETE} ORDER BY start_time DESC
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchall()

    def get_duel_problem_names(self, userid, guild_id):
        query = f'''
            SELECT problem_name FROM duel WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND (status == {Duel.COMPLETE} OR status == {Duel.INVALID})
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchall()

    def get_pair_duels(self, userid1, userid2, guild_id):
        query = f'''
            SELECT id, start_time, finish_time, problem_name, challenger, challengee, winner FROM duel
            WHERE ((challenger = ? AND challengee = ?) OR (challenger = ? AND challengee = ?)) AND guild_id = ? AND status == {Duel.COMPLETE} ORDER BY start_time DESC
        '''
        return self.conn.execute(query, (userid1, userid2, userid2, userid1, guild_id)).fetchall()

    def get_recent_duels(self, guild_id):
        query = f'''
            SELECT id, start_time, finish_time, problem_name, challenger, challengee, winner FROM duel WHERE status == {Duel.COMPLETE} AND guild_id = ? ORDER BY start_time DESC LIMIT 7
        '''
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_ongoing_duels(self, guild_id):
        query = f'''
            SELECT id, challenger, challengee, start_time, problem_name, contest_id, p_index, type FROM duel
            WHERE status == {Duel.ONGOING} AND guild_id = ? ORDER BY start_time DESC
        '''
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_num_duel_completed(self, userid, guild_id):
        query = f'''
            SELECT COUNT(*) FROM duel WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND status == {Duel.COMPLETE}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()[0]

    def get_num_duel_draws(self, userid, guild_id):
        query = f'''
            SELECT COUNT(*) FROM duel WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND winner == {Winner.DRAW}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()[0]

    def get_num_duel_losses(self, userid, guild_id):
        query = f'''
            SELECT COUNT(*) FROM duel
            WHERE ((challengee = ? AND winner == {Winner.CHALLENGER}) OR (challenger = ? AND winner == {Winner.CHALLENGEE})) AND guild_id = ? AND status = {Duel.COMPLETE}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()[0]

    def get_num_duel_declined(self, userid, guild_id):
        query = f'''
            SELECT COUNT(*) FROM duel WHERE challengee = ? AND guild_id = ? AND status == {Duel.DECLINED}
        '''
        return self.conn.execute(query, (userid, guild_id)).fetchone()[0]

    def get_num_duel_rdeclined(self, userid, guild_id):
        query = f'''
            SELECT COUNT(*) FROM duel WHERE challenger = ? AND guild_id = ? AND status == {Duel.DECLINED}
        '''
        return self.conn.execute(query, (userid,guild_id)).fetchone()[0]

    def get_duel_rating(self, userid, guild_id):
        query = '''
            SELECT rating FROM duelist WHERE user_id = ? AND guild_id = ?
        '''
        return self.conn.execute(query, (userid,guild_id)).fetchone()[0]

    def is_duelist(self, userid, guild_id):
        query = '''
            SELECT 1 FROM duelist WHERE user_id = ? AND guild_id = ?
        '''
        return self.conn.execute(query, (userid,guild_id)).fetchone()

    def register_duelist(self, userid, guild_id):
        query = '''
            INSERT OR IGNORE INTO duelist (user_id, rating, guild_id)
            VALUES (?, 1500, ?)
        '''
        with self.conn:
            return self.conn.execute(query, (userid,guild_id)).rowcount

    def get_duelists(self, guild_id):
        query = '''
            SELECT user_id, rating FROM duelist WHERE guild_id = ? ORDER BY rating DESC
        '''
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_complete_official_duels(self, guild_id):
        query = f'''
            SELECT challenger, challengee, winner, finish_time FROM duel WHERE status={Duel.COMPLETE}
            AND (type={DuelType.OFFICIAL} OR type={DuelType.ADJOFFICIAL}) AND guild_id = ? ORDER BY finish_time ASC
        '''
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_rankup_channel(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM rankup '
                 'WHERE guild_id = ?')
        channel_id = self.conn.execute(query, (guild_id,)).fetchone()
        return int(channel_id[0]) if channel_id else None

    def set_rankup_channel(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO rankup '
                 '(guild_id, channel_id) '
                 'VALUES (?, ?)')
        with self.conn:
            self.conn.execute(query, (guild_id, channel_id))

    def clear_rankup_channel(self, guild_id):
        query = ('DELETE FROM rankup '
                 'WHERE guild_id = ?')
        with self.conn:
            return self.conn.execute(query, (guild_id,)).rowcount

    def enable_auto_role_update(self, guild_id):
        query = ('INSERT OR REPLACE INTO auto_role_update '
                 '(guild_id) '
                 'VALUES (?)')
        with self.conn:
            return self.conn.execute(query, (guild_id,)).rowcount

    def disable_auto_role_update(self, guild_id):
        query = ('DELETE FROM auto_role_update '
                 'WHERE guild_id = ?')
        with self.conn:
            return self.conn.execute(query, (guild_id,)).rowcount

    def has_auto_role_update_enabled(self, guild_id):
        query = ('SELECT 1 '
                 'FROM auto_role_update '
                 'WHERE guild_id = ?')
        return self.conn.execute(query, (guild_id,)).fetchone() is not None

    def reset_status(self, id):
        inactive_query = '''
            UPDATE user_handle
            SET active = 0
            WHERE guild_id = ?
        '''
        self.conn.execute(inactive_query, (id,))
        self.conn.commit()

    def update_status(self, guild_id: str, active_ids: list):
        placeholders = ', '.join(['?'] * len(active_ids))
        if not active_ids: return 0
        active_query = '''
            UPDATE user_handle
            SET active = 1
            WHERE user_id IN ({})
            AND guild_id = ?
        '''.format(placeholders)
        rc = self.conn.execute(active_query, (*active_ids, guild_id)).rowcount
        self.conn.commit()
        return rc

    # Rated VC stuff

    def create_rated_vc(self, contest_id: int, start_time: float, finish_time: float, guild_id: str, user_ids: [str]):
        """ Creates a rated vc and returns its id.
        """
        query = ('INSERT INTO rated_vcs '
                 '(contest_id, start_time, finish_time, status, guild_id) '
                 'VALUES ( ?, ?, ?, ?, ?)')
        id = None
        with self.conn:
            id = self.conn.execute(query, (contest_id, start_time, finish_time, RatedVC.ONGOING, guild_id)).lastrowid
            for user_id in user_ids:
                query = ('INSERT INTO rated_vc_users '
                         '(vc_id, user_id) '
                         'VALUES (? , ?)')
                self.conn.execute(query, (id, user_id))
        return id

    def get_rated_vc(self, vc_id: int):
        query = ('SELECT * '
                'FROM rated_vcs '
                'WHERE id = ? ')
        vc = self._fetchone(query, params=(vc_id,), row_factory=namedtuple_factory)
        return vc

    def get_ongoing_rated_vc_ids(self):
        query = ('SELECT id '
                 'FROM rated_vcs '
                 'WHERE status = ? '
                 )
        vcs = self._fetchall(query, params=(RatedVC.ONGOING,), row_factory=namedtuple_factory)
        vc_ids = [vc.id for vc in vcs]
        return vc_ids

    def get_rated_vc_user_ids(self, vc_id: int):
        query = ('SELECT user_id '
                 'FROM rated_vc_users '
                 'WHERE vc_id = ? '
                 )
        users = self._fetchall(query, params=(vc_id,), row_factory=namedtuple_factory)
        user_ids = [user.user_id for user in users]
        return user_ids

    def finish_rated_vc(self, vc_id: int):
        query = ('UPDATE rated_vcs '
                'SET status = ? '
                'WHERE id = ? ')

        with self.conn:
            self.conn.execute(query, (RatedVC.FINISHED, vc_id))

    def update_vc_rating(self, vc_id: int, user_id: str, rating: int):
        query = ('INSERT OR REPLACE INTO rated_vc_users '
                 '(vc_id, user_id, rating) '
                 'VALUES (?, ?, ?) ')

        with self.conn:
            self.conn.execute(query, (vc_id, user_id, rating))

    def get_vc_rating(self, user_id: str, default_if_not_exist: bool = True):
        query = ('SELECT MAX(vc_id) AS latest_vc_id, rating '
                 'FROM rated_vc_users '
                 'WHERE user_id = ? AND rating IS NOT NULL'
                 )
        rating = self._fetchone(query, params=(user_id, ), row_factory=namedtuple_factory).rating
        if rating is None:
            if default_if_not_exist:
                return _DEFAULT_VC_RATING
            return None
        return rating

    def get_vc_rating_history(self, user_id: str):
        """ Return [vc_id, rating].
        """
        query = ('SELECT vc_id, rating '
                 'FROM rated_vc_users '
                 'WHERE user_id = ? AND rating IS NOT NULL'
                 )
        ratings = self._fetchall(query, params=(user_id,), row_factory=namedtuple_factory)
        return ratings

    def set_rated_vc_channel(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO rated_vc_settings '
                 ' (guild_id, channel_id) VALUES (?, ?)'
                 )
        with self.conn:
            self.conn.execute(query, (guild_id, channel_id))

    def get_rated_vc_channel(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM rated_vc_settings '
                 'WHERE guild_id = ?')
        channel_id = self.conn.execute(query, (guild_id,)).fetchone()
        return int(channel_id[0]) if channel_id else None

    def remove_last_ratedvc_participation(self, user_id: str):
        query = ('SELECT MAX(vc_id) AS vc_id '
                 'FROM rated_vc_users '
                 'WHERE user_id = ? '
                 )
        vc_id = self._fetchone(query, params=(user_id, ), row_factory=namedtuple_factory).vc_id
        query = ('DELETE FROM rated_vc_users '
                 'WHERE user_id = ? AND vc_id = ? ')
        with self.conn:
            return self.conn.execute(query, (user_id, vc_id)).rowcount

    def set_training_channel(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO training_settings '
                 ' (guild_id, channel_id) VALUES (?, ?)'
                 )
        with self.conn:
            self.conn.execute(query, (guild_id, channel_id))

    def get_training_channel(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM training_settings '
                 'WHERE guild_id = ?')
        channel_id = self.conn.execute(query, (guild_id,)).fetchone()
        return int(channel_id[0]) if channel_id else None

    def new_training(self, user_id, issue_time, prob, mode, score, lives, time_left):
        query1 = f'''
            INSERT INTO trainings
            (user_id, score, lives, time_left, mode, status)
            VALUES
            (?, 0, ?, ?, ?, {Training.ACTIVE})
        '''
        query2 = f'''
            INSERT INTO training_problems (training_id, issue_time, problem_name, contest_id, p_index, rating, status)
            VALUES (?, ?, ?, ?, ?, ?, {TrainingProblemStatus.ACTIVE})
        '''
        cur = self.conn.cursor()
        cur.execute(query1, (user_id, lives, time_left, mode))
        training_id, rc = cur.lastrowid, cur.rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        cur.execute(query2, (training_id, issue_time, prob.name, prob.contestId, prob.index, prob.rating))
        if cur.rowcount != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return 1


    def get_active_training(self, user_id):
        query1 = f'''
            SELECT id, mode, score, lives, time_left FROM trainings
            WHERE user_id = ? AND status = {Training.ACTIVE}
        '''
        res = self.conn.execute(query1, (user_id,)).fetchone()
        if res is None: return None
        training_id,mode,score,lives,time_left = res
        query2 = f'''
            SELECT issue_time, problem_name, contest_id, p_index, rating FROM training_problems
            WHERE training_id = ? AND status = {TrainingProblemStatus.ACTIVE}
        '''
        res = self.conn.execute(query2, (training_id,)).fetchone()
        if res is None: return None
        return training_id, res[0], res[1], res[2], res[3], res[4], mode, score, lives,time_left

    def get_latest_training(self, user_id):
        query1 = f'''
            SELECT id, mode, score, lives, time_left FROM trainings
            WHERE user_id = ? AND status = {Training.COMPLETED} ORDER BY id DESC
        '''
        res = self.conn.execute(query1, (user_id,)).fetchone()
        if res is None: return None
        training_id,mode,score,lives,time_left = res
        return training_id, None, None, None, None, None, mode, score, lives,time_left


    def end_current_training_problem(self, training_id, finish_time, status, score, lives, time_left):
        query1 = f'''
            UPDATE training_problems SET finish_time = ?, status = ?
            WHERE training_id = ? AND status = {TrainingProblemStatus.ACTIVE}
        '''
        query2 = '''
            UPDATE trainings SET score = ?, lives = ?, time_left = ?
            WHERE id = ?
        '''
        rc = self.conn.execute(query1, (finish_time, status, training_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return -1
        rc = self.conn.execute(query2, (score, lives, time_left, training_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return -2
        self.conn.commit()
        return 1

    def assign_training_problem(self, training_id, issue_time, prob):
        query1 = f'''
            INSERT INTO training_problems (training_id, issue_time, problem_name, contest_id, p_index, rating, status)
            VALUES (?, ?, ?, ?, ?, ?, {TrainingProblemStatus.ACTIVE})
        '''

        cur = self.conn.cursor()
        cur.execute(query1, (training_id, issue_time, prob.name, prob.contestId, prob.index, prob.rating))
        if cur.rowcount != 1:
            self.conn.rollback()
            return -1
        self.conn.commit()
        return 1

    def finish_training(self, training_id):
        query1 = f'''
            UPDATE trainings SET status = {Training.COMPLETED}
            WHERE id = ?
        '''
        rc = self.conn.execute(query1, (training_id,)).rowcount
        if rc != 1:
            self.conn.rollback()
            return -1
        self.conn.commit()
        return 1

    def get_training_skips(self, user_id):
        query = f'''
            SELECT tp.problem_name
            FROM training_problems tp, trainings tr
            WHERE tp.training_id = tr.id
            AND (tp.status = {TrainingProblemStatus.SKIPPED} OR tp.status = {TrainingProblemStatus.INVALIDATED})
            AND tr.user_id = ?
        '''
        return {name for name, in self.conn.execute(query, (user_id,)).fetchall()}


    def train_get_num_solves(self, training_id):
        query = f'''
            SELECT COUNT(*) FROM training_problems
            WHERE training_id = ? AND status == {TrainingProblemStatus.SOLVED}
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_num_skips(self, training_id):
        query = f'''
            SELECT COUNT(*) FROM training_problems
            WHERE training_id = ? AND status == {TrainingProblemStatus.SKIPPED}
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_num_slow_solves(self, training_id):
        query = f'''
            SELECT COUNT(*) FROM training_problems
            WHERE training_id = ? AND status == {TrainingProblemStatus.SOLVED_TOO_SLOW}
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_start_rating(self, training_id):
        query = f'''
            SELECT rating FROM training_problems
            WHERE training_id = ?
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_max_rating(self, training_id):
        query = f'''
            SELECT MAX(rating) FROM training_problems
            WHERE training_id = ? AND status == {TrainingProblemStatus.SOLVED}
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_fastest_solves(self):
        query = f'''
            SELECT tr.user_id, tp.rating, min(tp.finish_time-tp.issue_time)
            FROM training_problems tp, trainings tr
            WHERE tp.training_id = tr.id
            AND (tp.status = {TrainingProblemStatus.SOLVED} OR tp.status = {TrainingProblemStatus.SOLVED_TOO_SLOW})
            GROUP BY tp.rating
        '''
        return self.conn.execute(query).fetchall()

    ### Lockout round


    def set_round_channel(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO round_settings '
                 ' (guild_id, channel_id) VALUES (?, ?)'
                 )
        with self.conn:
            self.conn.execute(query, (guild_id, channel_id))

    def get_round_channel(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM round_settings '
                 'WHERE guild_id = ?')
        channel_id = self.conn.execute(query, (guild_id,)).fetchone()
        return int(channel_id[0]) if channel_id else None

    def create_ongoing_round(self, guild_id, timestamp, users, rating, points, problems, duration, repeat):
        query = f'''
            INSERT INTO lockout_ongoing_rounds (guild, users, rating, points, time, problems, status, duration, repeat, times)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cur = self.conn.cursor()
        cur.execute(query, (guild_id, ' '.join([f"{x.id}" for x in users]), 
                                      ' '.join(map(str, rating)),
                                      ' '.join(map(str, points)), 
                                      timestamp, 
                                      ' '.join([f"{x.contestId}/{x.index}" for x in problems]), 
                                      ' '.join('0' for i in range(len(users))),
                                      duration, 
                                      repeat, 
                                      ' '.join(['0'] * len(users)))
                    )
        self.conn.commit()
        cur.close()

    def create_finished_round(self, round_info, timestamp):
        query = f'''
                    INSERT INTO lockout_finished_rounds (guild, users, rating, points, time, problems, status, duration, repeat, times, end_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
        cur = self.conn.cursor()
        cur.execute(query, (round_info.guild, round_info.users, round_info.rating, round_info.points, round_info.time,
                                round_info.problems, round_info.status, round_info.duration, round_info.repeat,
                                round_info.times, timestamp))
        self.conn.commit()
        cur.close()                

    def update_round_status(self, guild, user, status, problems, timestamp):
        query = f"""
                    UPDATE lockout_ongoing_rounds 
                    SET
                    status = ?, 
                    problems = ?,
                    times = ?
                    WHERE
                    guild = ? AND users LIKE ? 
                """
        cur = self.conn.cursor()
        cur.execute(query,
                     (' '.join([str(x) for x in status]), ' '.join(problems), ' '.join([str(x) for x in timestamp]),
                      guild, f"%{user}%"))
        self.conn.commit()
        cur.close()

    def get_round_info(self, guild_id, users):
        query = f'''
                    SELECT * FROM lockout_ongoing_rounds
                    WHERE
                    guild = ? AND users LIKE ?
                 '''
        cur = self.conn.cursor()
        cur.execute(query, (guild_id, f"%{users}%"))
        data = cur.fetchone()
        cur.close()
        Round = namedtuple('Round', 'guild users rating points time problems status duration repeat times')
        return Round(data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10])

    def check_if_user_in_ongoing_round(self, guild, user):
        query = f'''
                    SELECT * FROM lockout_ongoing_rounds
                    WHERE
                    users LIKE ? AND guild = ?
                '''
        cur = self.conn.cursor()
        cur.execute(query, (f"%{user}%", guild))
        data = cur.fetchall()
        cur.close()
        if len(data) > 0:
            return True
        return False

    def delete_round(self, guild, user):
        query = f'''
                    DELETE FROM lockout_ongoing_rounds
                    WHERE
                    guild = ? AND users LIKE ?
                '''
        cur = self.conn.cursor()
        cur.execute(query, (guild, f"%{user}%"))
        self.conn.commit()
        cur.close()    

    def get_ongoing_rounds(self, guild):
        query = f'''
                    SELECT * FROM lockout_ongoing_rounds WHERE guild = ?
                '''
        cur = self.conn.cursor()
        cur.execute(query, (guild,))
        res = cur.fetchall()
        cur.close()
        Round = namedtuple('Round', 'guild users rating points time problems status duration repeat times')
        return [Round(data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10]) for data in res]

    def get_recent_rounds(self, guild, user=None):
        query = f'''
                    SELECT * FROM lockout_finished_rounds 
                    WHERE guild = ? AND users LIKE ?
                    ORDER BY end_time DESC
                '''
        cur = self.conn.cursor()
        cur.execute(query, (guild, '%' if user is None else f'%{user}%'))
        res = cur.fetchall()
        cur.close()
        Round = namedtuple('Round', 'guild users rating points time problems status duration repeat times end_time')
        return [Round(data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11]) for data in res]

    # --- Rating-weighted polls ---

    def create_rpoll(self, guild_id, channel_id, question, options, created_by, created_at,
                     anonymous=False, expires_at=None, formula='sum'):
        """Create a poll and its options. Returns the poll_id."""
        if expires_at is None:
            expires_at = created_at + 86400  # Default 24h
        query = ('INSERT INTO rpoll (guild_id, channel_id, question, created_by, created_at, anonymous, expires_at, formula) '
                 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
        cur = self.conn.execute(query, (str(guild_id), str(channel_id), question,
                                        str(created_by), created_at, int(anonymous), expires_at, formula))
        poll_id = cur.lastrowid
        for i, label in enumerate(options):
            self.conn.execute(
                'INSERT INTO rpoll_option (poll_id, option_index, label) VALUES (?, ?, ?)',
                (poll_id, i, label)
            )
        self.conn.commit()
        return poll_id

    def set_rpoll_message_id(self, poll_id, message_id):
        """Set the Discord message_id after the poll message is sent."""
        with self.conn:
            self.conn.execute(
                'UPDATE rpoll SET message_id = ? WHERE poll_id = ?',
                (str(message_id), poll_id)
            )

    def get_rpoll(self, poll_id):
        """Get a poll by ID. Returns namedtuple or None."""
        return self._fetchone(
            'SELECT poll_id, guild_id, channel_id, message_id, question, created_by, created_at, '
            'anonymous, expires_at, closed, formula '
            'FROM rpoll WHERE poll_id = ?',
            params=(poll_id,), row_factory=namedtuple_factory
        )

    def get_rpoll_by_message_id(self, message_id):
        """Get a poll by its Discord message_id."""
        return self._fetchone(
            'SELECT poll_id, guild_id, channel_id, message_id, question, created_by, created_at, '
            'anonymous, expires_at, closed, formula '
            'FROM rpoll WHERE message_id = ?',
            params=(str(message_id),), row_factory=namedtuple_factory
        )

    def get_active_rpolls_in_channel(self, channel_id, now):
        """Active (open, not yet expired) polls created in this channel."""
        return self._fetchall(
            'SELECT poll_id, guild_id, channel_id, message_id, question, created_by, created_at, '
            'anonymous, expires_at, closed, formula '
            'FROM rpoll WHERE channel_id = ? AND closed = 0 AND expires_at > ? '
            'ORDER BY expires_at ASC',
            params=(str(channel_id), now), row_factory=namedtuple_factory
        )

    def get_rpoll_options(self, poll_id):
        """Get all options for a poll, ordered by index."""
        return self._fetchall(
            'SELECT poll_id, option_index, label FROM rpoll_option '
            'WHERE poll_id = ? ORDER BY option_index',
            params=(poll_id,), row_factory=namedtuple_factory
        )

    def toggle_rpoll_vote(self, poll_id, user_id, option_index, rating):
        """Toggle a vote. Returns True if vote was added, False if removed."""
        user_id = str(user_id)
        existing = self.conn.execute(
            'SELECT 1 FROM rpoll_vote WHERE poll_id = ? AND user_id = ? AND option_index = ?',
            (poll_id, user_id, option_index)
        ).fetchone()
        if existing:
            self.conn.execute(
                'DELETE FROM rpoll_vote WHERE poll_id = ? AND user_id = ? AND option_index = ?',
                (poll_id, user_id, option_index)
            )
            self.conn.commit()
            return False
        else:
            self.conn.execute(
                'INSERT INTO rpoll_vote (poll_id, user_id, option_index, rating) VALUES (?, ?, ?, ?)',
                (poll_id, user_id, option_index, rating)
            )
            self.conn.commit()
            return True

    def get_rpoll_totals(self, poll_id):
        """Get the sum of ratings per option. Returns list of (option_index, total_rating)."""
        return self._fetchall(
            'SELECT option_index, COALESCE(SUM(rating), 0) AS total_rating '
            'FROM rpoll_vote WHERE poll_id = ? GROUP BY option_index',
            params=(poll_id,), row_factory=namedtuple_factory
        )

    def get_rpoll_voters(self, poll_id):
        """Get all voters grouped by option. Returns list of (option_index, user_id) rows."""
        return self._fetchall(
            'SELECT option_index, user_id FROM rpoll_vote WHERE poll_id = ? '
            'ORDER BY option_index',
            params=(poll_id,), row_factory=namedtuple_factory
        )

    def get_rpoll_vote_count(self, poll_id):
        """Get total number of distinct voters for a poll."""
        row = self._fetchone(
            'SELECT COUNT(DISTINCT user_id) AS cnt FROM rpoll_vote WHERE poll_id = ?',
            params=(poll_id,)
        )
        return row[0] if row else 0

    def get_rpoll_user_rating(self, user_id, guild_id):
        """Get a user's Codeforces rating for poll voting. Returns 0 if not linked."""
        handle = self.get_handle(user_id, guild_id)
        if handle is None:
            return 0
        user = self.fetch_cf_user(handle)
        if user is None or user.rating is None:
            return 0
        return user.rating

    def get_all_active_rpolls(self):
        """Get all open polls that have a message_id (i.e., were successfully posted)."""
        return self._fetchall(
            'SELECT poll_id, guild_id, channel_id, message_id, question, created_by, created_at, '
            'anonymous, expires_at, closed, formula '
            'FROM rpoll WHERE message_id IS NOT NULL AND closed = 0',
            row_factory=namedtuple_factory
        )

    def close_rpoll(self, poll_id):
        """Mark a poll as closed."""
        with self.conn:
            self.conn.execute('UPDATE rpoll SET closed = 1 WHERE poll_id = ?', (poll_id,))

    def get_expired_unclosed_rpolls(self):
        """Get polls that have expired but haven't been closed yet."""
        import time
        return self._fetchall(
            'SELECT poll_id, guild_id, channel_id, message_id, question, created_by, created_at, '
            'anonymous, expires_at, closed, formula '
            'FROM rpoll WHERE closed = 0 AND expires_at <= ? AND message_id IS NOT NULL',
            params=(time.time(),), row_factory=namedtuple_factory
        )

    def get_rpoll_vote_ratings(self, poll_id):
        """Get individual vote ratings for formula-based scoring."""
        return self._fetchall(
            'SELECT option_index, rating FROM rpoll_vote WHERE poll_id = ?',
            params=(poll_id,), row_factory=namedtuple_factory
        )

    def get_rpoll_voter_ids(self, poll_id):
        """Get distinct user_ids who have voted on a poll."""
        return self._fetchall(
            'SELECT DISTINCT user_id FROM rpoll_vote WHERE poll_id = ?',
            params=(poll_id,), row_factory=namedtuple_factory
        )

    def update_rpoll_voter_rating(self, poll_id, user_id, rating):
        """Update the stored rating for all of a user's votes in a poll."""
        with self.conn:
            self.conn.execute(
                'UPDATE rpoll_vote SET rating = ? WHERE poll_id = ? AND user_id = ?',
                (rating, poll_id, str(user_id))
            )

    # ── Complaints ──────────────────────────────────────────────────────

    def add_complaint(self, guild_id, user_id, text, message_link=None):
        """Insert a complaint and return its id."""
        import time
        guild_id, user_id = str(guild_id), str(user_id)
        cur = self.conn.execute(
            'INSERT INTO complaint (guild_id, user_id, text, created_at, message_link) '
            'VALUES (?, ?, ?, ?, ?)',
            (guild_id, user_id, text, time.time(), message_link)
        )
        self.conn.commit()
        return cur.lastrowid

    def get_complaints(self, guild_id):
        """Return all active complaints for a guild, newest first."""
        guild_id = str(guild_id)
        return self.conn.execute(
            'SELECT id, guild_id, user_id, text, created_at, message_link '
            'FROM complaint WHERE guild_id = ? AND active = 1 ORDER BY created_at DESC',
            (guild_id,)
        ).fetchall()

    def get_complaint(self, complaint_id):
        """Return a single active complaint by id, or None."""
        row = self.conn.execute(
            'SELECT id, guild_id, user_id, text, created_at, message_link '
            'FROM complaint WHERE id = ? AND active = 1',
            (complaint_id,)
        ).fetchone()
        return row

    def delete_complaint(self, complaint_id):
        """Soft-delete a complaint by id. Returns True if a row was deactivated."""
        cur = self.conn.execute(
            'UPDATE complaint SET active = 0 WHERE id = ? AND active = 1',
            (complaint_id,)
        )
        self.conn.commit()
        return cur.rowcount > 0

    def delete_complaints(self, complaint_ids, guild_id):
        """Soft-delete multiple complaints by id, scoped to a guild.

        Returns the number of rows deactivated.
        """
        if not complaint_ids:
            return 0
        guild_id = str(guild_id)
        placeholders = ','.join('?' for _ in complaint_ids)
        cur = self.conn.execute(
            f'UPDATE complaint SET active = 0 '
            f'WHERE id IN ({placeholders}) AND guild_id = ? AND active = 1',
            [*complaint_ids, guild_id]
        )
        self.conn.commit()
        return cur.rowcount

    def count_recent_complaints(self, guild_id, user_id, since):
        """Count complaints by a user in a guild filed since a timestamp.

        Includes soft-deleted (withdrawn/removed) complaints so that the
        rate limit cannot be bypassed by withdrawing and immediately
        refiling. The rate limit caps *filings*, not active complaints.
        """
        guild_id, user_id = str(guild_id), str(user_id)
        row = self.conn.execute(
            'SELECT COUNT(*) AS cnt FROM complaint '
            'WHERE guild_id = ? AND user_id = ? AND created_at >= ?',
            (guild_id, user_id, since)
        ).fetchone()
        return row.cnt

    # ── General key-value store ──────────────────────────────────────────

    # ── Great Day ───────────────────────────────────────────────────────

    def greatday_signup(self, guild_id, user_id):
        """Add a user to the great day list. Returns True if newly added."""
        rc = self.conn.execute(
            'INSERT OR IGNORE INTO greatday_signup (guild_id, user_id) VALUES (?, ?)',
            (str(guild_id), str(user_id))).rowcount
        self.conn.commit()
        return rc > 0

    def greatday_remove(self, guild_id, user_id):
        """Remove a user from the great day list. Returns True if removed."""
        rc = self.conn.execute(
            'DELETE FROM greatday_signup WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))).rowcount
        self.conn.commit()
        return rc > 0

    def greatday_get_signups(self, guild_id):
        """Return all signed-up user IDs for a guild."""
        return self.conn.execute(
            'SELECT user_id FROM greatday_signup WHERE guild_id = ?',
            (str(guild_id),)).fetchall()

    def greatday_ban(self, guild_id, user_id):
        """Ban a user from great day. Also removes their signup. Returns True if newly banned."""
        rc = self.conn.execute(
            'INSERT OR IGNORE INTO greatday_ban (guild_id, user_id) VALUES (?, ?)',
            (str(guild_id), str(user_id))).rowcount
        self.conn.execute(
            'DELETE FROM greatday_signup WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id)))
        self.conn.commit()
        return rc > 0

    def greatday_unban(self, guild_id, user_id):
        """Unban a user from great day. Returns True if was banned."""
        rc = self.conn.execute(
            'DELETE FROM greatday_ban WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))).rowcount
        self.conn.commit()
        return rc > 0

    def greatday_record_picks(self, guild_id, user_ids, message_id, picked_at):
        """Insert one row per picked user. Idempotent on (guild, user, message)."""
        if not user_ids:
            return 0
        guild_id = str(guild_id)
        message_id = str(message_id)
        cur = self.conn.executemany(
            'INSERT OR IGNORE INTO greatday_pick '
            '(guild_id, user_id, message_id, picked_at) VALUES (?, ?, ?, ?)',
            [(guild_id, str(uid), message_id, picked_at) for uid in user_ids]
        )
        self.conn.commit()
        return cur.rowcount

    def greatday_get_stats(self, guild_id):
        """Return [(user_id, count)] for all users picked in the guild, most-first."""
        return self.conn.execute(
            'SELECT user_id, COUNT(*) AS cnt FROM greatday_pick '
            'WHERE guild_id = ? GROUP BY user_id ORDER BY cnt DESC, user_id ASC',
            (str(guild_id),)
        ).fetchall()

    def greatday_get_count(self, guild_id, user_id):
        """Return how many times a user has been picked in the guild."""
        row = self.conn.execute(
            'SELECT COUNT(*) AS cnt FROM greatday_pick '
            'WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))
        ).fetchone()
        return row.cnt

    def greatday_is_banned(self, guild_id, user_id):
        """Check if a user is banned from great day."""
        row = self.conn.execute(
            'SELECT 1 FROM greatday_ban WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))).fetchone()
        return row is not None

    def greatday_get_banned(self, guild_id):
        """Return all banned user_ids for the guild."""
        return self.conn.execute(
            'SELECT user_id FROM greatday_ban WHERE guild_id = ? '
            'ORDER BY rowid ASC',
            (str(guild_id),)
        ).fetchall()

    def kvs_set(self, key, value):
        """Set a key-value pair. Overwrites if key exists."""
        with self.conn:
            self.conn.execute(
                'INSERT OR REPLACE INTO kvs (key, value) VALUES (?, ?)',
                (key, value)
            )

    def kvs_get(self, key):
        """Get a value by key, or None if not found."""
        row = self.conn.execute(
            'SELECT value FROM kvs WHERE key = ?', (key,)
        ).fetchone()
        return row[0] if row else None

    def kvs_delete(self, key):
        """Delete a key-value pair."""
        with self.conn:
            self.conn.execute('DELETE FROM kvs WHERE key = ?', (key,))

    # CFVC cache methods

    def get_cfvc_cache(self, handle):
        """Return cached CF virtual contest ranks for a handle.
        Returns list of (contest_id, rank) sorted by contest_id.
        """
        query = ('SELECT contest_id, rank FROM cfvc_cache '
                 'WHERE handle = ? ORDER BY contest_id')
        return self.conn.execute(query, (handle.lower(),)).fetchall()

    def save_cfvc_cache(self, handle, entries):
        """Save CF virtual contest ranks. entries: list of (contest_id, rank)."""
        with self.conn:
            for contest_id, rank in entries:
                self.conn.execute(
                    'INSERT OR REPLACE INTO cfvc_cache (handle, contest_id, rank) '
                    'VALUES (?, ?, ?)',
                    (handle.lower(), contest_id, rank))

    def get_cfvc_cached_contest_ids(self, handle):
        """Return set of contest_ids already cached for a handle."""
        query = 'SELECT contest_id FROM cfvc_cache WHERE handle = ?'
        rows = self.conn.execute(query, (handle.lower(),)).fetchall()
        return {row[0] for row in rows}

    # ── Soccer betting minigame ─────────────────────────────────────────

    # -- Wallet --

    def _bet_log_wallet_txn(self, guild_id, user_id, actor_id, action, amount,
                            balance_after, market_id=None, note=None,
                            created_at=None):
        self.conn.execute(
            'INSERT INTO bet_wallet_txn '
            '(guild_id, user_id, actor_id, action, amount, balance_after, '
            'market_id, note, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (str(guild_id), str(user_id), None if actor_id is None else str(actor_id),
             action, int(amount), int(balance_after), market_id, note,
             time.time() if created_at is None else created_at)
        )

    def _bet_ensure_wallet_txn(self, guild_id, user_id, start_balance,
                               created_at=None):
        """Create a betting wallet inside the caller's transaction."""
        guild_id, user_id = str(guild_id), str(user_id)
        cur = self.conn.execute(
            'INSERT OR IGNORE INTO bet_wallet (guild_id, user_id, balance) '
            'VALUES (?, ?, ?)',
            (guild_id, user_id, start_balance)
        )
        if cur.rowcount:
            self._bet_log_wallet_txn(
                guild_id, user_id, None, 'init', start_balance, start_balance,
                created_at=created_at)
        row = self.conn.execute(
            'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
            (guild_id, user_id)
        ).fetchone()
        return row.balance

    def bet_ensure_wallet(self, guild_id, user_id, start_balance):
        """Create a wallet seeded at start_balance if absent; return the
        current balance either way."""
        with self.conn:
            return self._bet_ensure_wallet_txn(
                guild_id, user_id, start_balance)

    def bet_get_balance(self, guild_id, user_id):
        """Return a user's balance, or None if they have no wallet yet."""
        row = self.conn.execute(
            'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))
        ).fetchone()
        return row.balance if row else None

    def bet_wallet_history(self, guild_id, user_id, limit=15):
        """Return recent wallet audit entries for a user, newest first."""
        limit = max(1, min(int(limit), 50))
        return self.conn.execute(
            'SELECT txn_id, guild_id, user_id, actor_id, action, amount, '
            'balance_after, market_id, note, created_at '
            'FROM bet_wallet_txn WHERE guild_id = ? AND user_id = ? '
            'ORDER BY created_at DESC, txn_id DESC LIMIT ?',
            (str(guild_id), str(user_id), limit)
        ).fetchall()

    def bet_claim_daily(self, guild_id, user_id, today, amount, start_balance):
        """Grant a flat `amount` once per UTC day.

        Returns (granted, new_balance, reason) where reason is 'ok' or
        'already' (already claimed today).
        """
        guild_id, user_id = str(guild_id), str(user_id)
        self.bet_ensure_wallet(guild_id, user_id, start_balance)
        row = self.conn.execute(
            'SELECT balance, last_daily FROM bet_wallet '
            'WHERE guild_id = ? AND user_id = ?',
            (guild_id, user_id)
        ).fetchone()
        if row.last_daily == today:
            return (False, row.balance, 'already')
        new_balance = row.balance + amount
        self.conn.execute(
            'UPDATE bet_wallet SET balance = ?, last_daily = ? '
            'WHERE guild_id = ? AND user_id = ?',
            (new_balance, today, guild_id, user_id)
        )
        self._bet_log_wallet_txn(
            guild_id, user_id, user_id, 'daily', amount, new_balance,
            note=today)
        self.conn.commit()
        return (True, new_balance, 'ok')

    def bet_transfer(self, guild_id, sender_id, receiver_id, amount,
                     start_balance, transferred_at=None, actor_id=None):
        """Move coins from one user wallet to another.

        Returns (ok, reason, sender_balance, receiver_balance). On
        insufficient funds, the receiver wallet is not created or modified.
        """
        guild_id = str(guild_id)
        sender_id = str(sender_id)
        receiver_id = str(receiver_id)
        actor_id = sender_id if actor_id is None else str(actor_id)
        amount = int(amount)
        if sender_id == receiver_id:
            bal = self.bet_get_balance(guild_id, sender_id)
            return (False, 'self', bal, bal)
        if amount <= 0:
            bal = self.bet_get_balance(guild_id, sender_id)
            return (False, 'invalid', bal, self.bet_get_balance(guild_id, receiver_id))

        when = time.time() if transferred_at is None else transferred_at
        with self.conn:
            sender_balance = self._bet_ensure_wallet_txn(
                guild_id, sender_id, start_balance, created_at=when)
            if amount > sender_balance:
                return (False, 'insufficient', sender_balance,
                        self.bet_get_balance(guild_id, receiver_id))
            receiver_balance = self._bet_ensure_wallet_txn(
                guild_id, receiver_id, start_balance, created_at=when)
            new_sender = sender_balance - amount
            new_receiver = receiver_balance + amount
            self.conn.execute(
                'UPDATE bet_wallet SET balance = ? '
                'WHERE guild_id = ? AND user_id = ?',
                (new_sender, guild_id, sender_id)
            )
            self.conn.execute(
                'UPDATE bet_wallet SET balance = ? '
                'WHERE guild_id = ? AND user_id = ?',
                (new_receiver, guild_id, receiver_id)
            )
            self._bet_log_wallet_txn(
                guild_id, sender_id, actor_id, 'transfer_out', -amount,
                new_sender, note=receiver_id, created_at=when)
            self._bet_log_wallet_txn(
                guild_id, receiver_id, actor_id, 'transfer_in', amount,
                new_receiver, note=sender_id, created_at=when)
            return (True, 'ok', new_sender, new_receiver)

    def bet_balance_leaderboard(self, guild_id):
        """Return [(user_id, balance)] for the guild, richest first."""
        return self.conn.execute(
            'SELECT user_id, balance FROM bet_wallet WHERE guild_id = ? '
            'ORDER BY balance DESC, user_id ASC',
            (str(guild_id),)
        ).fetchall()

    def bet_profit_leaderboard(self, guild_id):
        """Return realized profit per user over settled markets.

        Payout is computed, not stored: for a winning wager it is
        round(stake × the market's frozen odds for that pick), else 0. Each
        row: user_id, profit (sum payout − sum stake), bets, wins. Voided/open
        markets are excluded by the status filter.
        """
        rows = self.conn.execute(
            'SELECT w.user_id, w.pick, w.stake, m.result, '
            '       m.odds_home, m.odds_draw, m.odds_away '
            'FROM bet_wager w JOIN bet_market m ON m.market_id = w.market_id '
            "WHERE m.guild_id = ? AND m.status = 'settled'",
            (str(guild_id),)
        ).fetchall()
        totals = defaultdict(lambda: {'profit': 0, 'bets': 0, 'wins': 0})
        for row in rows:
            odds_map = {'home': row.odds_home, 'draw': row.odds_draw,
                        'away': row.odds_away}
            odds = _bet_odds_for_pick(odds_map, row.pick)
            won = _bet_pick_wins(row.pick, row.result)
            payout = int(round(row.stake * odds)) if won and odds is not None else 0
            acc = totals[row.user_id]
            acc['profit'] += payout - row.stake
            acc['bets'] += 1
            acc['wins'] += 1 if won else 0
        Row = namedtuple('BetProfitRow', 'user_id profit bets wins')
        out = [Row(user_id, values['profit'], values['bets'], values['wins'])
               for user_id, values in totals.items()]
        out.sort(key=lambda r: (-r.profit, -r.wins, r.user_id))
        return out

    # -- Markets --

    def bet_market_create(self, guild_id, channel_id, event_id, sport_key,
                          home_team, away_team, commence_time,
                          odds_home, odds_draw, odds_away, created_by, created_at):
        """Open a betting market and return its id, or None if already open."""
        guild_id, channel_id, event_id = str(guild_id), str(channel_id), str(event_id)
        fixture_key = bet_fixture_key(
            sport_key, home_team, away_team, commence_time)
        try:
            cur = self.conn.execute(
                'INSERT INTO bet_market '
                '(guild_id, channel_id, event_id, fixture_key, sport_key, '
                'home_team, away_team, commence_time, odds_home, odds_draw, '
                'odds_away, created_by, created_at, status) '
                'SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \'open\' '
                'WHERE NOT EXISTS ('
                '    SELECT 1 FROM bet_market '
                '    WHERE guild_id = ? AND event_id = ? AND status = \'open\''
                ') AND NOT EXISTS ('
                '    SELECT 1 FROM bet_market '
                '    WHERE guild_id = ? AND fixture_key = ? AND status = \'open\''
                ')',
                (guild_id, channel_id, event_id, fixture_key, sport_key,
                 home_team, away_team,
                 commence_time, odds_home, odds_draw, odds_away, str(created_by),
                 created_at, guild_id, event_id, guild_id, fixture_key)
            )
        except sqlite3.IntegrityError as e:
            self.conn.rollback()
            if 'UNIQUE constraint failed' in str(e):
                return None
            raise
        self.conn.commit()
        return cur.lastrowid if cur.rowcount else None

    def bet_market_set_message(self, market_id, message_id):
        """Record the market announcement message id."""
        self.conn.execute(
            'UPDATE bet_market SET message_id = ? WHERE market_id = ?',
            (str(message_id), market_id)
        )
        self.conn.commit()

    def bet_market_set_thread(self, market_id, thread_id):
        """Record the betting thread id (where wagers are posted)."""
        self.conn.execute(
            'UPDATE bet_market SET thread_id = ? WHERE market_id = ?',
            (str(thread_id), market_id)
        )
        self.conn.commit()

    def bet_market_get_active_by_thread(self, guild_id, thread_id):
        """Return the open market whose betting thread is thread_id, or None."""
        return self.conn.execute(
            "SELECT * FROM bet_market "
            "WHERE guild_id = ? AND thread_id = ? AND status = 'open' LIMIT 1",
            (str(guild_id), str(thread_id))
        ).fetchone()

    def bet_market_get(self, market_id):
        """Return a market by id, or None."""
        return self.conn.execute(
            'SELECT * FROM bet_market WHERE market_id = ?', (market_id,)
        ).fetchone()

    def bet_market_get_active(self, guild_id, channel_id):
        """Return the open market in a channel, or None (newest wins)."""
        return self.conn.execute(
            "SELECT * FROM bet_market "
            "WHERE guild_id = ? AND channel_id = ? AND status = 'open' "
            'ORDER BY created_at DESC, market_id DESC LIMIT 1',
            (str(guild_id), str(channel_id))
        ).fetchone()

    def bet_market_exists_open_for_event(self, guild_id, event_id):
        """True if the guild already has an open market on this event."""
        row = self.conn.execute(
            "SELECT 1 FROM bet_market WHERE guild_id = ? AND event_id = ? "
            "AND status = 'open' LIMIT 1",
            (str(guild_id), str(event_id))
        ).fetchone()
        return row is not None

    def bet_market_get_open_for_event(self, guild_id, event_id):
        """Return the guild's open market on this event, or None.

        Used by the auto-open watcher to decide whether to create a market or
        (if one exists without a thread) just attach a thread.
        """
        return self.conn.execute(
            "SELECT * FROM bet_market WHERE guild_id = ? AND event_id = ? "
            "AND status = 'open' ORDER BY market_id DESC LIMIT 1",
            (str(guild_id), str(event_id))
        ).fetchone()

    def bet_market_get_open_for_fixture(self, guild_id, fixture_key):
        """Return the guild's open market for a canonical fixture key, or None."""
        return self.conn.execute(
            "SELECT * FROM bet_market WHERE guild_id = ? AND fixture_key = ? "
            "AND status = 'open' ORDER BY market_id DESC LIMIT 1",
            (str(guild_id), fixture_key)
        ).fetchone()

    def bet_markets_pending_settlement(self, before_time):
        """Return all open markets whose kickoff is at/before before_time —
        the auto-settle poller's work-list, across all guilds."""
        return self.conn.execute(
            "SELECT * FROM bet_market WHERE status = 'open' "
            'AND commence_time <= ? ORDER BY commence_time ASC',
            (before_time,)
        ).fetchall()

    # -- Wagers --

    def bet_place(self, guild_id, market_id, user_id, pick, stake,
                  placed_at, start_balance):
        """Place or replace a user's wager, escrowing the stake from their
        wallet. Re-betting refunds the previous stake first. Odds are not
        stored — they are the market's frozen odds_<pick>.

        Returns (ok, reason, new_balance) where reason is 'ok' or
        'insufficient'. Atomic: wallet debit and wager write commit together.
        """
        guild_id, user_id = str(guild_id), str(user_id)
        with self.conn:
            cur = self.conn.execute(
                'INSERT OR IGNORE INTO bet_wallet (guild_id, user_id, balance) '
                'VALUES (?, ?, ?)',
                (guild_id, user_id, start_balance)
            )
            if cur.rowcount:
                self._bet_log_wallet_txn(
                    guild_id, user_id, None, 'init', start_balance, start_balance,
                    created_at=placed_at)
            balance = self.conn.execute(
                'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
                (guild_id, user_id)
            ).fetchone().balance
            prev = self.conn.execute(
                'SELECT stake FROM bet_wager WHERE market_id = ? AND user_id = ?',
                (market_id, user_id)
            ).fetchone()
            available = balance + (prev.stake if prev else 0)
            if stake > available:
                return (False, 'insufficient', balance)
            new_balance = available - stake
            if prev:
                self._bet_log_wallet_txn(
                    guild_id, user_id, user_id, 'wager_refund', prev.stake,
                    available, market_id=market_id, created_at=placed_at)
            self.conn.execute(
                'INSERT OR REPLACE INTO bet_wager '
                '(market_id, user_id, pick, stake, placed_at) '
                'VALUES (?, ?, ?, ?, ?)',
                (market_id, user_id, pick, stake, placed_at)
            )
            self.conn.execute(
                'UPDATE bet_wallet SET balance = ? WHERE guild_id = ? AND user_id = ?',
                (new_balance, guild_id, user_id)
            )
            self._bet_log_wallet_txn(
                guild_id, user_id, user_id, 'wager_stake', -stake, new_balance,
                market_id=market_id, note=pick, created_at=placed_at)
            return (True, 'ok', new_balance)

    def bet_get_wager(self, market_id, user_id):
        """Return a user's wager on a market, or None."""
        return self.conn.execute(
            'SELECT market_id, user_id, pick, stake, placed_at '
            'FROM bet_wager WHERE market_id = ? AND user_id = ?',
            (market_id, str(user_id))
        ).fetchone()

    def bet_get_wagers(self, market_id):
        """Return all wagers on a market, earliest first."""
        return self.conn.execute(
            'SELECT market_id, user_id, pick, stake, placed_at '
            'FROM bet_wager WHERE market_id = ? ORDER BY placed_at ASC',
            (market_id,)
        ).fetchall()

    def bet_pool(self, market_id):
        """Return (pick, count, total_stake) grouped by pick for a market."""
        return self.conn.execute(
            'SELECT pick, COUNT(*) AS cnt, SUM(stake) AS total '
            'FROM bet_wager WHERE market_id = ? GROUP BY pick',
            (market_id,)
        ).fetchall()

    def _market_odds_map(self, market_id):
        """{'home':o,'draw':o,'away':o} of a market's frozen odds, or None."""
        row = self.conn.execute(
            'SELECT odds_home, odds_draw, odds_away FROM bet_market '
            'WHERE market_id = ?', (market_id,)
        ).fetchone()
        if row is None:
            return None
        return {'home': row.odds_home, 'draw': row.odds_draw,
                'away': row.odds_away}

    def bet_settle(self, guild_id, market_id, result, result_home, result_away,
                   settled_at):
        """Settle a market: credit each winning wager round(stake×odds) to its
        wallet and mark the market settled. Odds are the market's frozen
        odds_<pick>; payout is computed, not stored. Returns
        [(user_id, pick, stake, odds, payout)] for the announcement, or None if
        the market was not open (already settled or cancelled).

        The terminal status flip is the guard: it runs first and is scoped to
        ``status='open'``, so a second settle (or a settle racing the
        auto-poller) is a no-op and nobody is paid twice. Atomic.
        """
        guild_id = str(guild_id)
        with self.conn:
            changed = self.conn.execute(
                "UPDATE bet_market SET status = 'settled', result = ?, "
                'result_home = ?, result_away = ?, settled_at = ? '
                "WHERE market_id = ? AND status = 'open'",
                (result, result_home, result_away, settled_at, market_id)
            ).rowcount
            if changed == 0:
                return None
            odds_map = self._market_odds_map(market_id)
            outcome = []
            wagers = self.conn.execute(
                'SELECT user_id, pick, stake FROM bet_wager WHERE market_id = ?',
                (market_id,)
            ).fetchall()
            for w in wagers:
                odds = _bet_odds_for_pick(odds_map, w.pick)
                payout = int(round(w.stake * odds)) \
                    if _bet_pick_wins(w.pick, result) and odds is not None else 0
                if payout:
                    self.conn.execute(
                        'UPDATE bet_wallet SET balance = balance + ? '
                        'WHERE guild_id = ? AND user_id = ?',
                        (payout, guild_id, w.user_id)
                    )
                    balance_after = self.conn.execute(
                        'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
                        (guild_id, w.user_id)
                    ).fetchone().balance
                    self._bet_log_wallet_txn(
                        guild_id, w.user_id, None, 'payout', payout,
                        balance_after, market_id=market_id, note=result,
                        created_at=settled_at)
                outcome.append((w.user_id, w.pick, w.stake, odds, payout))
        return outcome

    def bet_resettle(self, guild_id, market_id, new_result, new_home, new_away,
                     resettled_at):
        """Correct a settled market's result. Reverses the payouts that were
        credited under the OLD result and applies the new ones (per-wager
        delta), then stamps the new result. Returns
        [(user_id, pick, stake, odds, new_payout, delta)], or None if the
        market is not currently settled. Balances may go negative if a winner
        already spent the erroneous payout — use `;bet set` to reconcile. Atomic.
        """
        guild_id = str(guild_id)
        with self.conn:
            market = self.conn.execute(
                'SELECT result, odds_home, odds_draw, odds_away FROM bet_market '
                "WHERE market_id = ? AND status = 'settled'", (market_id,)
            ).fetchone()
            if market is None:
                return None
            old_result = market.result
            odds_map = {'home': market.odds_home, 'draw': market.odds_draw,
                        'away': market.odds_away}
            self.conn.execute(
                'UPDATE bet_market SET result = ?, result_home = ?, '
                'result_away = ?, settled_at = ? WHERE market_id = ?',
                (new_result, new_home, new_away, resettled_at, market_id)
            )
            outcome = []
            wagers = self.conn.execute(
                'SELECT user_id, pick, stake FROM bet_wager WHERE market_id = ?',
                (market_id,)
            ).fetchall()
            for w in wagers:
                odds = _bet_odds_for_pick(odds_map, w.pick)
                old_pay = int(round(w.stake * odds)) \
                    if _bet_pick_wins(w.pick, old_result) and odds is not None else 0
                new_pay = int(round(w.stake * odds)) \
                    if _bet_pick_wins(w.pick, new_result) and odds is not None else 0
                delta = new_pay - old_pay
                if delta:
                    self.conn.execute(
                        'UPDATE bet_wallet SET balance = balance + ? '
                        'WHERE guild_id = ? AND user_id = ?',
                        (delta, guild_id, w.user_id)
                    )
                    balance_after = self.conn.execute(
                        'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
                        (guild_id, w.user_id)
                    ).fetchone().balance
                    self._bet_log_wallet_txn(
                        guild_id, w.user_id, None, 'resettle_delta', delta,
                        balance_after, market_id=market_id, note=new_result,
                        created_at=resettled_at)
                outcome.append((w.user_id, w.pick, w.stake, odds, new_pay, delta))
        return outcome

    def bet_void(self, guild_id, market_id, voided_at):
        """Cancel an open market: refund every stake to its wallet and mark the
        market cancelled. Returns [(user_id, stake)] refunded, or None if the
        market was not open.

        Same guard as bet_settle: the status flip is scoped to ``status='open'``
        and runs first, so a void can't double-refund or undo a settlement.
        Atomic.
        """
        guild_id = str(guild_id)
        with self.conn:
            changed = self.conn.execute(
                "UPDATE bet_market SET status = 'cancelled', settled_at = ? "
                "WHERE market_id = ? AND status = 'open'",
                (voided_at, market_id)
            ).rowcount
            if changed == 0:
                return None
            refunds = []
            wagers = self.conn.execute(
                'SELECT user_id, stake FROM bet_wager WHERE market_id = ?',
                (market_id,)
            ).fetchall()
            for w in wagers:
                self.conn.execute(
                    'UPDATE bet_wallet SET balance = balance + ? '
                    'WHERE guild_id = ? AND user_id = ?',
                    (w.stake, guild_id, w.user_id)
                )
                balance_after = self.conn.execute(
                    'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
                    (guild_id, w.user_id)
                ).fetchone().balance
                self._bet_log_wallet_txn(
                    guild_id, w.user_id, None, 'void_refund', w.stake,
                    balance_after, market_id=market_id, created_at=voided_at)
                refunds.append((w.user_id, w.stake))
        return refunds

    def bet_markets_open(self, guild_id):
        """Return all open markets for a guild, kickoff-soonest first."""
        return self.conn.execute(
            "SELECT * FROM bet_market WHERE guild_id = ? AND status = 'open' "
            'ORDER BY commence_time ASC',
            (str(guild_id),)
        ).fetchall()

    # -- Moderator tools --

    def bet_adjust_balance(self, guild_id, user_id, delta, start_balance,
                           actor_id=None, action=None, note=None):
        """Add delta (may be negative) to a wallet, creating it at
        start_balance first. Floors at 0. Returns the new balance."""
        guild_id, user_id = str(guild_id), str(user_id)
        self.bet_ensure_wallet(guild_id, user_id, start_balance)
        before = self.bet_get_balance(guild_id, user_id)
        new_balance = max(0, before + int(delta))
        self.conn.execute(
            'UPDATE bet_wallet SET balance = ? WHERE guild_id = ? AND user_id = ?',
            (new_balance, guild_id, user_id)
        )
        actual_delta = new_balance - before
        if actual_delta or action:
            self._bet_log_wallet_txn(
                guild_id, user_id, actor_id, action or 'adjust', actual_delta,
                new_balance, note=note)
        self.conn.commit()
        return new_balance

    def bet_set_balance(self, guild_id, user_id, value, start_balance,
                        actor_id=None, action='setbalance', note=None):
        """Set a wallet to an absolute value (floored at 0). Returns it."""
        guild_id, user_id = str(guild_id), str(user_id)
        value = max(0, int(value))
        self.bet_ensure_wallet(guild_id, user_id, start_balance)
        before = self.bet_get_balance(guild_id, user_id)
        self.conn.execute(
            'UPDATE bet_wallet SET balance = ? WHERE guild_id = ? AND user_id = ?',
            (value, guild_id, user_id)
        )
        self._bet_log_wallet_txn(
            guild_id, user_id, actor_id, action, value - before, value, note=note)
        self.conn.commit()
        return value

    def bet_market_count_wagers(self, market_id):
        """Number of wagers placed on a market."""
        return self.conn.execute(
            'SELECT COUNT(*) AS cnt FROM bet_wager WHERE market_id = ?',
            (market_id,)
        ).fetchone().cnt

    def bet_market_close_betting(self, market_id):
        """Lock betting on an open market early. Returns True if it changed."""
        cur = self.conn.execute(
            "UPDATE bet_market SET bets_closed = 1 "
            "WHERE market_id = ? AND status = 'open' AND bets_closed = 0",
            (market_id,)
        )
        self.conn.commit()
        return cur.rowcount > 0

    def bet_market_set_odds(self, market_id, odds_home, odds_draw, odds_away):
        """Override an open market's frozen odds (only safe with no wagers yet
        — the caller enforces that). Returns True if a row changed."""
        cur = self.conn.execute(
            "UPDATE bet_market SET odds_home = ?, odds_draw = ?, odds_away = ? "
            "WHERE market_id = ? AND status = 'open'",
            (odds_home, odds_draw, odds_away, market_id)
        )
        self.conn.commit()
        return cur.rowcount > 0

    def bet_market_get_latest_settled_by_thread(self, guild_id, thread_id):
        """Most recently settled market for a thread, or None (for ;bet correct)."""
        return self.conn.execute(
            "SELECT * FROM bet_market WHERE guild_id = ? AND thread_id = ? "
            "AND status = 'settled' ORDER BY settled_at DESC, market_id DESC LIMIT 1",
            (str(guild_id), str(thread_id))
        ).fetchone()

    def bet_market_get_latest_settled_by_channel(self, guild_id, channel_id):
        """Most recently settled market for a channel, or None (for ;bet correct)."""
        return self.conn.execute(
            "SELECT * FROM bet_market WHERE guild_id = ? AND channel_id = ? "
            "AND status = 'settled' ORDER BY settled_at DESC, market_id DESC LIMIT 1",
            (str(guild_id), str(channel_id))
        ).fetchone()

    def close(self):
        self.conn.close()
