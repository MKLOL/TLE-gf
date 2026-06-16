"""Minigame table DDL.

Holds the ``_create_minigame_tables`` fragment of ``UserDbConn.create_tables``
for the minigame domain. Kept in its own mixin so ``minigame_db`` stays under
the 500-line limit; ``MinigameDbMixin`` inherits it.
"""


class MinigameSchemaDbMixin:
    """Creates every minigame-domain table/index (called from create_tables)."""

    def _create_minigame_tables(self):
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
