"""Personal, guild-scoped games credentials, isolated from complaint access."""
import hashlib
import re
import secrets
import time

TOKEN_PATTERN = re.compile(r'tlegames_[A-Za-z0-9_-]{43}')
PERMANENT_EXPIRY = -1


def create_games_token_schema(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS games_api_token (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_hash TEXT NOT NULL UNIQUE, guild_id TEXT NOT NULL,
        user_id TEXT NOT NULL, created_at REAL NOT NULL,
        expires_at REAL NOT NULL, revoked_at REAL
    )''')


class GamesTokenDbMixin:
    def create_games_token(self, guild_id, user_id, days=None):
        if days is not None and (type(days) is not int or not 1 <= days <= 365):
            raise ValueError('Token lifetime must be 1 to 365 days.')
        token = 'tlegames_' + secrets.token_urlsafe(32)
        now = time.time()
        expires_at = PERMANENT_EXPIRY if days is None else now + days * 86400
        with self.conn:
            cur = self.conn.execute('''INSERT INTO games_api_token
                (token_hash, guild_id, user_id, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)''',
                (hashlib.sha256(token.encode('ascii')).hexdigest(),
                 str(guild_id), str(user_id), now, expires_at))
        return cur.lastrowid, token

    def authenticate_games_token(self, token):
        if not isinstance(token, str) or not TOKEN_PATTERN.fullmatch(token):
            return None
        digest = hashlib.sha256(token.encode('ascii')).hexdigest()
        return self.conn.execute('''SELECT id, guild_id, user_id, expires_at
            FROM games_api_token WHERE token_hash = ?
            AND revoked_at IS NULL AND (expires_at = ? OR expires_at > ?)''',
            (digest, PERMANENT_EXPIRY, time.time())).fetchone()

    def list_games_tokens(self, guild_id, user_id):
        return self.conn.execute('''SELECT id, created_at, expires_at
            FROM games_api_token WHERE guild_id = ? AND user_id = ?
            AND revoked_at IS NULL AND (expires_at = ? OR expires_at > ?)
            ORDER BY id DESC LIMIT 100''',
            (str(guild_id), str(user_id), PERMANENT_EXPIRY, time.time())).fetchall()

    def revoke_games_token(self, token_id, guild_id, user_id):
        with self.conn:
            return bool(self.conn.execute('''UPDATE games_api_token
                SET revoked_at = ? WHERE id = ? AND guild_id = ? AND user_id = ?
                AND revoked_at IS NULL''',
                (time.time(), token_id, str(guild_id), str(user_id))).rowcount)
