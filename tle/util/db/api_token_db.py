"""Guild-scoped, expiring bearer credentials. Only SHA-256 digests persist."""
import hashlib
import re
import secrets
import time

TOKEN_PATTERN = re.compile(r'tlegf_[A-Za-z0-9_-]{43}')


def create_api_token_schema(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS complaint_api_token (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_hash TEXT NOT NULL UNIQUE, guild_id TEXT NOT NULL,
        user_id TEXT NOT NULL, created_at REAL NOT NULL,
        expires_at REAL NOT NULL, revoked_at REAL
    )''')


class ApiTokenDbMixin:
    def create_complaint_token(self, guild_id, user_id, days=30):
        if type(days) is not int or not 1 <= days <= 365:
            raise ValueError('Token lifetime must be 1 to 365 days.')
        token = 'tlegf_' + secrets.token_urlsafe(32)
        digest = hashlib.sha256(token.encode('ascii')).hexdigest()
        now = time.time()
        with self.conn:
            cur = self.conn.execute('''INSERT INTO complaint_api_token
                (token_hash, guild_id, user_id, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)''',
                (digest, str(guild_id), str(user_id), now, now + days * 86400))
        return cur.lastrowid, token

    def authenticate_complaint_token(self, token):
        if not isinstance(token, str) or not TOKEN_PATTERN.fullmatch(token):
            return None
        digest = hashlib.sha256(token.encode('ascii')).hexdigest()
        return self.conn.execute('''SELECT id, guild_id, user_id, expires_at
            FROM complaint_api_token WHERE token_hash = ?
            AND revoked_at IS NULL AND expires_at > ?''',
            (digest, time.time())).fetchone()

    def list_complaint_tokens(self, guild_id):
        return self.conn.execute('''SELECT id, user_id, created_at, expires_at
            FROM complaint_api_token WHERE guild_id = ? AND revoked_at IS NULL
            AND expires_at > ? ORDER BY id DESC LIMIT 100''',
            (str(guild_id), time.time())).fetchall()

    def revoke_complaint_token(self, token_id, guild_id):
        with self.conn:
            return bool(self.conn.execute('''UPDATE complaint_api_token
                SET revoked_at = ? WHERE id = ? AND guild_id = ?
                AND revoked_at IS NULL''',
                (time.time(), token_id, str(guild_id))).rowcount)
