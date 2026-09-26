"""Durable personal submission receipts; one post per player/game/day."""
import secrets
import time


def create_games_submission_schema(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS games_submission (
        guild_id TEXT NOT NULL, user_id TEXT NOT NULL, game TEXT NOT NULL,
        puzzle_number INTEGER NOT NULL, channel_id TEXT NOT NULL,
        content TEXT NOT NULL, nonce TEXT NOT NULL UNIQUE,
        created_at REAL NOT NULL, message_id TEXT, completed INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (guild_id, user_id, game, puzzle_number)
    )''')


class GamesSubmissionDbMixin:
    def get_games_submission(self, guild_id, user_id, game, puzzle_number):
        return self.conn.execute('''SELECT * FROM games_submission WHERE
            guild_id = ? AND user_id = ? AND game = ? AND puzzle_number = ?''',
            (str(guild_id), str(user_id), game, puzzle_number)).fetchone()

    def claim_games_submission(self, guild_id, user_id, game, puzzle_number, channel_id, content):
        with self.conn:
            self.conn.execute('''INSERT OR IGNORE INTO games_submission
                (guild_id, user_id, game, puzzle_number, channel_id, content, nonce, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                (str(guild_id), str(user_id), game, puzzle_number, str(channel_id),
                 content, secrets.token_hex(12), time.time()))
        return self.get_games_submission(guild_id, user_id, game, puzzle_number)

    def mark_games_submission_posted(self, nonce, message_id):
        with self.conn:
            self.conn.execute('UPDATE games_submission SET message_id = ? WHERE nonce = ?',
                              (str(message_id), nonce))

    def finish_games_submission(self, nonce):
        with self.conn:
            self.conn.execute('UPDATE games_submission SET completed = 1 WHERE nonce = ?', (nonce,))

    def release_games_submission(self, nonce):
        # Only definitely rejected Discord sends can be safely retried from scratch.
        with self.conn:
            self.conn.execute('DELETE FROM games_submission WHERE nonce = ? AND message_id IS NULL',
                              (nonce,))

    def mark_games_submission_removed(self, message_id):
        with self.conn:
            self.conn.execute('UPDATE games_submission SET completed = -1 WHERE message_id = ?',
                              (str(message_id),))
