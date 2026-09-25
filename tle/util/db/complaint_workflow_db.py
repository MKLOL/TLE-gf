"""Resolution records, notification outbox, and complaint audit history."""
import time


def upgrade_complaint_schema(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS complaint (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT NOT NULL,
        user_id TEXT NOT NULL, text TEXT NOT NULL, created_at REAL NOT NULL,
        message_link TEXT
    )''')
    columns = {row[1] for row in conn.execute('PRAGMA table_info(complaint)')}
    additions = {
        'message_link': 'TEXT',
        'active': 'INTEGER NOT NULL DEFAULT 1',
        'resolved_at': 'REAL', 'resolved_by': 'TEXT', 'resolution': 'TEXT',
        'commit_url': 'TEXT', 'notification_status': 'TEXT',
        'notification_link': 'TEXT', 'notification_attempt_at': 'REAL',
        'notification_attempts': 'INTEGER NOT NULL DEFAULT 0',
    }
    for name, definition in additions.items():
        if name not in columns:
            conn.execute(f'ALTER TABLE complaint ADD COLUMN {name} {definition}')
    conn.execute('''CREATE TABLE IF NOT EXISTS complaint_event (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        complaint_id INTEGER NOT NULL, guild_id TEXT NOT NULL,
        actor_id TEXT NOT NULL, token_id INTEGER, action TEXT NOT NULL,
        resolution TEXT, commit_url TEXT, created_at REAL NOT NULL
    )''')
    conn.execute('''CREATE INDEX IF NOT EXISTS idx_complaint_event
        ON complaint_event (guild_id, complaint_id, id)''')
    conn.execute('''CREATE INDEX IF NOT EXISTS idx_complaint_notifications
        ON complaint (notification_status, notification_attempt_at)
        WHERE active = 1 AND resolved_at IS NOT NULL''')


class ComplaintWorkflowDbMixin:
    def _complaint_event(self, complaint_id, guild_id, actor_id, token_id,
                         action, resolution=None, commit_url=None):
        self.conn.execute('''INSERT INTO complaint_event
            (complaint_id, guild_id, actor_id, token_id, action, resolution,
             commit_url, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (complaint_id, str(guild_id), str(actor_id), token_id, action,
             resolution, commit_url, time.time()))

    def resolve_complaint(self, complaint_id, guild_id, actor_id, resolution,
                          commit_url, token_id=None):
        """Atomically resolve an open complaint and enqueue its notification."""
        with self.conn:
            changed = self.conn.execute('''UPDATE complaint SET
                resolved_at = ?, resolved_by = ?, resolution = ?, commit_url = ?,
                notification_status = 'pending', notification_link = NULL,
                notification_attempt_at = NULL, notification_attempts = 0
                WHERE id = ? AND guild_id = ? AND active = 1
                AND resolved_at IS NULL''',
                (time.time(), str(actor_id), resolution, commit_url,
                 complaint_id, str(guild_id))).rowcount
            if changed:
                self._complaint_event(complaint_id, guild_id, actor_id, token_id,
                                      'resolve', resolution, commit_url)
        return bool(changed)

    def reopen_complaint(self, complaint_id, guild_id, actor_id, token_id=None):
        with self.conn:
            changed = self.conn.execute('''UPDATE complaint SET
                resolved_at = NULL, resolved_by = NULL, resolution = NULL,
                commit_url = NULL, notification_status = NULL,
                notification_link = NULL, notification_attempt_at = NULL,
                notification_attempts = 0
                WHERE id = ? AND guild_id = ? AND active = 1
                AND resolved_at IS NOT NULL''',
                (complaint_id, str(guild_id))).rowcount
            if changed:
                self._complaint_event(complaint_id, guild_id, actor_id, token_id,
                                      'reopen')
        return bool(changed)

    def get_complaint_events(self, complaint_id, guild_id):
        return self.conn.execute('''SELECT * FROM complaint_event
            WHERE complaint_id = ? AND guild_id = ? ORDER BY id DESC LIMIT 100''',
            (complaint_id, str(guild_id))).fetchall()

    def pending_complaint_notifications(self):
        return self.conn.execute('''SELECT id, guild_id FROM complaint
            WHERE active = 1 AND resolved_at IS NOT NULL
            AND notification_status IN ('pending', 'failed')
            AND notification_attempts < 5
            AND (notification_attempt_at IS NULL OR notification_attempt_at < ?)
            ORDER BY id LIMIT 20''', (time.time() - 300,)).fetchall()

    def record_complaint_notification(self, complaint_id, status, link=None):
        with self.conn:
            self.conn.execute('''UPDATE complaint SET notification_status = ?,
                notification_link = ?, notification_attempt_at = ?,
                notification_attempts = notification_attempts + 1
                WHERE id = ? AND active = 1 AND resolved_at IS NOT NULL''',
                (status, link, time.time(), complaint_id))
