"""Complaint DB methods — extracted from user_db_conn.py.

Owns the ``complaint`` and ``complaint_tag`` tables and composes the
resolution workflow.
"""
import logging
import re
import time

from tle.util.db.complaint_workflow_db import (
    ComplaintWorkflowDbMixin, upgrade_complaint_schema,
)

logger = logging.getLogger(__name__)

# Tags are free-form but have to survive being typed into a command line and
# a URL query, and must never collide with the list command's status words.
_TAG_RE = re.compile(r'^[a-z0-9][a-z0-9_-]{0,31}$')
_RESERVED_TAGS = frozenset({'all', 'open', 'resolved', 'untagged'})


def normalize_tag(tag):
    """Lower-case a tag and reject anything that is not a plain word."""
    tag = (tag or '').strip().lower()
    if not _TAG_RE.match(tag):
        raise ValueError(
            'Tags are 1-32 characters: letters, digits, `_` or `-`.')
    if tag in _RESERVED_TAGS:
        raise ValueError(f'`{tag}` is reserved.')
    return tag


class ComplaintDbMixin(ComplaintWorkflowDbMixin):
    """Mixin providing complaint DB methods."""

    def _create_complaint_tables(self):
        existed = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'complaint'"
        ).fetchone()
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
        if not existed:
            upgrade_complaint_schema(self.conn)

    def add_complaint(self, guild_id, user_id, text, message_link=None,
                      context=None):
        """Insert a complaint and return its id.

        ``context`` is the JSON transcript of the messages preceding the
        report, or None when none could be read.
        """
        guild_id, user_id = str(guild_id), str(user_id)
        cur = self.conn.execute(
            'INSERT INTO complaint '
            '(guild_id, user_id, text, created_at, message_link, context) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (guild_id, user_id, text, time.time(), message_link, context)
        )
        self.conn.commit()
        return cur.lastrowid

    def set_complaint_context(self, complaint_id, context):
        """Attach a transcript to an already-recorded complaint."""
        self.conn.execute(
            'UPDATE complaint SET context = ? WHERE id = ?',
            (context, complaint_id))
        self.conn.commit()

    def get_complaints(self, guild_id, status='open', limit=None, before=None,
                       *, tag=None, include_tagged=False):
        """Return visible complaints, optionally bounded by an ID cursor.

        Tagged complaints are hidden unless ``include_tagged`` is set or a
        ``tag`` selects them: tagging exists precisely to move a complaint out
        of the default view without deleting it.
        """
        if status not in ('open', 'resolved', 'all'):
            raise ValueError('Status must be open, resolved, or all.')
        guild_id = str(guild_id)
        query = 'SELECT * FROM complaint WHERE guild_id = ? AND active = 1'
        args = [guild_id]
        if status != 'all':
            query += ' AND resolved_at IS ' + ('NULL' if status == 'open' else 'NOT NULL')
        if tag is not None:
            query += (' AND EXISTS (SELECT 1 FROM complaint_tag t '
                      'WHERE t.complaint_id = complaint.id AND t.tag = ?)')
            args.append(tag)
        elif not include_tagged:
            query += (' AND NOT EXISTS (SELECT 1 FROM complaint_tag t '
                      'WHERE t.complaint_id = complaint.id)')
        if before is not None:
            query += ' AND id < ?'
            args.append(before)
        query += ' ORDER BY id DESC'
        if limit is not None:
            query += ' LIMIT ?'
            args.append(limit)
        return self.conn.execute(query, args).fetchall()

    def add_complaint_tag(self, complaint_id, guild_id, tag):
        """Tag a complaint. Returns False when it already carried the tag."""
        with self.conn:
            changed = self.conn.execute(
                'INSERT OR IGNORE INTO complaint_tag '
                '(complaint_id, guild_id, tag, created_at) VALUES (?, ?, ?, ?)',
                (complaint_id, str(guild_id), tag, time.time())).rowcount
        return bool(changed)

    def remove_complaint_tag(self, complaint_id, guild_id, tag):
        with self.conn:
            changed = self.conn.execute(
                'DELETE FROM complaint_tag WHERE complaint_id = ? '
                'AND guild_id = ? AND tag = ?',
                (complaint_id, str(guild_id), tag)).rowcount
        return bool(changed)

    def get_complaint_tags(self, complaint_id):
        return [row[0] for row in self.conn.execute(
            'SELECT tag FROM complaint_tag WHERE complaint_id = ? ORDER BY tag',
            (complaint_id,)).fetchall()]

    def get_tags_for_complaints(self, complaint_ids):
        """Map complaint id -> sorted tags, one query for a whole listing."""
        ids = list(complaint_ids)
        if not ids:
            return {}
        placeholders = ','.join('?' * len(ids))
        tags = {}
        for complaint_id, tag in self.conn.execute(
                f'SELECT complaint_id, tag FROM complaint_tag '
                f'WHERE complaint_id IN ({placeholders}) ORDER BY tag', ids):
            tags.setdefault(complaint_id, []).append(tag)
        return tags

    def get_complaint_tag_counts(self, guild_id):
        """Every tag in use in a guild with how many complaints carry it."""
        return self.conn.execute(
            'SELECT tag, COUNT(*) AS count FROM complaint_tag t '
            'JOIN complaint c ON c.id = t.complaint_id '
            'WHERE t.guild_id = ? AND c.active = 1 '
            'GROUP BY tag ORDER BY tag', (str(guild_id),)).fetchall()

    def get_complaint(self, complaint_id):
        """Return a single active complaint by id, or None."""
        row = self.conn.execute(
            'SELECT * '
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
