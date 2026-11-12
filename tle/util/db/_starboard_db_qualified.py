"""Persistent per-message history of independently qualified reaction groups."""


def create_starboard_qualified_schema(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS starboard_qualified (
            original_msg_id TEXT NOT NULL,
            main_emoji TEXT NOT NULL,
            reaction_emoji TEXT NOT NULL,
            PRIMARY KEY (original_msg_id, main_emoji, reaction_emoji)
        )
    ''')
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' "
                    "AND name = 'starboard_message_v1'").fetchone():
        # Cover every deletion path, including board config removal and bulk
        # cleanup. Reposting a deleted entry must qualify from scratch.
        conn.execute('''
            CREATE TRIGGER IF NOT EXISTS starboard_qualified_delete
            AFTER DELETE ON starboard_message_v1
            BEGIN
                DELETE FROM starboard_qualified
                WHERE original_msg_id = OLD.original_msg_id
                  AND main_emoji = OLD.emoji;
            END
        ''')


class StarboardQualifiedDbMixin:
    def _insert_starboard_qualifications(self, original_msg_id, main_emoji, emojis):
        """Insert within the caller's transaction; posting and marks commit together."""
        self.conn.executemany(
            'INSERT OR IGNORE INTO starboard_qualified '
            '(original_msg_id, main_emoji, reaction_emoji) VALUES (?, ?, ?)',
            [(str(original_msg_id), main_emoji, emoji) for emoji in emojis])

    def get_starboard_qualified_emojis(self, original_msg_id, main_emoji):
        rows = self.conn.execute(
            'SELECT reaction_emoji FROM starboard_qualified '
            'WHERE original_msg_id = ? AND main_emoji = ?',
            (str(original_msg_id), main_emoji)).fetchall()
        return {row.reaction_emoji for row in rows}

    def remember_starboard_qualifications(self, original_msg_id, main_emoji, *, commit=True):
        """Record threshold crossings only for a successfully posted entry.

        Called on every count update, including repairs and proxy reactions.
        Use independent group counts, never the combined leaderboard total.
        """
        row = self.get_starboard_message_v1(original_msg_id, main_emoji)
        if row is None:
            return
        entry = self.get_starboard_entry(row.guild_id, main_emoji)
        if entry is None:
            return
        counts, _ = self.get_starboard_reaction_counts(row.guild_id, original_msg_id, main_emoji)
        self._insert_starboard_qualifications(
            original_msg_id, main_emoji,
            [emoji for emoji, count in counts if count >= entry.threshold])
        if commit:
            self.conn.commit()

    def seed_starboard_qualifications(self):
        """Recover legacy single-family posts and currently qualifying groups.

        An old post with no fallback reactions was admitted by its configured
        family, even if that count has since dropped. Mixed posts do not tell
        us which groups qualified in the past: only seed their current hits.
        Never label a sub-threshold main group qualified just because some
        fallback was responsible for posting the message.
        """
        for row in self.conn.execute(
                'SELECT original_msg_id, guild_id, emoji FROM starboard_message_v1'):
            groups = self.get_starboard_reaction_groups(
                row.guild_id, row.emoji, row.original_msg_id)
            if len(groups) == 1:
                self._insert_starboard_qualifications(
                    row.original_msg_id, row.emoji, [row.emoji])
            self.remember_starboard_qualifications(row.original_msg_id, row.emoji, commit=False)
        self.conn.commit()
