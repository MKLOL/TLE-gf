"""Unresolved external-account imports and player-identity-link DB methods.

Split out of ``minigame_db`` as a mixin to keep that module under the 500-line
limit. ``MinigameDbMixin`` inherits this.
"""

_MINIGAME_UNRESOLVED_UPSERT = '''
    INSERT OR REPLACE INTO minigame_unresolved_result (
        guild_id, game, normalized_name, external_name, channel_id,
        puzzle_number, puzzle_date, accuracy, time_seconds,
        is_perfect, raw_content
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''


class MinigameLinksDbMixin:
    """Unresolved external-name imports and Discord<->external identity links."""

    # ── Unresolved external-account imports ───────────────────────────

    @staticmethod
    def _minigame_unresolved_values(guild_id, game, row):
        (
            normalized_name, external_name, channel_id, puzzle_number,
            puzzle_date, accuracy, time_seconds, is_perfect, raw_content,
        ) = row
        return (
            str(guild_id), game, str(normalized_name), str(external_name),
            str(channel_id), int(puzzle_number), str(puzzle_date),
            int(accuracy), int(time_seconds), int(bool(is_perfect)),
            str(raw_content),
        )

    def save_minigame_unresolved_result(
            self, guild_id, game, normalized_name, external_name, channel_id,
            puzzle_number, puzzle_date, accuracy, time_seconds, is_perfect,
            raw_content):
        self.conn.execute(
            _MINIGAME_UNRESOLVED_UPSERT,
            self._minigame_unresolved_values(guild_id, game, (
                normalized_name, external_name, channel_id, puzzle_number,
                puzzle_date, accuracy, time_seconds, is_perfect, raw_content,
            )),
        )
        self.conn.commit()

    def apply_minigame_source_migration(
            self, guild_id, game, source_rows, stored_rows):
        """Atomically create external sources and remove their legacy rows.

        This method is an explicit transaction boundary.  Values and storage
        table names are validated before any write, then sources are inserted
        before legacy rows are deleted so a failure cannot lose the only copy.
        """
        source_values = [
            self._minigame_unresolved_values(guild_id, game, row)
            for row in source_rows
        ]
        delete_values = {'live': [], 'imported': []}
        for storage, message_id, puzzle_number in stored_rows:
            if storage not in delete_values:
                raise ValueError(f'Unsupported minigame result storage: {storage}')
            delete_values[storage].append((
                str(guild_id), game, str(message_id), int(puzzle_number),
            ))
        if not source_values and not any(delete_values.values()):
            return 0, 0

        tables = {
            'live': 'minigame_result',
            'imported': 'minigame_import_result',
        }
        deleted = 0
        with self.conn:
            if source_values:
                self.conn.executemany(
                    _MINIGAME_UNRESOLVED_UPSERT, source_values)
            for storage, values in delete_values.items():
                if not values:
                    continue
                cursor = self.conn.executemany(
                    f'''
                    DELETE FROM {tables[storage]}
                    WHERE guild_id = ? AND game = ? AND message_id = ?
                      AND puzzle_number = ?
                    ''',
                    values,
                )
                deleted += cursor.rowcount
        return len(source_values), deleted

    def get_minigame_unresolved_results_for_name(
            self, guild_id, game, normalized_name):
        return self.conn.execute(
            '''
            SELECT guild_id, game, normalized_name, external_name, channel_id,
                   puzzle_number, puzzle_date, accuracy, time_seconds,
                   is_perfect, raw_content
            FROM minigame_unresolved_result
            WHERE guild_id = ? AND game = ? AND normalized_name = ?
            ORDER BY puzzle_number ASC
            ''',
            (str(guild_id), game, str(normalized_name))
        ).fetchall()

    def get_minigame_unresolved_results_for_puzzle(
            self, guild_id, game, puzzle_number):
        return self.conn.execute(
            '''
            SELECT guild_id, game, normalized_name, external_name, channel_id,
                   puzzle_number, puzzle_date, accuracy, time_seconds,
                   is_perfect, raw_content
            FROM minigame_unresolved_result
            WHERE guild_id = ? AND game = ? AND puzzle_number = ?
            ORDER BY time_seconds ASC, normalized_name ASC
            ''',
            (str(guild_id), game, int(puzzle_number))
        ).fetchall()

    def get_minigame_unresolved_results_for_guild(self, guild_id, game):
        return self.conn.execute(
            '''
            SELECT guild_id, game, normalized_name, external_name, channel_id,
                   puzzle_number, puzzle_date, accuracy, time_seconds,
                   is_perfect, raw_content
            FROM minigame_unresolved_result
            WHERE guild_id = ? AND game = ?
            ORDER BY puzzle_date DESC, puzzle_number DESC, time_seconds ASC,
                     normalized_name ASC
            ''',
            (str(guild_id), game)
        ).fetchall()

    def delete_minigame_unresolved_results_for_name(
            self, guild_id, game, normalized_name):
        rc = self.conn.execute(
            '''
            DELETE FROM minigame_unresolved_result
            WHERE guild_id = ? AND game = ? AND normalized_name = ?
            ''',
            (str(guild_id), game, str(normalized_name))
        ).rowcount
        self.conn.commit()
        return rc

    def delete_minigame_unresolved_result_for_name_puzzle(
            self, guild_id, game, normalized_name, puzzle_number):
        rc = self.conn.execute(
            '''
            DELETE FROM minigame_unresolved_result
            WHERE guild_id = ? AND game = ? AND normalized_name = ?
              AND puzzle_number = ?
            ''',
            (str(guild_id), game, str(normalized_name), int(puzzle_number))
        ).rowcount
        self.conn.commit()
        return rc

    def delete_minigame_unresolved_results_for_puzzle(
            self, guild_id, game, puzzle_number):
        rc = self.conn.execute(
            '''
            DELETE FROM minigame_unresolved_result
            WHERE guild_id = ? AND game = ? AND puzzle_number = ?
            ''',
            (str(guild_id), game, int(puzzle_number))
        ).rowcount
        self.conn.commit()
        return rc

    def delete_minigame_unresolved_results_for_date_range(
            self, guild_id, game, start_date, end_date_exclusive):
        rc = self.conn.execute(
            '''
            DELETE FROM minigame_unresolved_result
            WHERE guild_id = ? AND game = ?
              AND puzzle_date >= ? AND puzzle_date < ?
            ''',
            (str(guild_id), game, str(start_date), str(end_date_exclusive))
        ).rowcount
        self.conn.commit()
        return rc

    # ── Generic minigame identity links ───────────────────────────────

    def set_minigame_player_link(self, guild_id, game, user_id, external_name,
                                 normalized_name, external_url, linked_at,
                                 linked_by):
        """Link a Discord user to an external game account/name.

        ``normalized_name`` is unique per ``(guild, game)`` so a pasted
        leaderboard name resolves to exactly one Discord user.  Callers should
        normalize consistently before passing the value in.
        """
        self.conn.execute(
            '''
            INSERT INTO minigame_player_link (
                guild_id, game, user_id, external_name, normalized_name,
                external_url, linked_at, linked_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, game, user_id) DO UPDATE SET
                external_name = excluded.external_name,
                normalized_name = excluded.normalized_name,
                external_url = excluded.external_url,
                linked_at = excluded.linked_at,
                linked_by = excluded.linked_by
            ''',
            (
                str(guild_id), game, str(user_id), str(external_name),
                str(normalized_name), external_url, float(linked_at),
                str(linked_by),
            )
        )
        self.conn.commit()

    def get_minigame_player_link(self, guild_id, game, user_id):
        return self.conn.execute(
            '''
            SELECT guild_id, game, user_id, external_name, normalized_name,
                   external_url, linked_at, linked_by
            FROM minigame_player_link
            WHERE guild_id = ? AND game = ? AND user_id = ?
            ''',
            (str(guild_id), game, str(user_id))
        ).fetchone()

    def get_minigame_player_link_by_name(self, guild_id, game, normalized_name):
        return self.conn.execute(
            '''
            SELECT guild_id, game, user_id, external_name, normalized_name,
                   external_url, linked_at, linked_by
            FROM minigame_player_link
            WHERE guild_id = ? AND game = ? AND normalized_name = ?
            ''',
            (str(guild_id), game, str(normalized_name))
        ).fetchone()

    def get_minigame_player_links(self, guild_id, game):
        return self.conn.execute(
            '''
            SELECT guild_id, game, user_id, external_name, normalized_name,
                   external_url, linked_at, linked_by
            FROM minigame_player_link
            WHERE guild_id = ? AND game = ?
            ORDER BY normalized_name ASC
            ''',
            (str(guild_id), game)
        ).fetchall()

    def delete_minigame_player_link(self, guild_id, game, user_id):
        rc = self.conn.execute(
            '''
            DELETE FROM minigame_player_link
            WHERE guild_id = ? AND game = ? AND user_id = ?
            ''',
            (str(guild_id), game, str(user_id))
        ).rowcount
        self.conn.commit()
        return rc
