"""Minigame config, result storage, raw-message and result-query DB methods.

Split out of ``minigame_db`` as a mixin to keep that module under the 500-line
limit. ``MinigameDbMixin`` inherits this so every method resolves on the
combined connection object exactly as before.
"""

from tle.util.db._minigame_db_common import _NO_TIME_BOUND, _timestamp_to_date_text


_MINIGAME_RESULT_UPSERT = '''
    INSERT OR REPLACE INTO minigame_result (
        message_id, guild_id, game, channel_id, user_id, puzzle_number,
        puzzle_date, accuracy, time_seconds, is_perfect, raw_content
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''


class MinigameResultsDbMixin:
    """Config, result save/delete, raw-message storage and result queries."""

    @staticmethod
    def _minigame_select(table_name):
        return f'''
            SELECT
                message_id,
                guild_id,
                game,
                channel_id,
                user_id,
                puzzle_number,
                puzzle_date,
                accuracy,
                time_seconds,
                is_perfect,
                raw_content
            FROM {table_name}
        '''

    def _minigame_filtered_union_query(self, guild_id, game, dlo=0, dhi=_NO_TIME_BOUND,
                                        plo=0, phi=0):
        guild_id = str(guild_id)
        base_params = [guild_id, game]
        extra_clauses = []
        extra_params = []

        dlo_text = _timestamp_to_date_text(dlo)
        if dlo_text is not None:
            extra_clauses.append('puzzle_date >= ?')
            extra_params.append(dlo_text)
        dhi_text = _timestamp_to_date_text(dhi)
        if dhi_text is not None and dhi < _NO_TIME_BOUND:
            extra_clauses.append('puzzle_date < ?')
            extra_params.append(dhi_text)
        if plo > 0:
            extra_clauses.append('puzzle_number >= ?')
            extra_params.append(int(plo))
        if phi > 0:
            extra_clauses.append('puzzle_number < ?')
            extra_params.append(int(phi))

        extra = ''
        if extra_clauses:
            extra = ' AND ' + ' AND '.join(extra_clauses)

        # Each UNION leg needs its own copy of (base + extra) params
        leg_params = base_params + extra_params
        params = leg_params + leg_params

        query = f'''
            WITH minigame_all AS (
                {self._minigame_select('minigame_result')}
                WHERE guild_id = ? AND game = ? {extra}
                UNION ALL
                {self._minigame_select('minigame_import_result')}
                WHERE guild_id = ? AND game = ? {extra}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM minigame_result live
                      WHERE live.message_id = minigame_import_result.message_id
                        AND live.game = minigame_import_result.game
                        AND live.puzzle_number = minigame_import_result.puzzle_number
                  )
            ),
            first_per_user_puzzle AS (
                SELECT
                    guild_id,
                    user_id,
                    puzzle_number,
                    MIN(CAST(message_id AS INTEGER)) AS first_message_id
                FROM minigame_all
                GROUP BY guild_id, user_id, puzzle_number
            )
            SELECT *
            FROM (
                SELECT all_rows.*
                FROM minigame_all all_rows
                JOIN first_per_user_puzzle first_rows
                  ON all_rows.guild_id = first_rows.guild_id
                 AND all_rows.user_id = first_rows.user_id
                 AND all_rows.puzzle_number = first_rows.puzzle_number
                 AND CAST(all_rows.message_id AS INTEGER) = first_rows.first_message_id
            )
        '''
        return query, tuple(params)

    # ── Config ──────────────────────────────────────────────────────────

    def get_minigame_channel(self, guild_id, game):
        row = self.conn.execute(
            'SELECT channel_id FROM minigame_config WHERE guild_id = ? AND game = ?',
            (str(guild_id), game)
        ).fetchone()
        return row.channel_id if row else None

    def set_minigame_channel(self, guild_id, game, channel_id):
        self.conn.execute(
            'INSERT OR REPLACE INTO minigame_config (guild_id, game, channel_id) VALUES (?, ?, ?)',
            (str(guild_id), game, str(channel_id))
        )
        self.conn.commit()

    def clear_minigame_channel(self, guild_id, game):
        rc = self.conn.execute(
            'DELETE FROM minigame_config WHERE guild_id = ? AND game = ?',
            (str(guild_id), game)
        ).rowcount
        self.conn.commit()
        return rc

    # ── Results ─────────────────────────────────────────────────────────

    @staticmethod
    def _minigame_result_values(row):
        (message_id, guild_id, game, channel_id, user_id, puzzle_number,
         puzzle_date, accuracy, time_seconds, is_perfect,
         raw_content) = row
        return (
            str(message_id), str(guild_id), game, str(channel_id),
            str(user_id), int(puzzle_number), str(puzzle_date),
            int(accuracy), int(time_seconds), int(bool(is_perfect)),
            str(raw_content),
        )

    def save_minigame_result(self, message_id, guild_id, game, channel_id, user_id,
                             puzzle_number, puzzle_date, accuracy, time_seconds, is_perfect,
                             raw_content):
        self.conn.execute(
            _MINIGAME_RESULT_UPSERT,
            self._minigame_result_values((
                message_id, guild_id, game, channel_id, user_id,
                puzzle_number, puzzle_date, accuracy, time_seconds,
                is_perfect, raw_content))
        )
        self.conn.commit()

    def save_minigame_results(self, rows):
        """Atomically upsert an iterable of result argument tuples."""
        values = [self._minigame_result_values(row) for row in rows]
        if not values:
            return 0
        with self.conn:
            self.conn.executemany(_MINIGAME_RESULT_UPSERT, values)
        return len(values)

    def save_imported_minigame_result(self, message_id, guild_id, game, channel_id, user_id,
                                      puzzle_number, puzzle_date, accuracy, time_seconds,
                                      is_perfect, raw_content, commit=True):
        self.conn.execute(
            '''
            INSERT OR REPLACE INTO minigame_import_result (
                message_id, guild_id, game, channel_id, user_id, puzzle_number,
                puzzle_date, accuracy, time_seconds, is_perfect, raw_content
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                str(message_id), str(guild_id), game, str(channel_id), str(user_id),
                int(puzzle_number), str(puzzle_date), int(accuracy), int(time_seconds),
                int(bool(is_perfect)), str(raw_content)
            )
        )
        if commit:
            self.conn.commit()

    def delete_minigame_result(self, message_id):
        rc = self.conn.execute(
            'DELETE FROM minigame_result WHERE message_id = ?',
            (str(message_id),)
        ).rowcount
        self.conn.commit()
        return rc

    def delete_imported_minigame_result(self, message_id):
        rc = self.conn.execute(
            'DELETE FROM minigame_import_result WHERE message_id = ?',
            (str(message_id),)
        ).rowcount
        self.conn.commit()
        return rc

    def clear_imported_minigame_results(self, guild_id, game, channel_id=None):
        if channel_id is not None:
            rc = self.conn.execute(
                'DELETE FROM minigame_import_result WHERE guild_id = ? AND game = ? AND channel_id = ?',
                (str(guild_id), game, str(channel_id))
            ).rowcount
        else:
            rc = self.conn.execute(
                'DELETE FROM minigame_import_result WHERE guild_id = ? AND game = ?',
                (str(guild_id), game)
            ).rowcount
        self.conn.commit()
        return rc

    # ── Raw messages ─────────────────────────────────────────────────

    def save_raw_message(self, message_id, guild_id, channel_id, user_id,
                         created_at, raw_content, commit=True):
        self.conn.execute(
            '''
            INSERT OR IGNORE INTO minigame_raw_message
                (message_id, guild_id, channel_id, user_id, created_at, raw_content)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (str(message_id), str(guild_id), str(channel_id), str(user_id),
             str(created_at), str(raw_content))
        )
        if commit:
            self.conn.commit()

    def update_raw_message(self, message_id, raw_content):
        self.conn.execute(
            'UPDATE minigame_raw_message SET raw_content = ? WHERE message_id = ?',
            (str(raw_content), str(message_id))
        )
        self.conn.commit()

    def delete_raw_message(self, message_id):
        rc = self.conn.execute(
            'DELETE FROM minigame_raw_message WHERE message_id = ?',
            (str(message_id),)
        ).rowcount
        self.conn.commit()
        return rc

    def get_raw_messages_for_guild(self, guild_id):
        return self.conn.execute(
            'SELECT * FROM minigame_raw_message WHERE guild_id = ? ORDER BY CAST(message_id AS INTEGER)',
            (str(guild_id),)
        ).fetchall()

    def clear_raw_messages(self, guild_id, channel_id=None):
        if channel_id is not None:
            rc = self.conn.execute(
                'DELETE FROM minigame_raw_message WHERE guild_id = ? AND channel_id = ?',
                (str(guild_id), str(channel_id))
            ).rowcount
        else:
            rc = self.conn.execute(
                'DELETE FROM minigame_raw_message WHERE guild_id = ?',
                (str(guild_id),)
            ).rowcount
        self.conn.commit()
        return rc

    # ── Queries ──────────────────────────────────────────────────────

    def get_minigame_result(self, message_id):
        return self.conn.execute(
            'SELECT * FROM minigame_result WHERE message_id = ?',
            (str(message_id),)
        ).fetchone()

    def get_minigame_result_for_user_puzzle(self, guild_id, game, user_id, puzzle_number):
        query, params = self._minigame_filtered_union_query(guild_id, game)
        return self.conn.execute(
            f'''
            {query}
            WHERE user_id = ? AND puzzle_number = ?
            ORDER BY CAST(message_id AS INTEGER) ASC
            LIMIT 1
            ''',
            params + (str(user_id), int(puzzle_number))
        ).fetchone()

    def get_minigame_results_for_user(self, guild_id, game, user_id,
                                       dlo=0, dhi=_NO_TIME_BOUND, plo=0, phi=0):
        query, params = self._minigame_filtered_union_query(guild_id, game, dlo, dhi, plo, phi)
        return self.conn.execute(
            f'''
            {query}
            WHERE user_id = ?
            ORDER BY puzzle_date DESC, puzzle_number DESC, time_seconds ASC, message_id DESC
            ''',
            params + (str(user_id),)
        ).fetchall()

    def get_minigame_results_for_guild(self, guild_id, game,
                                        dlo=0, dhi=_NO_TIME_BOUND, plo=0, phi=0):
        query, params = self._minigame_filtered_union_query(guild_id, game, dlo, dhi, plo, phi)
        return self.conn.execute(
            f'''
            {query}
            ORDER BY puzzle_date DESC, puzzle_number DESC, time_seconds ASC, message_id DESC
            ''',
            params
        ).fetchall()

    def get_stored_minigame_results_for_guild(self, guild_id, game):
        return self.conn.execute(
            f'''
            SELECT 'live' AS storage, live_rows.*
            FROM (
                {self._minigame_select('minigame_result')}
                WHERE guild_id = ? AND game = ?
            ) live_rows
            UNION ALL
            SELECT 'imported' AS storage, imported_rows.*
            FROM (
                {self._minigame_select('minigame_import_result')}
                WHERE guild_id = ? AND game = ?
            ) imported_rows
            ORDER BY puzzle_date DESC, puzzle_number DESC, message_id DESC
            ''',
            (str(guild_id), game, str(guild_id), game)
        ).fetchall()

    def get_live_minigame_results_for_guild(self, guild_id, game):
        return self.conn.execute(
            f'''
            {self._minigame_select('minigame_result')}
            WHERE guild_id = ? AND game = ?
            ORDER BY puzzle_date DESC, puzzle_number DESC, message_id DESC
            ''',
            (str(guild_id), game)
        ).fetchall()

    def get_import_only_minigame_results(self, guild_id, game):
        """Imported results that have no live counterpart for the same
        (user, puzzle).

        Audit helper: surfaces rows that exist only because of an
        ``import start`` backfill — useful for spotting junk left behind by a
        bad import.  A result is keyed on (user_id, puzzle_number); an imported
        row is "orphaned" when the user has no live result for that puzzle.
        """
        return self.conn.execute(
            f'''
            {self._minigame_select('minigame_import_result')}
            WHERE guild_id = ? AND game = ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM minigame_result live
                  WHERE live.guild_id = minigame_import_result.guild_id
                    AND live.game = minigame_import_result.game
                    AND live.user_id = minigame_import_result.user_id
                    AND live.puzzle_number = minigame_import_result.puzzle_number
              )
            ORDER BY CAST(puzzle_number AS INTEGER) DESC, user_id
            ''',
            (str(guild_id), game)
        ).fetchall()

    def delete_minigame_result_for_user_puzzle(self, guild_id, game, user_id, puzzle_number):
        live_rc = self.conn.execute(
            '''
            DELETE FROM minigame_result
            WHERE guild_id = ? AND game = ? AND user_id = ? AND puzzle_number = ?
            ''',
            (str(guild_id), game, str(user_id), int(puzzle_number))
        ).rowcount
        imported_rc = self.conn.execute(
            '''
            DELETE FROM minigame_import_result
            WHERE guild_id = ? AND game = ? AND user_id = ? AND puzzle_number = ?
            ''',
            (str(guild_id), game, str(user_id), int(puzzle_number))
        ).rowcount
        self.conn.commit()
        return live_rc + imported_rc

    def delete_stored_minigame_result_row(
            self, guild_id, game, storage, message_id, puzzle_number):
        tables = {
            'live': 'minigame_result',
            'imported': 'minigame_import_result',
        }
        table = tables[storage]
        rc = self.conn.execute(
            f'''
            DELETE FROM {table}
            WHERE guild_id = ? AND game = ? AND message_id = ?
              AND puzzle_number = ?
            ''',
            (str(guild_id), game, str(message_id), int(puzzle_number))
        ).rowcount
        self.conn.commit()
        return rc

    def delete_minigame_results_for_puzzle(self, guild_id, game, puzzle_number):
        live_rc = self.conn.execute(
            '''
            DELETE FROM minigame_result
            WHERE guild_id = ? AND game = ? AND puzzle_number = ?
            ''',
            (str(guild_id), game, int(puzzle_number))
        ).rowcount
        imported_rc = self.conn.execute(
            '''
            DELETE FROM minigame_import_result
            WHERE guild_id = ? AND game = ? AND puzzle_number = ?
            ''',
            (str(guild_id), game, int(puzzle_number))
        ).rowcount
        self.conn.commit()
        return live_rc + imported_rc

    def delete_minigame_results_for_date_range(
            self, guild_id, game, start_date, end_date_exclusive):
        with self.conn:
            live_rc = self.conn.execute(
                '''
                DELETE FROM minigame_result
                WHERE guild_id = ? AND game = ?
                  AND puzzle_date >= ? AND puzzle_date < ?
                ''',
                (str(guild_id), game, str(start_date), str(end_date_exclusive))
            ).rowcount
            imported_rc = self.conn.execute(
                '''
                DELETE FROM minigame_import_result
                WHERE guild_id = ? AND game = ?
                  AND puzzle_date >= ? AND puzzle_date < ?
                ''',
                (str(guild_id), game, str(start_date), str(end_date_exclusive))
            ).rowcount
        return live_rc + imported_rc

    def delete_minigame_results_for_game(self, guild_id, game):
        live_rc = self.conn.execute(
            '''
            DELETE FROM minigame_result
            WHERE guild_id = ? AND game = ?
            ''',
            (str(guild_id), game)
        ).rowcount
        imported_rc = self.conn.execute(
            '''
            DELETE FROM minigame_import_result
            WHERE guild_id = ? AND game = ?
            ''',
            (str(guild_id), game)
        ).rowcount
        self.conn.commit()
        return live_rc + imported_rc
