"""Database methods for the minigames system (Daily Akari, etc.)."""

import datetime as dt


_NO_TIME_BOUND = 10 ** 10


def _timestamp_to_date_text(timestamp):
    if timestamp <= 0:
        return None
    return dt.datetime.fromtimestamp(timestamp).date().isoformat()


class MinigameDbMixin:
    """Mixin providing minigame config and result storage methods.

    All methods take a ``game`` parameter (e.g. ``'akari'``) to identify
    which minigame the operation applies to.
    """

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

    def save_minigame_result(self, message_id, guild_id, game, channel_id, user_id,
                             puzzle_number, puzzle_date, accuracy, time_seconds, is_perfect,
                             raw_content):
        self.conn.execute(
            '''
            INSERT OR REPLACE INTO minigame_result (
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
        self.conn.commit()

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
