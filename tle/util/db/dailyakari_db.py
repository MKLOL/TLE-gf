"""Database methods for the Daily Akari add-on."""

import datetime as dt


_NO_TIME_BOUND = 10 ** 10


def _timestamp_to_date_text(timestamp):
    if timestamp <= 0:
        return None
    return dt.datetime.fromtimestamp(timestamp).date().isoformat()


class DailyAkariDbMixin:
    """Mixin providing Daily Akari config and result storage methods."""

    @staticmethod
    def _dailyakari_select(table_name):
        return f'''
            SELECT
                message_id,
                guild_id,
                channel_id,
                user_id,
                puzzle_number,
                puzzle_date,
                accuracy,
                time_seconds,
                is_perfect
            FROM {table_name}
        '''

    def _dailyakari_filtered_union_query(self, guild_id, dlo=0, dhi=_NO_TIME_BOUND):
        guild_id = str(guild_id)
        params = [guild_id, guild_id]
        date_clauses = []
        dlo_text = _timestamp_to_date_text(dlo)
        if dlo_text is not None:
            date_clauses.append('puzzle_date >= ?')
            params.extend([dlo_text, dlo_text])
        dhi_text = _timestamp_to_date_text(dhi)
        if dhi_text is not None and dhi < _NO_TIME_BOUND:
            date_clauses.append('puzzle_date < ?')
            params.extend([dhi_text, dhi_text])

        extra = ''
        if date_clauses:
            extra = ' AND ' + ' AND '.join(date_clauses)

        query = f'''
            WITH dailyakari_all AS (
                {self._dailyakari_select('dailyakari_result')}
                WHERE guild_id = ? {extra}
                UNION ALL
                {self._dailyakari_select('dailyakari_import_result')}
                WHERE guild_id = ? {extra}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM dailyakari_result live
                      WHERE live.message_id = dailyakari_import_result.message_id
                  )
            ),
            first_per_user_puzzle AS (
                SELECT
                    guild_id,
                    user_id,
                    puzzle_number,
                    MIN(CAST(message_id AS INTEGER)) AS first_message_id
                FROM dailyakari_all
                GROUP BY guild_id, user_id, puzzle_number
            )
            SELECT *
            FROM (
                SELECT DISTINCT all_rows.*
                FROM dailyakari_all all_rows
                JOIN first_per_user_puzzle first_rows
                  ON all_rows.guild_id = first_rows.guild_id
                 AND all_rows.user_id = first_rows.user_id
                 AND all_rows.puzzle_number = first_rows.puzzle_number
                 AND CAST(all_rows.message_id AS INTEGER) = first_rows.first_message_id
            )
        '''
        return query, tuple(params)

    def get_dailyakari_channel(self, guild_id):
        guild_id = str(guild_id)
        row = self.conn.execute(
            'SELECT channel_id FROM dailyakari_config WHERE guild_id = ?',
            (guild_id,)
        ).fetchone()
        return row.channel_id if row else None

    def set_dailyakari_channel(self, guild_id, channel_id):
        guild_id = str(guild_id)
        self.conn.execute(
            'INSERT OR REPLACE INTO dailyakari_config (guild_id, channel_id) VALUES (?, ?)',
            (guild_id, str(channel_id))
        )
        self.conn.commit()

    def clear_dailyakari_channel(self, guild_id):
        guild_id = str(guild_id)
        rc = self.conn.execute(
            'DELETE FROM dailyakari_config WHERE guild_id = ?',
            (guild_id,)
        ).rowcount
        self.conn.commit()
        return rc

    def save_dailyakari_result(self, message_id, guild_id, channel_id, user_id, puzzle_number,
                               puzzle_date, accuracy, time_seconds, is_perfect):
        self.conn.execute(
            '''
            INSERT OR REPLACE INTO dailyakari_result (
                message_id, guild_id, channel_id, user_id, puzzle_number,
                puzzle_date, accuracy, time_seconds, is_perfect
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                str(message_id), str(guild_id), str(channel_id), str(user_id), int(puzzle_number),
                str(puzzle_date), int(accuracy), int(time_seconds), int(bool(is_perfect))
            )
        )
        self.conn.commit()

    def save_imported_dailyakari_result(self, message_id, guild_id, channel_id, user_id, puzzle_number,
                                        puzzle_date, accuracy, time_seconds, is_perfect):
        self.conn.execute(
            '''
            INSERT OR REPLACE INTO dailyakari_import_result (
                message_id, guild_id, channel_id, user_id, puzzle_number,
                puzzle_date, accuracy, time_seconds, is_perfect
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                str(message_id), str(guild_id), str(channel_id), str(user_id), int(puzzle_number),
                str(puzzle_date), int(accuracy), int(time_seconds), int(bool(is_perfect))
            )
        )
        self.conn.commit()

    def delete_dailyakari_result(self, message_id):
        rc = self.conn.execute(
            'DELETE FROM dailyakari_result WHERE message_id = ?',
            (str(message_id),)
        ).rowcount
        self.conn.commit()
        return rc

    def delete_imported_dailyakari_result(self, message_id):
        rc = self.conn.execute(
            'DELETE FROM dailyakari_import_result WHERE message_id = ?',
            (str(message_id),)
        ).rowcount
        self.conn.commit()
        return rc

    def clear_imported_dailyakari_results(self, guild_id):
        rc = self.conn.execute(
            'DELETE FROM dailyakari_import_result WHERE guild_id = ?',
            (str(guild_id),)
        ).rowcount
        self.conn.commit()
        return rc

    def get_dailyakari_result(self, message_id):
        return self.conn.execute(
            'SELECT * FROM dailyakari_result WHERE message_id = ?',
            (str(message_id),)
        ).fetchone()

    def get_dailyakari_result_for_user_puzzle(self, guild_id, user_id, puzzle_number):
        query, params = self._dailyakari_filtered_union_query(guild_id)
        return self.conn.execute(
            f'''
            {query}
            WHERE user_id = ? AND puzzle_number = ?
            ORDER BY CAST(message_id AS INTEGER) ASC
            LIMIT 1
            ''',
            params + (str(user_id), int(puzzle_number))
        ).fetchone()

    def get_dailyakari_results_for_user(self, guild_id, user_id, dlo=0, dhi=_NO_TIME_BOUND):
        query, params = self._dailyakari_filtered_union_query(guild_id, dlo, dhi)
        return self.conn.execute(
            f'''
            {query}
            WHERE user_id = ?
            ORDER BY puzzle_date DESC, puzzle_number DESC, time_seconds ASC, message_id DESC
            ''',
            params + (str(user_id),)
        ).fetchall()

    def get_dailyakari_results_for_users(self, guild_id, user_ids, dlo=0, dhi=_NO_TIME_BOUND):
        user_ids = [str(user_id) for user_id in user_ids]
        if not user_ids:
            return []
        placeholders = ','.join('?' * len(user_ids))
        query, params = self._dailyakari_filtered_union_query(guild_id, dlo, dhi)
        return self.conn.execute(
            f'''
            {query}
            WHERE user_id IN ({placeholders})
            ORDER BY puzzle_date DESC, puzzle_number DESC, time_seconds ASC, message_id DESC
            ''',
            params + tuple(user_ids)
        ).fetchall()

    def get_dailyakari_results_for_guild(self, guild_id, dlo=0, dhi=_NO_TIME_BOUND):
        query, params = self._dailyakari_filtered_union_query(guild_id, dlo, dhi)
        return self.conn.execute(
            f'''
            {query}
            ORDER BY puzzle_date DESC, puzzle_number DESC, time_seconds ASC, message_id DESC
            ''',
            params
        ).fetchall()

    def delete_dailyakari_result_for_user_puzzle(self, guild_id, user_id, puzzle_number):
        live_rc = self.conn.execute(
            '''
            DELETE FROM dailyakari_result
            WHERE guild_id = ? AND user_id = ? AND puzzle_number = ?
            ''',
            (str(guild_id), str(user_id), int(puzzle_number))
        ).rowcount
        imported_rc = self.conn.execute(
            '''
            DELETE FROM dailyakari_import_result
            WHERE guild_id = ? AND user_id = ? AND puzzle_number = ?
            ''',
            (str(guild_id), str(user_id), int(puzzle_number))
        ).rowcount
        self.conn.commit()
        return live_rc + imported_rc
