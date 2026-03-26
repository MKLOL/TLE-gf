"""Database methods for the Daily Akari add-on."""


class DailyAkariDbMixin:
    """Mixin providing Daily Akari config and result storage methods."""

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

    def delete_dailyakari_result(self, message_id):
        rc = self.conn.execute(
            'DELETE FROM dailyakari_result WHERE message_id = ?',
            (str(message_id),)
        ).rowcount
        self.conn.commit()
        return rc

    def get_dailyakari_result(self, message_id):
        return self.conn.execute(
            'SELECT * FROM dailyakari_result WHERE message_id = ?',
            (str(message_id),)
        ).fetchone()

    def get_dailyakari_result_for_user_puzzle(self, guild_id, user_id, puzzle_number):
        return self.conn.execute(
            '''
            SELECT *
            FROM dailyakari_result
            WHERE guild_id = ? AND user_id = ? AND puzzle_number = ?
            ''',
            (str(guild_id), str(user_id), int(puzzle_number))
        ).fetchone()

    def get_dailyakari_results_for_user(self, guild_id, user_id):
        return self.conn.execute(
            '''
            SELECT *
            FROM dailyakari_result
            WHERE guild_id = ? AND user_id = ?
            ORDER BY puzzle_date DESC, puzzle_number DESC, time_seconds ASC, message_id DESC
            ''',
            (str(guild_id), str(user_id))
        ).fetchall()

    def get_dailyakari_results_for_users(self, guild_id, user_ids):
        user_ids = [str(user_id) for user_id in user_ids]
        if not user_ids:
            return []
        placeholders = ','.join('?' * len(user_ids))
        return self.conn.execute(
            f'''
            SELECT *
            FROM dailyakari_result
            WHERE guild_id = ? AND user_id IN ({placeholders})
            ORDER BY puzzle_date DESC, puzzle_number DESC, time_seconds ASC, message_id DESC
            ''',
            (str(guild_id), *user_ids)
        ).fetchall()

    def delete_dailyakari_result_for_user_puzzle(self, guild_id, user_id, puzzle_number):
        rc = self.conn.execute(
            '''
            DELETE FROM dailyakari_result
            WHERE guild_id = ? AND user_id = ? AND puzzle_number = ?
            ''',
            (str(guild_id), str(user_id), int(puzzle_number))
        ).rowcount
        self.conn.commit()
        return rc
