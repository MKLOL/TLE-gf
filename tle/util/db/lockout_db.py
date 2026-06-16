"""Lockout round DB methods — extracted from user_db_conn.py.

Owns the ``round_settings``, ``lockout_ongoing_rounds`` and
``lockout_finished_rounds`` tables.
"""
import logging
from collections import namedtuple

logger = logging.getLogger(__name__)


class LockoutDbMixin:
    """Mixin providing lockout-round DB methods."""

    def _create_lockout_tables(self):
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS round_settings (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS lockout_ongoing_rounds (
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "guild" TEXT,
                "users" TEXT,
                "rating" TEXT,
                "points" TEXT,
                "time" INT,
                "problems" TEXT,
                "status" TEXT,
                "duration" INTEGER,
                "repeat" INTEGER,
                "times" TEXT
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS lockout_finished_rounds(
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "guild" TEXT,
                "users" TEXT,
                "rating" TEXT,
                "points" TEXT,
                "time" INT,
                "problems" TEXT,
                "status" TEXT,
                "duration" INTEGER,
                "repeat" INTEGER,
                "times" TEXT,
                "end_time" INT
            )
            ''')

    def set_round_channel(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO round_settings '
                 ' (guild_id, channel_id) VALUES (?, ?)'
                 )
        with self.conn:
            self.conn.execute(query, (guild_id, channel_id))

    def get_round_channel(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM round_settings '
                 'WHERE guild_id = ?')
        channel_id = self.conn.execute(query, (guild_id,)).fetchone()
        return int(channel_id[0]) if channel_id else None

    def create_ongoing_round(self, guild_id, timestamp, users, rating, points, problems, duration, repeat):
        query = f'''
            INSERT INTO lockout_ongoing_rounds (guild, users, rating, points, time, problems, status, duration, repeat, times)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cur = self.conn.cursor()
        cur.execute(query, (guild_id, ' '.join([f"{x.id}" for x in users]),
                                      ' '.join(map(str, rating)),
                                      ' '.join(map(str, points)),
                                      timestamp,
                                      ' '.join([f"{x.contestId}/{x.index}" for x in problems]),
                                      ' '.join('0' for i in range(len(users))),
                                      duration,
                                      repeat,
                                      ' '.join(['0'] * len(users)))
                    )
        self.conn.commit()
        cur.close()

    def create_finished_round(self, round_info, timestamp):
        query = f'''
                    INSERT INTO lockout_finished_rounds (guild, users, rating, points, time, problems, status, duration, repeat, times, end_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
        cur = self.conn.cursor()
        cur.execute(query, (round_info.guild, round_info.users, round_info.rating, round_info.points, round_info.time,
                                round_info.problems, round_info.status, round_info.duration, round_info.repeat,
                                round_info.times, timestamp))
        self.conn.commit()
        cur.close()

    def update_round_status(self, guild, user, status, problems, timestamp):
        query = f"""
                    UPDATE lockout_ongoing_rounds
                    SET
                    status = ?,
                    problems = ?,
                    times = ?
                    WHERE
                    guild = ? AND users LIKE ?
                """
        cur = self.conn.cursor()
        cur.execute(query,
                     (' '.join([str(x) for x in status]), ' '.join(problems), ' '.join([str(x) for x in timestamp]),
                      guild, f"%{user}%"))
        self.conn.commit()
        cur.close()

    def get_round_info(self, guild_id, users):
        query = f'''
                    SELECT * FROM lockout_ongoing_rounds
                    WHERE
                    guild = ? AND users LIKE ?
                 '''
        cur = self.conn.cursor()
        cur.execute(query, (guild_id, f"%{users}%"))
        data = cur.fetchone()
        cur.close()
        Round = namedtuple('Round', 'guild users rating points time problems status duration repeat times')
        return Round(data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10])

    def check_if_user_in_ongoing_round(self, guild, user):
        query = f'''
                    SELECT * FROM lockout_ongoing_rounds
                    WHERE
                    users LIKE ? AND guild = ?
                '''
        cur = self.conn.cursor()
        cur.execute(query, (f"%{user}%", guild))
        data = cur.fetchall()
        cur.close()
        if len(data) > 0:
            return True
        return False

    def delete_round(self, guild, user):
        query = f'''
                    DELETE FROM lockout_ongoing_rounds
                    WHERE
                    guild = ? AND users LIKE ?
                '''
        cur = self.conn.cursor()
        cur.execute(query, (guild, f"%{user}%"))
        self.conn.commit()
        cur.close()

    def get_ongoing_rounds(self, guild):
        query = f'''
                    SELECT * FROM lockout_ongoing_rounds WHERE guild = ?
                '''
        cur = self.conn.cursor()
        cur.execute(query, (guild,))
        res = cur.fetchall()
        cur.close()
        Round = namedtuple('Round', 'guild users rating points time problems status duration repeat times')
        return [Round(data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10]) for data in res]

    def get_recent_rounds(self, guild, user=None):
        query = f'''
                    SELECT * FROM lockout_finished_rounds
                    WHERE guild = ? AND users LIKE ?
                    ORDER BY end_time DESC
                '''
        cur = self.conn.cursor()
        cur.execute(query, (guild, '%' if user is None else f'%{user}%'))
        res = cur.fetchall()
        cur.close()
        Round = namedtuple('Round', 'guild users rating points time problems status duration repeat times end_time')
        return [Round(data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11]) for data in res]
