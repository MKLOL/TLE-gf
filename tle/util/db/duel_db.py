"""Duel DB methods — extracted from user_db_conn.py.

Owns the ``duelist``, ``duel`` and ``duel_settings`` tables. The ``Duel``,
``Winner`` and ``DuelType`` enums are imported lazily from the composing
module to avoid an import cycle.
"""
import logging

logger = logging.getLogger(__name__)


class DuelDbMixin:
    """Mixin providing duel DB methods."""

    def _create_duel_tables(self):
        # TODO: Make duel tables guild-aware.
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS duelist(
                "user_id"	INTEGER PRIMARY KEY NOT NULL,
                "rating"	INTEGER NOT NULL,
                "guild_id"  TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS duel(
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "challenger"	INTEGER NOT NULL,
                "challengee"	INTEGER NOT NULL,
                "issue_time"	REAL NOT NULL,
                "start_time"	REAL,
                "finish_time"	REAL,
                "problem_name"	TEXT,
                "contest_id"	INTEGER,
                "p_index"	INTEGER,
                "status"	INTEGER,
                "winner"	INTEGER,
                "type"		INTEGER,
                "guild_id"  TEXT
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS duel_settings (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT
            )
        ''')

    def set_duel_channel(self, guild_id, channel_id):
        query = ('INSERT OR REPLACE INTO duel_settings '
                 ' (guild_id, channel_id) VALUES (?, ?)'
                 )
        with self.conn:
            self.conn.execute(query, (guild_id, channel_id))

    def get_duel_channel(self, guild_id):
        query = ('SELECT channel_id '
                 'FROM duel_settings '
                 'WHERE guild_id = ?')
        channel_id = self.conn.execute(query, (guild_id,)).fetchone()
        return int(channel_id[0]) if channel_id else None

    def check_duel_challenge(self, userid, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT id FROM duel
            WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND (status == {Duel.ONGOING} OR status == {Duel.PENDING})
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()

    def check_duel_accept(self, challengee, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT id, challenger, problem_name FROM duel
            WHERE challengee = ? AND guild_id = ? AND status == {Duel.PENDING}
        '''
        return self.conn.execute(query, (challengee,guild_id)).fetchone()

    def check_duel_decline(self, challengee, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT id, challenger FROM duel
            WHERE challengee = ? AND guild_id = ? AND status == {Duel.PENDING}
        '''
        return self.conn.execute(query, (challengee,guild_id)).fetchone()

    def check_duel_withdraw(self, challenger, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT id, challengee FROM duel
            WHERE challenger = ? AND guild_id = ? AND status == {Duel.PENDING}
        '''
        return self.conn.execute(query, (challenger,guild_id)).fetchone()

    def check_duel_draw(self, userid, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT id, challenger, challengee, start_time, type FROM duel
            WHERE (challenger = ? OR challengee = ?) AND guild_id = ? AND status == {Duel.ONGOING}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()

    def check_duel_giveup(self, userid, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT id, challenger, challengee, start_time, problem_name, contest_id, p_index, type FROM duel
            WHERE (challenger = ? OR challengee = ?) AND guild_id = ? AND status == {Duel.ONGOING}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()


    def check_duel_complete(self, userid, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT id, challenger, challengee, start_time, problem_name, contest_id, p_index, type FROM duel
            WHERE (challenger = ? OR challengee = ?) AND guild_id = ? AND status == {Duel.ONGOING}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()

    def create_duel(self, challenger, challengee, issue_time, prob, dtype, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            INSERT INTO duel (challenger, challengee, issue_time, problem_name, contest_id, p_index, status, type, guild_id) VALUES (?, ?, ?, ?, ?, ?, {Duel.PENDING}, ?, ?)
        '''
        duelid = self.conn.execute(query, (challenger, challengee, issue_time, prob.name, prob.contestId, prob.index, dtype, guild_id)).lastrowid
        self.conn.commit()
        return duelid

    def cancel_duel(self, duelid, guild_id, status):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            UPDATE duel SET status = ? WHERE id = ? AND guild_id = ? AND status = {Duel.PENDING}
        '''
        rc = self.conn.execute(query, (status, duelid, guild_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return rc

    def invalidate_duel(self, duelid, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            UPDATE duel SET status = {Duel.INVALID} WHERE id = ? AND guild_id = ? AND status = {Duel.ONGOING}
        '''
        rc = self.conn.execute(query, (duelid,guild_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return rc

    def start_duel(self, duelid, guild_id, start_time):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            UPDATE duel SET start_time = ?, status = {Duel.ONGOING}
            WHERE id = ? AND guild_id = ? AND status = {Duel.PENDING}
        '''
        rc = self.conn.execute(query, (start_time, duelid, guild_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return rc

    def complete_duel(self, duelid, guild_id, winner, finish_time, winner_id = -1, loser_id = -1, delta = 0, dtype = None):
        from tle.util.db.user_db_conn import Duel, DuelType
        if dtype is None:
            dtype = DuelType.OFFICIAL
        query = f'''
            UPDATE duel SET status = {Duel.COMPLETE}, finish_time = ?, winner = ? WHERE id = ? AND guild_id = ? AND status = {Duel.ONGOING}
        '''
        rc = self.conn.execute(query, (finish_time, winner, duelid, guild_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return 0

        if dtype == DuelType.OFFICIAL or dtype == DuelType.ADJOFFICIAL:
            self.update_duel_rating(winner_id, guild_id, +delta)
            self.update_duel_rating(loser_id, guild_id, -delta)

        self.conn.commit()
        return 1

    def update_duel_rating(self, userid, guild_id, delta):
        query = '''
            UPDATE duelist SET rating = rating + ? WHERE user_id = ? AND guild_id = ?
        '''
        rc = self.conn.execute(query, (delta, userid, guild_id)).rowcount
        self.conn.commit()
        return rc

    def get_duel_wins(self, userid, guild_id):
        from tle.util.db.user_db_conn import Duel, Winner
        query = f'''
            SELECT start_time, finish_time, problem_name, challenger, challengee FROM duel
            WHERE ((challenger = ? AND winner == {Winner.CHALLENGER}) OR (challengee = ? AND winner == {Winner.CHALLENGEE})) AND status = {Duel.COMPLETE} AND guild_id = ?
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchall()

    def get_duels(self, userid, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT id, start_time, finish_time, problem_name, challenger, challengee, winner FROM duel WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND status == {Duel.COMPLETE} ORDER BY start_time DESC
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchall()

    def get_duel_problem_names(self, userid, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT problem_name FROM duel WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND (status == {Duel.COMPLETE} OR status == {Duel.INVALID})
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchall()

    def get_pair_duels(self, userid1, userid2, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT id, start_time, finish_time, problem_name, challenger, challengee, winner FROM duel
            WHERE ((challenger = ? AND challengee = ?) OR (challenger = ? AND challengee = ?)) AND guild_id = ? AND status == {Duel.COMPLETE} ORDER BY start_time DESC
        '''
        return self.conn.execute(query, (userid1, userid2, userid2, userid1, guild_id)).fetchall()

    def get_recent_duels(self, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT id, start_time, finish_time, problem_name, challenger, challengee, winner FROM duel WHERE status == {Duel.COMPLETE} AND guild_id = ? ORDER BY start_time DESC LIMIT 7
        '''
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_ongoing_duels(self, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT id, challenger, challengee, start_time, problem_name, contest_id, p_index, type FROM duel
            WHERE status == {Duel.ONGOING} AND guild_id = ? ORDER BY start_time DESC
        '''
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_num_duel_completed(self, userid, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT COUNT(*) FROM duel WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND status == {Duel.COMPLETE}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()[0]

    def get_num_duel_draws(self, userid, guild_id):
        from tle.util.db.user_db_conn import Winner
        query = f'''
            SELECT COUNT(*) FROM duel WHERE (challengee = ? OR challenger = ?) AND guild_id = ? AND winner == {Winner.DRAW}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()[0]

    def get_num_duel_losses(self, userid, guild_id):
        from tle.util.db.user_db_conn import Duel, Winner
        query = f'''
            SELECT COUNT(*) FROM duel
            WHERE ((challengee = ? AND winner == {Winner.CHALLENGER}) OR (challenger = ? AND winner == {Winner.CHALLENGEE})) AND guild_id = ? AND status = {Duel.COMPLETE}
        '''
        return self.conn.execute(query, (userid, userid, guild_id)).fetchone()[0]

    def get_num_duel_declined(self, userid, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT COUNT(*) FROM duel WHERE challengee = ? AND guild_id = ? AND status == {Duel.DECLINED}
        '''
        return self.conn.execute(query, (userid, guild_id)).fetchone()[0]

    def get_num_duel_rdeclined(self, userid, guild_id):
        from tle.util.db.user_db_conn import Duel
        query = f'''
            SELECT COUNT(*) FROM duel WHERE challenger = ? AND guild_id = ? AND status == {Duel.DECLINED}
        '''
        return self.conn.execute(query, (userid,guild_id)).fetchone()[0]

    def get_duel_rating(self, userid, guild_id):
        query = '''
            SELECT rating FROM duelist WHERE user_id = ? AND guild_id = ?
        '''
        return self.conn.execute(query, (userid,guild_id)).fetchone()[0]

    def is_duelist(self, userid, guild_id):
        query = '''
            SELECT 1 FROM duelist WHERE user_id = ? AND guild_id = ?
        '''
        return self.conn.execute(query, (userid,guild_id)).fetchone()

    def register_duelist(self, userid, guild_id):
        query = '''
            INSERT OR IGNORE INTO duelist (user_id, rating, guild_id)
            VALUES (?, 1500, ?)
        '''
        with self.conn:
            return self.conn.execute(query, (userid,guild_id)).rowcount

    def get_duelists(self, guild_id):
        query = '''
            SELECT user_id, rating FROM duelist WHERE guild_id = ? ORDER BY rating DESC
        '''
        return self.conn.execute(query, (guild_id,)).fetchall()

    def get_complete_official_duels(self, guild_id):
        from tle.util.db.user_db_conn import Duel, DuelType
        query = f'''
            SELECT challenger, challengee, winner, finish_time FROM duel WHERE status={Duel.COMPLETE}
            AND (type={DuelType.OFFICIAL} OR type={DuelType.ADJOFFICIAL}) AND guild_id = ? ORDER BY finish_time ASC
        '''
        return self.conn.execute(query, (guild_id,)).fetchall()
