"""Training DB methods — extracted from user_db_conn.py.

Owns the ``training_settings``, ``trainings`` and ``training_problems``
tables. The ``Training`` and ``TrainingProblemStatus`` enums are imported
lazily from the composing module to avoid an import cycle.
"""
import logging

logger = logging.getLogger(__name__)


class TrainingDbMixin:
    """Mixin providing training DB methods."""

    def _create_training_tables(self):
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS training_settings (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS trainings (
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "user_id" TEXT,
                "score" INTEGER,
                "lives" INTEGER,
                "time_left"     REAL,
                "mode"  INTEGER NOT NULL,
                "status" INTEGER NOT NULL
            )
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS training_problems (
                "id"	INTEGER PRIMARY KEY AUTOINCREMENT,
                "training_id"   INTEGER NOT NULL,
                "issue_time"	REAL NOT NULL,
                "finish_time"	REAL,
                "problem_name"	TEXT NOT NULL,
                "contest_id"	INTEGER NOT NULL,
                "p_index"	INTEGER NOT NULL,
                "rating"	INTEGER NOT NULL,
                "status"	INTEGER NOT NULL
            )
        ''')

    def set_training_channel(self, guild_id, channel_id):
        self._set_channel_setting('training_settings', guild_id, channel_id)

    def get_training_channel(self, guild_id):
        return self._get_channel_setting('training_settings', guild_id)

    def new_training(self, user_id, issue_time, prob, mode, score, lives, time_left):
        from tle.util.db.user_db_conn import Training, TrainingProblemStatus
        query1 = f'''
            INSERT INTO trainings
            (user_id, score, lives, time_left, mode, status)
            VALUES
            (?, 0, ?, ?, ?, {Training.ACTIVE})
        '''
        query2 = f'''
            INSERT INTO training_problems (training_id, issue_time, problem_name, contest_id, p_index, rating, status)
            VALUES (?, ?, ?, ?, ?, ?, {TrainingProblemStatus.ACTIVE})
        '''
        cur = self.conn.cursor()
        cur.execute(query1, (user_id, lives, time_left, mode))
        training_id, rc = cur.lastrowid, cur.rowcount
        if rc != 1:
            self.conn.rollback()
            return 0
        cur.execute(query2, (training_id, issue_time, prob.name, prob.contestId, prob.index, prob.rating))
        if cur.rowcount != 1:
            self.conn.rollback()
            return 0
        self.conn.commit()
        return 1


    def get_active_training(self, user_id):
        from tle.util.db.user_db_conn import Training, TrainingProblemStatus
        query1 = f'''
            SELECT id, mode, score, lives, time_left FROM trainings
            WHERE user_id = ? AND status = {Training.ACTIVE}
        '''
        res = self.conn.execute(query1, (user_id,)).fetchone()
        if res is None: return None
        training_id,mode,score,lives,time_left = res
        query2 = f'''
            SELECT issue_time, problem_name, contest_id, p_index, rating FROM training_problems
            WHERE training_id = ? AND status = {TrainingProblemStatus.ACTIVE}
        '''
        res = self.conn.execute(query2, (training_id,)).fetchone()
        if res is None: return None
        return training_id, res[0], res[1], res[2], res[3], res[4], mode, score, lives,time_left

    def get_latest_training(self, user_id):
        from tle.util.db.user_db_conn import Training
        query1 = f'''
            SELECT id, mode, score, lives, time_left FROM trainings
            WHERE user_id = ? AND status = {Training.COMPLETED} ORDER BY id DESC
        '''
        res = self.conn.execute(query1, (user_id,)).fetchone()
        if res is None: return None
        training_id,mode,score,lives,time_left = res
        return training_id, None, None, None, None, None, mode, score, lives,time_left


    def end_current_training_problem(self, training_id, finish_time, status, score, lives, time_left):
        from tle.util.db.user_db_conn import TrainingProblemStatus
        query1 = f'''
            UPDATE training_problems SET finish_time = ?, status = ?
            WHERE training_id = ? AND status = {TrainingProblemStatus.ACTIVE}
        '''
        query2 = '''
            UPDATE trainings SET score = ?, lives = ?, time_left = ?
            WHERE id = ?
        '''
        rc = self.conn.execute(query1, (finish_time, status, training_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return -1
        rc = self.conn.execute(query2, (score, lives, time_left, training_id)).rowcount
        if rc != 1:
            self.conn.rollback()
            return -2
        self.conn.commit()
        return 1

    def assign_training_problem(self, training_id, issue_time, prob):
        from tle.util.db.user_db_conn import TrainingProblemStatus
        query1 = f'''
            INSERT INTO training_problems (training_id, issue_time, problem_name, contest_id, p_index, rating, status)
            VALUES (?, ?, ?, ?, ?, ?, {TrainingProblemStatus.ACTIVE})
        '''

        cur = self.conn.cursor()
        cur.execute(query1, (training_id, issue_time, prob.name, prob.contestId, prob.index, prob.rating))
        if cur.rowcount != 1:
            self.conn.rollback()
            return -1
        self.conn.commit()
        return 1

    def finish_training(self, training_id):
        from tle.util.db.user_db_conn import Training
        query1 = f'''
            UPDATE trainings SET status = {Training.COMPLETED}
            WHERE id = ?
        '''
        rc = self.conn.execute(query1, (training_id,)).rowcount
        if rc != 1:
            self.conn.rollback()
            return -1
        self.conn.commit()
        return 1

    def get_training_skips(self, user_id):
        from tle.util.db.user_db_conn import TrainingProblemStatus
        query = f'''
            SELECT tp.problem_name
            FROM training_problems tp, trainings tr
            WHERE tp.training_id = tr.id
            AND (tp.status = {TrainingProblemStatus.SKIPPED} OR tp.status = {TrainingProblemStatus.INVALIDATED})
            AND tr.user_id = ?
        '''
        return {name for name, in self.conn.execute(query, (user_id,)).fetchall()}


    def train_get_num_solves(self, training_id):
        from tle.util.db.user_db_conn import TrainingProblemStatus
        query = f'''
            SELECT COUNT(*) FROM training_problems
            WHERE training_id = ? AND status == {TrainingProblemStatus.SOLVED}
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_num_skips(self, training_id):
        from tle.util.db.user_db_conn import TrainingProblemStatus
        query = f'''
            SELECT COUNT(*) FROM training_problems
            WHERE training_id = ? AND status == {TrainingProblemStatus.SKIPPED}
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_num_slow_solves(self, training_id):
        from tle.util.db.user_db_conn import TrainingProblemStatus
        query = f'''
            SELECT COUNT(*) FROM training_problems
            WHERE training_id = ? AND status == {TrainingProblemStatus.SOLVED_TOO_SLOW}
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_start_rating(self, training_id):
        query = f'''
            SELECT rating FROM training_problems
            WHERE training_id = ?
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_max_rating(self, training_id):
        from tle.util.db.user_db_conn import TrainingProblemStatus
        query = f'''
            SELECT MAX(rating) FROM training_problems
            WHERE training_id = ? AND status == {TrainingProblemStatus.SOLVED}
        '''
        return self.conn.execute(query, (training_id,)).fetchone()[0]

    def train_get_fastest_solves(self):
        from tle.util.db.user_db_conn import TrainingProblemStatus
        query = f'''
            SELECT tr.user_id, tp.rating, min(tp.finish_time-tp.issue_time)
            FROM training_problems tp, trainings tr
            WHERE tp.training_id = tr.id
            AND (tp.status = {TrainingProblemStatus.SOLVED} OR tp.status = {TrainingProblemStatus.SOLVED_TOO_SLOW})
            GROUP BY tp.rating
        '''
        return self.conn.execute(query).fetchall()
