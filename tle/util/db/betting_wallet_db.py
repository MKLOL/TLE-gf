"""Soccer betting wallet DB methods — extracted from user_db_conn.py.

Owns the betting tables (``bet_wallet``, ``bet_market``, ``bet_wager``,
``bet_wallet_txn``) plus wallet bookkeeping and leaderboards. The market /
wager / settlement methods live in ``BettingMarketDbMixin``. The
``bet_fixture_key`` and ``_bet_*`` module helpers live in ``user_db_conn`` and
are imported lazily where needed to avoid an import cycle.
"""
import logging
import time
from collections import defaultdict, namedtuple

logger = logging.getLogger(__name__)


class BettingWalletDbMixin:
    """Mixin providing betting wallet DB methods and table creation."""

    def _create_betting_tables(self):
        # Soccer betting minigame (see tle/cogs/betting.py). Kept in sync with
        # betting upgrades — fresh DBs get the tables here, existing DBs via
        # migrations.
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS bet_wallet (
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                balance     INTEGER NOT NULL,
                last_daily  TEXT,
                PRIMARY KEY (guild_id, user_id)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS bet_market (
                market_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id       TEXT NOT NULL,
                channel_id     TEXT NOT NULL,
                message_id     TEXT,
                thread_id      TEXT,
                thread_intro_id TEXT,
                event_id       TEXT NOT NULL,
                fixture_key    TEXT NOT NULL,
                sport_key      TEXT NOT NULL,
                home_team      TEXT NOT NULL,
                away_team      TEXT NOT NULL,
                commence_time  REAL NOT NULL,
                odds_home      REAL NOT NULL,
                odds_draw      REAL NOT NULL,
                odds_away      REAL NOT NULL,
                status         TEXT NOT NULL DEFAULT 'open',
                bets_closed    INTEGER NOT NULL DEFAULT 0,
                result         TEXT,
                result_home    INTEGER,
                result_away    INTEGER,
                created_by     TEXT NOT NULL,
                created_at     REAL NOT NULL,
                settled_at     REAL
            )
        ''')
        bet_market_cols = [
            row.name for row in self.conn.execute('PRAGMA table_info(bet_market)')
        ]
        if 'fixture_key' not in bet_market_cols:
            self.conn.execute('ALTER TABLE bet_market ADD COLUMN fixture_key TEXT')
        if 'thread_intro_id' not in bet_market_cols:
            self.conn.execute('ALTER TABLE bet_market ADD COLUMN thread_intro_id TEXT')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_bet_market_active
                ON bet_market (guild_id, channel_id, status)
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_bet_market_pending
                ON bet_market (status, commence_time)
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_bet_market_thread
                ON bet_market (guild_id, thread_id, status)
        ''')
        self.conn.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS idx_bet_market_open_event
                ON bet_market (guild_id, event_id)
                WHERE status = 'open'
        ''')
        missing_fixture_key = self.conn.execute(
            'SELECT 1 FROM bet_market '
            'WHERE fixture_key IS NULL OR fixture_key = "" LIMIT 1'
        ).fetchone()
        if missing_fixture_key is None:
            self.conn.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bet_market_open_fixture
                    ON bet_market (guild_id, fixture_key)
                    WHERE status = 'open'
            ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS bet_wager (
                market_id   INTEGER NOT NULL,
                user_id     TEXT NOT NULL,
                pick        TEXT NOT NULL,
                stake       INTEGER NOT NULL,
                placed_at   REAL NOT NULL,
                PRIMARY KEY (market_id, user_id, pick)
            )
        ''')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS bet_wallet_txn (
                txn_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id      TEXT NOT NULL,
                user_id       TEXT NOT NULL,
                actor_id      TEXT,
                action        TEXT NOT NULL,
                amount        INTEGER NOT NULL,
                balance_after INTEGER NOT NULL,
                market_id     INTEGER,
                note          TEXT,
                created_at    REAL NOT NULL
            )
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_bet_wallet_txn_user
                ON bet_wallet_txn (guild_id, user_id, created_at DESC)
        ''')

    # -- Wallet --

    def _bet_log_wallet_txn(self, guild_id, user_id, actor_id, action, amount,
                            balance_after, market_id=None, note=None,
                            created_at=None):
        self.conn.execute(
            'INSERT INTO bet_wallet_txn '
            '(guild_id, user_id, actor_id, action, amount, balance_after, '
            'market_id, note, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (str(guild_id), str(user_id), None if actor_id is None else str(actor_id),
             action, int(amount), int(balance_after), market_id, note,
             time.time() if created_at is None else created_at)
        )

    def _bet_ensure_wallet_txn(self, guild_id, user_id, start_balance,
                               created_at=None):
        """Create a betting wallet inside the caller's transaction."""
        guild_id, user_id = str(guild_id), str(user_id)
        cur = self.conn.execute(
            'INSERT OR IGNORE INTO bet_wallet (guild_id, user_id, balance) '
            'VALUES (?, ?, ?)',
            (guild_id, user_id, start_balance)
        )
        if cur.rowcount:
            self._bet_log_wallet_txn(
                guild_id, user_id, None, 'init', start_balance, start_balance,
                created_at=created_at)
        row = self.conn.execute(
            'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
            (guild_id, user_id)
        ).fetchone()
        return row.balance

    def bet_ensure_wallet(self, guild_id, user_id, start_balance):
        """Create a wallet seeded at start_balance if absent; return the
        current balance either way."""
        with self.conn:
            return self._bet_ensure_wallet_txn(
                guild_id, user_id, start_balance)

    def bet_get_balance(self, guild_id, user_id):
        """Return a user's balance, or None if they have no wallet yet."""
        row = self.conn.execute(
            'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))
        ).fetchone()
        return row.balance if row else None

    def _bet_steal_key(self, guild_id, thief_id):
        return f'bet_steal:{guild_id}:{thief_id}'

    def _bet_steal_preview_txn(self, guild_id, thief_id, victim_id, today,
                               start_balance, created_at=None):
        last = self.conn.execute(
            'SELECT value FROM kvs WHERE key = ?',
            (self._bet_steal_key(guild_id, thief_id),)
        ).fetchone()
        thief_balance = self._bet_ensure_wallet_txn(
            guild_id, thief_id, start_balance, created_at=created_at)
        if last is not None and last.value == today:
            victim_balance = self.bet_get_balance(guild_id, victim_id)
            return (False, 'already', thief_balance, victim_balance, 0)

        victim_balance = self.bet_get_balance(guild_id, victim_id)
        if victim_balance is None:
            return (False, 'missing', thief_balance, None, 0)
        max_stolen = min(victim_balance // 2, thief_balance // 2)
        if max_stolen <= 0:
            return (False, 'empty', thief_balance, victim_balance, 0)
        return (True, 'ok', thief_balance, victim_balance, max_stolen)

    def bet_steal_preview(self, guild_id, thief_id, victim_id, today,
                          start_balance):
        """Return whether a steal can be confirmed and the max steal amount."""
        guild_id = str(guild_id)
        thief_id = str(thief_id)
        victim_id = str(victim_id)
        if thief_id == victim_id:
            bal = self.bet_get_balance(guild_id, thief_id)
            return (False, 'self', bal, bal, 0)
        with self.conn:
            return self._bet_steal_preview_txn(
                guild_id, thief_id, victim_id, today, start_balance)

    def bet_attempt_steal(self, guild_id, thief_id, victim_id, today, success,
                          start_balance, attempted_at=None):
        """Run a once-per-UTC-day steal attempt.

        The victim must already have a wallet. On success, the thief takes up
        to half of their own wallet, capped by half of the victim's wallet. On
        failure, the thief's wallet is zeroed. Returns
        (attempted, reason, thief_balance, victim_balance, stolen).
        """
        guild_id = str(guild_id)
        thief_id = str(thief_id)
        victim_id = str(victim_id)
        if thief_id == victim_id:
            bal = self.bet_get_balance(guild_id, thief_id)
            return (False, 'self', bal, bal, 0)

        key = self._bet_steal_key(guild_id, thief_id)
        when = time.time() if attempted_at is None else attempted_at
        with self.conn:
            attempted, reason, thief_balance, victim_balance, max_stolen = (
                self._bet_steal_preview_txn(
                    guild_id, thief_id, victim_id, today, start_balance,
                    created_at=when))
            if not attempted:
                return (False, reason, thief_balance, victim_balance, 0)
            self.conn.execute(
                'INSERT OR REPLACE INTO kvs (key, value) VALUES (?, ?)',
                (key, today)
            )
            if success:
                stolen = max_stolen
                new_thief = thief_balance + stolen
                new_victim = victim_balance - stolen
                self.conn.execute(
                    'UPDATE bet_wallet SET balance = ? '
                    'WHERE guild_id = ? AND user_id = ?',
                    (new_thief, guild_id, thief_id)
                )
                self.conn.execute(
                    'UPDATE bet_wallet SET balance = ? '
                    'WHERE guild_id = ? AND user_id = ?',
                    (new_victim, guild_id, victim_id)
                )
                self._bet_log_wallet_txn(
                    guild_id, thief_id, thief_id, 'steal_success', stolen,
                    new_thief, note=victim_id, created_at=when)
                self._bet_log_wallet_txn(
                    guild_id, victim_id, thief_id, 'steal_victim', -stolen,
                    new_victim, note=thief_id, created_at=when)
                return (True, 'ok', new_thief, new_victim, stolen)

            stolen = thief_balance
            self.conn.execute(
                'UPDATE bet_wallet SET balance = 0 '
                'WHERE guild_id = ? AND user_id = ?',
                (guild_id, thief_id)
            )
            self._bet_log_wallet_txn(
                guild_id, thief_id, thief_id, 'steal_caught', -stolen, 0,
                note=victim_id, created_at=when)
            return (True, 'caught', 0, victim_balance, 0)

    def bet_wallet_get(self, guild_id, user_id):
        """Return a wallet row, or None if the user has no wallet yet."""
        return self.conn.execute(
            'SELECT guild_id, user_id, balance, last_daily '
            'FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))
        ).fetchone()

    def bet_wallet_history(self, guild_id, user_id, limit=15):
        """Return recent wallet audit entries for a user, newest first."""
        limit = max(1, min(int(limit), 50))
        return self.conn.execute(
            'SELECT txn_id, guild_id, user_id, actor_id, action, amount, '
            'balance_after, market_id, note, created_at '
            'FROM bet_wallet_txn WHERE guild_id = ? AND user_id = ? '
            'ORDER BY created_at DESC, txn_id DESC LIMIT ?',
            (str(guild_id), str(user_id), limit)
        ).fetchall()

    def bet_claim_daily(self, guild_id, user_id, today, amount, start_balance):
        """Grant a flat `amount` once per UTC day.

        Returns (granted, new_balance, reason) where reason is 'ok' or
        'already' (already claimed today).
        """
        guild_id, user_id = str(guild_id), str(user_id)
        self.bet_ensure_wallet(guild_id, user_id, start_balance)
        row = self.conn.execute(
            'SELECT balance, last_daily FROM bet_wallet '
            'WHERE guild_id = ? AND user_id = ?',
            (guild_id, user_id)
        ).fetchone()
        if row.last_daily == today:
            return (False, row.balance, 'already')
        new_balance = row.balance + amount
        self.conn.execute(
            'UPDATE bet_wallet SET balance = ?, last_daily = ? '
            'WHERE guild_id = ? AND user_id = ?',
            (new_balance, today, guild_id, user_id)
        )
        self._bet_log_wallet_txn(
            guild_id, user_id, user_id, 'daily', amount, new_balance,
            note=today)
        self.conn.commit()
        return (True, new_balance, 'ok')

    def bet_transfer(self, guild_id, sender_id, receiver_id, amount,
                     start_balance, transferred_at=None, actor_id=None):
        """Move coins from one user wallet to another.

        Returns (ok, reason, sender_balance, receiver_balance). On
        insufficient funds, the receiver wallet is not created or modified.
        """
        guild_id = str(guild_id)
        sender_id = str(sender_id)
        receiver_id = str(receiver_id)
        actor_id = sender_id if actor_id is None else str(actor_id)
        amount = int(amount)
        if sender_id == receiver_id:
            bal = self.bet_get_balance(guild_id, sender_id)
            return (False, 'self', bal, bal)
        if amount <= 0:
            bal = self.bet_get_balance(guild_id, sender_id)
            return (False, 'invalid', bal, self.bet_get_balance(guild_id, receiver_id))

        when = time.time() if transferred_at is None else transferred_at
        with self.conn:
            sender_balance = self._bet_ensure_wallet_txn(
                guild_id, sender_id, start_balance, created_at=when)
            if amount > sender_balance:
                return (False, 'insufficient', sender_balance,
                        self.bet_get_balance(guild_id, receiver_id))
            receiver_balance = self._bet_ensure_wallet_txn(
                guild_id, receiver_id, start_balance, created_at=when)
            new_sender = sender_balance - amount
            new_receiver = receiver_balance + amount
            self.conn.execute(
                'UPDATE bet_wallet SET balance = ? '
                'WHERE guild_id = ? AND user_id = ?',
                (new_sender, guild_id, sender_id)
            )
            self.conn.execute(
                'UPDATE bet_wallet SET balance = ? '
                'WHERE guild_id = ? AND user_id = ?',
                (new_receiver, guild_id, receiver_id)
            )
            self._bet_log_wallet_txn(
                guild_id, sender_id, actor_id, 'transfer_out', -amount,
                new_sender, note=receiver_id, created_at=when)
            self._bet_log_wallet_txn(
                guild_id, receiver_id, actor_id, 'transfer_in', amount,
                new_receiver, note=sender_id, created_at=when)
            return (True, 'ok', new_sender, new_receiver)

    def bet_balance_leaderboard(self, guild_id):
        """Return [(user_id, balance)] for the guild, richest first.

        Only users who have actually placed at least one wager in the guild
        are included; wallets created via daily claims or balance checks
        without any bets are excluded.
        """
        return self.conn.execute(
            'SELECT wal.user_id, wal.balance FROM bet_wallet wal '
            'WHERE wal.guild_id = ? AND EXISTS ('
            '    SELECT 1 FROM bet_wager w '
            '    JOIN bet_market m ON m.market_id = w.market_id '
            '    WHERE m.guild_id = wal.guild_id AND w.user_id = wal.user_id'
            ') '
            'ORDER BY wal.balance DESC, wal.user_id ASC',
            (str(guild_id),)
        ).fetchall()

    def bet_profit_leaderboard(self, guild_id):
        """Return realized profit per user over settled markets.

        Payout is computed, not stored: for a winning wager it is
        round(stake × the market's frozen odds for that pick), else 0. Each
        row: user_id, profit (sum payout − sum stake), bets, wins. Voided/open
        markets are excluded by the status filter.
        """
        from tle.util.db.user_db_conn import _bet_odds_for_pick, _bet_pick_wins
        rows = self.conn.execute(
            'SELECT w.user_id, w.pick, w.stake, m.result, '
            '       m.odds_home, m.odds_draw, m.odds_away '
            'FROM bet_wager w JOIN bet_market m ON m.market_id = w.market_id '
            "WHERE m.guild_id = ? AND m.status = 'settled'",
            (str(guild_id),)
        ).fetchall()
        totals = defaultdict(lambda: {'profit': 0, 'bets': 0, 'wins': 0})
        for row in rows:
            odds_map = {'home': row.odds_home, 'draw': row.odds_draw,
                        'away': row.odds_away}
            odds = _bet_odds_for_pick(odds_map, row.pick)
            won = _bet_pick_wins(row.pick, row.result)
            payout = int(round(row.stake * odds)) if won and odds is not None else 0
            acc = totals[row.user_id]
            acc['profit'] += payout - row.stake
            acc['bets'] += 1
            acc['wins'] += 1 if won else 0
        Row = namedtuple('BetProfitRow', 'user_id profit bets wins')
        out = [Row(user_id, values['profit'], values['bets'], values['wins'])
               for user_id, values in totals.items()]
        out.sort(key=lambda r: (-r.profit, -r.wins, r.user_id))
        return out

    # -- Moderator tools --

    def bet_adjust_balance(self, guild_id, user_id, delta, start_balance,
                           actor_id=None, action=None, note=None):
        """Add delta (may be negative) to a wallet, creating it at
        start_balance first. Floors at 0. Returns the new balance."""
        guild_id, user_id = str(guild_id), str(user_id)
        self.bet_ensure_wallet(guild_id, user_id, start_balance)
        before = self.bet_get_balance(guild_id, user_id)
        new_balance = max(0, before + int(delta))
        self.conn.execute(
            'UPDATE bet_wallet SET balance = ? WHERE guild_id = ? AND user_id = ?',
            (new_balance, guild_id, user_id)
        )
        actual_delta = new_balance - before
        if actual_delta or action:
            self._bet_log_wallet_txn(
                guild_id, user_id, actor_id, action or 'adjust', actual_delta,
                new_balance, note=note)
        self.conn.commit()
        return new_balance

    def bet_set_balance(self, guild_id, user_id, value, start_balance,
                        actor_id=None, action='setbalance', note=None):
        """Set a wallet to an absolute value (floored at 0). Returns it."""
        guild_id, user_id = str(guild_id), str(user_id)
        value = max(0, int(value))
        self.bet_ensure_wallet(guild_id, user_id, start_balance)
        before = self.bet_get_balance(guild_id, user_id)
        self.conn.execute(
            'UPDATE bet_wallet SET balance = ? WHERE guild_id = ? AND user_id = ?',
            (value, guild_id, user_id)
        )
        self._bet_log_wallet_txn(
            guild_id, user_id, actor_id, action, value - before, value, note=note)
        self.conn.commit()
        return value
