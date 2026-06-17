"""Soccer betting steal DB methods — extracted from betting_wallet_db.py to
keep that module under the file-size limit.

Owns the once-per-UTC-day ``;bet steal`` flow. Relies on wallet helpers
(``_bet_ensure_wallet_txn``, ``bet_get_balance``, ``_bet_log_wallet_txn``)
provided by ``BettingWalletDbMixin`` on the same composed ``UserDbConn``.
"""
import logging
import time

logger = logging.getLogger(__name__)


class BettingStealDbMixin:
    """Mixin providing the betting steal DB methods."""

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
