"""Soccer betting wager / settlement DB methods — extracted from user_db_conn.py.

Operates on the ``bet_market`` and ``bet_wager`` tables created by
``BettingWalletDbMixin._create_betting_tables`` and uses wallet helpers
(``_bet_log_wallet_txn``, ``bet_get_balance``) provided by that mixin on the
composed ``UserDbConn``. The ``_bet_*`` module helpers live in
``user_db_conn`` and are imported lazily to avoid an import cycle.
"""
import logging

logger = logging.getLogger(__name__)


class BettingWagerDbMixin:
    """Mixin providing betting wager / settlement DB methods."""

    # -- Wagers --

    def _bet_market_is_bettable(self, guild_id, market_id):
        row = self.conn.execute(
            'SELECT status, bets_closed FROM bet_market '
            'WHERE guild_id = ? AND market_id = ?',
            (str(guild_id), market_id)
        ).fetchone()
        return row is not None and row.status == 'open' and not row.bets_closed

    def bet_place(self, guild_id, market_id, user_id, pick, stake,
                  placed_at, start_balance):
        """Place or replace a user's wager on one pick, escrowing the stake
        from their wallet. Re-betting the same pick refunds the previous stake
        first; other picks by the same user stay in place. Odds are not stored
        — they are the market's frozen odds_<pick>.

        Returns (ok, reason, new_balance) where reason is 'ok', 'unchanged',
        'invalid', 'closed', or 'insufficient'. Atomic: wallet debit and wager
        write commit together.
        """
        guild_id, user_id = str(guild_id), str(user_id)
        pick = str(pick)
        stake = int(stake)
        if stake <= 0:
            return (False, 'invalid', self.bet_get_balance(guild_id, user_id))
        with self.conn:
            balance_row = self.conn.execute(
                'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
                (guild_id, user_id)
            ).fetchone()
            if not self._bet_market_is_bettable(guild_id, market_id):
                balance = balance_row.balance if balance_row else None
                return (False, 'closed', balance)
            cur = self.conn.execute(
                'INSERT OR IGNORE INTO bet_wallet (guild_id, user_id, balance) '
                'VALUES (?, ?, ?)',
                (guild_id, user_id, start_balance)
            )
            if cur.rowcount:
                self._bet_log_wallet_txn(
                    guild_id, user_id, None, 'init', start_balance, start_balance,
                    created_at=placed_at)
            balance = self.conn.execute(
                'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
                (guild_id, user_id)
            ).fetchone().balance
            prev = self.conn.execute(
                'SELECT stake FROM bet_wager WHERE market_id = ? '
                'AND user_id = ? AND pick = ?',
                (market_id, user_id, pick)
            ).fetchone()
            if prev and prev.stake == stake:
                return (True, 'unchanged', balance)
            available = balance + (prev.stake if prev else 0)
            if stake > available:
                return (False, 'insufficient', balance)
            new_balance = available - stake
            if prev:
                self._bet_log_wallet_txn(
                    guild_id, user_id, user_id, 'wager_refund', prev.stake,
                    available, market_id=market_id, note=pick,
                    created_at=placed_at)
            self.conn.execute(
                'INSERT OR REPLACE INTO bet_wager '
                '(market_id, user_id, pick, stake, placed_at) '
                'VALUES (?, ?, ?, ?, ?)',
                (market_id, user_id, pick, stake, placed_at)
            )
            self.conn.execute(
                'UPDATE bet_wallet SET balance = ? WHERE guild_id = ? AND user_id = ?',
                (new_balance, guild_id, user_id)
            )
            self._bet_log_wallet_txn(
                guild_id, user_id, user_id, 'wager_stake', -stake, new_balance,
                market_id=market_id, note=pick, created_at=placed_at)
            return (True, 'ok', new_balance)

    def bet_remove_wager(self, guild_id, market_id, user_id, pick, removed_at):
        """Remove one wager pick for a user and refund its stake.

        Returns (ok, reason, new_balance, refunded) where reason is 'removed',
        'missing', or 'closed'. Other picks by the same user stay in place.
        """
        guild_id, user_id, pick = str(guild_id), str(user_id), str(pick)
        with self.conn:
            balance_row = self.conn.execute(
                'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
                (guild_id, user_id)
            ).fetchone()
            balance = balance_row.balance if balance_row else None
            if not self._bet_market_is_bettable(guild_id, market_id):
                return (False, 'closed', balance, 0)
            prev = self.conn.execute(
                'SELECT stake FROM bet_wager WHERE market_id = ? '
                'AND user_id = ? AND pick = ?',
                (market_id, user_id, pick)
            ).fetchone()
            if prev is None:
                return (False, 'missing', balance, 0)
            if balance_row is None:
                self.conn.execute(
                    'INSERT INTO bet_wallet (guild_id, user_id, balance) '
                    'VALUES (?, ?, 0)',
                    (guild_id, user_id)
                )
                balance = 0
            new_balance = (balance or 0) + prev.stake
            self.conn.execute(
                'DELETE FROM bet_wager WHERE market_id = ? AND user_id = ? '
                'AND pick = ?',
                (market_id, user_id, pick)
            )
            self.conn.execute(
                'UPDATE bet_wallet SET balance = ? WHERE guild_id = ? AND user_id = ?',
                (new_balance, guild_id, user_id)
            )
            self._bet_log_wallet_txn(
                guild_id, user_id, user_id, 'wager_refund', prev.stake,
                new_balance, market_id=market_id, note=pick,
                created_at=removed_at)
            return (True, 'removed', new_balance, prev.stake)

    def bet_remove_wagers_for_user(self, guild_id, market_id, user_id, removed_at):
        """Remove all wagers by one user on one market and refund their stakes.

        Returns (ok, reason, new_balance, refunded, count) where reason is
        'removed', 'missing', or 'closed'. Wagers by the same user on other
        markets are untouched.
        """
        guild_id, user_id = str(guild_id), str(user_id)
        with self.conn:
            balance_row = self.conn.execute(
                'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
                (guild_id, user_id)
            ).fetchone()
            balance = balance_row.balance if balance_row else None
            if not self._bet_market_is_bettable(guild_id, market_id):
                return (False, 'closed', balance, 0, 0)
            wagers = self.conn.execute(
                'SELECT pick, stake FROM bet_wager '
                'WHERE market_id = ? AND user_id = ? '
                'ORDER BY placed_at ASC, pick ASC',
                (market_id, user_id)
            ).fetchall()
            if not wagers:
                return (False, 'missing', balance, 0, 0)
            if balance_row is None:
                self.conn.execute(
                    'INSERT INTO bet_wallet (guild_id, user_id, balance) '
                    'VALUES (?, ?, 0)',
                    (guild_id, user_id)
                )
                balance = 0
            total_refunded = sum(w.stake for w in wagers)
            new_balance = balance + total_refunded
            self.conn.execute(
                'DELETE FROM bet_wager WHERE market_id = ? AND user_id = ?',
                (market_id, user_id)
            )
            self.conn.execute(
                'UPDATE bet_wallet SET balance = ? WHERE guild_id = ? AND user_id = ?',
                (new_balance, guild_id, user_id)
            )
            running_balance = balance
            for wager in wagers:
                running_balance += wager.stake
                self._bet_log_wallet_txn(
                    guild_id, user_id, user_id, 'wager_refund', wager.stake,
                    running_balance, market_id=market_id, note=wager.pick,
                    created_at=removed_at)
            return (True, 'removed', new_balance, total_refunded, len(wagers))

    def bet_get_wager(self, market_id, user_id, pick=None):
        """Return a user's wager on a market, optionally scoped to one pick."""
        if pick is not None:
            return self.conn.execute(
                'SELECT market_id, user_id, pick, stake, placed_at '
                'FROM bet_wager WHERE market_id = ? AND user_id = ? AND pick = ?',
                (market_id, str(user_id), str(pick))
            ).fetchone()
        return self.conn.execute(
            'SELECT market_id, user_id, pick, stake, placed_at '
            'FROM bet_wager WHERE market_id = ? AND user_id = ? '
            'ORDER BY placed_at DESC, pick ASC LIMIT 1',
            (market_id, str(user_id))
        ).fetchone()

    def bet_get_wagers_for_user(self, market_id, user_id):
        """Return all of a user's wagers on one market."""
        return self.conn.execute(
            'SELECT market_id, user_id, pick, stake, placed_at '
            'FROM bet_wager WHERE market_id = ? AND user_id = ? '
            'ORDER BY placed_at ASC, pick ASC',
            (market_id, str(user_id))
        ).fetchall()

    def bet_get_wagers(self, market_id):
        """Return all wagers on a market, earliest first."""
        return self.conn.execute(
            'SELECT market_id, user_id, pick, stake, placed_at '
            'FROM bet_wager WHERE market_id = ? ORDER BY placed_at ASC',
            (market_id,)
        ).fetchall()

    def bet_active_wagers_for_user(self, guild_id, user_id, limit=10):
        """Return this user's wagers on open markets, kickoff-soonest first."""
        limit = max(1, min(int(limit), 25))
        return self.conn.execute(
            'SELECT m.market_id, m.channel_id, m.thread_id, m.home_team, '
            '       m.away_team, m.commence_time, m.odds_home, m.odds_draw, '
            '       m.odds_away, m.bets_closed, w.pick, w.stake, w.placed_at '
            'FROM bet_wager w JOIN bet_market m ON m.market_id = w.market_id '
            "WHERE m.guild_id = ? AND w.user_id = ? AND m.status = 'open' "
            'ORDER BY m.commence_time ASC, m.market_id ASC LIMIT ?',
            (str(guild_id), str(user_id), limit)
        ).fetchall()

    def bet_pool(self, market_id):
        """Return (pick, count, total_stake) grouped by pick for a market."""
        return self.conn.execute(
            'SELECT pick, COUNT(*) AS cnt, SUM(stake) AS total '
            'FROM bet_wager WHERE market_id = ? GROUP BY pick',
            (market_id,)
        ).fetchall()

    def _market_odds_map(self, market_id):
        """{'home':o,'draw':o,'away':o} of a market's frozen odds, or None."""
        row = self.conn.execute(
            'SELECT odds_home, odds_draw, odds_away FROM bet_market '
            'WHERE market_id = ?', (market_id,)
        ).fetchone()
        if row is None:
            return None
        return {'home': row.odds_home, 'draw': row.odds_draw,
                'away': row.odds_away}

    def bet_settle(self, guild_id, market_id, result, result_home, result_away,
                   settled_at):
        """Settle a market: credit each winning wager round(stake×odds) to its
        wallet and mark the market settled. Odds are the market's frozen
        odds_<pick>; payout is computed, not stored. Returns
        [(user_id, pick, stake, odds, payout)] for the announcement, or None if
        the market was not open (already settled or cancelled).

        The terminal status flip is the guard: it runs first and is scoped to
        ``status='open'``, so a second settle (or a settle racing the
        auto-poller) is a no-op and nobody is paid twice. Atomic.
        """
        from tle.util.db.user_db_conn import _bet_odds_for_pick, _bet_pick_wins
        guild_id = str(guild_id)
        with self.conn:
            changed = self.conn.execute(
                "UPDATE bet_market SET status = 'settled', result = ?, "
                'result_home = ?, result_away = ?, settled_at = ? '
                "WHERE market_id = ? AND status = 'open'",
                (result, result_home, result_away, settled_at, market_id)
            ).rowcount
            if changed == 0:
                return None
            odds_map = self._market_odds_map(market_id)
            outcome = []
            wagers = self.conn.execute(
                'SELECT user_id, pick, stake FROM bet_wager WHERE market_id = ?',
                (market_id,)
            ).fetchall()
            for w in wagers:
                odds = _bet_odds_for_pick(odds_map, w.pick)
                payout = int(round(w.stake * odds)) \
                    if _bet_pick_wins(w.pick, result) and odds is not None else 0
                if payout:
                    self.conn.execute(
                        'UPDATE bet_wallet SET balance = balance + ? '
                        'WHERE guild_id = ? AND user_id = ?',
                        (payout, guild_id, w.user_id)
                    )
                    balance_after = self.conn.execute(
                        'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
                        (guild_id, w.user_id)
                    ).fetchone().balance
                    self._bet_log_wallet_txn(
                        guild_id, w.user_id, None, 'payout', payout,
                        balance_after, market_id=market_id, note=result,
                        created_at=settled_at)
                outcome.append((w.user_id, w.pick, w.stake, odds, payout))
        return outcome

    def bet_resettle(self, guild_id, market_id, new_result, new_home, new_away,
                     resettled_at):
        """Correct a settled market's result. Reverses the payouts that were
        credited under the OLD result and applies the new ones (per-wager
        delta), then stamps the new result. Returns
        [(user_id, pick, stake, odds, new_payout, delta)], or None if the
        market is not currently settled. Balances may go negative if a winner
        already spent the erroneous payout — use `;bet set` to reconcile. Atomic.
        """
        from tle.util.db.user_db_conn import _bet_odds_for_pick, _bet_pick_wins
        guild_id = str(guild_id)
        with self.conn:
            market = self.conn.execute(
                'SELECT result, odds_home, odds_draw, odds_away FROM bet_market '
                "WHERE market_id = ? AND status = 'settled'", (market_id,)
            ).fetchone()
            if market is None:
                return None
            old_result = market.result
            odds_map = {'home': market.odds_home, 'draw': market.odds_draw,
                        'away': market.odds_away}
            self.conn.execute(
                'UPDATE bet_market SET result = ?, result_home = ?, '
                'result_away = ?, settled_at = ? WHERE market_id = ?',
                (new_result, new_home, new_away, resettled_at, market_id)
            )
            outcome = []
            wagers = self.conn.execute(
                'SELECT user_id, pick, stake FROM bet_wager WHERE market_id = ?',
                (market_id,)
            ).fetchall()
            for w in wagers:
                odds = _bet_odds_for_pick(odds_map, w.pick)
                old_pay = int(round(w.stake * odds)) \
                    if _bet_pick_wins(w.pick, old_result) and odds is not None else 0
                new_pay = int(round(w.stake * odds)) \
                    if _bet_pick_wins(w.pick, new_result) and odds is not None else 0
                delta = new_pay - old_pay
                if delta:
                    self.conn.execute(
                        'UPDATE bet_wallet SET balance = balance + ? '
                        'WHERE guild_id = ? AND user_id = ?',
                        (delta, guild_id, w.user_id)
                    )
                    balance_after = self.conn.execute(
                        'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
                        (guild_id, w.user_id)
                    ).fetchone().balance
                    self._bet_log_wallet_txn(
                        guild_id, w.user_id, None, 'resettle_delta', delta,
                        balance_after, market_id=market_id, note=new_result,
                        created_at=resettled_at)
                outcome.append((w.user_id, w.pick, w.stake, odds, new_pay, delta))
        return outcome

    def bet_void(self, guild_id, market_id, voided_at):
        """Cancel an open market: refund every stake to its wallet and mark the
        market cancelled. Returns [(user_id, stake)] refunded, or None if the
        market was not open.

        Same guard as bet_settle: the status flip is scoped to ``status='open'``
        and runs first, so a void can't double-refund or undo a settlement.
        Atomic.
        """
        guild_id = str(guild_id)
        with self.conn:
            changed = self.conn.execute(
                "UPDATE bet_market SET status = 'cancelled', settled_at = ? "
                "WHERE market_id = ? AND status = 'open'",
                (voided_at, market_id)
            ).rowcount
            if changed == 0:
                return None
            refunds = []
            wagers = self.conn.execute(
                'SELECT user_id, stake FROM bet_wager WHERE market_id = ?',
                (market_id,)
            ).fetchall()
            for w in wagers:
                self.conn.execute(
                    'UPDATE bet_wallet SET balance = balance + ? '
                    'WHERE guild_id = ? AND user_id = ?',
                    (w.stake, guild_id, w.user_id)
                )
                balance_after = self.conn.execute(
                    'SELECT balance FROM bet_wallet WHERE guild_id = ? AND user_id = ?',
                    (guild_id, w.user_id)
                ).fetchone().balance
                self._bet_log_wallet_txn(
                    guild_id, w.user_id, None, 'void_refund', w.stake,
                    balance_after, market_id=market_id, created_at=voided_at)
                refunds.append((w.user_id, w.stake))
        return refunds
