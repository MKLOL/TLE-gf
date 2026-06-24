"""Soccer betting market DB methods — extracted from user_db_conn.py.

Operates on the ``bet_market`` and ``bet_wager`` tables created by
``BettingWalletDbMixin._create_betting_tables``. Wager placement and
settlement live in ``BettingWagerDbMixin``. ``bet_fixture_key`` lives in
``user_db_conn`` and is imported lazily to avoid an import cycle.
"""
import logging
import sqlite3

logger = logging.getLogger(__name__)


class BettingMarketDbMixin:
    """Mixin providing betting market CRUD / query DB methods."""

    # -- Markets --

    def bet_market_create(self, guild_id, channel_id, event_id, sport_key,
                          home_team, away_team, commence_time,
                          odds_home, odds_draw, odds_away, created_by, created_at):
        """Open a betting market and return its id, or None if already open."""
        from tle.util.db.user_db_conn import bet_fixture_key
        guild_id, channel_id, event_id = str(guild_id), str(channel_id), str(event_id)
        fixture_key = bet_fixture_key(
            sport_key, home_team, away_team, commence_time)
        try:
            cur = self.conn.execute(
                'INSERT INTO bet_market '
                '(guild_id, channel_id, event_id, fixture_key, sport_key, '
                'home_team, away_team, commence_time, odds_home, odds_draw, '
                'odds_away, created_by, created_at, status) '
                'SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \'open\' '
                'WHERE NOT EXISTS ('
                '    SELECT 1 FROM bet_market '
                '    WHERE guild_id = ? AND event_id = ? AND status = \'open\''
                ') AND NOT EXISTS ('
                '    SELECT 1 FROM bet_market '
                '    WHERE guild_id = ? AND fixture_key = ? AND status = \'open\''
                ')',
                (guild_id, channel_id, event_id, fixture_key, sport_key,
                 home_team, away_team,
                 commence_time, odds_home, odds_draw, odds_away, str(created_by),
                 created_at, guild_id, event_id, guild_id, fixture_key)
            )
        except sqlite3.IntegrityError as e:
            self.conn.rollback()
            if 'UNIQUE constraint failed' in str(e):
                return None
            raise
        self.conn.commit()
        return cur.lastrowid if cur.rowcount else None

    def bet_market_set_message(self, market_id, message_id):
        """Record the market announcement message id."""
        self.conn.execute(
            'UPDATE bet_market SET message_id = ? WHERE market_id = ?',
            (str(message_id), market_id)
        )
        self.conn.commit()

    def bet_market_set_thread(self, market_id, thread_id):
        """Record the betting thread id (where wagers are posted)."""
        self.conn.execute(
            'UPDATE bet_market SET thread_id = ? WHERE market_id = ?',
            (str(thread_id), market_id)
        )
        self.conn.commit()

    def bet_market_set_thread_intro(self, market_id, message_id):
        """Record the first bot message in the betting thread."""
        self.conn.execute(
            'UPDATE bet_market SET thread_intro_id = ? WHERE market_id = ?',
            (str(message_id), market_id)
        )
        self.conn.commit()

    def bet_market_get_active_by_thread(self, guild_id, thread_id):
        """Return the open market whose betting thread is thread_id, or None."""
        return self.conn.execute(
            "SELECT * FROM bet_market "
            "WHERE guild_id = ? AND thread_id = ? AND status = 'open' LIMIT 1",
            (str(guild_id), str(thread_id))
        ).fetchone()

    def bet_market_get(self, market_id):
        """Return a market by id, or None."""
        return self.conn.execute(
            'SELECT * FROM bet_market WHERE market_id = ?', (market_id,)
        ).fetchone()

    def bet_market_get_active(self, guild_id, channel_id):
        """Return the open market in a channel, or None (newest wins)."""
        return self.conn.execute(
            "SELECT * FROM bet_market "
            "WHERE guild_id = ? AND channel_id = ? AND status = 'open' "
            'ORDER BY created_at DESC, market_id DESC LIMIT 1',
            (str(guild_id), str(channel_id))
        ).fetchone()

    def bet_market_exists_open_for_event(self, guild_id, event_id):
        """True if the guild already has an open market on this event."""
        row = self.conn.execute(
            "SELECT 1 FROM bet_market WHERE guild_id = ? AND event_id = ? "
            "AND status = 'open' LIMIT 1",
            (str(guild_id), str(event_id))
        ).fetchone()
        return row is not None

    def bet_market_get_open_for_event(self, guild_id, event_id):
        """Return the guild's open market on this event, or None.

        Used by the auto-open watcher to decide whether to create a market or
        (if one exists without a thread) just attach a thread.
        """
        return self.conn.execute(
            "SELECT * FROM bet_market WHERE guild_id = ? AND event_id = ? "
            "AND status = 'open' ORDER BY market_id DESC LIMIT 1",
            (str(guild_id), str(event_id))
        ).fetchone()

    def bet_market_get_open_for_fixture(self, guild_id, fixture_key):
        """Return the guild's open market for a canonical fixture key, or None."""
        return self.conn.execute(
            "SELECT * FROM bet_market WHERE guild_id = ? AND fixture_key = ? "
            "AND status = 'open' ORDER BY market_id DESC LIMIT 1",
            (str(guild_id), fixture_key)
        ).fetchone()

    def bet_market_has_earlier_open_at_kickoff(self, guild_id, channel_id,
                                               commence_time, market_id):
        """True if another open market in the same channel shares this kickoff
        time and was created earlier (smaller market_id).

        Used to ping the notify role only once when several games kick off at
        the same time in one channel: the earliest-created market of the group
        pings, the rest stay quiet. Ordering-independent — market_id reflects
        insert order, so exactly one market in the group sees no earlier sibling.
        """
        row = self.conn.execute(
            "SELECT 1 FROM bet_market "
            "WHERE guild_id = ? AND channel_id = ? AND status = 'open' "
            'AND commence_time = ? AND market_id < ? LIMIT 1',
            (str(guild_id), str(channel_id), commence_time, market_id)
        ).fetchone()
        return row is not None

    def bet_markets_pending_settlement(self, before_time):
        """Return all open markets whose kickoff is at/before before_time —
        the auto-settle poller's work-list, across all guilds."""
        return self.conn.execute(
            "SELECT * FROM bet_market WHERE status = 'open' "
            'AND commence_time <= ? ORDER BY commence_time ASC',
            (before_time,)
        ).fetchall()

    def bet_markets_open(self, guild_id):
        """Return all open markets for a guild, kickoff-soonest first."""
        return self.conn.execute(
            "SELECT * FROM bet_market WHERE guild_id = ? AND status = 'open' "
            'ORDER BY commence_time ASC',
            (str(guild_id),)
        ).fetchall()

    def bet_market_count_wagers(self, market_id):
        """Number of wagers placed on a market."""
        return self.conn.execute(
            'SELECT COUNT(*) AS cnt FROM bet_wager WHERE market_id = ?',
            (market_id,)
        ).fetchone().cnt

    def bet_market_close_betting(self, market_id):
        """Lock betting on an open market early. Returns True if it changed."""
        cur = self.conn.execute(
            "UPDATE bet_market SET bets_closed = 1 "
            "WHERE market_id = ? AND status = 'open' AND bets_closed = 0",
            (market_id,)
        )
        self.conn.commit()
        return cur.rowcount > 0

    def bet_market_set_odds(self, market_id, odds_home, odds_draw, odds_away):
        """Override an open market's frozen odds (only safe with no wagers yet
        — the caller enforces that). Returns True if a row changed."""
        cur = self.conn.execute(
            "UPDATE bet_market SET odds_home = ?, odds_draw = ?, odds_away = ? "
            "WHERE market_id = ? AND status = 'open'",
            (odds_home, odds_draw, odds_away, market_id)
        )
        self.conn.commit()
        return cur.rowcount > 0

    def bet_market_get_latest_settled_by_thread(self, guild_id, thread_id):
        """Most recently settled market for a thread, or None (for ;bet correct)."""
        return self.conn.execute(
            "SELECT * FROM bet_market WHERE guild_id = ? AND thread_id = ? "
            "AND status = 'settled' ORDER BY settled_at DESC, market_id DESC LIMIT 1",
            (str(guild_id), str(thread_id))
        ).fetchone()

    def bet_market_get_latest_settled_by_channel(self, guild_id, channel_id):
        """Most recently settled market for a channel, or None (for ;bet correct)."""
        return self.conn.execute(
            "SELECT * FROM bet_market WHERE guild_id = ? AND channel_id = ? "
            "AND status = 'settled' ORDER BY settled_at DESC, market_id DESC LIMIT 1",
            (str(guild_id), str(channel_id))
        ).fetchone()
