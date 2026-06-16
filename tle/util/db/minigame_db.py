"""Database methods for the minigames system (Daily Akari, etc.).

The config/result/query and import-link methods are split into
``_minigame_db_results`` and ``_minigame_db_links`` (inherited below) to keep
this module under the 500-line limit. Shared helpers live in
``_minigame_db_common`` and are re-exported here for backwards compatibility.
"""

from tle.util.db._minigame_db_common import (
    _MERGED_WINNERS_SQL,
    _NO_TIME_BOUND,
    _timestamp_to_date_text,
    diff_merged_winners,
    merged_minigame_winners,
)
from tle.util.db._minigame_db_results import MinigameResultsDbMixin
from tle.util.db._minigame_db_links import MinigameLinksDbMixin
from tle.util.db._minigame_db_schema import MinigameSchemaDbMixin


class MinigameDbMixin(MinigameResultsDbMixin, MinigameLinksDbMixin,
                      MinigameSchemaDbMixin):
    """Mixin providing minigame config and result storage methods.

    All methods take a ``game`` parameter (e.g. ``'akari'``) to identify
    which minigame the operation applies to. Config/result/query and identity
    -link methods come from the inherited mixins; rating snapshots, bans and the
    Akari-specific helpers are defined here.
    """

    # ── Generic rating snapshots ─────────────────────────────────────

    def replace_minigame_ratings(self, guild_id, game, states, updated_at):
        """Atomically replace a guild/game's cached rating snapshot."""
        guild_id = str(guild_id)
        rows = [
            (guild_id, game, str(state.user_id), float(state.rating),
             int(state.games), float(state.peak), float(state.last_delta),
             int(state.skip_streak), int(state.last_puzzle), float(updated_at))
            for state in states
        ]
        with self.conn:
            self.conn.execute(
                'DELETE FROM minigame_rating WHERE guild_id = ? AND game = ?',
                (guild_id, game))
            self.conn.executemany(
                '''
                INSERT INTO minigame_rating
                    (guild_id, game, user_id, rating, games, peak, last_delta,
                     skip_streak, last_puzzle, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                rows
            )
        return len(rows)

    def get_minigame_ratings(self, guild_id, game):
        return self.conn.execute(
            '''
            SELECT user_id, rating, games, peak, last_delta, skip_streak,
                   last_puzzle, updated_at
            FROM minigame_rating
            WHERE guild_id = ? AND game = ?
            ORDER BY rating DESC, games DESC, user_id ASC
            ''',
            (str(guild_id), game)
        ).fetchall()

    def get_minigame_rating(self, guild_id, game, user_id):
        return self.conn.execute(
            '''
            SELECT user_id, rating, games, peak, last_delta, skip_streak,
                   last_puzzle, updated_at
            FROM minigame_rating
            WHERE guild_id = ? AND game = ? AND user_id = ?
            ''',
            (str(guild_id), game, str(user_id))
        ).fetchone()

    # ── Generic minigame bans ──────────────────────────────────────────

    def ban_minigame_user(self, guild_id, game, user_id, banned_at, banned_by,
                          reason=None):
        rc = self.conn.execute(
            '''
            INSERT OR IGNORE INTO minigame_ban
                (guild_id, game, user_id, banned_at, banned_by, reason)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (str(guild_id), game, str(user_id), float(banned_at),
             str(banned_by), reason)
        ).rowcount
        self.conn.commit()
        return rc

    def unban_minigame_user(self, guild_id, game, user_id):
        rc = self.conn.execute(
            '''
            DELETE FROM minigame_ban
            WHERE guild_id = ? AND game = ? AND user_id = ?
            ''',
            (str(guild_id), game, str(user_id))
        ).rowcount
        self.conn.commit()
        return rc

    def is_minigame_banned(self, guild_id, game, user_id):
        row = self.conn.execute(
            '''
            SELECT user_id
            FROM minigame_ban
            WHERE guild_id = ? AND game = ? AND user_id = ?
            ''',
            (str(guild_id), game, str(user_id))
        ).fetchone()
        return row is not None

    def get_minigame_ban(self, guild_id, game, user_id):
        return self.conn.execute(
            '''
            SELECT user_id, banned_at, banned_by, reason
            FROM minigame_ban
            WHERE guild_id = ? AND game = ? AND user_id = ?
            ''',
            (str(guild_id), game, str(user_id))
        ).fetchone()

    def get_minigame_bans(self, guild_id, game):
        return self.conn.execute(
            '''
            SELECT user_id, banned_at, banned_by, reason
            FROM minigame_ban
            WHERE guild_id = ? AND game = ?
            ORDER BY banned_at DESC, user_id ASC
            ''',
            (str(guild_id), game)
        ).fetchall()

    # ── Akari rating: registration ───────────────────────────────────
    #
    # Default opt-in: everyone with any Akari result is registered (visible in
    # rating displays).  The only way to be hidden is an explicit ``unregister``
    # call, which writes a row to ``akari_optout``.  ``register`` just deletes
    # any opt-out row for that user.
    #
    # ``akari_registrant`` is legacy — pre-default-opt-in rows live there but
    # nothing currently writes or reads it.

    def register_akari_user(self, guild_id, user_id):
        """Clear any explicit opt-out so the user is visible again.

        Default visibility means users not in ``akari_optout`` are already
        registered; this is a no-op for users who weren't opted out.  Returns
        True iff an opt-out row was lifted.
        """
        cleared = self.conn.execute(
            'DELETE FROM akari_optout WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))
        ).rowcount
        self.conn.commit()
        return cleared > 0

    def unregister_akari_user(self, guild_id, user_id, opted_out_at):
        """Explicitly opt a user out of rating displays.

        Sticky: the opt-out row persists until a future ``register`` call
        clears it, so a user who unregisters never auto-rejoins regardless of
        how many puzzles they post afterwards.  Returns True iff a new opt-out
        row was added (False if they were already opted out).
        """
        added = self.conn.execute(
            '''
            INSERT OR IGNORE INTO akari_optout (guild_id, user_id, opted_out_at)
            VALUES (?, ?, ?)
            ''',
            (str(guild_id), str(user_id), float(opted_out_at))
        ).rowcount
        self.conn.commit()
        return added > 0

    def is_akari_opted_out(self, guild_id, user_id):
        row = self.conn.execute(
            'SELECT user_id FROM akari_optout WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))
        ).fetchone()
        return row is not None

    def is_akari_registered(self, guild_id, user_id):
        """True iff the user is currently visible in rating displays.

        Default-opt-in: just the inverse of explicit opt-out.  No result-count
        check — even users with zero puzzles are formally "registered"; they
        just have nothing to show in any display.
        """
        return not self.is_akari_opted_out(guild_id, user_id)

    def get_akari_registrants(self, guild_id):
        """All currently-visible user_ids for a guild.

        Users with any Akari result (live or imported), minus those in
        ``akari_optout``.  Users with zero results are excluded because they'd
        contribute nothing to any display anyway.
        """
        guild_id = str(guild_id)
        rows = self.conn.execute(
            '''
            SELECT DISTINCT user_id FROM (
                SELECT user_id FROM minigame_result
                WHERE guild_id = ? AND game = 'akari'
                UNION
                SELECT user_id FROM minigame_import_result
                WHERE guild_id = ? AND game = 'akari'
            )
            WHERE user_id NOT IN (
                SELECT user_id FROM akari_optout WHERE guild_id = ?
            )
            ''',
            (guild_id, guild_id, guild_id)
        ).fetchall()
        return {row.user_id for row in rows}

    # ── Akari rating: banlist ────────────────────────────────────────

    def ban_akari_user(self, guild_id, user_id, banned_at, banned_by, reason=None):
        """Ban a user from Akari ingestion.

        Returns 1 if newly banned, 0 if already banned (existing ban metadata
        is preserved).  To update the reason of an existing ban, unban first.
        """
        rc = self.conn.execute(
            '''
            INSERT OR IGNORE INTO akari_ban
                (guild_id, user_id, banned_at, banned_by, reason)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (str(guild_id), str(user_id), float(banned_at),
             str(banned_by), reason)
        ).rowcount
        self.conn.commit()
        return rc

    def unban_akari_user(self, guild_id, user_id):
        """Lift a ban. Returns the number of rows removed (1 or 0)."""
        rc = self.conn.execute(
            'DELETE FROM akari_ban WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))
        ).rowcount
        self.conn.commit()
        return rc

    def is_akari_banned(self, guild_id, user_id):
        row = self.conn.execute(
            'SELECT user_id FROM akari_ban WHERE guild_id = ? AND user_id = ?',
            (str(guild_id), str(user_id))
        ).fetchone()
        return row is not None

    def get_akari_ban(self, guild_id, user_id):
        """Return a banned user's ``(user_id, banned_at, banned_by, reason)`` row, or None.

        Use this when the caller needs the ban metadata (e.g. the reason for a
        notice embed); :meth:`is_akari_banned` is the bool-only fast path.
        """
        return self.conn.execute(
            '''
            SELECT user_id, banned_at, banned_by, reason
            FROM akari_ban
            WHERE guild_id = ? AND user_id = ?
            ''',
            (str(guild_id), str(user_id))
        ).fetchone()

    def get_akari_bans(self, guild_id):
        """List bans for a guild, newest first.

        Returns rows with ``user_id``, ``banned_at``, ``banned_by``, ``reason``.
        """
        return self.conn.execute(
            '''
            SELECT user_id, banned_at, banned_by, reason
            FROM akari_ban
            WHERE guild_id = ?
            ORDER BY banned_at DESC, user_id ASC
            ''',
            (str(guild_id),)
        ).fetchall()

    # ── Rating: snapshot ─────────────────────────────────────────────

    def replace_akari_ratings(self, guild_id, states, updated_at):
        """Atomically replace a guild's cached Akari rating snapshot.

        ``states`` is an iterable of objects exposing ``user_id``, ``rating``,
        ``games``, ``peak`` and ``last_delta`` (e.g. ``RatingState`` from
        ``tle.util.akari_rating``).  Ratings are stored as floats; callers round
        for display.  Returns the number of rows written.
        """
        guild_id = str(guild_id)
        states = list(states)
        count = self.replace_minigame_ratings(
            guild_id, 'akari', states, updated_at)
        rows = [
            (guild_id, str(state.user_id), float(state.rating), int(state.games),
             float(state.peak), float(state.last_delta), int(state.skip_streak),
             int(state.last_puzzle), float(updated_at))
            for state in states
        ]
        with self.conn:
            self.conn.execute(
                'DELETE FROM akari_rating WHERE guild_id = ?', (guild_id,))
            self.conn.executemany(
                '''
                INSERT INTO akari_rating
                    (guild_id, user_id, rating, games, peak, last_delta,
                     skip_streak, last_puzzle, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                rows
            )
        return count

    def get_akari_ratings(self, guild_id):
        """All rated users for a guild, strongest first."""
        rows = self.get_minigame_ratings(guild_id, 'akari')
        if rows:
            return rows
        return self.conn.execute(
            '''
            SELECT user_id, rating, games, peak, last_delta, skip_streak,
                   last_puzzle, updated_at
            FROM akari_rating
            WHERE guild_id = ?
            ORDER BY rating DESC, games DESC, user_id ASC
            ''',
            (str(guild_id),)
        ).fetchall()

    def get_akari_rating(self, guild_id, user_id):
        row = self.get_minigame_rating(guild_id, 'akari', user_id)
        if row is not None:
            return row
        return self.conn.execute(
            '''
            SELECT user_id, rating, games, peak, last_delta, skip_streak,
                   last_puzzle, updated_at
            FROM akari_rating
            WHERE guild_id = ? AND user_id = ?
            ''',
            (str(guild_id), str(user_id))
        ).fetchone()
