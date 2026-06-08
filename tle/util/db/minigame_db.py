"""Database methods for the minigames system (Daily Akari, etc.)."""

import datetime as dt


_NO_TIME_BOUND = 10 ** 10


def _timestamp_to_date_text(timestamp):
    if timestamp <= 0:
        return None
    return dt.datetime.fromtimestamp(timestamp).date().isoformat()


class MinigameDbMixin:
    """Mixin providing minigame config and result storage methods.

    All methods take a ``game`` parameter (e.g. ``'akari'``) to identify
    which minigame the operation applies to.
    """

    @staticmethod
    def _minigame_select(table_name):
        return f'''
            SELECT
                message_id,
                guild_id,
                game,
                channel_id,
                user_id,
                puzzle_number,
                puzzle_date,
                accuracy,
                time_seconds,
                is_perfect,
                raw_content
            FROM {table_name}
        '''

    def _minigame_filtered_union_query(self, guild_id, game, dlo=0, dhi=_NO_TIME_BOUND,
                                        plo=0, phi=0):
        guild_id = str(guild_id)
        base_params = [guild_id, game]
        extra_clauses = []
        extra_params = []

        dlo_text = _timestamp_to_date_text(dlo)
        if dlo_text is not None:
            extra_clauses.append('puzzle_date >= ?')
            extra_params.append(dlo_text)
        dhi_text = _timestamp_to_date_text(dhi)
        if dhi_text is not None and dhi < _NO_TIME_BOUND:
            extra_clauses.append('puzzle_date < ?')
            extra_params.append(dhi_text)
        if plo > 0:
            extra_clauses.append('puzzle_number >= ?')
            extra_params.append(int(plo))
        if phi > 0:
            extra_clauses.append('puzzle_number < ?')
            extra_params.append(int(phi))

        extra = ''
        if extra_clauses:
            extra = ' AND ' + ' AND '.join(extra_clauses)

        # Each UNION leg needs its own copy of (base + extra) params
        leg_params = base_params + extra_params
        params = leg_params + leg_params

        query = f'''
            WITH minigame_all AS (
                {self._minigame_select('minigame_result')}
                WHERE guild_id = ? AND game = ? {extra}
                UNION ALL
                {self._minigame_select('minigame_import_result')}
                WHERE guild_id = ? AND game = ? {extra}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM minigame_result live
                      WHERE live.message_id = minigame_import_result.message_id
                        AND live.game = minigame_import_result.game
                        AND live.puzzle_number = minigame_import_result.puzzle_number
                  )
            ),
            first_per_user_puzzle AS (
                SELECT
                    guild_id,
                    user_id,
                    puzzle_number,
                    MIN(CAST(message_id AS INTEGER)) AS first_message_id
                FROM minigame_all
                GROUP BY guild_id, user_id, puzzle_number
            )
            SELECT *
            FROM (
                SELECT all_rows.*
                FROM minigame_all all_rows
                JOIN first_per_user_puzzle first_rows
                  ON all_rows.guild_id = first_rows.guild_id
                 AND all_rows.user_id = first_rows.user_id
                 AND all_rows.puzzle_number = first_rows.puzzle_number
                 AND CAST(all_rows.message_id AS INTEGER) = first_rows.first_message_id
            )
        '''
        return query, tuple(params)

    # ── Config ──────────────────────────────────────────────────────────

    def get_minigame_channel(self, guild_id, game):
        row = self.conn.execute(
            'SELECT channel_id FROM minigame_config WHERE guild_id = ? AND game = ?',
            (str(guild_id), game)
        ).fetchone()
        return row.channel_id if row else None

    def set_minigame_channel(self, guild_id, game, channel_id):
        self.conn.execute(
            'INSERT OR REPLACE INTO minigame_config (guild_id, game, channel_id) VALUES (?, ?, ?)',
            (str(guild_id), game, str(channel_id))
        )
        self.conn.commit()

    def clear_minigame_channel(self, guild_id, game):
        rc = self.conn.execute(
            'DELETE FROM minigame_config WHERE guild_id = ? AND game = ?',
            (str(guild_id), game)
        ).rowcount
        self.conn.commit()
        return rc

    # ── Results ─────────────────────────────────────────────────────────

    def save_minigame_result(self, message_id, guild_id, game, channel_id, user_id,
                             puzzle_number, puzzle_date, accuracy, time_seconds, is_perfect,
                             raw_content):
        self.conn.execute(
            '''
            INSERT OR REPLACE INTO minigame_result (
                message_id, guild_id, game, channel_id, user_id, puzzle_number,
                puzzle_date, accuracy, time_seconds, is_perfect, raw_content
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                str(message_id), str(guild_id), game, str(channel_id), str(user_id),
                int(puzzle_number), str(puzzle_date), int(accuracy), int(time_seconds),
                int(bool(is_perfect)), str(raw_content)
            )
        )
        self.conn.commit()

    def save_imported_minigame_result(self, message_id, guild_id, game, channel_id, user_id,
                                      puzzle_number, puzzle_date, accuracy, time_seconds,
                                      is_perfect, raw_content, commit=True):
        self.conn.execute(
            '''
            INSERT OR REPLACE INTO minigame_import_result (
                message_id, guild_id, game, channel_id, user_id, puzzle_number,
                puzzle_date, accuracy, time_seconds, is_perfect, raw_content
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                str(message_id), str(guild_id), game, str(channel_id), str(user_id),
                int(puzzle_number), str(puzzle_date), int(accuracy), int(time_seconds),
                int(bool(is_perfect)), str(raw_content)
            )
        )
        if commit:
            self.conn.commit()

    def delete_minigame_result(self, message_id):
        rc = self.conn.execute(
            'DELETE FROM minigame_result WHERE message_id = ?',
            (str(message_id),)
        ).rowcount
        self.conn.commit()
        return rc

    def delete_imported_minigame_result(self, message_id):
        rc = self.conn.execute(
            'DELETE FROM minigame_import_result WHERE message_id = ?',
            (str(message_id),)
        ).rowcount
        self.conn.commit()
        return rc

    def clear_imported_minigame_results(self, guild_id, game, channel_id=None):
        if channel_id is not None:
            rc = self.conn.execute(
                'DELETE FROM minigame_import_result WHERE guild_id = ? AND game = ? AND channel_id = ?',
                (str(guild_id), game, str(channel_id))
            ).rowcount
        else:
            rc = self.conn.execute(
                'DELETE FROM minigame_import_result WHERE guild_id = ? AND game = ?',
                (str(guild_id), game)
            ).rowcount
        self.conn.commit()
        return rc

    # ── Raw messages ─────────────────────────────────────────────────

    def save_raw_message(self, message_id, guild_id, channel_id, user_id,
                         created_at, raw_content, commit=True):
        self.conn.execute(
            '''
            INSERT OR IGNORE INTO minigame_raw_message
                (message_id, guild_id, channel_id, user_id, created_at, raw_content)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (str(message_id), str(guild_id), str(channel_id), str(user_id),
             str(created_at), str(raw_content))
        )
        if commit:
            self.conn.commit()

    def update_raw_message(self, message_id, raw_content):
        self.conn.execute(
            'UPDATE minigame_raw_message SET raw_content = ? WHERE message_id = ?',
            (str(raw_content), str(message_id))
        )
        self.conn.commit()

    def delete_raw_message(self, message_id):
        rc = self.conn.execute(
            'DELETE FROM minigame_raw_message WHERE message_id = ?',
            (str(message_id),)
        ).rowcount
        self.conn.commit()
        return rc

    def get_raw_messages_for_guild(self, guild_id):
        return self.conn.execute(
            'SELECT * FROM minigame_raw_message WHERE guild_id = ? ORDER BY CAST(message_id AS INTEGER)',
            (str(guild_id),)
        ).fetchall()

    def clear_raw_messages(self, guild_id, channel_id=None):
        if channel_id is not None:
            rc = self.conn.execute(
                'DELETE FROM minigame_raw_message WHERE guild_id = ? AND channel_id = ?',
                (str(guild_id), str(channel_id))
            ).rowcount
        else:
            rc = self.conn.execute(
                'DELETE FROM minigame_raw_message WHERE guild_id = ?',
                (str(guild_id),)
            ).rowcount
        self.conn.commit()
        return rc

    # ── Queries ──────────────────────────────────────────────────────

    def get_minigame_result(self, message_id):
        return self.conn.execute(
            'SELECT * FROM minigame_result WHERE message_id = ?',
            (str(message_id),)
        ).fetchone()

    def get_minigame_result_for_user_puzzle(self, guild_id, game, user_id, puzzle_number):
        query, params = self._minigame_filtered_union_query(guild_id, game)
        return self.conn.execute(
            f'''
            {query}
            WHERE user_id = ? AND puzzle_number = ?
            ORDER BY CAST(message_id AS INTEGER) ASC
            LIMIT 1
            ''',
            params + (str(user_id), int(puzzle_number))
        ).fetchone()

    def get_minigame_results_for_user(self, guild_id, game, user_id,
                                       dlo=0, dhi=_NO_TIME_BOUND, plo=0, phi=0):
        query, params = self._minigame_filtered_union_query(guild_id, game, dlo, dhi, plo, phi)
        return self.conn.execute(
            f'''
            {query}
            WHERE user_id = ?
            ORDER BY puzzle_date DESC, puzzle_number DESC, time_seconds ASC, message_id DESC
            ''',
            params + (str(user_id),)
        ).fetchall()

    def get_minigame_results_for_guild(self, guild_id, game,
                                        dlo=0, dhi=_NO_TIME_BOUND, plo=0, phi=0):
        query, params = self._minigame_filtered_union_query(guild_id, game, dlo, dhi, plo, phi)
        return self.conn.execute(
            f'''
            {query}
            ORDER BY puzzle_date DESC, puzzle_number DESC, time_seconds ASC, message_id DESC
            ''',
            params
        ).fetchall()

    def delete_minigame_result_for_user_puzzle(self, guild_id, game, user_id, puzzle_number):
        live_rc = self.conn.execute(
            '''
            DELETE FROM minigame_result
            WHERE guild_id = ? AND game = ? AND user_id = ? AND puzzle_number = ?
            ''',
            (str(guild_id), game, str(user_id), int(puzzle_number))
        ).rowcount
        imported_rc = self.conn.execute(
            '''
            DELETE FROM minigame_import_result
            WHERE guild_id = ? AND game = ? AND user_id = ? AND puzzle_number = ?
            ''',
            (str(guild_id), game, str(user_id), int(puzzle_number))
        ).rowcount
        self.conn.commit()
        return live_rc + imported_rc

    def delete_minigame_results_for_puzzle(self, guild_id, game, puzzle_number):
        live_rc = self.conn.execute(
            '''
            DELETE FROM minigame_result
            WHERE guild_id = ? AND game = ? AND puzzle_number = ?
            ''',
            (str(guild_id), game, int(puzzle_number))
        ).rowcount
        imported_rc = self.conn.execute(
            '''
            DELETE FROM minigame_import_result
            WHERE guild_id = ? AND game = ? AND puzzle_number = ?
            ''',
            (str(guild_id), game, int(puzzle_number))
        ).rowcount
        self.conn.commit()
        return live_rc + imported_rc

    # ── Generic minigame identity links ───────────────────────────────

    def set_minigame_player_link(self, guild_id, game, user_id, external_name,
                                 normalized_name, external_url, linked_at,
                                 linked_by):
        """Link a Discord user to an external game account/name.

        ``normalized_name`` is unique per ``(guild, game)`` so a pasted
        leaderboard name resolves to exactly one Discord user.  Callers should
        normalize consistently before passing the value in.
        """
        self.conn.execute(
            '''
            INSERT INTO minigame_player_link (
                guild_id, game, user_id, external_name, normalized_name,
                external_url, linked_at, linked_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, game, user_id) DO UPDATE SET
                external_name = excluded.external_name,
                normalized_name = excluded.normalized_name,
                external_url = excluded.external_url,
                linked_at = excluded.linked_at,
                linked_by = excluded.linked_by
            ''',
            (
                str(guild_id), game, str(user_id), str(external_name),
                str(normalized_name), external_url, float(linked_at),
                str(linked_by),
            )
        )
        self.conn.commit()

    def get_minigame_player_link(self, guild_id, game, user_id):
        return self.conn.execute(
            '''
            SELECT guild_id, game, user_id, external_name, normalized_name,
                   external_url, linked_at, linked_by
            FROM minigame_player_link
            WHERE guild_id = ? AND game = ? AND user_id = ?
            ''',
            (str(guild_id), game, str(user_id))
        ).fetchone()

    def get_minigame_player_link_by_name(self, guild_id, game, normalized_name):
        return self.conn.execute(
            '''
            SELECT guild_id, game, user_id, external_name, normalized_name,
                   external_url, linked_at, linked_by
            FROM minigame_player_link
            WHERE guild_id = ? AND game = ? AND normalized_name = ?
            ''',
            (str(guild_id), game, str(normalized_name))
        ).fetchone()

    def get_minigame_player_links(self, guild_id, game):
        return self.conn.execute(
            '''
            SELECT guild_id, game, user_id, external_name, normalized_name,
                   external_url, linked_at, linked_by
            FROM minigame_player_link
            WHERE guild_id = ? AND game = ?
            ORDER BY normalized_name ASC
            ''',
            (str(guild_id), game)
        ).fetchall()

    def delete_minigame_player_link(self, guild_id, game, user_id):
        rc = self.conn.execute(
            '''
            DELETE FROM minigame_player_link
            WHERE guild_id = ? AND game = ? AND user_id = ?
            ''',
            (str(guild_id), game, str(user_id))
        ).rowcount
        self.conn.commit()
        return rc

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
