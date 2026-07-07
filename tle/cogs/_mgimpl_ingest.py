"""Message ingestion and Discord listeners. (Minigames cog impl mixin; see minigames.py)."""

import logging

import discord
from discord.ext import commands

from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._migrate_retry import discord_retry, RetryExhaustedError
from tle.cogs._minigame_common import (
    strip_codeblock,
)
from tle.cogs._minigame_akari import (
    AKARI_GAME, akari_date_number_mismatch, looks_like_non_pro_akari,
)

logger = logging.getLogger(__name__)


class ImplIngestMixin:
    # ── Listeners ───────────────────────────────────────────────────────

    async def _ingest_message(self, message, game):
        results = game.parse(strip_codeblock(message.content))
        if not results:
            return 0

        puzzle_date_fallback = message.created_at.date()

        saved = 0
        for parsed in results:
            existing = cf_common.user_db.get_minigame_result_for_user_puzzle(
                message.guild.id, game.name, message.author.id, parsed.puzzle_number
            )
            if existing is not None and str(existing.message_id) != str(message.id):
                logger.info(
                    '%s result ignored (duplicate): guild=%s msg=%s user=%s puzzle=%s first_msg=%s',
                    game.display_name, message.guild.id, message.id,
                    message.author.id, parsed.puzzle_number, existing.message_id,
                )
                continue

            puzzle_date = parsed.puzzle_date or puzzle_date_fallback

            cf_common.user_db.save_minigame_result(
                message.id, message.guild.id, game.name, message.channel.id,
                message.author.id, parsed.puzzle_number,
                puzzle_date.isoformat(), parsed.accuracy,
                parsed.time_seconds, parsed.is_perfect, message.content,
            )
            saved += 1
        return saved

    @staticmethod
    def _is_ingest_banned(guild_id, user_id, game):
        """True iff this user is banned from submitting results for ``game``.

        Bans are forward-only for every game: they short-circuit the live
        entry points (messages, edits) but never touch existing rows.  The
        history entry points (import, reparse) must keep materializing
        pre-ban messages — use :meth:`_ingest_ban_cutoff` there.  Akari keeps
        its own legacy banlist table; other games use the generic
        ``minigame_ban`` table.
        """
        if game.name == AKARI_GAME.name:
            return cf_common.user_db.is_akari_banned(guild_id, user_id)
        return cf_common.user_db.is_minigame_banned(
            guild_id, game.name, user_id)

    @staticmethod
    def _ingest_ban_cutoff(guild_id, user_id, game):
        """Unix timestamp at which the user's ban took effect, or None.

        History entry points (import, reparse) compare message timestamps
        against this so a ban only drops messages sent after it — clearing
        and rebuilding history must not erase a banned user's pre-ban
        results ('existing results stay rated').
        """
        if game.name == AKARI_GAME.name:
            ban = cf_common.user_db.get_akari_ban(guild_id, user_id)
        else:
            ban = cf_common.user_db.get_minigame_ban(
                guild_id, game.name, user_id)
        return None if ban is None else float(ban.banned_at)

    async def _notify_non_pro_mode(self, message):
        """Reply to a non-pro Daily Akari submission asking the user to enable Pro Mode.

        Same best-effort pattern as :meth:`_notify_banned_submission` —
        a failed reply is logged and swallowed so the ingestion path can't be
        broken by a notice failure.
        """
        embed = discord_common.embed_alert(
            "Your result doesn't include accuracy. Please turn on "
            "Pro Mode \U0001f3af\U0001f31f in the settings and submit "
            "again for it to count.")
        try:
            await discord_retry(
                lambda: message.reply(embed=embed, mention_author=False))
        except (RetryExhaustedError, discord.HTTPException):
            logger.warning('Failed to notify non-pro mode for message %s',
                           message.id, exc_info=True)

    @staticmethod
    def _invalid_minigame_submission_message(game, content):
        if game.name != AKARI_GAME.name:
            return None
        mismatch = akari_date_number_mismatch(content)
        if mismatch is None:
            return None
        if getattr(mismatch, 'out_of_range', False):
            return (
                f'Invalid submission: puzzle number/date mismatch. '
                f'Daily Akari #{mismatch.puzzle_number} is outside the supported '
                f'Daily Akari date range, but this message says '
                f'{mismatch.puzzle_date.isoformat()} (that date is '
                f'#{mismatch.expected_number}). Result not counted. '
                f'You should never play this game again.')
        return (
            f'Invalid submission: puzzle number/date mismatch. '
            f'Daily Akari #{mismatch.puzzle_number} '
            f'is for {mismatch.expected_date.isoformat()}, but this message says '
            f'{mismatch.puzzle_date.isoformat()} (that date is '
            f'#{mismatch.expected_number}). Result not counted. '
            f'You should never play this game again.')

    async def _notify_invalid_minigame_submission(self, message, game, content):
        body = self._invalid_minigame_submission_message(game, content)
        if body is None:
            return False
        embed = discord_common.embed_alert(body)
        try:
            await discord_retry(
                lambda: message.reply(embed=embed, mention_author=False))
        except (RetryExhaustedError, discord.HTTPException):
            logger.warning('Failed to notify invalid minigame message %s',
                           message.id, exc_info=True)
        return True

    async def _notify_invalid_minigame_submission_from_raw(self, row, game, content):
        if self.bot is None:
            return False
        try:
            channel = await self._resolve_channel(int(row.channel_id))
            message = await channel.fetch_message(int(row.message_id))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException,
                AttributeError, TypeError, ValueError):
            logger.warning(
                'Failed to fetch invalid minigame message %s for retroactive reply',
                getattr(row, 'message_id', None), exc_info=True)
            return False
        return await self._notify_invalid_minigame_submission(
            message, game, content)

    async def _notify_banned_submission(self, message, game):
        """Reply to a banned user's parsable result post explaining the ban.

        Only called after we've confirmed the message *would have* produced a
        result — chat messages from banned users in the game channel stay
        silent so we don't spam unrelated conversation.  Best-effort: a failed
        reply (deleted message, missing perms) is logged and swallowed so the
        ingestion path can't be broken by a notice failure.
        """
        if game.name == AKARI_GAME.name:
            ban = cf_common.user_db.get_akari_ban(
                message.guild.id, message.author.id)
        else:
            ban = cf_common.user_db.get_minigame_ban(
                message.guild.id, game.name, message.author.id)
        reason = ban.reason if ban is not None else None
        body = f'You are banned from posting {game.display_name} results.'
        if reason:
            body += f'\nReason: {reason}'
        body += '\nAsk a moderator to lift the ban.'
        embed = discord_common.embed_alert(body)
        try:
            await discord_retry(
                lambda: message.reply(embed=embed, mention_author=False))
        except (RetryExhaustedError, discord.HTTPException):
            logger.warning('Failed to notify banned user for message %s',
                           message.id, exc_info=True)

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.guild is None or message.author.bot or cf_common.user_db is None:
            return
        game = self._game_for_channel(message)
        if game is not None:
            try:
                cleaned = strip_codeblock(message.content)
                invalid_message = self._invalid_minigame_submission_message(
                    game, cleaned)
                is_submission = bool(game.parse(cleaned)) or (
                    invalid_message is not None) or (
                    game.name == AKARI_GAME.name
                    and looks_like_non_pro_akari(message.content))
                # Only submission attempts pay the ban lookup — plain chat in
                # a busy game channel must not run a DB query per message.
                # (on_message_edit keeps its unconditional up-front check: its
                # early return protects pre-ban rows from the delete-and-
                # reingest path below it.)
                if is_submission and self._is_ingest_banned(
                        message.guild.id, message.author.id, game):
                    await self._notify_banned_submission(message, game)
                    return  # never ingest a banned user's submission
                if invalid_message is not None:
                    await self._notify_invalid_minigame_submission(
                        message, game, cleaned)
                    return
                # Save raw content for future reparse
                cf_common.user_db.save_raw_message(
                    message.id, message.guild.id, message.channel.id,
                    message.author.id, message.created_at.isoformat(),
                    message.content,
                )
                # Non-pro mode submissions look like results but lack accuracy;
                # ask the user to enable Pro Mode and skip the ingest.
                if (game.name == AKARI_GAME.name
                        and looks_like_non_pro_akari(message.content)):
                    await self._notify_non_pro_mode(message)
                    return
                saved = await self._ingest_message(message, game)
                if saved:
                    self._recompute_game_ratings(message.guild.id, game)
            except Exception:
                logger.error('Error ingesting message %s', message.id, exc_info=True)

    @commands.Cog.listener()
    async def on_message_edit(self, before, after):
        if after.guild is None or after.author.bot or cf_common.user_db is None:
            return
        game = self._game_for_channel(after)
        if game is None:
            return
        cleaned = strip_codeblock(after.content)
        invalid_message = self._invalid_minigame_submission_message(game, cleaned)
        is_non_pro = (game.name == AKARI_GAME.name
                      and looks_like_non_pro_akari(after.content))
        if self._is_ingest_banned(after.guild.id, after.author.id, game):
            try:
                if game.parse(cleaned) or is_non_pro or invalid_message is not None:
                    await self._notify_banned_submission(after, game)
            except Exception:
                logger.warning('Failed to notify banned edit %s',
                               after.id, exc_info=True)
            return  # leave pre-ban data untouched
        try:
            # Update raw content so future reparse uses the edited version
            cf_common.user_db.update_raw_message(after.id, after.content)
            if invalid_message is not None:
                changed = cf_common.user_db.delete_minigame_result(after.id)
                changed += cf_common.user_db.delete_imported_minigame_result(after.id)
                await self._notify_invalid_minigame_submission(after, game, cleaned)
                if changed:
                    self._recompute_game_ratings(after.guild.id, game)
                return
            # An edit into a non-pro shape: drop any prior result for this
            # message and tell the user.  Same skip-the-ingest path on_message
            # uses for fresh non-pro posts.
            if is_non_pro:
                changed = cf_common.user_db.delete_minigame_result(after.id)
                changed += cf_common.user_db.delete_imported_minigame_result(after.id)
                await self._notify_non_pro_mode(after)
                if changed:
                    self._recompute_game_ratings(after.guild.id, game)
                return
            # Delete all existing live results for this message, then re-ingest.
            # Handles the case where an edit removes some results from a multi-result message.
            changed = cf_common.user_db.delete_minigame_result(after.id)
            results = game.parse(cleaned)
            if results:
                changed += await self._ingest_message(after, game)
            else:
                changed += cf_common.user_db.delete_imported_minigame_result(after.id)
            if changed:
                self._recompute_game_ratings(after.guild.id, game)
        except Exception:
            logger.error('Error handling message edit %s', after.id, exc_info=True)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload):
        if payload.guild_id is None or cf_common.user_db is None:
            return
        try:
            old = cf_common.user_db.get_minigame_result(payload.message_id)
            deleted = cf_common.user_db.delete_minigame_result(payload.message_id)
            deleted += cf_common.user_db.delete_imported_minigame_result(payload.message_id)
            cf_common.user_db.delete_raw_message(payload.message_id)
            if deleted and old is not None and old.game in self.GAMES:
                self._recompute_game_ratings(
                    payload.guild_id, self.GAMES[old.game])
        except Exception:
            logger.error('Error handling message delete %s', payload.message_id, exc_info=True)
