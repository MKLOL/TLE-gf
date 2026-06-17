"""Queens registration queueing and pending-connect workflow. (Minigames cog impl mixin; see minigames.py)."""

import asyncio
import logging
import time


from tle.util import codeforces_common as cf_common
from tle.util import discord_common

from tle.cogs._minigame_queens import (
    QUEENS_GAME, normalize_queens_name,
)
from tle.cogs._minigame_helpers import (
    MinigameCogError, _safe_member_name,
)
from tle.cogs._minigame_queens_cog import (
    _QueensPendingRegistration,
    _QUEENS_ANONYMOUS_LINK_MARKER, _QUEENS_ANONYMOUS_LABEL,
    _QUEENS_PENDING_REGISTRATION_DELAY,
    _queens_public_link_name,
    _clean_queens_linkedin_name,
)

logger = logging.getLogger(__name__)


class ImplQueensRegMixin:
    @staticmethod
    def _queens_pending_registration_key(guild_id, user_id):
        return str(guild_id), str(user_id)

    @staticmethod
    def _queens_pending_public_link_name(pending):
        return _QUEENS_ANONYMOUS_LABEL if pending.anonymous else pending.name

    def _ensure_queens_link_available(self, guild, member, name,
                                      normalized_name, *,
                                      anonymous=False,
                                      ignore_pending_key=None,
                                      ignore_pending=False):
        public_name = _QUEENS_ANONYMOUS_LABEL if anonymous else name
        existing = cf_common.user_db.get_minigame_player_link_by_name(
            guild.id, QUEENS_GAME.name, normalized_name)
        if existing is not None and str(existing.user_id) != str(member.id):
            existing_label = self._queens_public_user_name(
                guild, existing.user_id, {str(existing.user_id): existing})
            raise MinigameCogError(
                f'LinkedIn name `{public_name}` is already linked to '
                f'{existing_label}.')
        if ignore_pending:
            return

        for key, pending in self._queens_pending_registrations.items():
            if key == ignore_pending_key:
                continue
            if str(pending.guild.id) != str(guild.id):
                continue
            if pending.normalized_name != normalized_name:
                continue
            if str(pending.member.id) == str(member.id):
                continue
            pending_label = self._queens_public_user_name(
                guild, pending.member.id)
            raise MinigameCogError(
                f'LinkedIn name `{public_name}` is already pending verification for '
                f'{pending_label}.')

    def _prepare_queens_registration_link(self, guild, member, linkedin_text,
                                          *, anonymous=False,
                                          ignore_pending_key=None,
                                          ignore_pending=False):
        self._ensure_not_minigame_banned(
            guild.id, QUEENS_GAME, member.id, _safe_member_name(member))
        name = _clean_queens_linkedin_name(linkedin_text)
        normalized = normalize_queens_name(name)
        self._ensure_queens_link_available(
            guild, member, name, normalized,
            anonymous=anonymous,
            ignore_pending_key=ignore_pending_key,
            ignore_pending=ignore_pending)
        return name, normalized, _QUEENS_ANONYMOUS_LINK_MARKER if anonymous else None

    def _save_queens_registration_link(self, guild_id, member_id, name,
                                       normalized_name, external_url, linked_by):
        previous_link = cf_common.user_db.get_minigame_player_link(
            guild_id, QUEENS_GAME.name, member_id)
        self._migrate_legacy_queens_results_to_external(guild_id)
        if previous_link is not None:
            self._delete_queens_materialized_results_for_link(
                guild_id, previous_link)
        cf_common.user_db.set_minigame_player_link(
            guild_id, QUEENS_GAME.name, member_id, name, normalized_name,
            external_url, time.time(), linked_by)
        self._migrate_legacy_queens_results_to_external(guild_id)
        claimed = self._claim_queens_unresolved_results(
            guild_id, member_id, normalized_name)
        self._recompute_minigame_ratings(guild_id, QUEENS_GAME)
        return claimed

    def _cmd_queens_register_link(self, ctx, member, linkedin_text,
                                  anonymous=False, ignore_pending=False):
        name, normalized, external_url = self._prepare_queens_registration_link(
            ctx.guild, member, linkedin_text, anonymous=anonymous,
            ignore_pending=ignore_pending)
        claimed = self._save_queens_registration_link(
            ctx.guild.id, member.id, name, normalized, external_url,
            ctx.author.id)
        self._clear_queens_pending_matching(ctx.guild.id, user_id=member.id)
        if ignore_pending:
            self._clear_queens_pending_matching(
                ctx.guild.id, normalized_name=normalized)
        return claimed

    async def _cmd_queens_set(self, ctx, member, linkedin_text,
                              anonymous=False):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        claimed = self._cmd_queens_register_link(
            ctx, member, linkedin_text, anonymous=anonymous,
            ignore_pending=True)
        link = cf_common.user_db.get_minigame_player_link(
            ctx.guild.id, QUEENS_GAME.name, member.id)
        display_name = self._queens_public_user_name(
            ctx.guild, member.id, {str(member.id): link})
        lines = [
            f'`{display_name}` is registered for {QUEENS_GAME.display_name} as '
            f'`{_queens_public_link_name(link)}`.',
        ]
        del claimed
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    async def _cmd_queens_register(self, ctx, member, linkedin_text,
                                   anonymous=False):
        self._require_enabled(ctx.guild.id, QUEENS_GAME)
        pending = self._queue_queens_registration(
            ctx, member, linkedin_text, anonymous=anonymous)
        display_name = self._queens_public_user_name(
            ctx.guild, member.id)
        who = 'Your' if member.id == ctx.author.id else f'`{display_name}`\'s'
        link_name = pending.name
        if anonymous and not getattr(ctx, 'reveal_queens_anonymous_name', False):
            link_name = _QUEENS_ANONYMOUS_LABEL
        await ctx.send(embed=discord_common.embed_neutral('\n'.join([
            f'{who} {QUEENS_GAME.display_name} registration is pending as '
            f'`{link_name}`.',
            self._queens_connection_instruction(ctx.guild.id),
            f'You have {_QUEENS_PENDING_REGISTRATION_DELAY} seconds to send '
            'the request. After that, I will check received LinkedIn requests '
            'and expire this registration if no matching request is found.',
        ])))

    def _queue_queens_registration(self, ctx, member, linkedin_text,
                                   *, anonymous=False):
        key = self._queens_pending_registration_key(ctx.guild.id, member.id)
        name, normalized, _external_url = self._prepare_queens_registration_link(
            ctx.guild, member, linkedin_text, anonymous=anonymous,
            ignore_pending_key=key)
        pending = _QueensPendingRegistration(
            guild=ctx.guild,
            member=member,
            channel_id=getattr(getattr(ctx, 'channel', None), 'id', None),
            linked_by=ctx.author.id,
            name=name,
            normalized_name=normalized,
            anonymous=anonymous,
            created_at=time.time(),
        )
        self._queens_pending_registrations[key] = pending
        self._schedule_queens_connect_worker(ctx.guild.id)
        return pending

    def _schedule_queens_connect_worker(self, guild_id):
        guild_key = str(guild_id)
        task = self._queens_connect_tasks.get(guild_key)
        if task is not None and not task.done():
            return
        task = asyncio.create_task(self._queens_connect_worker(guild_key))
        self._queens_connect_tasks[guild_key] = task

        def clear_done(done_task):
            if self._queens_connect_tasks.get(guild_key) is done_task:
                self._queens_connect_tasks.pop(guild_key, None)

        task.add_done_callback(clear_done)

    def _queens_pending_for_guild(self, guild_id):
        guild_key = str(guild_id)
        return [
            pending for pending in self._queens_pending_registrations.values()
            if str(pending.guild.id) == guild_key
        ]

    async def _queens_connect_worker(self, guild_id):
        try:
            while True:
                pending = self._queens_pending_for_guild(guild_id)
                if not pending:
                    return
                now = time.time()
                ready = [
                    item for item in pending
                    if item.created_at + _QUEENS_PENDING_REGISTRATION_DELAY <= now
                ]
                if not ready:
                    next_at = min(item.created_at for item in pending)
                    next_at += _QUEENS_PENDING_REGISTRATION_DELAY
                    await asyncio.sleep(max(0.1, next_at - now))
                    continue
                processed = await self._process_queens_pending_registrations(
                    guild_id, ready)
                if not processed:
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.error(
                'Queens pending registration worker failed for guild %s',
                guild_id, exc_info=True)

    async def _process_queens_pending_registrations(self, guild_id, pending):
        names = []
        seen = set()
        for item in pending:
            if item.normalized_name in seen:
                continue
            seen.add(item.normalized_name)
            names.append(item.name)
        payload, error = await self._run_queens_connect(guild_id, names)
        if error is not None:
            await self._notify_queens_pending_batch(
                pending,
                discord_common.embed_alert(
                    f'Could not check LinkedIn connection requests: {error}'))
            self._clear_queens_pending_batch(pending)
            return True

        status = payload.get('status')
        if status != 'ok':
            await self._notify_queens_pending_batch(
                pending,
                discord_common.embed_alert(
                    self._queens_status_message(status)))
            self._clear_queens_pending_batch(pending)
            return True

        accepted = set(payload.get('accepted_normalized') or [])
        for name in payload.get('accepted') or []:
            accepted.add(normalize_queens_name(name))

        for item in pending:
            key = self._queens_pending_registration_key(
                item.guild.id, item.member.id)
            if self._queens_pending_registrations.get(key) != item:
                continue
            if item.normalized_name in accepted:
                await self._complete_queens_pending_registration(item)
            else:
                self._queens_pending_registrations.pop(key, None)
                link_name = self._queens_pending_public_link_name(item)
                await self._send_queens_pending_message(
                    item,
                    discord_common.embed_alert(
                        f'I did not find a received LinkedIn connection '
                        f'request for `{link_name}`, so this '
                        f'{QUEENS_GAME.display_name} registration expired. '
                        'If you are already connected but not registered, '
                        'disconnect on LinkedIn and send the connection request '
                        'again, then run `;queens register` again.'))
        return True

    def _clear_queens_pending_batch(self, pending):
        for item in pending:
            key = self._queens_pending_registration_key(
                item.guild.id, item.member.id)
            if self._queens_pending_registrations.get(key) == item:
                self._queens_pending_registrations.pop(key, None)

    def _clear_queens_pending_matching(self, guild_id, *, user_id=None,
                                       normalized_name=None):
        guild_key = str(guild_id)
        for key, item in list(self._queens_pending_registrations.items()):
            if str(item.guild.id) != guild_key:
                continue
            if user_id is not None and str(item.member.id) != str(user_id):
                continue
            if normalized_name is not None and item.normalized_name != normalized_name:
                continue
            self._queens_pending_registrations.pop(key, None)

    async def _complete_queens_pending_registration(self, pending):
        key = self._queens_pending_registration_key(
            pending.guild.id, pending.member.id)
        external_url = (
            _QUEENS_ANONYMOUS_LINK_MARKER if pending.anonymous else None)
        try:
            self._prepare_queens_registration_link(
                pending.guild, pending.member, pending.name,
                anonymous=pending.anonymous, ignore_pending_key=key)
            claimed = self._save_queens_registration_link(
                pending.guild.id, pending.member.id, pending.name,
                pending.normalized_name, external_url, pending.linked_by)
        except MinigameCogError as exc:
            self._queens_pending_registrations.pop(key, None)
            await self._send_queens_pending_message(
                pending, discord_common.embed_alert(str(exc)))
            return

        self._queens_pending_registrations.pop(key, None)
        link = cf_common.user_db.get_minigame_player_link(
            pending.guild.id, QUEENS_GAME.name, pending.member.id)
        display_name = self._queens_public_user_name(
            pending.guild, pending.member.id, {str(pending.member.id): link})
        lines = [
            f'`{display_name}` is registered for {QUEENS_GAME.display_name} as '
            f'`{_queens_public_link_name(link)}`.',
        ]
        if claimed:
            lines.append(
                f'Claimed {claimed} stored Queens result(s) and recomputed ratings.')
        await self._send_queens_pending_message(
            pending, discord_common.embed_success('\n'.join(lines)))

    async def _notify_queens_pending_batch(self, pending, embed):
        notified = set()
        for item in pending:
            channel_id = item.channel_id
            if channel_id is None or channel_id in notified:
                continue
            notified.add(channel_id)
            await self._send_queens_pending_message(item, embed)

    async def _send_queens_pending_message(self, pending, embed):
        if self.bot is None or pending.channel_id is None:
            return
        channel = None
        try:
            if hasattr(self.bot, 'get_channel'):
                channel = self.bot.get_channel(int(pending.channel_id))
            if channel is None and hasattr(self.bot, 'fetch_channel'):
                channel = await self.bot.fetch_channel(int(pending.channel_id))
            if channel is not None:
                await channel.send(embed=embed)
        except Exception:
            logger.warning(
                'Failed to send Queens registration result to channel %s',
                pending.channel_id, exc_info=True)

