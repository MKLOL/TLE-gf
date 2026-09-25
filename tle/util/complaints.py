"""Shared complaint workflow for Discord commands and the HTTP API."""
import asyncio
import contextlib
import json
import logging
import re

import discord

from tle import constants

logger = logging.getLogger(__name__)
_COMMIT = re.compile(
    r'https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/commit/[a-fA-F0-9]{7,40}')
_MESSAGE = re.compile(r'https://(?:discord\.com|discordapp\.com)/channels/(\d+)/(\d+)/(\d+)')


class ComplaintError(Exception):
    def __init__(self, status, message):
        self.status = status
        super().__init__(message)


def is_complaint_admin(member):
    return bool(member and (
        getattr(getattr(member, 'guild_permissions', None), 'administrator', False)
        or any(role.name == constants.TLE_ADMIN for role in getattr(member, 'roles', ()))
    ))


def validate_resolution(resolution, commit_url):
    if not isinstance(resolution, str) or not 1 <= len(resolution.strip()) <= 1500:
        raise ComplaintError(400, 'Resolution must contain 1 to 1500 characters.')
    if (not isinstance(commit_url, str) or len(commit_url) > 300
            or not _COMMIT.fullmatch(commit_url)):
        raise ComplaintError(400, 'Provide a GitHub commit URL with a 7 to 40 digit SHA.')
    return resolution.strip(), commit_url


def load_context(raw):
    """Parse a stored complaint transcript, tolerating anything unexpected."""
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return []
    return data if isinstance(data, list) else []


def complaint_json(row, *, include_context=False):
    fields = ('id', 'guild_id', 'user_id', 'text', 'created_at', 'message_link',
              'resolved_at', 'resolved_by', 'resolution', 'commit_url',
              'notification_status', 'notification_link', 'notification_attempts')
    result = {field: getattr(row, field) for field in fields}
    result['status'] = 'resolved' if row.resolved_at is not None else 'open'
    if include_context:
        # Detail only: a 100-complaint listing would otherwise carry a
        # hundred transcripts.
        result['context'] = load_context(getattr(row, 'context', None))
    return result


class ComplaintService:
    def __init__(self, bot, db_getter):
        self.bot = bot
        self._db_getter = db_getter
        self._lock = asyncio.Lock()
        self._worker = None

    @property
    def db(self):
        return self._db_getter()

    def get(self, guild_id, complaint_id):
        row = self.db.get_complaint(complaint_id)
        if row is None or row.guild_id != str(guild_id):
            raise ComplaintError(404, 'Complaint not found.')
        return row

    async def resolve(self, guild_id, complaint_id, actor_id, resolution,
                      commit_url, token_id=None, *, authorize=None):
        resolution, commit_url = validate_resolution(resolution, commit_url)
        async with self._lock:
            if authorize is not None:
                await authorize()
            row = self.get(guild_id, complaint_id)
            if row.resolved_at is not None:
                if (row.resolution, row.commit_url) != (resolution, commit_url):
                    raise ComplaintError(409, 'Already resolved differently; reopen it first.')
            else:
                self.db.resolve_complaint(complaint_id, guild_id, actor_id,
                                          resolution, commit_url, token_id)
            await self._notify(complaint_id)
            return self.get(guild_id, complaint_id)

    async def reopen(self, guild_id, complaint_id, actor_id, token_id=None, *, authorize=None):
        async with self._lock:
            if authorize is not None:
                await authorize()
            self.get(guild_id, complaint_id)
            self.db.reopen_complaint(complaint_id, guild_id, actor_id, token_id)
            return self.get(guild_id, complaint_id)

    async def _deliver(self, row):
        embed = discord.Embed(
            title=f'Complaint #{row.id} resolved', description=row.resolution,
            color=0x28A745)
        embed.add_field(name='Fix', value=f'[View GitHub commit]({row.commit_url})')
        embed.add_field(name='Your complaint', value=row.text[:500], inline=False)
        match = _MESSAGE.fullmatch(row.message_link or '')
        if match and match[1] == row.guild_id:
            channel_id, message_id = int(match[2]), int(match[3])
            try:
                channel = self.bot.get_channel(channel_id)
                if channel is None:
                    channel = await self.bot.fetch_channel(channel_id)
                if str(getattr(getattr(channel, 'guild', None), 'id', '')) == row.guild_id:
                    reference = discord.MessageReference(
                        message_id=message_id, channel_id=channel_id,
                        guild_id=int(row.guild_id), fail_if_not_exists=True)
                    return await channel.send(
                        content=f'<@{row.user_id}>', embed=embed, reference=reference,
                        allowed_mentions=discord.AllowedMentions(
                            users=[discord.Object(id=int(row.user_id))],
                            roles=False, everyone=False, replied_user=False))
            except (discord.HTTPException, OSError):
                pass
        # Older complaints have no source link; inaccessible/deleted originals
        # also fall back to a DM so unrelated channels never receive the text.
        user = self.bot.get_user(int(row.user_id)) or await self.bot.fetch_user(int(row.user_id))
        return await user.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

    async def _notify(self, complaint_id):
        row = self.db.get_complaint(complaint_id)
        if row is None or row.resolved_at is None or row.notification_status == 'sent':
            return
        try:
            message = await asyncio.wait_for(self._deliver(row), timeout=20)
        except (discord.HTTPException, OSError, asyncio.TimeoutError):
            self.db.record_complaint_notification(row.id, 'failed')
            logger.warning('Could not deliver resolution for complaint %s', row.id)
        else:
            self.db.record_complaint_notification(row.id, 'sent', message.jump_url)

    async def retry_notifications(self):
        for row in self.db.pending_complaint_notifications():
            async with self._lock:
                await self._notify(row.id)

    async def _run_notifications(self):
        while True:
            try:
                if self.bot.is_ready():
                    await self.retry_notifications()
            except Exception:
                logger.exception('Complaint notification worker failed')
            await asyncio.sleep(30)

    def start(self):
        if self._worker is None:
            self._worker = asyncio.create_task(self._run_notifications())

    async def close(self):
        if self._worker is not None:
            self._worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker
            self._worker = None
