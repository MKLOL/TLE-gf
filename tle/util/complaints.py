"""Shared complaint workflow for Discord commands and the HTTP API."""
import asyncio
import contextlib
import json
import logging
import re

import discord

from tle.util import git_state

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


def complaint_json(row, *, include_context=False, tags=()):
    fields = ('id', 'guild_id', 'user_id', 'text', 'created_at', 'message_link',
              'resolved_at', 'resolved_by', 'resolution', 'commit_url',
              'notification_status', 'notification_link', 'notification_attempts')
    result = {field: getattr(row, field) for field in fields}
    result['status'] = 'resolved' if row.resolved_at is not None else 'open'
    result['tags'] = list(tags)
    if include_context:
        # Detail only: a 100-complaint listing would otherwise carry a
        # hundred transcripts.
        result['context'] = load_context(getattr(row, 'context', None))
    return result


class ComplaintService:
    def __init__(self, bot, db_getter, *,
                 is_deployed=git_state.commit_is_deployed, head=None):
        self.bot = bot
        self._db_getter = db_getter
        self._lock = asyncio.Lock()
        self._worker = None
        self._is_deployed = is_deployed
        self._released_this_run = False
        # Captured now, at cog load, not on first use: a `git pull` while the
        # bot is up moves HEAD but not the loaded code, so the SHA must be
        # read before anyone can have pulled. None means it cannot be known
        # here (no git, no repository — the Dockerfile copies the tree
        # without ``.git``); every API resolution then stays queued, and that
        # is said once per process at WARNING so it does not go unnoticed.
        self._head = head if head is not None else git_state.current_head()
        if self._head is None:
            logger.warning('Cannot determine the running commit; API '
                           'complaint resolutions will stay queued until '
                           'the bot runs from a git checkout')

    def running_head(self):
        """The commit this process runs, captured at construction."""
        return self._head

    async def _deployed(self, commit_url):
        sha = git_state.commit_sha(commit_url)
        head = self.running_head()
        if not sha or not head:
            return False
        # Python 3.8 remains supported; asyncio.to_thread arrived in 3.9.
        return await asyncio.get_running_loop().run_in_executor(
            None, self._is_deployed, sha, head)

    @property
    def db(self):
        return self._db_getter()

    def get(self, guild_id, complaint_id):
        row = self.db.get_complaint(complaint_id)
        if row is None or row.guild_id != str(guild_id):
            raise ComplaintError(404, 'Complaint not found.')
        return row

    async def resolve(self, guild_id, complaint_id, actor_id, resolution,
                      commit_url, token_id=None, *, authorize=None,
                      defer_notification=False):
        """Resolve a complaint; notify now, or queue until the fix is deployed.

        The API resolves from a checkout that has merely been pushed, so its
        notifications are deferred: telling someone their problem is fixed
        while the bot still runs the old code is worse than telling them
        late. A Discord admin resolving by hand keeps the immediate path.
        """
        resolution, commit_url = validate_resolution(resolution, commit_url)
        async with self._lock:
            if authorize is not None:
                await authorize()
            row = self.get(guild_id, complaint_id)
            if row.resolved_at is not None:
                if (row.resolution, row.commit_url) != (resolution, commit_url):
                    raise ComplaintError(409, 'Already resolved differently; reopen it first.')
                # An identical repeat re-runs the deploy check: a git timeout
                # at the first resolve must not park the row until a restart.
                if (row.notification_status == 'queued'
                        and await self._deployed(commit_url)):
                    self.db.release_complaint_notification(complaint_id, commit_url)
            else:
                self.db.resolve_complaint(complaint_id, guild_id, actor_id,
                                          resolution, commit_url, token_id,
                                          defer_notification=defer_notification)
                # If this process already runs the fix there is nothing to
                # wait for; the check is the same one a restart would make.
                if defer_notification and await self._deployed(commit_url):
                    self.db.release_complaint_notification(complaint_id, commit_url)
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
        if row is None or row.resolved_at is None:
            return
        # 'queued' is the one status this must never send from: only
        # release_deployed may move it on, and only after verifying the
        # commit is running. A repeated resolve — from the API or a Discord
        # admin — therefore cannot leak the notification out early.
        if row.notification_status in ('sent', 'queued'):
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

    async def release_deployed(self):
        """Hand queued notifications to the worker once their commit runs.

        Checked against ``git`` in the bot's own checkout, the same way
        ``;meta git`` reports it. HEAD cannot change without a restart, so
        this runs once per process; a fix pushed but not yet deployed simply
        stays queued until the next restart that has it. Returns how many
        were released.
        """
        released = 0
        for row in self.db.queued_complaint_notifications():
            if not await self._deployed(row.commit_url):
                logger.info('Complaint %s: fix %s is not in the running commit; '
                            'notification stays queued', row.id, row.commit_url)
                continue
            if self.db.release_complaint_notification(row.id, row.commit_url):
                released += 1
                logger.info('Complaint %s: fix %s is deployed, notification released',
                            row.id, row.commit_url)
        return released

    async def _run_notifications(self):
        while True:
            if self.bot.is_ready():
                # Two guards: a release that keeps failing must not starve
                # the ordinary retries of failed deliveries.
                if not self._released_this_run:
                    try:
                        await self.release_deployed()
                        self._released_this_run = True
                    except Exception:
                        logger.exception('Releasing queued complaint notifications failed')
                try:
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
