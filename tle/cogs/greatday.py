"""Great Day cog lifecycle and scheduled-message delivery."""
import asyncio
import logging
import random
from datetime import datetime
from zoneinfo import ZoneInfo

from discord.ext import commands

from tle.cogs._greatday_commands import (
    GreatDayCogError,
    GreatDayCommandsMixin,
    _DEFAULT_TIME,
    _DEFAULT_TZ,
)
from tle.cogs._greatday_history_commands import (
    GreatDayHistoryCommandsMixin,
    _BACKFILL_PROGRESS_INTERVAL,
    _HISTORY_PER_PAGE,
    _STATS_PER_PAGE,
)
from tle.cogs._greatday_helpers import (
    _BACKFILL_STOP_GAP_SECONDS,
    _GREATDAY_RE as _GREATDAY_RE,
    _MENTION_RE as _MENTION_RE,
    _format_pick_time,
    _parse_greatday_message,
    _personal_rank_line,
    _should_stop_backfill,
    _target_datetime,
)
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator  # Re-exported for compatibility with tests.
from tle.util import tasks


logger = logging.getLogger(__name__)

_CHECK_INTERVAL = 60  # seconds between coarse checks
_PRECISE_WINDOW = 300  # schedule precise timer when within 5 minutes
_PICK_COUNT = 5


class GreatDay(
        GreatDayCommandsMixin, GreatDayHistoryCommandsMixin, commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._pending_timers = {}  # guild_id -> asyncio.Task

    async def cog_unload(self):
        for task in self._pending_timers.values():
            task.cancel()

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        self._check_task.start()

    # ── Scheduled task ─────────────────────────────────────────────────

    @tasks.task_spec(name='GreatDayCheck',
                     waiter=tasks.Waiter.fixed_delay(_CHECK_INTERVAL))
    async def _check_task(self, _):
        now = datetime.now(ZoneInfo(_DEFAULT_TZ))
        today = now.strftime('%Y-%m-%d')

        for guild in self.bot.guilds:
            try:
                await self._check_guild(guild, now, today)
            except Exception:
                logger.warning('greatday check failed for guild=%s',
                               guild.id, exc_info=True)

    async def _check_guild(self, guild, now, today):
        kvs_key = f'greatday_last:{guild.id}'
        if cf_common.user_db.kvs_get(kvs_key) == today:
            return  # already sent today

        configured_time = cf_common.user_db.get_guild_config(
            guild.id, 'greatday_time') or _DEFAULT_TIME
        target = _target_datetime(now, configured_time)
        seconds_until = (target - now).total_seconds()

        if seconds_until <= 0:
            # Past target time — send now (catches missed windows / restarts)
            # but not if a precise timer is about to handle it
            if guild.id in self._pending_timers and not self._pending_timers[guild.id].done():
                return
            if await self._send_greatday(guild):
                cf_common.user_db.kvs_set(kvs_key, today)
        elif seconds_until <= _PRECISE_WINDOW:
            # Within 5 minutes — schedule a precise async timer
            if guild.id not in self._pending_timers or self._pending_timers[guild.id].done():
                logger.info('Scheduling precise greatday timer for guild=%s in %.0fs',
                            guild.id, seconds_until)
                self._pending_timers[guild.id] = asyncio.create_task(
                    self._precise_send(guild, seconds_until))

    async def _precise_send(self, guild, delay):
        """Sleep for the exact remaining seconds, then verify and send."""
        try:
            await asyncio.sleep(delay)
            today = datetime.now(ZoneInfo(_DEFAULT_TZ)).strftime('%Y-%m-%d')
            kvs_key = f'greatday_last:{guild.id}'
            if cf_common.user_db.kvs_get(kvs_key) == today:
                return  # already sent (e.g. via ;greatday now)
            if await self._send_greatday(guild):
                cf_common.user_db.kvs_set(kvs_key, today)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning('Precise greatday send failed for guild=%s',
                           guild.id, exc_info=True)
        finally:
            self._pending_timers.pop(guild.id, None)

    async def _send_greatday(self, guild):
        """Pick random users and send a great day message. Returns True if sent."""
        channel_id = cf_common.user_db.get_guild_config(
            guild.id, 'greatday_channel')
        if not channel_id:
            return False
        channel = guild.get_channel(int(channel_id))
        if channel is None:
            return False

        rows = cf_common.user_db.greatday_get_signups(guild.id)
        if not rows:
            return False

        user_ids = [r.user_id for r in rows
                    if guild.get_member(int(r.user_id)) is not None]
        if not user_ids:
            return False
        picked = random.sample(user_ids, min(_PICK_COUNT, len(user_ids)))
        mentions = ' '.join(f'<@{uid}>' for uid in picked)
        verb = 'is' if len(picked) == 1 else 'are'
        msg = await channel.send(f'I hope {mentions} {verb} having a great day!')
        # Record picks best-effort. Once the message is sent, the day is
        # 'done' from the user's perspective — if recording fails the caller
        # must still stamp the kvs sentinel, otherwise the 60s scheduler
        # will keep re-sending.
        try:
            cf_common.user_db.greatday_record_picks(
                guild.id, picked, msg.id, msg.created_at.timestamp())
        except Exception:
            logger.exception('Failed to record greatday picks for guild=%s msg=%s',
                             guild.id, msg.id)
        return True

    @discord_common.send_error_if(GreatDayCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(GreatDay(bot))
