import asyncio
import logging
import random
from datetime import datetime
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands

from tle import constants
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
from tle.util import paginator
from tle.util import ranking
from tle.util import tasks

logger = logging.getLogger(__name__)

_CHECK_INTERVAL = 60  # seconds between coarse checks
_PRECISE_WINDOW = 300  # schedule precise timer when within 5 minutes
_DEFAULT_TIME = '10:00'
_DEFAULT_TZ = 'US/Eastern'
_PICK_COUNT = 5
_DRAGOS_ID = 450089497263341586
_NEXT_DRAGOS_KEY_PREFIX = 'greatday_next_dragos:'
_BAN_REPLY = 'No. How about we stop doing this? 😊'
_STATS_PER_PAGE = 15
_HISTORY_PER_PAGE = 15
# Edit the backfill progress embed every N scanned messages. Discord rate-
# limits message edits to ~5/5s — 250 is a comfortable cadence even for
# multi-thousand-message channels.
_BACKFILL_PROGRESS_INTERVAL = 250


class GreatDayCogError(commands.CommandError):
    pass


class GreatDay(commands.Cog):
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
        dragos_key = f'{_NEXT_DRAGOS_KEY_PREFIX}{guild.id}'
        tag_dragos = cf_common.user_db.kvs_get(dragos_key) == str(_DRAGOS_ID)
        picked = ([str(_DRAGOS_ID)] * _PICK_COUNT if tag_dragos else
                  random.sample(user_ids, min(_PICK_COUNT, len(user_ids))))
        mentions = ' '.join(f'<@{uid}>' for uid in picked)
        verb = 'is' if len(picked) == 1 else 'are'
        msg = await channel.send(f'I hope {mentions} {verb} having a great day!')
        if tag_dragos:
            cf_common.user_db.kvs_delete(dragos_key)
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

    # ── Commands ───────────────────────────────────────────────────────

    @commands.group(name='greatday', brief='Great Day commands',
                    invoke_without_command=True)
    async def greatday(self, ctx):
        await ctx.send_help(ctx.command)

    @greatday.command(name='signup', brief='Sign up for daily great day pings')
    async def signup(self, ctx):
        added = cf_common.user_db.greatday_signup(ctx.guild.id, ctx.author.id)
        if added:
            await ctx.send(embed=discord_common.embed_success(
                'You have been signed up for great day pings!'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                'You are already signed up.'))

    @greatday.command(name='remove', brief='Remove yourself from the list')
    async def remove(self, ctx):
        removed = cf_common.user_db.greatday_remove(ctx.guild.id, ctx.author.id)
        if removed:
            await ctx.send(embed=discord_common.embed_success(
                'You have been removed from great day pings.'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                'You are not signed up.'))

    @greatday.command(name='add', brief='Add a user to the list (admin)',
                      usage='@user')
    @commands.has_role(constants.TLE_ADMIN)
    async def add_user(self, ctx, member: discord.Member):
        added = cf_common.user_db.greatday_signup(ctx.guild.id, member.id)
        name = discord.utils.escape_mentions(member.display_name)
        if added:
            await ctx.send(embed=discord_common.embed_success(
                f'`{name}` has been added to great day pings.'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                f'`{name}` is already signed up.'))

    @greatday.command(name='kick', brief='Remove a user from the list (admin)',
                      usage='@user')
    @commands.has_role(constants.TLE_ADMIN)
    async def kick_user(self, ctx, member: discord.Member):
        removed = cf_common.user_db.greatday_remove(ctx.guild.id, member.id)
        name = discord.utils.escape_mentions(member.display_name)
        if removed:
            await ctx.send(embed=discord_common.embed_success(
                f'`{name}` has been removed from great day pings.'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                f'`{name}` is not signed up.'))

    @greatday.command(name='ban', brief='Ban a user from great day (admin)',
                      usage='@user')
    async def ban_user(self, ctx, *, _target: str = ''):
        if ctx.author.id == _DRAGOS_ID:
            key = f'{_NEXT_DRAGOS_KEY_PREFIX}{ctx.guild.id}'
            cf_common.user_db.kvs_set(key, str(_DRAGOS_ID))
        await ctx.send(_BAN_REPLY)

    @greatday.command(name='unban', brief='Unban a user from great day (admin)',
                      usage='@user')
    @commands.has_role(constants.TLE_ADMIN)
    async def unban_user(self, ctx, member: discord.Member):
        unbanned = cf_common.user_db.greatday_unban(ctx.guild.id, member.id)
        name = discord.utils.escape_mentions(member.display_name)
        if unbanned:
            await ctx.send(embed=discord_common.embed_success(
                f'`{name}` has been unbanned from great day.'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                f'`{name}` is not banned.'))

    @greatday.command(name='banlist', brief='Show users banned from great day')
    async def banlist(self, ctx):
        rows = cf_common.user_db.greatday_get_banned(ctx.guild.id)
        if not rows:
            await ctx.send(embed=discord_common.embed_neutral(
                'No one is banned from great day.'))
            return
        lines = [f'<@{r.user_id}>' for r in rows]
        embed = discord.Embed(title='Great day ban list',
                              description='\n'.join(lines))
        await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

    @greatday.command(name='here', brief='Set the great day channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def here(self, ctx):
        cf_common.user_db.set_guild_config(
            ctx.guild.id, 'greatday_channel', str(ctx.channel.id))
        await ctx.send(embed=discord_common.embed_success(
            f'Great day channel set to {ctx.channel.mention}'))

    @greatday.command(name='now', brief='Send a great day message now')
    @commands.has_role(constants.TLE_ADMIN)
    async def now(self, ctx):
        channel_id = cf_common.user_db.get_guild_config(
            ctx.guild.id, 'greatday_channel')
        if not channel_id:
            raise GreatDayCogError(
                'No great day channel set. Use `;greatday here` first.')
        rows = cf_common.user_db.greatday_get_signups(ctx.guild.id)
        if not rows:
            raise GreatDayCogError('No one has signed up yet.')
        sent = await self._send_greatday(ctx.guild)
        if sent:
            today = datetime.now(ZoneInfo(_DEFAULT_TZ)).strftime('%Y-%m-%d')
            cf_common.user_db.kvs_set(f'greatday_last:{ctx.guild.id}', today)
            await ctx.send(embed=discord_common.embed_success(
                'Great day message sent!'))
        else:
            raise GreatDayCogError('Could not send great day message.')

    @greatday.command(name='time', brief='Set the daily time (HH:MM US/Eastern)',
                      usage='HH:MM')
    @commands.has_role(constants.TLE_ADMIN)
    async def set_time(self, ctx, time_str: str):
        parts = time_str.split(':')
        if len(parts) != 2:
            raise GreatDayCogError('Time must be in HH:MM format (e.g. `10:00`).')
        try:
            hour, minute = int(parts[0]), int(parts[1])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except ValueError:
            raise GreatDayCogError('Time must be in HH:MM format (e.g. `10:00`).')

        formatted = f'{hour:02d}:{minute:02d}'
        cf_common.user_db.set_guild_config(
            ctx.guild.id, 'greatday_time', formatted)
        await ctx.send(embed=discord_common.embed_success(
            f'Great day time set to **{formatted}** US/Eastern.'))

    @greatday.command(name='show', brief='Show current settings')
    async def show(self, ctx):
        channel_id = cf_common.user_db.get_guild_config(
            ctx.guild.id, 'greatday_channel')
        time_str = cf_common.user_db.get_guild_config(
            ctx.guild.id, 'greatday_time') or _DEFAULT_TIME
        rows = cf_common.user_db.greatday_get_signups(ctx.guild.id)
        channel = f'<#{channel_id}>' if channel_id else 'not set'
        lines = [
            f'Channel: {channel}',
            f'Time: **{time_str}** US/Eastern',
            f'Signed up: **{len(rows)}** user(s)',
        ]
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    @greatday.command(name='latest', aliases=['last'],
                      brief='Show the latest time a user was great-day\'d',
                      usage='[@user]')
    async def latest(self, ctx, member: discord.Member = None):
        target = member or ctx.author
        name = discord.utils.escape_markdown(
            discord.utils.escape_mentions(target.display_name))
        row = cf_common.user_db.greatday_get_latest_pick(
            ctx.guild.id, target.id)
        description = ('No Great Day picks have been recorded for this user.'
                       if row is None else
                       f'Last selected: {_format_pick_time(row.picked_at)}')
        await ctx.send(embed=discord.Embed(
            title=f'Latest Great Day — {name}',
            description=description,
            color=0x00aaff,
        ))

    @greatday.command(name='history',
                      brief='Show a user\'s Great Day history',
                      usage='[@user]')
    async def history(self, ctx, member: discord.Member = None):
        target = member or ctx.author
        name = discord.utils.escape_markdown(
            discord.utils.escape_mentions(target.display_name))
        rows = cf_common.user_db.greatday_get_pick_history(
            ctx.guild.id, target.id)
        title = f'Great Day history — {name}'
        if not rows:
            await ctx.send(embed=discord.Embed(
                title=title,
                description='No Great Day picks have been recorded for this user.',
                color=0x00aaff,
            ))
            return

        numbered = list(enumerate(rows, start=1))
        pages = []
        for chunk in paginator.chunkify(numbered, _HISTORY_PER_PAGE):
            lines = [f'**{number}.** {_format_pick_time(row.picked_at)}'
                     for number, row in chunk]
            pages.append((None, discord.Embed(
                title=title,
                description='\n'.join(lines),
                color=0x00aaff,
            )))
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60,
                           set_pagenum_footers=True, author_id=ctx.author.id)

    @greatday.command(name='stats', brief='Show how many times users have been great-day\'d',
                      usage='[@user]')
    async def stats(self, ctx, member: discord.Member = None):
        if member is not None:
            count = cf_common.user_db.greatday_get_count(ctx.guild.id, member.id)
            name = discord.utils.escape_mentions(member.display_name)
            await ctx.send(embed=discord_common.embed_neutral(
                f'`{name}` has been great-day\'d **{count}** time(s).'))
            return

        rows = cf_common.user_db.greatday_get_stats(ctx.guild.id)
        if not rows:
            raise GreatDayCogError(
                'No picks recorded yet. Admins can run `;greatday backfill` '
                'to seed history from the channel.')

        personal = _personal_rank_line(rows, ctx.author.id)
        # Rank the whole list once with standard competition ranking so tied
        # counts share a rank (and the tie still numbers correctly across page
        # boundaries), then paginate the (rank, row) pairs.
        ranked = ranking.rank_items(rows, lambda r: r.cnt)
        chunks = paginator.chunkify(ranked, _STATS_PER_PAGE)
        pages = []
        for chunk in chunks:
            lines = []
            for rank, row in chunk:
                m = ctx.guild.get_member(int(row.user_id))
                name = m.mention if m is not None else f'`{row.user_id}`'
                lines.append(f'**#{rank}** {name} — **{row.cnt}**')
            embed = discord.Embed(
                title='Great Day leaderboard',
                description='\n'.join(lines),
                color=0x00aaff,
            )
            pages.append((personal, embed))
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=5 * 60,
                           set_pagenum_footers=True, author_id=ctx.author.id)

    @greatday.command(name='backfill',
                      brief='Seed pick history from the greatday channel (admin)')
    @commands.has_role(constants.TLE_ADMIN)
    async def backfill(self, ctx):
        """Walk the greatday channel's history and insert one pick row per
        matched message and mentioned user. Idempotent — safe to re-run.
        """
        channel_id = cf_common.user_db.get_guild_config(
            ctx.guild.id, 'greatday_channel')
        if not channel_id:
            raise GreatDayCogError(
                'No great day channel set. Use `;greatday here` first.')
        channel = ctx.guild.get_channel(int(channel_id))
        if channel is None:
            raise GreatDayCogError('Configured great day channel is not accessible.')

        progress = await ctx.send(embed=discord_common.embed_neutral(
            f'Backfilling from {channel.mention}… (scanned **0**, matched **0**)'))

        bot_user_id = self.bot.user.id if self.bot and self.bot.user else None
        scanned = 0
        matched = 0
        inserted = 0
        last_match_ts = None
        stopped_early = False
        # Newest first — leaderboard updates with recent picks immediately,
        # and if the admin aborts (bot restart) the most relevant history
        # is already saved.
        async for msg in channel.history(limit=None, oldest_first=False):
            scanned += 1
            msg_ts = msg.created_at.timestamp()
            uids = _parse_greatday_message(msg, bot_user_id)
            if uids is not None:
                matched += 1
                inserted += cf_common.user_db.greatday_record_picks(
                    ctx.guild.id, uids, msg.id, msg_ts)
                last_match_ts = msg_ts
            elif _should_stop_backfill(last_match_ts, msg_ts,
                                        _BACKFILL_STOP_GAP_SECONDS):
                stopped_early = True
                break

            if scanned % _BACKFILL_PROGRESS_INTERVAL == 0:
                try:
                    await progress.edit(embed=discord_common.embed_neutral(
                        f'Backfilling from {channel.mention}… '
                        f'scanned **{scanned}**, matched **{matched}**, '
                        f'inserted **{inserted}** so far.'))
                except discord.HTTPException:
                    # Rate-limited or message deleted — keep scanning either way.
                    pass

        gap_days = _BACKFILL_STOP_GAP_SECONDS // 86400
        tail = (f' Stopped early after a {gap_days}-day gap with no further '
                'greatday messages — assumed full history captured.'
                if stopped_early else '')
        await progress.edit(embed=discord_common.embed_success(
            f'Backfill complete. Scanned **{scanned}** message(s), '
            f'matched **{matched}**, inserted **{inserted}** new pick row(s).'
            + tail))
        # Fresh ping so the invoker sees completion even if the progress
        # message has scrolled out of view.
        await ctx.send(f'{ctx.author.mention} `;greatday backfill` finished — '
                       f'inserted **{inserted}** new pick row(s).')

    # ── Error handler ──────────────────────────────────────────────────

    @discord_common.send_error_if(GreatDayCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(GreatDay(bot))
