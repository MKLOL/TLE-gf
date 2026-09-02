"""Completed/current Akari weekly ratings and scheduled announcements."""

import datetime as dt
import inspect
import logging
import re
from zoneinfo import ZoneInfo

from discord.ext import commands

from tle.util import codeforces_common as cf_common
from tle.util import discord_common, tasks
from tle.util.akari_weekly import week_start
from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_helpers import ChannelOrThread, MinigameCogError, _mg


logger = logging.getLogger(__name__)

_POST_CHANNEL_KEY = 'akari_weekly_post_channel'
_POST_TIME_KEY = 'akari_weekly_post_time'
_POST_LAST_PREFIX = 'akari_weekly_post_last:'
_POST_CHECK_INTERVAL = 5 * 60
_POST_TIMEZONE = 'America/New_York'
_DEFAULT_POST_TIME = '00:00'


def parse_weekly_post_time(value):
    value = str(value)
    if re.fullmatch(r'\d{2}:\d{2}', value) is None:
        raise ValueError('Time must use 24-hour `HH:MM` format.')
    try:
        parsed = dt.datetime.strptime(value, '%H:%M').time()
    except ValueError as exc:
        raise ValueError('Time must use 24-hour `HH:MM` format.') from exc
    return parsed.strftime('%H:%M'), parsed


def weekly_period_dates(now, post_time):
    """Rating cutoff and current standings date at ``now``."""
    _normalized, cutoff_time = parse_weekly_post_time(post_time)
    monday = week_start(now.date())
    cutoff = dt.datetime.combine(monday, cutoff_time, tzinfo=now.tzinfo)
    if now < cutoff:
        return monday - dt.timedelta(days=1), monday - dt.timedelta(days=7)
    return monday, monday


class ImplAkariWeeklyMixin:
    @staticmethod
    def _weekly_post_time(guild_id):
        value = cf_common.user_db.get_guild_config(guild_id, _POST_TIME_KEY)
        return value or _DEFAULT_POST_TIME

    def _weekly_display_dates(self, guild_id):
        now = dt.datetime.now(ZoneInfo(_POST_TIMEZONE))
        return weekly_period_dates(now, self._weekly_post_time(guild_id))

    async def _cmd_akari_completed_weekly_ratings(
            self, ctx, *, excluded_ids=None, included_ids=None,
            include_inactive=False, weekdays=None, date_bounds=None,
            show_all=False):
        as_of_date, standings_date = self._weekly_display_dates(ctx.guild.id)
        rows, _standings = await self._akari_weekly_preview(
            ctx.guild.id, excluded_ids=excluded_ids,
            included_ids=included_ids, weekdays=weekdays,
            date_bounds=date_bounds, as_of_date=as_of_date,
            standings_date=standings_date)
        if not rows:
            raise MinigameCogError(
                f'No completed {AKARI_GAME.display_name} weekly ratings yet.')

        registrants = cf_common.user_db.get_akari_registrants(ctx.guild.id)
        visible = registrants - self._akari_banned_user_ids(ctx.guild.id)
        eligible = rows if show_all else [row for row in rows
                                          if row.user_id in visible]
        shown = self._active_ranking_rows(
            eligible, include_inactive=include_inactive)
        if not shown:
            raise MinigameCogError(
                f'No eligible {AKARI_GAME.display_name} weekly ratings found.')

        qualifiers = []
        if show_all:
            qualifiers.append('all')
        if include_inactive:
            qualifiers.append('incl. inactive')
        suffix = f' ({", ".join(qualifiers)})' if qualifiers else ''
        rating_file = _mg()._get_akari_rating_table_image_file(
            ctx.guild, shown, registrants,
            title=f'Daily Akari Weekly Ratings{suffix}',
            mark_registered=show_all, games_label='Weeks')
        await ctx.send(file=rating_file)

    async def _cmd_akari_current_week_ratings(
            self, ctx, *, excluded_ids=None, included_ids=None,
            weekdays=None, date_bounds=None, show_all=False):
        as_of_date, standings_date = self._weekly_display_dates(ctx.guild.id)
        _rows, standings = await self._akari_weekly_preview(
            ctx.guild.id, excluded_ids=excluded_ids,
            included_ids=included_ids, weekdays=weekdays,
            date_bounds=date_bounds, as_of_date=as_of_date,
            standings_date=standings_date)
        if not show_all:
            visible = (cf_common.user_db.get_akari_registrants(ctx.guild.id)
                       - self._akari_banned_user_ids(ctx.guild.id))
            standings = [row for row in standings if row.user_id in visible]
        await self._send_akari_weekly_scores(ctx, standings)

    async def _cmd_akari_weekly_post(self, ctx, args):
        if not args:
            return await self._send_weekly_post_settings(ctx)
        action = args[0].casefold()
        if action == 'here' and len(args) == 1:
            return await self._set_weekly_post_channel(ctx, ctx.channel)
        if action == 'clear' and len(args) == 1:
            cf_common.user_db.delete_guild_config(
                ctx.guild.id, _POST_CHANNEL_KEY)
            return await ctx.send(embed=discord_common.embed_success(
                'Automatic weekly Akari result posting disabled.'))
        if action == 'time' and len(args) == 2:
            try:
                normalized, _parsed = parse_weekly_post_time(args[1])
            except ValueError as exc:
                raise MinigameCogError(str(exc)) from exc
            cf_common.user_db.set_guild_config(
                ctx.guild.id, _POST_TIME_KEY, normalized)
            return await ctx.send(embed=discord_common.embed_success(
                f'Weekly cutoff/post time set to `{normalized}` '
                f'({_POST_TIMEZONE}).'))
        if action == 'thread' and len(args) == 2:
            channel = await ChannelOrThread().convert(ctx, args[1])
            return await self._set_weekly_post_channel(ctx, channel)
        raise MinigameCogError(
            'Usage: `;akari weeklypost '
            '[here|thread CHANNEL|time HH:MM|clear]`.')

    async def _send_weekly_post_settings(self, ctx):
        channel_id = cf_common.user_db.get_guild_config(
            ctx.guild.id, _POST_CHANNEL_KEY)
        target = f'<#{channel_id}>' if channel_id else 'not set'
        await ctx.send(embed=discord_common.embed_neutral(
            f'Weekly result thread/channel: {target}\n'
            f'Weekly cutoff/post time: '
            f'`{self._weekly_post_time(ctx.guild.id)}` ({_POST_TIMEZONE})'))

    @staticmethod
    async def _set_weekly_post_channel(ctx, channel):
        cf_common.user_db.set_guild_config(
            ctx.guild.id, _POST_CHANNEL_KEY, str(channel.id))
        await ctx.send(embed=discord_common.embed_success(
            f'Weekly Akari results will be posted in <#{channel.id}>.'))

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        self._akari_weekly_announcement_check.start()

    async def _stop_akari_weekly_announcement(self):
        stopped = self._akari_weekly_announcement_check.stop()
        if inspect.isawaitable(stopped):
            await stopped

    @tasks.task_spec(
        name='AkariWeeklyAnnouncementCheck',
        waiter=tasks.Waiter.fixed_delay(_POST_CHECK_INTERVAL))
    async def _akari_weekly_announcement_check(self, _):
        if cf_common.user_db is None:
            return
        now = dt.datetime.now(ZoneInfo(_POST_TIMEZONE))
        for guild in self.bot.guilds:
            try:
                _as_of, current_start = weekly_period_dates(
                    now, self._weekly_post_time(guild.id))
                await self._check_akari_weekly_announcement(
                    guild, current_start - dt.timedelta(days=7))
            except Exception:
                logger.warning(
                    'Weekly Akari announcement check failed for guild=%s',
                    getattr(guild, 'id', None), exc_info=True)

    async def _check_akari_weekly_announcement(self, guild, completed_start):
        if not self._is_enabled(guild.id, AKARI_GAME.feature_flag):
            return False
        channel_id = cf_common.user_db.get_guild_config(
            guild.id, _POST_CHANNEL_KEY)
        if channel_id is None:
            return False
        week_key = completed_start.isoformat()
        kvs_key = f'{_POST_LAST_PREFIX}{guild.id}'
        if cf_common.user_db.kvs_get(kvs_key) == week_key:
            return False
        sent = await self._send_akari_weekly_announcement(
            guild, int(channel_id), completed_start)
        if sent:
            cf_common.user_db.kvs_set(kvs_key, week_key)
        return sent

    async def _send_akari_weekly_announcement(
            self, guild, channel_id, completed_start):
        try:
            channel = await self._resolve_channel(channel_id)
        except Exception:
            logger.warning(
                'Weekly Akari target missing for guild=%s channel=%s',
                guild.id, channel_id, exc_info=True)
            return False

        next_monday = completed_start + dt.timedelta(days=7)
        rating_rows, standings = await self._akari_weekly_preview(
            guild.id, as_of_date=next_monday,
            standings_date=completed_start)
        if not standings:
            return True
        registrants = cf_common.user_db.get_akari_registrants(guild.id)
        visible = registrants - self._akari_banned_user_ids(guild.id)
        standings = [row for row in standings if row.user_id in visible]
        ratings = [row for row in rating_rows if row.user_id in visible]
        ratings = self._active_ranking_rows(ratings, include_inactive=False)
        if not standings:
            return True

        completed_end = completed_start + dt.timedelta(days=6)
        files = [_mg()._get_akari_weekly_table_image_file(
            guild, standings,
            title=(f'Daily Akari Final Weekly Standings · '
                   f'{completed_start:%b %d}–'
                   f'{completed_end:%b %d}'))]
        if ratings:
            files.append(_mg()._get_akari_rating_table_image_file(
                guild, ratings, registrants,
                title='Daily Akari Weekly Ratings', mark_registered=False,
                games_label='Weeks'))
        await channel.send(
            f'🏆 **Daily Akari week complete · '
            f'{completed_start:%b %d}–{completed_end:%b %d}**\n'
            'Final standings and updated weekly ratings:', files=files)
        return True
