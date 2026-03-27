import asyncio
import datetime as dt
import logging
import re
import time
from dataclasses import dataclass

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator

logger = logging.getLogger(__name__)

_FEATURE_FLAG = 'dailyakari'
_TIMELINE_KEYWORDS = {'week', 'month', 'year'}
_NO_TIME_BOUND = 10 ** 10

_FIRST_LINE_RE = re.compile(r'^Daily\s+Akari\b.*?\b(\d+)\s*$', re.IGNORECASE)
_DATE_RE = re.compile(
    r'(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4}|[A-Za-z]+ \d{1,2}, \d{4})'
)
_TIME_RE = re.compile(r'🕓\s*([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)')
_ACCURACY_RE = re.compile(r'(\d{1,3})%')


class DailyAkariCogError(commands.CommandError):
    pass


@dataclass(frozen=True)
class ParsedDailyAkariResult:
    puzzle_number: int
    puzzle_date: dt.date
    accuracy: int
    time_seconds: int
    is_perfect: bool


def _parse_dailyakari_time(time_text):
    parts = [int(part) for part in time_text.split(':')]
    if len(parts) == 2:
        minutes, seconds = parts
        return minutes * 60 + seconds
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return hours * 3600 + minutes * 60 + seconds
    raise ValueError(f'Unrecognized time format: {time_text}')


def _parse_dailyakari_date(date_text):
    cleaned = date_text.strip().replace('/', '-')
    formats = (
        '%Y-%m-%d',
        '%m-%d-%Y',
        '%d-%m-%Y',
        '%B %d, %Y',
        '%b %d, %Y',
    )
    for fmt in formats:
        try:
            return dt.datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    raise ValueError(f'Unrecognized date format: {date_text}')


def _parse_dailyakari_message(content):
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if len(lines) < 3:
        return None

    first_line = lines[0]
    first_match = _FIRST_LINE_RE.match(first_line)
    if first_match is None:
        return None

    date_match = _DATE_RE.search(lines[1])
    if date_match is None:
        return None

    stats_line = None
    for line in lines[2:]:
        if '🕓' in line:
            stats_line = line
            break
    if stats_line is None:
        return None

    time_match = _TIME_RE.search(stats_line)
    if time_match is None:
        return None

    is_perfect = 'perfect' in stats_line.lower() or '🌟' in stats_line
    accuracy_match = _ACCURACY_RE.search(stats_line)
    if is_perfect:
        accuracy = 100
    elif accuracy_match is not None:
        accuracy = int(accuracy_match.group(1))
    else:
        return None

    try:
        puzzle_date = _parse_dailyakari_date(date_match.group(1))
        time_seconds = _parse_dailyakari_time(time_match.group(1))
    except ValueError:
        return None

    return ParsedDailyAkariResult(
        puzzle_number=int(first_match.group(1)),
        puzzle_date=puzzle_date,
        accuracy=accuracy,
        time_seconds=time_seconds,
        is_perfect=is_perfect,
    )


def _result_key(row):
    return _normalize_puzzle_date(row.puzzle_date), row.puzzle_number


def _normalize_puzzle_date(value):
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value))


def _result_sort_key(row):
    return (
        int(bool(row.is_perfect)),
        int(getattr(row, 'accuracy', 0)),
        -int(getattr(row, 'time_seconds', 0)),
        int(getattr(row, 'message_id', 0)),
    )


def _pick_best_results(rows):
    best = {}
    for row in rows:
        key = _result_key(row)
        prev = best.get(key)
        if prev is None or _result_sort_key(row) > _result_sort_key(prev):
            best[key] = row
    return best


def _format_duration(total_seconds):
    minutes, seconds = divmod(int(total_seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f'{hours}:{minutes:02d}:{seconds:02d}'
    return f'{minutes}:{seconds:02d}'


def _score_dailyakari_matchup(row1, row2):
    if row1.is_perfect and row2.is_perfect:
        if row1.time_seconds < row2.time_seconds:
            return 1.0, 0.0
        if row1.time_seconds > row2.time_seconds:
            return 0.0, 1.0
        return 0.5, 0.5
    if row1.is_perfect and not row2.is_perfect:
        return 1.0, 0.0
    if row2.is_perfect and not row1.is_perfect:
        return 0.0, 1.0
    return 0.5, 0.5


def _compute_dailyakari_vs(rows1, rows2):
    best1 = _pick_best_results(rows1)
    best2 = _pick_best_results(rows2)
    common = sorted(set(best1) & set(best2))

    score1 = 0.0
    score2 = 0.0
    wins1 = 0
    wins2 = 0
    ties = 0

    for key in common:
        row1 = best1[key]
        row2 = best2[key]
        pts1, pts2 = _score_dailyakari_matchup(row1, row2)
        score1 += pts1
        score2 += pts2
        if pts1 == pts2:
            ties += 1
        elif pts1 > pts2:
            wins1 += 1
        else:
            wins2 += 1

    return {
        'common_count': len(common),
        'score1': score1,
        'score2': score2,
        'wins1': wins1,
        'wins2': wins2,
        'ties': ties,
    }


def _compute_dailyakari_streak(rows):
    best_by_day = {}
    for row in rows:
        puzzle_date = _normalize_puzzle_date(row.puzzle_date)
        prev = best_by_day.get(puzzle_date)
        if prev is None or _result_sort_key(row) > _result_sort_key(prev):
            best_by_day[puzzle_date] = row

    if not best_by_day:
        return 0

    current_day = max(best_by_day)
    streak = 0
    while True:
        row = best_by_day.get(current_day)
        if row is None or not row.is_perfect:
            break
        streak += 1
        current_day -= dt.timedelta(days=1)
    return streak


def _parse_dailyakari_args(args):
    dlo = 0
    dhi = _NO_TIME_BOUND

    for arg in args:
        lower = arg.lower()
        if lower in _TIMELINE_KEYWORDS:
            now = dt.datetime.now()
            if lower == 'week':
                monday = now - dt.timedelta(days=now.weekday())
                dlo = time.mktime(monday.replace(hour=0, minute=0, second=0, microsecond=0).timetuple())
            elif lower == 'month':
                dlo = time.mktime(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timetuple())
            elif lower == 'year':
                dlo = time.mktime(now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0).timetuple())
        elif lower.startswith('d>='):
            dlo = max(dlo, cf_common.parse_date(arg[3:]))
        elif lower.startswith('d<'):
            dhi = min(dhi, cf_common.parse_date(arg[2:]))
        else:
            raise DailyAkariCogError(f'Unrecognized Daily Akari filter: `{arg}`.')
    return dlo, dhi


def _compute_dailyakari_top(rows):
    wins_by_user = {}
    best_by_user_puzzle = {}
    for row in rows:
        key = (str(row.user_id), _result_key(row))
        prev = best_by_user_puzzle.get(key)
        if prev is None or _result_sort_key(row) > _result_sort_key(prev):
            best_by_user_puzzle[key] = row

    best_per_puzzle = {}
    for (_, puzzle_key), row in best_by_user_puzzle.items():
        if not row.is_perfect:
            continue
        entry = best_per_puzzle.get(puzzle_key)
        if entry is None or row.time_seconds < entry['time_seconds']:
            best_per_puzzle[puzzle_key] = {
                'time_seconds': row.time_seconds,
                'rows': [row],
            }
        elif row.time_seconds == entry['time_seconds']:
            entry['rows'].append(row)

    for entry in best_per_puzzle.values():
        for row in entry['rows']:
            user_id = str(row.user_id)
            wins_by_user[user_id] = wins_by_user.get(user_id, 0) + 1

    return sorted(wins_by_user.items(), key=lambda item: (-item[1], int(item[0])))


def _safe_member_name(member):
    return discord.utils.escape_mentions(member.display_name)


def _safe_user_name(guild, user_id):
    member = guild.get_member(int(user_id))
    if member is not None:
        return _safe_member_name(member)
    return f'user `{user_id}`'


class DailyAkari(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._import_tasks = {}
        self._import_status = {}

    @staticmethod
    def _is_enabled(guild_id):
        return cf_common.user_db.get_guild_config(guild_id, _FEATURE_FLAG) == '1'

    @staticmethod
    def _is_configured_channel(message):
        channel_id = cf_common.user_db.get_dailyakari_channel(message.guild.id)
        return channel_id is not None and str(message.channel.id) == str(channel_id)

    @staticmethod
    def _require_enabled(guild_id):
        if cf_common.user_db.get_guild_config(guild_id, _FEATURE_FLAG) != '1':
            raise DailyAkariCogError(
                'Daily Akari is not enabled. An admin can enable it with `;meta config enable dailyakari`.'
            )

    async def _resolve_member(self, ctx, member_text):
        try:
            return await commands.MemberConverter().convert(ctx, member_text)
        except commands.BadArgument as exc:
            raise DailyAkariCogError(str(exc)) from exc

    async def _run_import(self, guild_id, channel_id):
        status = self._import_status[guild_id]
        try:
            channel = self.bot.get_channel(channel_id)
            if channel is None:
                raise DailyAkariCogError(f'Channel `{channel_id}` is not available.')

            async for message in channel.history(oldest_first=True, limit=None):
                if message.author.bot or not message.content:
                    continue
                parsed = _parse_dailyakari_message(message.content)
                if parsed is None:
                    continue

                cf_common.user_db.save_imported_dailyakari_result(
                    message.id,
                    guild_id,
                    channel_id,
                    message.author.id,
                    parsed.puzzle_number,
                    parsed.puzzle_date.isoformat(),
                    parsed.accuracy,
                    parsed.time_seconds,
                    parsed.is_perfect,
                )
                status['done'] += 1
                status['latest_message_id'] = str(message.id)

            status['state'] = 'done'
        except asyncio.CancelledError:
            status['state'] = 'cancelled'
            raise
        except Exception as exc:
            status['state'] = 'failed'
            status['error'] = str(exc)
            logger.error('DailyAkari import failed: guild=%s channel=%s', guild_id, channel_id, exc_info=True)
        finally:
            self._import_tasks.pop(guild_id, None)

    async def _ingest_message(self, message):
        if message.guild is None or message.author.bot or cf_common.user_db is None:
            return
        if not self._is_enabled(message.guild.id) or not self._is_configured_channel(message):
            return

        parsed = _parse_dailyakari_message(message.content)
        if parsed is None:
            return

        existing = cf_common.user_db.get_dailyakari_result_for_user_puzzle(
            message.guild.id, message.author.id, parsed.puzzle_number
        )
        if existing is not None and str(existing.message_id) != str(message.id):
            logger.info(
                'DailyAkari result ignored: guild=%s msg=%s user=%s puzzle=%s first_msg=%s',
                message.guild.id,
                message.id,
                message.author.id,
                parsed.puzzle_number,
                existing.message_id,
            )
            return

        cf_common.user_db.save_dailyakari_result(
            message.id,
            message.guild.id,
            message.channel.id,
            message.author.id,
            parsed.puzzle_number,
            parsed.puzzle_date.isoformat(),
            parsed.accuracy,
            parsed.time_seconds,
            parsed.is_perfect,
        )
        logger.info(
            'DailyAkari result stored: guild=%s channel=%s msg=%s user=%s puzzle=%s date=%s '
            'accuracy=%s time=%s perfect=%s',
            message.guild.id,
            message.channel.id,
            message.id,
            message.author.id,
            parsed.puzzle_number,
            parsed.puzzle_date.isoformat(),
            parsed.accuracy,
            parsed.time_seconds,
            parsed.is_perfect,
        )

    @commands.Cog.listener()
    async def on_message(self, message):
        await self._ingest_message(message)

    @commands.Cog.listener()
    async def on_message_edit(self, before, after):
        if after.guild is None or after.author.bot or cf_common.user_db is None:
            return
        if not self._is_enabled(after.guild.id):
            return
        if self._is_configured_channel(after):
            parsed = _parse_dailyakari_message(after.content)
            if parsed is not None:
                await self._ingest_message(after)
                return
        cf_common.user_db.delete_dailyakari_result(after.id)
        cf_common.user_db.delete_imported_dailyakari_result(after.id)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload):
        if payload.guild_id is None or cf_common.user_db is None:
            return
        cf_common.user_db.delete_dailyakari_result(payload.message_id)
        cf_common.user_db.delete_imported_dailyakari_result(payload.message_id)

    @commands.group(name='akari', aliases=['dailyakari'], brief='Daily Akari commands',
                    invoke_without_command=True)
    async def akari(self, ctx):
        """Daily Akari add-on commands."""
        await ctx.send_help(ctx.command)

    @akari.command(brief='Set the Daily Akari channel to the current channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def here(self, ctx):
        cf_common.user_db.set_dailyakari_channel(ctx.guild.id, ctx.channel.id)
        await ctx.send(embed=discord_common.embed_success(
            f'Daily Akari channel set to {ctx.channel.mention}'
        ))

    @akari.command(brief='Clear the Daily Akari channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def clear(self, ctx):
        cf_common.user_db.clear_dailyakari_channel(ctx.guild.id)
        await ctx.send(embed=discord_common.embed_success('Daily Akari channel cleared.'))

    @akari.command(brief='Remove a user result for a puzzle', usage='@user puzzle_id')
    @commands.has_role(constants.TLE_ADMIN)
    async def remove(self, ctx, member: discord.Member, puzzle_id: int):
        rc = cf_common.user_db.delete_dailyakari_result_for_user_puzzle(
            ctx.guild.id, member.id, puzzle_id
        )
        if not rc:
            raise DailyAkariCogError(
                f'No Daily Akari result found for `{_safe_member_name(member)}` '
                f'on puzzle `{puzzle_id}`.'
            )
        await ctx.send(embed=discord_common.embed_success(
            f'Removed Daily Akari result for `{_safe_member_name(member)}` on puzzle `{puzzle_id}`.'
        ))

    @akari.command(brief='Show Daily Akari settings')
    async def show(self, ctx):
        enabled = self._is_enabled(ctx.guild.id)
        channel_id = cf_common.user_db.get_dailyakari_channel(ctx.guild.id)
        channel = f'<#{channel_id}>' if channel_id else 'not set'
        lines = [
            f'feature: `{"enabled" if enabled else "disabled"}`',
            f'channel: {channel}',
        ]
        if not enabled:
            lines.append('Enable it with `;meta config enable dailyakari`.')
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    @akari.command(brief='Head-to-head Daily Akari comparison',
                   usage='@user1 @user2 [week|month|year] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def vs(self, ctx, member1: discord.Member, member2: discord.Member, *args):
        self._require_enabled(ctx.guild.id)
        dlo, dhi = _parse_dailyakari_args(args)
        rows1, rows2 = (
            cf_common.user_db.get_dailyakari_results_for_user(ctx.guild.id, member1.id, dlo, dhi),
            cf_common.user_db.get_dailyakari_results_for_user(ctx.guild.id, member2.id, dlo, dhi),
        )
        stats = _compute_dailyakari_vs(rows1, rows2)
        if stats['common_count'] == 0:
            raise DailyAkariCogError('These users have no common Daily Akari puzzles yet.')

        description = '\n'.join([
            f'`{_safe_member_name(member1)}`: **{stats["score1"]:g}** points, **{stats["wins1"]}** wins',
            f'`{_safe_member_name(member2)}`: **{stats["score2"]:g}** points, **{stats["wins2"]}** wins',
            f'Ties: **{stats["ties"]}**',
            f'Common puzzles: **{stats["common_count"]}**',
        ])
        embed = discord.Embed(
            title='Daily Akari Head to Head',
            description=description,
            color=discord_common.random_cf_color(),
        )
        await ctx.send(embed=embed)

    @akari.command(brief='Show current Daily Akari perfect streak',
                   usage='[@user] [week|month|year] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def streak(self, ctx, *args):
        self._require_enabled(ctx.guild.id)

        filter_args = list(args)
        member = ctx.author
        if filter_args:
            try:
                member = await self._resolve_member(ctx, filter_args[0])
                filter_args = filter_args[1:]
            except DailyAkariCogError:
                member = ctx.author

        dlo, dhi = _parse_dailyakari_args(filter_args)
        rows = cf_common.user_db.get_dailyakari_results_for_user(ctx.guild.id, member.id, dlo, dhi)
        streak = _compute_dailyakari_streak(rows)
        if not rows:
            raise DailyAkariCogError(f'No Daily Akari results found for `{_safe_member_name(member)}`.')

        best = _pick_best_results(rows)
        latest_row = best[max(best)]
        latest_status = 'Perfect' if latest_row.is_perfect else f'{latest_row.accuracy}%'
        embed = discord.Embed(
            title='Daily Akari Streak',
            description='\n'.join([
                f'`{_safe_member_name(member)}`: **{streak}** consecutive perfect day(s)',
                f'Latest result: **{latest_status}** in **{_format_duration(latest_row.time_seconds)}**',
            ]),
            color=discord_common.random_cf_color(),
        )
        await ctx.send(embed=embed)

    @akari.command(brief='Show Daily Akari winners leaderboard',
                   usage='[week|month|year] [d>=[[dd]mm]yyyy] [d<[[dd]mm]yyyy]')
    async def top(self, ctx, *args):
        self._require_enabled(ctx.guild.id)
        dlo, dhi = _parse_dailyakari_args(args)
        rows = cf_common.user_db.get_dailyakari_results_for_guild(ctx.guild.id, dlo, dhi)
        winners = _compute_dailyakari_top(rows)
        if not winners:
            raise DailyAkariCogError('No Daily Akari winners found for this range.')

        pages = []
        per_page = 10
        for page_idx, chunk in enumerate(paginator.chunkify(winners, per_page)):
            lines = []
            for i, (user_id, wins) in enumerate(chunk):
                rank = page_idx * per_page + i + 1
                name = _safe_user_name(ctx.guild, user_id)
                lines.append(f'**#{rank}** `{name}` — **{wins}** wins')
            embed = discord.Embed(
                title='Daily Akari Winners',
                description='\n'.join(lines),
                color=discord_common.random_cf_color(),
            )
            pages.append((None, embed))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id,
        )

    @akari.group(name='import', brief='Manage imported Daily Akari history', invoke_without_command=True)
    @commands.has_role(constants.TLE_ADMIN)
    async def import_group(self, ctx):
        await ctx.send_help(ctx.command)

    @import_group.command(name='start', brief='Rebuild imported Daily Akari history')
    @commands.has_role(constants.TLE_ADMIN)
    async def import_start(self, ctx, channel: discord.TextChannel = None):
        if ctx.guild.id in self._import_tasks:
            task = self._import_tasks[ctx.guild.id]
            if not task.done():
                raise DailyAkariCogError('A Daily Akari import is already running.')

        configured_channel_id = cf_common.user_db.get_dailyakari_channel(ctx.guild.id)
        if channel is None and configured_channel_id is not None:
            channel = ctx.guild.get_channel(int(configured_channel_id))
        channel = channel or ctx.channel

        deleted = cf_common.user_db.clear_imported_dailyakari_results(ctx.guild.id)
        self._import_status[ctx.guild.id] = {
            'state': 'running',
            'channel_id': channel.id,
            'done': 0,
            'error': None,
            'latest_message_id': None,
            'cleared': deleted,
            'started_at': dt.datetime.now(),
        }
        task = asyncio.create_task(self._run_import(ctx.guild.id, channel.id))
        self._import_tasks[ctx.guild.id] = task

        await ctx.send(embed=discord_common.embed_success(
            f'Daily Akari import started for {channel.mention}. Cleared {deleted} imported row(s) first.'
        ))

    @import_group.command(name='status', brief='Show Daily Akari import status')
    @commands.has_role(constants.TLE_ADMIN)
    async def import_status(self, ctx):
        status = self._import_status.get(ctx.guild.id)
        if status is None:
            raise DailyAkariCogError('No Daily Akari import has been started.')

        lines = [
            f'state: `{status["state"]}`',
            f'channel: <#{status["channel_id"]}>',
            f'imported rows: **{status["done"]}**',
        ]
        if status['latest_message_id'] is not None:
            lines.append(f'latest message: `{status["latest_message_id"]}`')
        if status['error']:
            lines.append(f'error: `{status["error"]}`')
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    @import_group.command(name='cancel', brief='Cancel a running Daily Akari import')
    @commands.has_role(constants.TLE_ADMIN)
    async def import_cancel(self, ctx):
        task = self._import_tasks.get(ctx.guild.id)
        if task is None or task.done():
            raise DailyAkariCogError('No Daily Akari import is currently running.')
        task.cancel()
        await ctx.send(embed=discord_common.embed_success('Daily Akari import cancelled.'))

    @import_group.command(name='clear', brief='Delete imported Daily Akari history for this guild')
    @commands.has_role(constants.TLE_ADMIN)
    async def import_clear(self, ctx):
        task = self._import_tasks.get(ctx.guild.id)
        if task is not None and not task.done():
            raise DailyAkariCogError('Cancel the running Daily Akari import before clearing it.')

        deleted = cf_common.user_db.clear_imported_dailyakari_results(ctx.guild.id)
        self._import_status.pop(ctx.guild.id, None)
        await ctx.send(embed=discord_common.embed_success(
            f'Deleted {deleted} imported Daily Akari row(s).'
        ))

    @discord_common.send_error_if(DailyAkariCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(DailyAkari(bot))
