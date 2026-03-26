import datetime as dt
import logging
import re
from dataclasses import dataclass

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common

logger = logging.getLogger(__name__)

_FEATURE_FLAG = 'dailyakari'

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


class DailyAkari(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @staticmethod
    def _is_enabled(guild_id):
        return cf_common.user_db.get_guild_config(guild_id, _FEATURE_FLAG) == '1'

    @staticmethod
    def _is_configured_channel(message):
        channel_id = cf_common.user_db.get_dailyakari_channel(message.guild.id)
        return channel_id is not None and str(message.channel.id) == str(channel_id)

    async def _ingest_message(self, message):
        if message.guild is None or message.author.bot or cf_common.user_db is None:
            return
        if not self._is_enabled(message.guild.id) or not self._is_configured_channel(message):
            return

        parsed = _parse_dailyakari_message(message.content)
        if parsed is None:
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

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload):
        if payload.guild_id is None or cf_common.user_db is None:
            return
        cf_common.user_db.delete_dailyakari_result(payload.message_id)

    @commands.group(brief='Daily Akari commands', invoke_without_command=True)
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

    @akari.command(brief='Head-to-head Daily Akari comparison', usage='@user1 @user2')
    async def vs(self, ctx, member1: discord.Member, member2: discord.Member):
        if not self._is_enabled(ctx.guild.id):
            raise DailyAkariCogError(
                'Daily Akari is not enabled. An admin can enable it with `;meta config enable dailyakari`.'
            )

        rows1, rows2 = (
            cf_common.user_db.get_dailyakari_results_for_user(ctx.guild.id, member1.id),
            cf_common.user_db.get_dailyakari_results_for_user(ctx.guild.id, member2.id),
        )
        stats = _compute_dailyakari_vs(rows1, rows2)
        if stats['common_count'] == 0:
            raise DailyAkariCogError('These users have no common Daily Akari puzzles yet.')

        description = '\n'.join([
            f'{member1.mention}: **{stats["score1"]:g}** points, **{stats["wins1"]}** wins',
            f'{member2.mention}: **{stats["score2"]:g}** points, **{stats["wins2"]}** wins',
            f'Ties: **{stats["ties"]}**',
            f'Common puzzles: **{stats["common_count"]}**',
        ])
        embed = discord.Embed(
            title='Daily Akari Head to Head',
            description=description,
            color=discord_common.random_cf_color(),
        )
        await ctx.send(embed=embed)

    @akari.command(brief='Show current Daily Akari perfect streak', usage='[@user]')
    async def streak(self, ctx, member: discord.Member = None):
        if not self._is_enabled(ctx.guild.id):
            raise DailyAkariCogError(
                'Daily Akari is not enabled. An admin can enable it with `;meta config enable dailyakari`.'
            )

        member = member or ctx.author
        rows = cf_common.user_db.get_dailyakari_results_for_user(ctx.guild.id, member.id)
        streak = _compute_dailyakari_streak(rows)
        if not rows:
            raise DailyAkariCogError(f'No Daily Akari results found for {member.mention}.')

        best = _pick_best_results(rows)
        latest_row = best[max(best)]
        latest_status = 'Perfect' if latest_row.is_perfect else f'{latest_row.accuracy}%'
        embed = discord.Embed(
            title='Daily Akari Streak',
            description='\n'.join([
                f'{member.mention}: **{streak}** consecutive perfect day(s)',
                f'Latest result: **{latest_status}** in **{_format_duration(latest_row.time_seconds)}**',
            ]),
            color=discord_common.random_cf_color(),
        )
        await ctx.send(embed=embed)

    @discord_common.send_error_if(DailyAkariCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(DailyAkari(bot))
