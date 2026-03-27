import asyncio
import datetime as dt
import logging

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator

from tle.cogs._minigame_common import (
    compute_vs, compute_streak, compute_top, pick_best_results,
    format_duration, parse_date_args,
)
from tle.cogs._minigame_akari import AKARI_GAME
from tle.cogs._minigame_guessgame import GUESSGAME_GAME
from tle.cogs._migrate_retry import discord_retry, RetryExhaustedError

logger = logging.getLogger(__name__)

_IMPORT_BATCH_SIZE = 500
_IMPORT_RATE_DELAY = 0.5


class MinigameCogError(commands.CommandError):
    pass


def _safe_member_name(member):
    return discord.utils.escape_mentions(member.display_name)


def _safe_user_name(guild, user_id):
    member = guild.get_member(int(user_id))
    if member is not None:
        return _safe_member_name(member)
    return f'user `{user_id}`'


class Minigames(commands.Cog):
    GAMES = {
        'akari': AKARI_GAME,
        'guessgame': GUESSGAME_GAME,
    }

    def __init__(self, bot):
        self.bot = bot
        self._import_tasks = {}   # (guild_id, game_name) -> asyncio.Task
        self._import_status = {}  # (guild_id, game_name) -> dict

    async def cog_unload(self):
        tasks = list(self._import_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _is_enabled(guild_id, feature_flag):
        return cf_common.user_db.get_guild_config(guild_id, feature_flag) == '1'

    @staticmethod
    def _get_channel(guild_id, game_name):
        return cf_common.user_db.get_minigame_channel(guild_id, game_name)

    def _game_for_channel(self, message):
        """Return the GameDef whose configured channel matches, or None."""
        for game in self.GAMES.values():
            if not self._is_enabled(message.guild.id, game.feature_flag):
                continue
            channel_id = self._get_channel(message.guild.id, game.name)
            if channel_id is not None and str(message.channel.id) == str(channel_id):
                return game
        return None

    @staticmethod
    def _require_enabled(guild_id, game):
        if cf_common.user_db.get_guild_config(guild_id, game.feature_flag) != '1':
            raise MinigameCogError(
                f'{game.display_name} is not enabled. '
                f'An admin can enable it with `;meta config enable {game.feature_flag}`.'
            )

    async def _resolve_member(self, ctx, member_text):
        try:
            return await commands.MemberConverter().convert(ctx, member_text)
        except commands.BadArgument as exc:
            raise MinigameCogError(str(exc)) from exc

    # ── Listeners ───────────────────────────────────────────────────────

    async def _ingest_message(self, message, game):
        results = game.parse(message.content)
        if not results:
            return

        puzzle_date_fallback = message.created_at.date()

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

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.guild is None or message.author.bot or cf_common.user_db is None:
            return
        game = self._game_for_channel(message)
        if game is not None:
            try:
                await self._ingest_message(message, game)
            except Exception:
                logger.error('Error ingesting message %s', message.id, exc_info=True)

    @commands.Cog.listener()
    async def on_message_edit(self, before, after):
        if after.guild is None or after.author.bot or cf_common.user_db is None:
            return
        game = self._game_for_channel(after)
        if game is None:
            return
        try:
            # Delete all existing live results for this message, then re-ingest.
            # Handles the case where an edit removes some results from a multi-result message.
            cf_common.user_db.delete_minigame_result(after.id)
            results = game.parse(after.content)
            if results:
                await self._ingest_message(after, game)
            else:
                cf_common.user_db.delete_imported_minigame_result(after.id)
        except Exception:
            logger.error('Error handling message edit %s', after.id, exc_info=True)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload):
        if payload.guild_id is None or cf_common.user_db is None:
            return
        try:
            cf_common.user_db.delete_minigame_result(payload.message_id)
            cf_common.user_db.delete_imported_minigame_result(payload.message_id)
        except Exception:
            logger.error('Error handling message delete %s', payload.message_id, exc_info=True)

    # ── Import ──────────────────────────────────────────────────────────

    async def _run_import(self, guild_id, channel_id, game):
        key = (guild_id, game.name)
        status = self._import_status[key]
        try:
            channel = self.bot.get_channel(channel_id)
            if channel is None:
                raise MinigameCogError(f'Channel `{channel_id}` is not available.')

            batch_count = 0
            async for message in channel.history(oldest_first=True, limit=None):
                status['scanned'] += 1
                if message.author.bot or not message.content:
                    continue
                results = game.parse(message.content)
                if not results:
                    continue

                puzzle_date_fallback = message.created_at.date()
                for parsed in results:
                    puzzle_date = parsed.puzzle_date or puzzle_date_fallback
                    cf_common.user_db.save_imported_minigame_result(
                        message.id, guild_id, game.name, channel_id,
                        message.author.id, parsed.puzzle_number,
                        puzzle_date.isoformat(), parsed.accuracy,
                        parsed.time_seconds, parsed.is_perfect,
                        message.content, commit=False,
                    )
                    status['done'] += 1
                status['latest_message_id'] = str(message.id)
                batch_count += 1

                if batch_count >= _IMPORT_BATCH_SIZE:
                    cf_common.user_db.conn.commit()
                    logger.info(
                        '%s import progress: guild=%s channel=%s scanned=%d imported=%d latest_msg=%s',
                        game.display_name, guild_id, channel_id,
                        status['scanned'], status['done'], status['latest_message_id'],
                    )
                    batch_count = 0
                    await asyncio.sleep(_IMPORT_RATE_DELAY)

            if batch_count > 0:
                cf_common.user_db.conn.commit()

            status['state'] = 'done'
            logger.info(
                '%s import complete: guild=%s channel=%s scanned=%d imported=%d',
                game.display_name, guild_id, channel_id,
                status['scanned'], status['done'],
            )
        except asyncio.CancelledError:
            status['state'] = 'cancelled'
            cf_common.user_db.conn.rollback()
            logger.info('%s import cancelled: guild=%s scanned=%d imported=%d',
                        game.display_name, guild_id, status['scanned'], status['done'])
            raise
        except RetryExhaustedError as exc:
            status['state'] = 'failed'
            status['error'] = f'Discord API retries exhausted: {exc.last_exception}'
            cf_common.user_db.conn.rollback()
            logger.error(
                '%s import failed (retries exhausted): guild=%s channel=%s',
                game.display_name, guild_id, channel_id, exc_info=True,
            )
        except Exception as exc:
            status['state'] = 'failed'
            status['error'] = str(exc)
            cf_common.user_db.conn.rollback()
            logger.error(
                '%s import failed: guild=%s channel=%s',
                game.display_name, guild_id, channel_id, exc_info=True,
            )
        finally:
            self._import_tasks.pop(key, None)

    # ── Shared command implementations ──────────────────────────────────

    async def _cmd_here(self, ctx, game):
        cf_common.user_db.set_minigame_channel(ctx.guild.id, game.name, ctx.channel.id)
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} channel set to {ctx.channel.mention}'
        ))

    async def _cmd_clear(self, ctx, game):
        cf_common.user_db.clear_minigame_channel(ctx.guild.id, game.name)
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} channel cleared.'
        ))

    async def _cmd_show(self, ctx, game):
        enabled = self._is_enabled(ctx.guild.id, game.feature_flag)
        channel_id = self._get_channel(ctx.guild.id, game.name)
        channel = f'<#{channel_id}>' if channel_id else 'not set'
        lines = [
            f'feature: `{"enabled" if enabled else "disabled"}`',
            f'channel: {channel}',
        ]
        if not enabled:
            lines.append(f'Enable it with `;meta config enable {game.feature_flag}`.')
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    async def _cmd_vs(self, ctx, game, member1, member2, *args):
        self._require_enabled(ctx.guild.id, game)
        try:
            dlo, dhi, plo, phi = parse_date_args(args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows1 = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member1.id, dlo, dhi, plo, phi)
        rows2 = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member2.id, dlo, dhi, plo, phi)
        stats = compute_vs(rows1, rows2, game.score_matchup, game.missing_is_loss)
        if stats['common_count'] == 0:
            raise MinigameCogError(
                f'These users have no {game.display_name} puzzles to compare.')

        description = '\n'.join([
            f'`{_safe_member_name(member1)}`: **{stats["score1"]:g}** points, **{stats["wins1"]}** wins',
            f'`{_safe_member_name(member2)}`: **{stats["score2"]:g}** points, **{stats["wins2"]}** wins',
            f'Ties: **{stats["ties"]}**',
            f'Puzzles: **{stats["common_count"]}**',
        ])
        embed = discord.Embed(
            title=f'{game.display_name} Head to Head',
            description=description,
            color=discord_common.random_cf_color(),
        )
        await ctx.send(embed=embed)

    async def _cmd_streak(self, ctx, game, *args):
        self._require_enabled(ctx.guild.id, game)
        filter_args = list(args)
        member = ctx.author
        if filter_args:
            try:
                member = await self._resolve_member(ctx, filter_args[0])
                filter_args = filter_args[1:]
            except MinigameCogError:
                member = ctx.author

        try:
            dlo, dhi, plo, phi = parse_date_args(filter_args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows = cf_common.user_db.get_minigame_results_for_user(
            ctx.guild.id, game.name, member.id, dlo, dhi, plo, phi)
        streak = compute_streak(rows)
        if not rows:
            raise MinigameCogError(
                f'No {game.display_name} results found for `{_safe_member_name(member)}`.')

        best = pick_best_results(rows)
        latest_row = best[max(best)]
        latest_status = 'Perfect' if latest_row.is_perfect else f'{latest_row.accuracy}%'
        embed = discord.Embed(
            title=f'{game.display_name} Streak',
            description='\n'.join([
                f'`{_safe_member_name(member)}`: **{streak}** consecutive perfect day(s)',
                f'Latest result: **{latest_status}** in **{format_duration(latest_row.time_seconds)}**',
            ]),
            color=discord_common.random_cf_color(),
        )
        await ctx.send(embed=embed)

    async def _cmd_top(self, ctx, game, *args):
        self._require_enabled(ctx.guild.id, game)
        try:
            dlo, dhi, plo, phi = parse_date_args(args)
        except ValueError as e:
            raise MinigameCogError(str(e)) from e

        rows = cf_common.user_db.get_minigame_results_for_guild(
            ctx.guild.id, game.name, dlo, dhi, plo, phi)
        winners = compute_top(rows, game.is_eligible_winner)
        if not winners:
            raise MinigameCogError(
                f'No {game.display_name} winners found for this range.')

        pages = []
        per_page = 10
        for page_idx, chunk in enumerate(paginator.chunkify(winners, per_page)):
            lines = []
            for i, (user_id, wins) in enumerate(chunk):
                rank = page_idx * per_page + i + 1
                name = _safe_user_name(ctx.guild, user_id)
                lines.append(f'**#{rank}** `{name}` — **{wins}** wins')
            embed = discord.Embed(
                title=f'{game.display_name} Winners',
                description='\n'.join(lines),
                color=discord_common.random_cf_color(),
            )
            pages.append((None, embed))
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=300,
            set_pagenum_footers=True, author_id=ctx.author.id,
        )

    async def _cmd_remove(self, ctx, game, member, puzzle_id):
        rc = cf_common.user_db.delete_minigame_result_for_user_puzzle(
            ctx.guild.id, game.name, member.id, puzzle_id)
        if not rc:
            raise MinigameCogError(
                f'No {game.display_name} result found for '
                f'`{_safe_member_name(member)}` on puzzle `{puzzle_id}`.')
        await ctx.send(embed=discord_common.embed_success(
            f'Removed {game.display_name} result for '
            f'`{_safe_member_name(member)}` on puzzle `{puzzle_id}`.'))

    async def _cmd_import_start(self, ctx, game, channel=None):
        key = (ctx.guild.id, game.name)
        if key in self._import_tasks:
            task = self._import_tasks[key]
            if not task.done():
                raise MinigameCogError(
                    f'A {game.display_name} import is already running.')

        configured_channel_id = self._get_channel(ctx.guild.id, game.name)
        if channel is None and configured_channel_id is not None:
            channel = ctx.guild.get_channel(int(configured_channel_id))
        channel = channel or ctx.channel

        deleted = cf_common.user_db.clear_imported_minigame_results(
            ctx.guild.id, game.name)
        self._import_status[key] = {
            'state': 'running',
            'channel_id': channel.id,
            'scanned': 0,
            'done': 0,
            'error': None,
            'latest_message_id': None,
            'cleared': deleted,
            'started_at': dt.datetime.now(),
        }
        task = asyncio.create_task(self._run_import(ctx.guild.id, channel.id, game))
        self._import_tasks[key] = task

        logger.info(
            '%s import started: guild=%s channel=%s cleared=%d',
            game.display_name, ctx.guild.id, channel.id, deleted,
        )
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} import started for {channel.mention}. '
            f'Cleared {deleted} imported row(s) first.'))

    async def _cmd_import_status(self, ctx, game):
        key = (ctx.guild.id, game.name)
        status = self._import_status.get(key)
        if status is None:
            raise MinigameCogError(
                f'No {game.display_name} import has been started.')

        elapsed = dt.datetime.now() - status['started_at']
        elapsed_str = str(elapsed).split('.')[0]  # drop microseconds
        lines = [
            f'state: `{status["state"]}`',
            f'channel: <#{status["channel_id"]}>',
            f'messages scanned: **{status["scanned"]}**',
            f'results imported: **{status["done"]}**',
            f'elapsed: `{elapsed_str}`',
        ]
        if status['latest_message_id'] is not None:
            lines.append(f'latest message: `{status["latest_message_id"]}`')
        if status['error']:
            lines.append(f'error: `{status["error"]}`')
        await ctx.send(embed=discord_common.embed_neutral('\n'.join(lines)))

    async def _cmd_import_cancel(self, ctx, game):
        key = (ctx.guild.id, game.name)
        task = self._import_tasks.get(key)
        if task is None or task.done():
            raise MinigameCogError(
                f'No {game.display_name} import is currently running.')
        task.cancel()
        await ctx.send(embed=discord_common.embed_success(
            f'{game.display_name} import cancelled.'))

    async def _cmd_import_clear(self, ctx, game):
        key = (ctx.guild.id, game.name)
        task = self._import_tasks.get(key)
        if task is not None and not task.done():
            raise MinigameCogError(
                f'Cancel the running {game.display_name} import before clearing it.')

        deleted = cf_common.user_db.clear_imported_minigame_results(
            ctx.guild.id, game.name)
        self._import_status.pop(key, None)
        await ctx.send(embed=discord_common.embed_success(
            f'Deleted {deleted} imported {game.display_name} row(s).'))

    # ── Command tree: ;minigames ────────────────────────────────────────

    @commands.group(name='minigames', aliases=['mg'], brief='Daily puzzle minigame commands',
                    invoke_without_command=True)
    async def minigames(self, ctx):
        """Daily puzzle minigame commands."""
        await ctx.send_help(ctx.command)

    # ── Akari commands: ;minigames akari … ──────────────────────────────

    @minigames.group(name='akari', aliases=['dailyakari'], brief='Daily Akari commands',
                     invoke_without_command=True)
    async def akari(self, ctx):
        """Daily Akari commands."""
        await ctx.send_help(ctx.command)

    @akari.command(name='here', brief='Set the Daily Akari channel to the current channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def akari_here(self, ctx):
        await self._cmd_here(ctx, AKARI_GAME)

    @akari.command(name='clear', brief='Clear the Daily Akari channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def akari_clear(self, ctx):
        await self._cmd_clear(ctx, AKARI_GAME)

    @akari.command(name='show', brief='Show Daily Akari settings')
    async def akari_show(self, ctx):
        await self._cmd_show(ctx, AKARI_GAME)

    @akari.command(name='vs', brief='Head-to-head comparison',
                   usage='@user1 @user2 [filters...]')
    async def akari_vs(self, ctx, member1: discord.Member, member2: discord.Member, *args):
        await self._cmd_vs(ctx, AKARI_GAME, member1, member2, *args)

    @akari.command(name='streak', brief='Show current perfect streak',
                   usage='[@user] [filters...]')
    async def akari_streak(self, ctx, *args):
        await self._cmd_streak(ctx, AKARI_GAME, *args)

    @akari.command(name='top', brief='Show winners leaderboard',
                   usage='[filters...]')
    async def akari_top(self, ctx, *args):
        await self._cmd_top(ctx, AKARI_GAME, *args)

    @akari.command(name='remove', brief='Remove a user result for a puzzle',
                   usage='@user puzzle_id')
    @commands.has_role(constants.TLE_ADMIN)
    async def akari_remove(self, ctx, member: discord.Member, puzzle_id: int):
        await self._cmd_remove(ctx, AKARI_GAME, member, puzzle_id)

    @akari.group(name='import', brief='Manage imported history',
                 invoke_without_command=True)
    @commands.has_role(constants.TLE_ADMIN)
    async def akari_import(self, ctx):
        await ctx.send_help(ctx.command)

    @akari_import.command(name='start', brief='Rebuild imported history')
    @commands.has_role(constants.TLE_ADMIN)
    async def akari_import_start(self, ctx, channel: discord.TextChannel = None):
        await self._cmd_import_start(ctx, AKARI_GAME, channel)

    @akari_import.command(name='status', brief='Show import status')
    @commands.has_role(constants.TLE_ADMIN)
    async def akari_import_status(self, ctx):
        await self._cmd_import_status(ctx, AKARI_GAME)

    @akari_import.command(name='cancel', brief='Cancel a running import')
    @commands.has_role(constants.TLE_ADMIN)
    async def akari_import_cancel(self, ctx):
        await self._cmd_import_cancel(ctx, AKARI_GAME)

    @akari_import.command(name='clear', brief='Delete imported history')
    @commands.has_role(constants.TLE_ADMIN)
    async def akari_import_clear(self, ctx):
        await self._cmd_import_clear(ctx, AKARI_GAME)

    # ── GuessGame commands: ;minigames guessgame … ──────────────────────

    @minigames.group(name='guessgame', aliases=['gg'], brief='GuessThe.Game commands',
                     invoke_without_command=True)
    async def guessgame(self, ctx):
        """GuessThe.Game commands."""
        await ctx.send_help(ctx.command)

    @guessgame.command(name='here', brief='Set the GuessGame channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_here(self, ctx):
        await self._cmd_here(ctx, GUESSGAME_GAME)

    @guessgame.command(name='clear', brief='Clear the GuessGame channel')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_clear(self, ctx):
        await self._cmd_clear(ctx, GUESSGAME_GAME)

    @guessgame.command(name='show', brief='Show GuessGame settings')
    async def gg_show(self, ctx):
        await self._cmd_show(ctx, GUESSGAME_GAME)

    @guessgame.command(name='vs', brief='Head-to-head comparison',
                       usage='@user1 @user2 [p>=N] [p<N] [filters...]')
    async def gg_vs(self, ctx, member1: discord.Member, member2: discord.Member, *args):
        await self._cmd_vs(ctx, GUESSGAME_GAME, member1, member2, *args)

    @guessgame.command(name='streak', brief='Show current win streak',
                       usage='[@user] [filters...]')
    async def gg_streak(self, ctx, *args):
        await self._cmd_streak(ctx, GUESSGAME_GAME, *args)

    @guessgame.command(name='top', brief='Show winners leaderboard',
                       usage='[p>=N] [p<N] [filters...]')
    async def gg_top(self, ctx, *args):
        await self._cmd_top(ctx, GUESSGAME_GAME, *args)

    @guessgame.command(name='remove', brief='Remove a user result for a puzzle',
                       usage='@user puzzle_id')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_remove(self, ctx, member: discord.Member, puzzle_id: int):
        await self._cmd_remove(ctx, GUESSGAME_GAME, member, puzzle_id)

    @guessgame.group(name='import', brief='Manage imported history',
                     invoke_without_command=True)
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import(self, ctx):
        await ctx.send_help(ctx.command)

    @gg_import.command(name='start', brief='Rebuild imported history')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import_start(self, ctx, channel: discord.TextChannel = None):
        await self._cmd_import_start(ctx, GUESSGAME_GAME, channel)

    @gg_import.command(name='status', brief='Show import status')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import_status(self, ctx):
        await self._cmd_import_status(ctx, GUESSGAME_GAME)

    @gg_import.command(name='cancel', brief='Cancel a running import')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import_cancel(self, ctx):
        await self._cmd_import_cancel(ctx, GUESSGAME_GAME)

    @gg_import.command(name='clear', brief='Delete imported history')
    @commands.has_role(constants.TLE_ADMIN)
    async def gg_import_clear(self, ctx):
        await self._cmd_import_clear(ctx, GUESSGAME_GAME)

    # ── Error handler ───────────────────────────────────────────────────

    @discord_common.send_error_if(MinigameCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Minigames(bot))
