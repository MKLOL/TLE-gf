import asyncio
import logging
import time

import discord
from discord.ext import commands

from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import tasks
from tle.cogs._rpoll_helpers import (
    MAX_OPTIONS,
    _DEFAULT_DURATION,
    _SAFETY_NET_INTERVAL,
    _VALID_FORMULAS,
    _FORMULA_LABELS,
    RpollError,
    _parse_duration,
    _apply_formula,
    _get_elo_win_probability,
    _compose_team_rating,
    _compose_osu_score,
    _calculate_gitgud_score_for_delta,
    _get_monthly_gitgud_score,
    _get_vote_weight,
    _refresh_poll_ratings,
    _compute_totals_map,
    _build_poll_embed,
    _build_results_embed,
    _build_disabled_view,
)
from tle.cogs._rpoll_views import RpollView, RpollButton

logger = logging.getLogger(__name__)


class Rpoll(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._scheduled_timers = {}  # poll_id -> asyncio.Task
        logger.info('Rpoll cog initialized')

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        """Re-register persistent views once the DB is available."""
        # user_db is initialized in the bot's on_ready handler, which may run
        # after cog listeners.  Wait briefly for it to become available.
        for _ in range(30):
            if cf_common.user_db is not None:
                break
            await asyncio.sleep(1)
        if cf_common.user_db is None:
            logger.warning('rpoll: user_db still None after waiting, skipping view registration')
            return
        self._register_persistent_views()
        self._schedule_all_active_polls()
        self._safety_net_task.start()

    def _register_persistent_views(self):
        """Register persistent views for all active polls so buttons work after restart."""
        try:
            polls = cf_common.user_db.get_all_active_rpolls()
            for poll in polls:
                options = cf_common.user_db.get_rpoll_options(poll.poll_id)
                view = RpollView(poll.poll_id, len(options))
                self.bot.add_view(view, message_id=int(poll.message_id))
            if polls:
                logger.info(f'rpoll: Re-registered {len(polls)} persistent poll views')
        except Exception as e:
            logger.error(f'rpoll: Failed to re-register poll views: {e}', exc_info=True)

    def _schedule_all_active_polls(self):
        """On startup, schedule a timer for every open poll."""
        try:
            polls = cf_common.user_db.get_all_active_rpolls()
            for poll in polls:
                self._schedule_expiry(poll.poll_id, poll.expires_at)
            if polls:
                logger.info(f'rpoll: Scheduled expiry timers for {len(polls)} active polls')
        except Exception as e:
            logger.error(f'rpoll: Failed to schedule poll timers: {e}', exc_info=True)

    def _schedule_expiry(self, poll_id, expires_at):
        """Schedule an asyncio task that sleeps until expires_at, then closes the poll."""
        # Cancel existing timer for this poll if any
        old = self._scheduled_timers.pop(poll_id, None)
        if old and not old.done():
            old.cancel()

        delay = max(0, expires_at - time.time())
        task = asyncio.create_task(self._expiry_timer(poll_id, delay))
        self._scheduled_timers[poll_id] = task

    async def _expiry_timer(self, poll_id, delay):
        """Sleep then close a specific poll."""
        try:
            await asyncio.sleep(delay)
            if cf_common.user_db is None:
                return
            poll = cf_common.user_db.get_rpoll(poll_id)
            if poll is None or poll.closed:
                return
            await self._close_poll(poll)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f'rpoll: Timer failed for poll {poll_id}: {e}', exc_info=True)
        finally:
            self._scheduled_timers.pop(poll_id, None)

    @tasks.task_spec(name='RpollSafetyNet',
                     waiter=tasks.Waiter.fixed_delay(_SAFETY_NET_INTERVAL))
    async def _safety_net_task(self, _):
        """Safety-net sweep for polls that slipped through (e.g. bot restart race)."""
        if cf_common.user_db is None:
            return
        try:
            expired = cf_common.user_db.get_expired_unclosed_rpolls()
        except Exception as e:
            logger.error(f'rpoll safety net: Failed to query expired polls: {e}', exc_info=True)
            return

        for poll in expired:
            try:
                await self._close_poll(poll)
            except Exception as e:
                logger.error(f'rpoll safety net: Failed to close poll {poll.poll_id}: {e}',
                             exc_info=True)

    async def _close_poll(self, poll):
        """Close an expired poll: mark in DB, edit message, send results."""
        cf_common.user_db.close_rpoll(poll.poll_id)
        logger.info(f'rpoll: Closed expired poll {poll.poll_id}')

        options = cf_common.user_db.get_rpoll_options(poll.poll_id)
        totals_map = _compute_totals_map(poll.poll_id, poll.formula)
        vote_count = cf_common.user_db.get_rpoll_vote_count(poll.poll_id)

        voters_map = None
        if not poll.anonymous:
            voters = cf_common.user_db.get_rpoll_voters(poll.poll_id)
            voters_map = {}
            for row in voters:
                voters_map.setdefault(row.option_index, []).append(int(row.user_id))

        option_pairs = [(opt.option_index, opt.label) for opt in options]
        disabled_view = _build_disabled_view(poll.poll_id, len(options))

        channel = self.bot.get_channel(int(poll.channel_id))
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(int(poll.channel_id))
            except Exception:
                logger.warning(f'rpoll: Could not fetch channel {poll.channel_id} for poll {poll.poll_id}')
                return

        # Preserve the original embed color when closing
        existing_color = None
        try:
            msg = await channel.fetch_message(int(poll.message_id))
            if msg.embeds:
                existing_color = msg.embeds[0].color
        except Exception:
            msg = None

        closed_embed = _build_poll_embed(
            poll.question, option_pairs, totals_map, vote_count,
            voters_map, expires_at=poll.expires_at, closed=True,
            formula=poll.formula, color=existing_color,
        )

        # Edit original message to disable buttons and show "ended"
        try:
            if msg is None:
                msg = await channel.fetch_message(int(poll.message_id))
            await msg.edit(embed=closed_embed, view=disabled_view)
        except Exception as e:
            logger.warning(f'rpoll: Could not edit message for poll {poll.poll_id}: {e}')

        # Reply to original message with a compact embed so mentions stay inert.
        try:
            results_embed = _build_results_embed(
                poll.question, option_pairs, totals_map, vote_count,
                formula=poll.formula,
            )
            ref = discord.MessageReference(
                message_id=int(poll.message_id), channel_id=int(poll.channel_id),
                fail_if_not_exists=False,
            )
            await channel.send('Poll done!', embed=results_embed, reference=ref)
        except Exception as e:
            logger.warning(f'rpoll: Could not send results for poll {poll.poll_id}: {e}')

    @commands.group(brief='Create a rating-weighted poll', invoke_without_command=True)
    async def rpoll(self, ctx, *, args: str = None):
        """Create a poll where votes are weighted by Codeforces rating.
        Vote for multiple options, click again to un-vote. No CF handle = 0.

        Example: ;rpoll +anon +2h "Best approach?" BFS,DFS,Dijkstra

        Flags: +anon (hide voters), +Nm/+Nh/+Nd (duration, default 24h).
        Scoring (default +exp):
          +exp: exponential `2^(rating/400) * 100`
          +sum: sum of ratings
          +team: team Elo (solo rating with 50% win vs all)
          +osu: top vote full, then 0.67x decay
          +gg / +mgg: all-time / monthly gitgud score
          +akari / +akariexp: Daily Akari rating (sum / exponential)
        """
        if args is None:
            await ctx.send_help(ctx.command)
            return
        args = args.strip()
        # Normalize smart/curly quotes (common on macOS) to straight quotes
        args = args.replace('“', '"').replace('”', '"')
        args = args.replace('‘', "'").replace('’', "'")
        anonymous = False
        duration = _DEFAULT_DURATION
        formula = 'exp'

        # Parse flags: +anon, +duration, +formula (in any order, before the question)
        while args.startswith('+'):
            token = args.split(None, 1)[0]
            if token == '+anon':
                anonymous = True
                args = args[len(token):].lstrip()
            elif token.lstrip('+') in _VALID_FORMULAS:
                formula = token.lstrip('+')
                args = args[len(token):].lstrip()
            else:
                parsed = _parse_duration(token)
                if parsed is not None:
                    duration = parsed
                    args = args[len(token):].lstrip()
                else:
                    break  # Not a flag, stop parsing

        # Extract quoted question, then comma-separated options
        if args.startswith('"'):
            end = args.find('"', 1)
            if end == -1:
                raise RpollError('Missing closing quote for question.')
            question = args[1:end]
            options_str = args[end + 1:].lstrip()
        else:
            # No quotes — first word is the question (legacy support)
            parts = args.split(None, 1)
            if len(parts) < 2:
                raise RpollError('Usage: ;rpoll "Question" Option1,Option2')
            question, options_str = parts

        options = [opt.strip() for opt in options_str.split(',')]
        options = [opt for opt in options if opt]  # Remove empty

        if len(options) < 2:
            raise RpollError('Need at least 2 options (comma-separated).')
        if len(options) > MAX_OPTIONS:
            raise RpollError(f'Maximum {MAX_OPTIONS} options allowed.')

        now = time.time()
        expires_at = now + duration

        poll_id = cf_common.user_db.create_rpoll(
            ctx.guild.id, ctx.channel.id, question, options,
            ctx.author.id, now, anonymous=anonymous, expires_at=expires_at,
            formula=formula,
        )

        embed = _build_poll_embed(
            question,
            list(enumerate(options)),
            {},
            0,
            expires_at=expires_at,
            formula=formula,
        )
        view = RpollView(poll_id, len(options))
        msg = await ctx.send(embed=embed, view=view)

        cf_common.user_db.set_rpoll_message_id(poll_id, msg.id)
        self._schedule_expiry(poll_id, expires_at)
        logger.info(f'rpoll: Created poll={poll_id} question={question!r} '
                    f'options={options} duration={duration}s by user={ctx.author.id} msg={msg.id}')

    @rpoll.command(name='list', brief='List active polls in this channel')
    async def list_polls(self, ctx):
        """List currently-open polls created in this channel."""
        now = time.time()
        polls = cf_common.user_db.get_active_rpolls_in_channel(ctx.channel.id, now)
        if not polls:
            await ctx.send(embed=discord_common.embed_neutral(
                'No active polls in this channel.'))
            return
        lines = []
        for p in polls:
            remaining = int(p.expires_at - now)
            jump = (f'https://discord.com/channels/{p.guild_id}/{p.channel_id}/{p.message_id}'
                    if p.message_id else None)
            header = f'**#{p.poll_id}** — {p.question}'
            if jump:
                header += f' ([jump]({jump}))'
            lines.append(f'{header}\nCloses <t:{int(p.expires_at)}:R>  |  formula: `{p.formula}`')
        embed = discord.Embed(title='Active polls', description='\n\n'.join(lines))
        await ctx.send(embed=embed)

    @discord_common.send_error_if(RpollError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Rpoll(bot))
