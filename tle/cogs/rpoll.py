import asyncio
import logging
import re
import time

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import tasks

logger = logging.getLogger(__name__)

# Number emojis for options 0-4
_NUMBER_EMOJIS = ['1\N{COMBINING ENCLOSING KEYCAP}',
                  '2\N{COMBINING ENCLOSING KEYCAP}',
                  '3\N{COMBINING ENCLOSING KEYCAP}',
                  '4\N{COMBINING ENCLOSING KEYCAP}',
                  '5\N{COMBINING ENCLOSING KEYCAP}']

MAX_OPTIONS = 5
_DEFAULT_DURATION = 86400  # 24 hours in seconds
_EXPIRY_CHECK_INTERVAL = 60  # Check every 60 seconds
_DURATION_RE = re.compile(r'^\+(\d+)([mhd])$')


class RpollError(commands.CommandError):
    pass


def _parse_duration(token):
    """Parse a duration token like +1h, +30m, +2d. Returns seconds."""
    m = _DURATION_RE.match(token)
    if not m:
        return None
    value = int(m.group(1))
    unit = m.group(2)
    if unit == 'm':
        return value * 60
    elif unit == 'h':
        return value * 3600
    elif unit == 'd':
        return value * 86400
    return None


def _build_poll_embed(question, options, totals_map, vote_count, voters_map=None,
                      expires_at=None, closed=False):
    """Build the embed for a rating poll.

    Args:
        question: The poll question.
        options: List of (option_index, label) tuples.
        totals_map: Dict of option_index -> total_rating.
        vote_count: Total number of distinct voters.
        voters_map: Optional dict of option_index -> list of user_ids.
        expires_at: Optional UNIX timestamp when poll expires.
        closed: Whether the poll has ended.
    """
    grand_total = sum(totals_map.get(idx, 0) for idx, _ in options)
    show_pct = grand_total > 0

    lines = []
    for idx, label in options:
        total = totals_map.get(idx, 0)
        emoji = _NUMBER_EMOJIS[idx] if idx < len(_NUMBER_EMOJIS) else f'{idx + 1}.'
        if show_pct:
            pct = round(total / grand_total * 100)
            lines.append(f'{emoji} {label} — **{total}** ({pct}%)')
        else:
            lines.append(f'{emoji} {label} — **{total}**')

    # Winner / tied line
    if grand_total > 0:
        max_total = max(totals_map.get(idx, 0) for idx, _ in options)
        leaders = [label for idx, label in options if totals_map.get(idx, 0) == max_total]
        if len(leaders) == 1:
            second = sorted((totals_map.get(idx, 0) for idx, _ in options), reverse=True)
            lead = max_total - (second[1] if len(second) > 1 else 0)
            lines.append(f'\nLeader: **{leaders[0]}** (+{lead})')
        else:
            lines.append(f'\nTied: {", ".join(f"**{l}**" for l in leaders)}')

    # Voter breakdown per option
    if voters_map:
        lines.append('')
        for idx, label in options:
            user_ids = voters_map.get(idx, [])
            if user_ids:
                mentions = ', '.join(f'<@{uid}>' for uid in user_ids)
                lines.append(f'{label}: {mentions}')

    # Expiry info in description
    if closed:
        lines.append('\n**Poll has ended.**')
    elif expires_at:
        lines.append(f'\nEnds <t:{int(expires_at)}:R>')

    embed = discord.Embed(
        title=question,
        description='\n'.join(lines),
        color=discord_common.random_cf_color(),
    )
    embed.set_footer(text=f'{vote_count} vote{"s" if vote_count != 1 else ""}')
    return embed


def _build_disabled_view(poll_id, option_count):
    """Build a view with all buttons disabled."""
    view = discord.ui.View(timeout=None)
    for i in range(option_count):
        emoji = _NUMBER_EMOJIS[i] if i < len(_NUMBER_EMOJIS) else None
        btn = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            emoji=emoji,
            custom_id=f'rpoll:{poll_id}:{i}',
            disabled=True,
        )
        view.add_item(btn)
    return view


class RpollView(discord.ui.View):
    """Persistent view with buttons for each poll option."""

    def __init__(self, poll_id, option_count):
        super().__init__(timeout=None)
        for i in range(option_count):
            self.add_item(RpollButton(poll_id, i))


class RpollButton(discord.ui.Button):
    """A single poll option button."""

    def __init__(self, poll_id, option_index):
        emoji = _NUMBER_EMOJIS[option_index] if option_index < len(_NUMBER_EMOJIS) else None
        super().__init__(
            style=discord.ButtonStyle.secondary,
            emoji=emoji,
            custom_id=f'rpoll:{poll_id}:{option_index}',
        )
        self.poll_id = poll_id
        self.option_index = option_index

    async def callback(self, interaction: discord.Interaction):
        if cf_common.user_db is None:
            await interaction.response.send_message('Bot is still starting up.', ephemeral=True)
            return

        # Check if poll is closed or expired before allowing vote
        poll = cf_common.user_db.get_rpoll(self.poll_id)
        if poll is None:
            await interaction.response.send_message('Poll not found.', ephemeral=True)
            return
        if poll.closed or poll.expires_at <= time.time():
            await interaction.response.send_message('This poll has ended.', ephemeral=True)
            return

        user_id = interaction.user.id
        guild_id = interaction.guild_id

        rating = cf_common.user_db.get_rpoll_user_rating(user_id, guild_id)
        added = cf_common.user_db.toggle_rpoll_vote(
            self.poll_id, user_id, self.option_index, rating
        )

        options = cf_common.user_db.get_rpoll_options(self.poll_id)
        totals = cf_common.user_db.get_rpoll_totals(self.poll_id)
        totals_map = {row.option_index: row.total_rating for row in totals}
        vote_count = cf_common.user_db.get_rpoll_vote_count(self.poll_id)

        voters_map = None
        if not poll.anonymous:
            voters = cf_common.user_db.get_rpoll_voters(self.poll_id)
            voters_map = {}
            for row in voters:
                voters_map.setdefault(row.option_index, []).append(int(row.user_id))

        embed = _build_poll_embed(
            poll.question,
            [(opt.option_index, opt.label) for opt in options],
            totals_map,
            vote_count,
            voters_map,
            expires_at=poll.expires_at,
        )

        action = 'voted for' if added else 'removed vote from'
        option_label = next((opt.label for opt in options if opt.option_index == self.option_index), '?')
        await interaction.response.edit_message(embed=embed)
        logger.info(f'rpoll: user={user_id} {action} option {self.option_index} '
                    f'({option_label}) on poll={self.poll_id} rating={rating}')


class Rpoll(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
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
        self._expiry_task.start()

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

    @tasks.task_spec(name='RpollExpiry',
                     waiter=tasks.Waiter.fixed_delay(_EXPIRY_CHECK_INTERVAL))
    async def _expiry_task(self, _):
        """Check for expired polls and close them."""
        if cf_common.user_db is None:
            return
        try:
            expired = cf_common.user_db.get_expired_unclosed_rpolls()
        except Exception as e:
            logger.error(f'rpoll expiry: Failed to query expired polls: {e}', exc_info=True)
            return

        for poll in expired:
            try:
                await self._close_poll(poll)
            except Exception as e:
                logger.error(f'rpoll expiry: Failed to close poll {poll.poll_id}: {e}',
                             exc_info=True)

    async def _close_poll(self, poll):
        """Close an expired poll: mark in DB, edit message, send results."""
        cf_common.user_db.close_rpoll(poll.poll_id)
        logger.info(f'rpoll: Closed expired poll {poll.poll_id}')

        options = cf_common.user_db.get_rpoll_options(poll.poll_id)
        totals = cf_common.user_db.get_rpoll_totals(poll.poll_id)
        totals_map = {row.option_index: row.total_rating for row in totals}
        vote_count = cf_common.user_db.get_rpoll_vote_count(poll.poll_id)

        voters_map = None
        if not poll.anonymous:
            voters = cf_common.user_db.get_rpoll_voters(poll.poll_id)
            voters_map = {}
            for row in voters:
                voters_map.setdefault(row.option_index, []).append(int(row.user_id))

        option_pairs = [(opt.option_index, opt.label) for opt in options]
        closed_embed = _build_poll_embed(
            poll.question, option_pairs, totals_map, vote_count,
            voters_map, expires_at=poll.expires_at, closed=True,
        )
        disabled_view = _build_disabled_view(poll.poll_id, len(options))

        channel = self.bot.get_channel(int(poll.channel_id))
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(int(poll.channel_id))
            except Exception:
                logger.warning(f'rpoll: Could not fetch channel {poll.channel_id} for poll {poll.poll_id}')
                return

        # Edit original message to disable buttons and show "ended"
        try:
            msg = await channel.fetch_message(int(poll.message_id))
            await msg.edit(embed=closed_embed, view=disabled_view)
        except Exception as e:
            logger.warning(f'rpoll: Could not edit message for poll {poll.poll_id}: {e}')

        # Send "Poll done" reply with results
        try:
            results_embed = _build_poll_embed(
                poll.question, option_pairs, totals_map, vote_count,
                voters_map, closed=True,
            )
            await channel.send('Poll done!', embed=results_embed)
        except Exception as e:
            logger.warning(f'rpoll: Could not send results for poll {poll.poll_id}: {e}')

    @commands.command(brief='Create a rating-weighted poll')
    async def rpoll(self, ctx, *, args: str):
        """Create a poll where votes are weighted by Codeforces rating.

        Usage: ;rpoll "What's the best approach?" BFS,DFS,Dijkstra
               ;rpoll +anon "What's the best approach?" BFS,DFS,Dijkstra
               ;rpoll +2h "What's the best approach?" BFS,DFS,Dijkstra

        Each voter's CF rating is added to their chosen option(s).
        Users without a linked CF handle count as 0.
        You can vote for multiple options. Click again to un-vote.
        Use +anon to hide who voted for what.
        Duration: +Nm (minutes), +Nh (hours), +Nd (days). Default: 24h.
        """
        args = args.strip()
        anonymous = False
        duration = _DEFAULT_DURATION

        # Parse flags: +anon and +duration (in any order, before the question)
        while args.startswith('+'):
            token = args.split(None, 1)[0]
            if token == '+anon':
                anonymous = True
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
            ctx.author.id, now, anonymous=anonymous, expires_at=expires_at
        )

        embed = _build_poll_embed(
            question,
            list(enumerate(options)),
            {},
            0,
            expires_at=expires_at,
        )
        view = RpollView(poll_id, len(options))
        msg = await ctx.send(embed=embed, view=view)

        cf_common.user_db.set_rpoll_message_id(poll_id, msg.id)
        logger.info(f'rpoll: Created poll={poll_id} question={question!r} '
                    f'options={options} duration={duration}s by user={ctx.author.id} msg={msg.id}')

    @discord_common.send_error_if(RpollError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Rpoll(bot))
