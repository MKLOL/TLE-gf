import asyncio
import logging
import time

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common

logger = logging.getLogger(__name__)

# Number emojis for options 0-4
_NUMBER_EMOJIS = ['1\N{COMBINING ENCLOSING KEYCAP}',
                  '2\N{COMBINING ENCLOSING KEYCAP}',
                  '3\N{COMBINING ENCLOSING KEYCAP}',
                  '4\N{COMBINING ENCLOSING KEYCAP}',
                  '5\N{COMBINING ENCLOSING KEYCAP}']

MAX_OPTIONS = 5


class RpollError(commands.CommandError):
    pass


def _build_poll_embed(question, options, totals_map, vote_count):
    """Build the embed for a rating poll.

    Args:
        question: The poll question.
        options: List of (option_index, label) tuples.
        totals_map: Dict of option_index -> total_rating.
        vote_count: Total number of distinct voters.
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

    embed = discord.Embed(
        title=question,
        description='\n'.join(lines),
        color=discord_common.random_cf_color(),
    )
    embed.set_footer(text=f'{vote_count} vote{"s" if vote_count != 1 else ""}')
    return embed


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

        user_id = interaction.user.id
        guild_id = interaction.guild_id

        rating = cf_common.user_db.get_rpoll_user_rating(user_id, guild_id)
        added = cf_common.user_db.toggle_rpoll_vote(
            self.poll_id, user_id, self.option_index, rating
        )

        # Rebuild embed with updated totals
        poll = cf_common.user_db.get_rpoll(self.poll_id)
        if poll is None:
            await interaction.response.send_message('Poll not found.', ephemeral=True)
            return

        options = cf_common.user_db.get_rpoll_options(self.poll_id)
        totals = cf_common.user_db.get_rpoll_totals(self.poll_id)
        totals_map = {row.option_index: row.total_rating for row in totals}
        vote_count = cf_common.user_db.get_rpoll_vote_count(self.poll_id)

        embed = _build_poll_embed(
            poll.question,
            [(opt.option_index, opt.label) for opt in options],
            totals_map,
            vote_count,
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

    @commands.command(brief='Create a rating-weighted poll')
    @commands.has_role(constants.TLE_ADMIN)
    async def rpoll(self, ctx, question: str, *, options_str: str):
        """Create a poll where votes are weighted by Codeforces rating.

        Usage: ;rpoll "What's the best approach?" BFS,DFS,Dijkstra

        Each voter's CF rating is added to their chosen option(s).
        Users without a linked CF handle count as 0.
        You can vote for multiple options. Click again to un-vote.
        """
        options = [opt.strip() for opt in options_str.split(',')]
        options = [opt for opt in options if opt]  # Remove empty

        if len(options) < 2:
            raise RpollError('Need at least 2 options (comma-separated).')
        if len(options) > MAX_OPTIONS:
            raise RpollError(f'Maximum {MAX_OPTIONS} options allowed.')

        poll_id = cf_common.user_db.create_rpoll(
            ctx.guild.id, ctx.channel.id, question, options,
            ctx.author.id, time.time()
        )

        embed = _build_poll_embed(
            question,
            list(enumerate(options)),
            {},
            0,
        )
        view = RpollView(poll_id, len(options))
        msg = await ctx.send(embed=embed, view=view)

        cf_common.user_db.set_rpoll_message_id(poll_id, msg.id)
        logger.info(f'rpoll: Created poll={poll_id} question={question!r} '
                    f'options={options} by user={ctx.author.id} msg={msg.id}')

    @discord_common.send_error_if(RpollError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Rpoll(bot))
