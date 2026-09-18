"""Persistent button views for the rpoll cog.

Split out of ``rpoll.py`` so the cog file stays small.
"""
import logging
import time

import discord

from tle.util import codeforces_common as cf_common
from tle.cogs._rpoll_locks import poll_lock
from tle.cogs._rpoll_helpers import (
    _NUMBER_EMOJIS,
    _get_vote_weight,
    _refresh_poll_ratings,
    _compute_totals_map,
    _build_poll_embed,
)

logger = logging.getLogger(__name__)


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
        lock = poll_lock(self.poll_id)
        deferred = lock.locked()
        if deferred:
            # A repair/expiry or Discord rate limit may hold the lock beyond
            # the interaction's initial response deadline.
            await interaction.response.defer()
        async with lock:
            await self._vote(interaction, deferred=deferred)

    async def _vote(self, interaction, *, deferred=False):
        send = interaction.followup.send if deferred else interaction.response.send_message
        if cf_common.user_db is None:
            await send('Bot is still starting up.', ephemeral=True)
            return

        # Check if poll is closed or expired before allowing vote
        poll = cf_common.user_db.get_rpoll(self.poll_id)
        if poll is None:
            await send('Poll not found.', ephemeral=True)
            return
        if poll.closed or poll.expires_at <= time.time():
            await send('This poll has ended.', ephemeral=True)
            return

        user_id = interaction.user.id
        guild_id = interaction.guild_id

        rating = _get_vote_weight(poll, user_id, guild_id)
        added = cf_common.user_db.toggle_rpoll_vote(
            self.poll_id, user_id, self.option_index, rating
        )

        _refresh_poll_ratings(poll, guild_id)

        options = cf_common.user_db.get_rpoll_options(self.poll_id)
        totals_map = _compute_totals_map(self.poll_id, poll.formula)
        vote_count = cf_common.user_db.get_rpoll_vote_count(self.poll_id)

        voters_map = None
        if not poll.anonymous:
            voters = cf_common.user_db.get_rpoll_voters(self.poll_id)
            voters_map = {}
            for row in voters:
                voters_map.setdefault(row.option_index, []).append(int(row.user_id))

        # Preserve the original embed color
        existing_color = None
        if interaction.message and interaction.message.embeds:
            existing_color = interaction.message.embeds[0].color

        embed = _build_poll_embed(
            poll.question,
            [(opt.option_index, opt.label) for opt in options],
            totals_map,
            vote_count,
            voters_map,
            expires_at=poll.expires_at,
            formula=poll.formula,
            color=existing_color,
        )

        action = 'voted for' if added else 'removed vote from'
        option_label = next((opt.label for opt in options if opt.option_index == self.option_index), '?')
        kwargs = {}
        if interaction.message and interaction.message.flags.suppress_embeds:
            kwargs['suppress' if deferred else 'suppress_embeds'] = False
        if deferred:
            await interaction.message.edit(embed=embed, **kwargs)
        else:
            await interaction.response.edit_message(embed=embed, **kwargs)
        logger.info(f'rpoll: user={user_id} {action} option {self.option_index} '
                    f'({option_label}) on poll={self.poll_id} rating={rating}')
