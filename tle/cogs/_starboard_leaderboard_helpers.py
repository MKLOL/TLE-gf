"""Argument and page helpers for starboard leaderboard commands."""

import re

import discord

from tle.util import discord_common
from tle.util import paginator
from tle.util import ranking
from tle.cogs._starboard_core import StarboardCogError
from tle.cogs._starboard_helpers import _looks_like_emoji
from tle.cogs._starboard_render import _TIMELINE_KEYWORDS


class LeaderboardHelpersMixin:
    def _parse_top_args(self, ctx, args):
        """Split top args into member, emoji, and timeline arguments."""
        target_member = None
        emoji_arg = None
        timeline_args = []
        for arg in args:
            if target_member is None:
                match = re.match(r'<@!?(\d+)>$', arg)
                if match:
                    member = ctx.guild.get_member(int(match.group(1)))
                    if member is not None:
                        target_member = member
                        continue
            lower = arg.lower()
            if (
                    lower in _TIMELINE_KEYWORDS
                    or lower.startswith('d>=')
                    or lower.startswith('d<')):
                timeline_args.append(arg)
                continue
            if emoji_arg is None and _looks_like_emoji(arg):
                emoji_arg = arg
                continue
            if target_member is None:
                member = discord.utils.find(
                    lambda value, name=lower: value.name.lower() == name
                    or value.display_name.lower() == name,
                    ctx.guild.members)
                if member is not None:
                    target_member = member
                    continue
                raise StarboardCogError(
                    f'User `{arg}` not found in this server.')
        return target_member, emoji_arg, timeline_args

    def _make_top_pages(
            self, ctx, rows, emoji, target_member, scope_label=None):
        """Build paginated embed pages for ``;starboard top``."""
        if target_member:
            title = (
                f'{emoji} Top Starred Messages — '
                f'{target_member.display_name}')
        else:
            title = f'{emoji} Top Starred Messages'
        if scope_label:
            title += f' · {scope_label}'

        ranked = ranking.rank_items(rows, lambda row: row.star_count)
        pages = []
        for chunk in paginator.chunkify(ranked, 10):
            lines = []
            for rank, row in chunk:
                jump_url = (
                    f'https://discord.com/channels/{ctx.guild.id}/'
                    f'{row.channel_id}/{row.original_msg_id}')
                member = ctx.guild.get_member(int(row.author_id))
                name = member.mention if member else f'<@{row.author_id}>'
                lines.append(
                    f'**#{rank}** {name} — **{row.star_count}** '
                    f'{emoji} — {jump_url}')
            embed = discord.Embed(
                title=title,
                description='\n'.join(lines),
                color=discord_common.random_cf_color(),
            )
            pages.append((None, embed))
        return pages
