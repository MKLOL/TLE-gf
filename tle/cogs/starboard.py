import asyncio
import logging

import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import paginator
from tle.cogs._starboard_helpers import (
    _emoji_str, _looks_like_emoji, _CUSTOM_EMOJI_RE,
)
from tle.cogs._starboard_backfill import BackfillMixin, _BACKFILL_UNKNOWN
from tle.cogs._starboard_leaderboard_helpers import LeaderboardHelpersMixin
from tle.util.discord_common import requires_guild_feature
from tle.cogs._starboard_core import (
    CoreMixin,
    StarboardCogError,
    FULL_RE_RENDER,
)
from tle.cogs._starboard_impls import ImplMixin
from tle.cogs._starboard_proxy import ProxyReactionMixin
from tle.cogs._starboard_render import (
    _starboard_content,
    _parse_starboard_args,
    _REPLY_EMBED_COLOR,
    _IMAGE_EXTENSIONS,
    _VIDEO_EXTENSIONS,
    _NO_TIME_BOUND,
    build_starboard_message as _build_sb_msg,
)

logger = logging.getLogger(__name__)


class Starboard(
        LeaderboardHelpersMixin, CoreMixin, ProxyReactionMixin, ImplMixin,
        BackfillMixin, commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.locks = {}
        self._init_backfill_state()
        logger.info('Starboard cog initialized')

    @commands.Cog.listener()
    @discord_common.once
    async def on_ready(self):
        logger.info('Starboard cog on_ready fired, launching backfill task')
        asyncio.create_task(self._backfill_star_counts())

    # --- Event listeners ---

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload):
        await self._handle_reaction_add(payload)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload):
        await self._handle_reaction_remove(payload)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload):
        self._handle_message_delete(payload)

    # --- Commands ---

    @commands.group(brief='Starboard commands', invoke_without_command=True)
    async def starboard(self, ctx):
        """Group for commands involving the starboard."""
        await ctx.send_help(ctx.command)

    @starboard.command(brief='Add an emoji to the starboard')
    @commands.has_role(constants.TLE_ADMIN)
    async def add(self, ctx, emoji: str = constants._DEFAULT_STAR, threshold: int = 3, color: str = None):
        """Add an emoji to the starboard with optional threshold and color.
        Example: ;starboard add ⭐ 3 #ffaa10"""
        await self._impl_add(ctx, emoji, threshold, color)

    @starboard.command(brief='Delete an emoji from starboard')
    @commands.has_role(constants.TLE_ADMIN)
    async def delete(self, ctx, emoji: str = constants._DEFAULT_STAR):
        """Remove an emoji and all its tracked messages from the starboard."""
        await self._impl_delete(ctx, emoji)

    @starboard.command(brief='Edit threshold for an emoji')
    @commands.has_role(constants.TLE_ADMIN)
    async def edit_threshold(self, ctx, threshold: int, emoji: str = constants._DEFAULT_STAR):
        """Update the reaction threshold for an emoji."""
        await self._impl_edit_threshold(ctx, threshold, emoji)

    @starboard.command(brief='Edit color for an emoji')
    @commands.has_role(constants.TLE_ADMIN)
    async def edit_color(self, ctx, color: str, emoji: str = constants._DEFAULT_STAR):
        """Update the embed color for an emoji. Use hex format like #ffaa10."""
        await self._impl_edit_color(ctx, color, emoji)

    @starboard.command(brief='Set starboard channel for an emoji')
    @commands.has_role(constants.TLE_ADMIN)
    async def here(self, ctx, emoji: str = constants._DEFAULT_STAR):
        """Set the current channel as the starboard channel for a specific emoji.
        Example: ;starboard here ⭐"""
        await self._impl_here(ctx, emoji)

    @starboard.command(brief='Clear starboard channel for an emoji')
    @commands.has_role(constants.TLE_ADMIN)
    async def clear(self, ctx, emoji: str = constants._DEFAULT_STAR):
        """Clear the starboard channel for a specific emoji.
        Example: ;starboard clear ⭐"""
        await self._impl_clear(ctx, emoji)

    @starboard.command(brief='Remove a message from starboard')
    @commands.has_role(constants.TLE_ADMIN)
    async def remove(self, ctx, original_message_id: int, emoji: str = constants._DEFAULT_STAR):
        """Remove a particular message from the starboard database for the given emoji."""
        await self._impl_remove(ctx, original_message_id, emoji)

    @starboard.command(brief='Show all configured starboard emojis')
    async def show(self, ctx):
        """Show all configured starboard emojis with their threshold, color, channel, and aliases."""
        await self._impl_show(ctx)

    # --- Alias commands ---

    @starboard.group(brief='Manage emoji aliases', invoke_without_command=True)
    async def alias(self, ctx):
        """Manage emoji aliases. Aliases count toward the main emoji's starboard."""
        await ctx.send_help(ctx.command)

    @alias.command(name='add', brief='Add an alias for a main emoji')
    @commands.has_role(constants.TLE_ADMIN)
    async def alias_add(self, ctx, alias_emoji: str, main_emoji: str = constants._DEFAULT_STAR):
        """Add an alias emoji that counts toward a main emoji's starboard.
        Example: ;starboard alias add 👍 ⭐"""
        # Validate main emoji is configured
        entry = cf_common.user_db.get_starboard_entry(ctx.guild.id, main_emoji)
        if entry is None:
            raise StarboardCogError(f'Main emoji {main_emoji} is not configured for this starboard.')
        # Can't alias a main emoji
        if cf_common.user_db.get_starboard_entry(ctx.guild.id, alias_emoji) is not None:
            raise StarboardCogError(f'{alias_emoji} is already a main starboard emoji. '
                                    f'Remove it first before using it as an alias.')
        # Can't alias an alias
        existing = cf_common.user_db.resolve_alias(ctx.guild.id, alias_emoji)
        if existing is not None:
            raise StarboardCogError(f'{alias_emoji} is already an alias for {existing}. '
                                    f'Remove it first with `;starboard alias remove {alias_emoji}`.')
        cf_common.user_db.add_starboard_alias(ctx.guild.id, alias_emoji, main_emoji)
        logger.info(f'CMD starboard alias add: guild={ctx.guild.id} alias={alias_emoji} '
                    f'main={main_emoji} by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(
            f'Added {alias_emoji} as alias for {main_emoji}'
        ))

    @alias.command(name='remove', brief='Remove an emoji alias')
    @commands.has_role(constants.TLE_ADMIN)
    async def alias_remove(self, ctx, alias_emoji: str):
        """Remove an alias emoji.
        Example: ;starboard alias remove 👍"""
        rc = cf_common.user_db.remove_starboard_alias(ctx.guild.id, alias_emoji)
        if not rc:
            raise StarboardCogError(f'{alias_emoji} is not an alias.')
        logger.info(f'CMD starboard alias remove: guild={ctx.guild.id} alias={alias_emoji} '
                    f'by user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(f'Removed alias {alias_emoji}'))

    @alias.command(name='list', brief='List all emoji aliases')
    async def alias_list(self, ctx):
        """Show all emoji aliases configured for this server."""
        rows = cf_common.user_db.get_all_aliases_for_guild(ctx.guild.id)
        if not rows:
            await ctx.send(embed=discord_common.embed_neutral('No aliases configured.'))
            return
        lines = [f'{r.alias_emoji} → {r.main_emoji}' for r in rows]
        await ctx.send(embed=discord_common.embed_success('\n'.join(lines)))

    # --- Per-user default emoji ---

    @starboard.group(name='default', brief='Manage your default starboard emoji',
                     invoke_without_command=True)
    async def default_(self, ctx):
        """Per-user default emoji for ``;starboard`` leaderboard commands.

        Use ``set <emoji>`` to choose one, ``show`` to see it, ``clear`` to
        remove it. Falls back to the server's default star (\N{WHITE MEDIUM STAR})
        when unset.
        """
        await ctx.send_help(ctx.command)

    @default_.command(name='set', brief='Set your default starboard emoji')
    async def default_set(self, ctx, emoji: str):
        """Set the emoji used by ``;starboard leaderboard`` / ``rank`` / etc.
        when you don't pass one. Must be a configured main emoji or alias."""
        main_emoji, entry = self._resolve_emoji(ctx.guild.id, emoji)
        if entry is None:
            raise StarboardCogError(
                f'{emoji} is not configured for this starboard. '
                f'Ask an admin to add it with `;starboard add {emoji}`.')
        # Store the main emoji so leaderboard lookups skip the alias hop.
        cf_common.user_db.set_user_starboard_default(
            ctx.guild.id, ctx.author.id, main_emoji)
        logger.info(f'CMD starboard default set: guild={ctx.guild.id} '
                    f'user={ctx.author.id} emoji={main_emoji} (input={emoji})')
        await ctx.send(embed=discord_common.embed_success(
            f'Your default starboard emoji is now {main_emoji}.'))

    @default_.command(name='show', brief='Show your default starboard emoji')
    async def default_show(self, ctx):
        """Show your saved default, or the fallback if unset."""
        saved = cf_common.user_db.get_user_starboard_default(
            ctx.guild.id, ctx.author.id)
        if saved is None:
            await ctx.send(embed=discord_common.embed_neutral(
                f'No default set. Falling back to {constants._DEFAULT_STAR}.'))
            return
        await ctx.send(embed=discord_common.embed_neutral(
            f'Your default starboard emoji is {saved}.'))

    @default_.command(name='clear', brief='Clear your default starboard emoji')
    async def default_clear(self, ctx):
        """Remove your saved default. Falls back to the server's default star."""
        rc = cf_common.user_db.clear_user_starboard_default(
            ctx.guild.id, ctx.author.id)
        if not rc:
            await ctx.send(embed=discord_common.embed_neutral(
                'You had no default set.'))
            return
        logger.info(f'CMD starboard default clear: guild={ctx.guild.id} '
                    f'user={ctx.author.id}')
        await ctx.send(embed=discord_common.embed_success(
            f'Cleared. Falling back to {constants._DEFAULT_STAR}.'))

    # --- Leaderboard commands ---

    @starboard.command(brief='Show starboard leaderboard by message count',
                       usage='[emoji] [day|week|month|year|all] [d>=date] [d<date]')
    async def leaderboard(self, ctx, *args):
        """Show top users by number of starboarded messages for an emoji.
        Requires the `starboard_leaderboard` feature to be enabled.
        Supports day/week/month/year-to-date, all-time, and date ranges."""
        emoji, dlo, dhi, scope = _parse_starboard_args(
            args, default_emoji=self._user_default_emoji(
                ctx.guild.id, ctx.author.id), include_label=True)
        emoji = self._leaderboard_entry(ctx, emoji)
        rows = cf_common.user_db.get_starboard_leaderboard(ctx.guild.id, emoji, dlo, dhi)
        if not rows:
            raise StarboardCogError(
                f'No starboarded messages found for {emoji} · {scope}.')
        logger.info(f'CMD starboard leaderboard: guild={ctx.guild.id} emoji={emoji} '
                    f'scope={scope} dlo={dlo} dhi={dhi} {len(rows)} users '
                    f'by user={ctx.author.id}')
        self._paginate_leaderboard(
            ctx, rows, emoji, f'Starboard Leaderboard · {scope}', 'messages')

    @starboard.command(name='star-leaderboard', aliases=['rank'],
                       brief='Show starboard leaderboard by star count',
                       usage='[emoji] [day|week|month|year|all] [d>=date] [d<date]')
    async def star_leaderboard(self, ctx, *args):
        """Show top users by total star count for an emoji.
        Requires the `starboard_leaderboard` feature to be enabled.
        Supports day/week/month/year-to-date, all-time, and date ranges."""
        emoji, dlo, dhi, scope = _parse_starboard_args(
            args, default_emoji=self._user_default_emoji(
                ctx.guild.id, ctx.author.id), include_label=True)
        emoji = self._leaderboard_entry(ctx, emoji)
        rows = cf_common.user_db.get_starboard_star_leaderboard(ctx.guild.id, emoji, dlo, dhi)
        if not rows:
            raise StarboardCogError(f'No star data found for {emoji} · {scope}. '
                                    'Star counts are populated via backfill and live tracking.')
        logger.info(f'CMD starboard star-leaderboard: guild={ctx.guild.id} emoji={emoji} '
                    f'scope={scope} dlo={dlo} dhi={dhi} {len(rows)} users '
                    f'by user={ctx.author.id}')
        self._paginate_leaderboard(
            ctx, rows, emoji, f'Star Leaderboard · {scope}', 'stars')

    @starboard.command(name='star-givers', brief='Show top star givers',
                       usage='[emoji] [day|week|month|year|all] [d>=date] [d<date]')
    async def star_givers(self, ctx, *args):
        """Show top users by number of stars given (reactions) for an emoji.
        Requires the `starboard_leaderboard` feature to be enabled.
        Supports day/week/month/year-to-date, all-time, and date ranges."""
        emoji, dlo, dhi, scope = _parse_starboard_args(
            args, default_emoji=self._user_default_emoji(
                ctx.guild.id, ctx.author.id), include_label=True)
        emoji = self._leaderboard_entry(ctx, emoji)
        emoji_family = cf_common.user_db.get_emoji_family(ctx.guild.id, emoji)
        rows = cf_common.user_db.get_star_givers_leaderboard(ctx.guild.id, emoji, dlo, dhi,
                                                              emoji_family=emoji_family)
        if not rows:
            raise StarboardCogError(
                f'No reactor data found for {emoji} · {scope}.')
        logger.info(f'CMD starboard star-givers: guild={ctx.guild.id} emoji={emoji} '
                    f'scope={scope} dlo={dlo} dhi={dhi} {len(rows)} users '
                    f'by user={ctx.author.id}')
        self._paginate_leaderboard(
            ctx, rows, emoji, f'Star Givers · {scope}', 'stars given')

    @starboard.command(brief='Show who stars their own messages the most',
                       usage='[emoji] [day|week|month|year|all] [d>=date] [d<date]')
    async def narcissus(self, ctx, *args):
        """Show users who star their own messages the most.
        Requires the `starboard_leaderboard` feature to be enabled.
        Supports day/week/month/year-to-date, all-time, and date ranges."""
        emoji, dlo, dhi, scope = _parse_starboard_args(
            args, default_emoji=self._user_default_emoji(
                ctx.guild.id, ctx.author.id), include_label=True)
        emoji = self._leaderboard_entry(ctx, emoji)
        emoji_family = cf_common.user_db.get_emoji_family(ctx.guild.id, emoji)
        rows = cf_common.user_db.get_narcissus_leaderboard(ctx.guild.id, emoji, dlo, dhi,
                                                            emoji_family=emoji_family)
        if not rows:
            raise StarboardCogError(
                f'No self-stars found for {emoji} · {scope}. How humble!')
        logger.info(f'CMD starboard narcissus: guild={ctx.guild.id} emoji={emoji} '
                    f'scope={scope} dlo={dlo} dhi={dhi} {len(rows)} users '
                    f'by user={ctx.author.id}')
        self._paginate_leaderboard(
            ctx, rows, emoji, f'Narcissus Leaderboard · {scope}', 'self-stars')

    @starboard.command(brief='Show top starred messages',
                       usage='[emoji] [user] [day|week|month|year|all] [d>=date] [d<date]')
    async def top(self, ctx, *args):
        """Show top starboarded messages sorted by star count for an emoji.
        Mention a user to see only their top messages.
        Requires the `starboard_leaderboard` feature to be enabled.
        Supports day/week/month/year-to-date, all-time, and date ranges."""
        # Parse args with emoji always first: [emoji] [user] [timeline...]
        target_member, emoji_arg, timeline_args = self._parse_top_args(ctx, args)
        # Build args for _parse_starboard_args (emoji + timeline filters)
        parse_args = []
        if emoji_arg is not None:
            parse_args.append(emoji_arg)
        parse_args.extend(timeline_args)
        emoji, dlo, dhi, scope = _parse_starboard_args(
            parse_args,
            default_emoji=self._user_default_emoji(
                ctx.guild.id, ctx.author.id), include_label=True)
        emoji = self._leaderboard_entry(ctx, emoji)

        author_id = target_member.id if target_member else None
        rows = cf_common.user_db.get_top_starboard_messages(ctx.guild.id, emoji, dlo, dhi,
                                                            author_id=author_id)
        if not rows:
            if target_member:
                raise StarboardCogError(
                    f'No starred messages found for {target_member.mention} with {emoji}.')
            raise StarboardCogError(f'No starred messages found for {emoji}.')

        logger.info(f'CMD starboard top: guild={ctx.guild.id} emoji={emoji} '
                    f'dlo={dlo} dhi={dhi} author_filter={author_id} '
                    f'{len(rows)} messages by user={ctx.author.id}')
        pages = self._make_top_pages(
            ctx, rows, emoji, target_member, scope_label=scope)
        paginator.paginate(self.bot, ctx.channel, pages, wait_time=300,
                           set_pagenum_footers=True, author_id=ctx.author.id)

    # --- Fix / resync commands ---

    @starboard.command(brief='Resync star count for a message from Discord')
    @commands.has_role(constants.TLE_ADMIN)
    @requires_guild_feature('migration_ops')
    async def fix(self, ctx, message_ref: str, emoji: str = None):
        """Resync reactors for a starboarded message from Discord.

        Accepts a message link or a bare message ID.  If no emoji is given,
        all emoji entries for that message are resynced.

        Examples:
            ;starboard fix https://discord.com/channels/123/456/789
            ;starboard fix 789 ⭐
        """
        await self._impl_fix(ctx, message_ref, emoji)

    # --- Backfill status ---

    @starboard.command(brief='Show backfill progress')
    @requires_guild_feature('migration_ops')
    async def backfill_status(self, ctx):
        """Show the progress of the background star count backfill."""
        await self._impl_backfill_status(ctx)

    @discord_common.send_error_if(StarboardCogError)
    async def cog_command_error(self, ctx, error):
        pass


async def setup(bot):
    await bot.add_cog(Starboard(bot))
