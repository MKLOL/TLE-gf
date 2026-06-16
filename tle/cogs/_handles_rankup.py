import asyncio
import contextlib

import discord
from discord.ext import commands

from tle import constants
from tle.util import cache_system2
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tle.util import events
from tle.util import paginator

from tle.cogs._handles_helpers import (
    HandleCogError,
    _MAX_RATING_CHANGES_PER_EMBED,
    _TOP_DELTAS_COUNT,
)


class RankUpMixin:
    """Mixin holding rank-role updates, rankup-embed publishing, and the
    pingable-role subscription commands."""

    @events.listener_spec(name='RatingChangesListener',
                          event_cls=events.RatingChangesUpdate,
                          with_lock=True)
    async def _on_rating_changes(self, event):
        contest, changes = event.contest, event.rating_changes
        change_by_handle = {change.handle: change for change in changes}

        async def update_for_guild(guild):
            if cf_common.user_db.has_auto_role_update_enabled(guild.id):
                with contextlib.suppress(HandleCogError):
                    await self._update_ranks_all(guild)
            channel_id = cf_common.user_db.get_rankup_channel(guild.id)
            channel = guild.get_channel(channel_id)
            if channel is not None:
                with contextlib.suppress(HandleCogError):
                    embeds = self._make_rankup_embeds(guild, contest, change_by_handle)
                    for embed in embeds:
                        await channel.send(embed=embed)

        await asyncio.gather(*(update_for_guild(guild) for guild in self.bot.guilds),
                             return_exceptions=True)
        self.logger.info(f'All guilds updated for contest {contest.id}.')

    @staticmethod
    async def update_member_rank_role(member, role_to_assign, *, reason):
        """Sets the `member` to only have the rank role of `role_to_assign`. All other rank roles
        on the member, if any, will be removed. If `role_to_assign` is None all existing rank roles
        on the member will be removed.
        """
        if member is None:
            return
        role_names_to_remove = {rank.title for rank in cf.RATED_RANKS}
        role_names_to_remove.add(cf.UNRATED_RANK.title)
        if role_to_assign is not None:
            role_names_to_remove.discard(role_to_assign.name)
            if role_to_assign.name not in ['Unrated', 'Newbie', 'Pupil', 'Specialist', 'Expert']:
                role_names_to_remove.add('Shadow Realm')
        to_remove = [role for role in member.roles if role.name in role_names_to_remove]
        if to_remove:
            await member.remove_roles(*to_remove, reason=reason)
        if role_to_assign is not None and role_to_assign not in member.roles:
            await member.add_roles(role_to_assign, reason=reason)

    async def _update_ranks_all(self, guild):
        """For each member in the guild, fetches their current ratings and updates their role if
        required.
        """
        res = cf_common.user_db.get_handles_for_guild(guild.id)
        await self._update_ranks(guild, res)

    async def _update_ranks(self, guild, res):
        member_handles = [(guild.get_member(user_id), handle) for user_id, handle in res]
        member_handles = [(member, handle) for member, handle in member_handles if member is not None]
        if not member_handles:
            raise HandleCogError('Handles not set for any user')
        members, handles = zip(*member_handles)
        users = await cf.user.info(handles=handles)
        for user in users:
            rc = cf_common.user_db.cache_cf_user(user)
            if rc != 1:
                raise HandleCogError('DB update for user {user.handle} failed.')

        required_roles = {user.rank.title for user in users}
        rank2role = {role.name: role for role in guild.roles if role.name in required_roles}
        missing_roles = required_roles - rank2role.keys()
        if missing_roles:
            roles_str = ', '.join(f'`{role}`' for role in missing_roles)
            plural = 's' if len(missing_roles) > 1 else ''
            raise HandleCogError(f'Role{plural} for rank{plural} {roles_str} not present in the server')

        for member, user in zip(members, users):
            role_to_assign = rank2role[user.rank.title]
            await self.update_member_rank_role(member, role_to_assign,
                                               reason='Codeforces rank update')

    @staticmethod
    def _make_rankup_embeds(guild, contest, change_by_handle):
        """Make an embed containing a list of rank changes and top rating increases for the members
        of this guild.
        """
        user_id_handle_pairs = cf_common.user_db.get_handles_for_guild(guild.id)
        member_handle_pairs = [(guild.get_member(user_id), handle)
                               for user_id, handle in user_id_handle_pairs]
        def ispurg(member):
            # TODO: temporary code, todo properly later
            return any(role.name == 'Shadow Realm' for role in member.roles)

        member_change_pairs = [(member, change_by_handle[handle])
                               for member, handle in member_handle_pairs
                               if member is not None and handle in change_by_handle and not ispurg(member)]
        if not member_change_pairs:
            raise HandleCogError(f'Contest `{contest.id} | {contest.name}` was not rated for any '
                                 'member of this server.')

        member_change_pairs.sort(key=lambda pair: pair[1].newRating, reverse=True)
        rank_to_role = {role.name: role for role in guild.roles}

        def rating_to_displayable_rank(rating):
            rank = cf.rating2rank(rating).title
            role = rank_to_role.get(rank)
            return role.mention if role else rank

        rank_changes_str = []
        for member, change in member_change_pairs:
            cache = cf_common.cache2.rating_changes_cache
            if (change.oldRating == 1500
                    and len(cache.get_rating_changes_for_handle(change.handle)) == 1):
                # If this is the user's first rated contest.
                old_role = 'Unrated'
            else:
                old_role = rating_to_displayable_rank(change.oldRating)
            new_role = rating_to_displayable_rank(change.newRating)
            if new_role != old_role:
                rank_change_str = (f'{member.mention} [{change.handle}]({cf.PROFILE_BASE_URL}{change.handle}): {old_role} '
                                   f'\N{LONG RIGHTWARDS ARROW} {new_role}')
                rank_changes_str.append(rank_change_str)

        member_change_pairs.sort(key=lambda pair: pair[1].newRating - pair[1].oldRating,
                                 reverse=True)
        top_increases_str = []
        for member, change in member_change_pairs[:_TOP_DELTAS_COUNT]:
            delta = change.newRating - change.oldRating
            increase_str = (f'{member.mention} [{change.handle}]({cf.PROFILE_BASE_URL}{change.handle}): {change.oldRating} '
                            f'\N{HORIZONTAL BAR} **{delta:+}** \N{LONG RIGHTWARDS ARROW} '
                            f'{change.newRating}')
            top_increases_str.append(increase_str)

        rank_changes_str = rank_changes_str or ['No rank changes']

        embed_heading = discord.Embed(
            title=contest.name, url=contest.url, description="")
        embed_heading.set_author(name="Rank updates")
        embeds = [embed_heading]

        for rank_changes_chunk in paginator.chunkify(
                rank_changes_str, _MAX_RATING_CHANGES_PER_EMBED):
            desc = '\n'.join(rank_changes_chunk)
            embed = discord.Embed(description=desc)
            embeds.append(embed)

        top_rating_increases_embed = discord.Embed(description='\n'.join(
            top_increases_str) or 'Nobody got a delta :(')
        top_rating_increases_embed.set_author(name='Top rating changes')

        embeds.append(top_rating_increases_embed)
        discord_common.set_same_cf_color(embeds)

        return embeds

    @commands.group(brief='Commands for role updates',
                    invoke_without_command=True)
    async def roleupdate(self, ctx):
        """Group for commands involving role updates."""
        await ctx.send_help(ctx.command)

    @roleupdate.command(brief='Update Codeforces rank roles')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def now(self, ctx):
        """Updates Codeforces rank roles for every member in this server."""
        await self._update_ranks_all(ctx.guild)
        await ctx.send(embed=discord_common.embed_success('Roles updated successfully.'))

    @roleupdate.command(brief='Enable or disable auto role updates',
                        usage='on|off')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def auto(self, ctx, arg):
        """Auto role update refers to automatic updating of rank roles when rating
        changes are released on Codeforces. 'on'/'off' disables or enables auto role
        updates.
        """
        if arg == 'on':
            rc = cf_common.user_db.enable_auto_role_update(ctx.guild.id)
            if not rc:
                raise HandleCogError('Auto role update is already enabled.')
            await ctx.send(embed=discord_common.embed_success('Auto role updates enabled.'))
        elif arg == 'off':
            rc = cf_common.user_db.disable_auto_role_update(ctx.guild.id)
            if not rc:
                raise HandleCogError('Auto role update is already disabled.')
            await ctx.send(embed=discord_common.embed_success('Auto role updates disabled.'))
        else:
            raise ValueError(f"arg must be 'on' or 'off', got '{arg}' instead.")

    @roleupdate.command(brief='Publish a rank update for the given contest',
                        usage='here|off|contest_id')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def publish(self, ctx, arg):
        """This is a feature to publish a summary of rank changes and top rating
        increases in a particular contest for members of this server. 'here' will
        automatically publish the summary to this channel whenever rating changes on
        Codeforces are released. 'off' will disable auto publishing. Specifying a
        contest id will publish the summary immediately.
        """
        if arg == 'here':
            cf_common.user_db.set_rankup_channel(ctx.guild.id, ctx.channel.id)
            await ctx.send(
                embed=discord_common.embed_success('Auto rank update publishing enabled.'))
        elif arg == 'off':
            rc = cf_common.user_db.clear_rankup_channel(ctx.guild.id)
            if not rc:
                raise HandleCogError('Rank update publishing is already disabled.')
            await ctx.send(embed=discord_common.embed_success('Rank update publishing disabled.'))
        else:
            try:
                contest_id = int(arg)
            except ValueError:
                raise ValueError(f"arg must be 'here', 'off' or a contest ID, got '{arg}' instead.")
            await self._publish_now(ctx, contest_id)

    async def _publish_now(self, ctx, contest_id):
        try:
            contest = cf_common.cache2.contest_cache.get_contest(contest_id)
        except cache_system2.ContestNotFound as e:
            raise HandleCogError(f'Contest with id `{e.contest_id}` not found.')
        if contest.phase != 'FINISHED':
            raise HandleCogError(f'Contest `{contest_id} | {contest.name}` has not finished.')
        try:
            changes = await cf.contest.ratingChanges(contest_id=contest_id)
        except cf.RatingChangesUnavailableError:
            changes = None
        if not changes:
            raise HandleCogError(f'Rating changes are not available for contest `{contest_id} | '
                                 f'{contest.name}`.')

        change_by_handle = {change.handle: change for change in changes}
        rankup_embeds = self._make_rankup_embeds(ctx.guild, contest, change_by_handle)
        for rankup_embed in rankup_embeds:
            await ctx.channel.send(embed=rankup_embed)

    async def _generic_remind(self, ctx, action, role_name, what):
        roles = [role for role in ctx.guild.roles if role.name == role_name]
        if not roles:
            raise HandleCogError(f'Role `{role_name}` not present in the server')
        role = roles[0]
        if action == 'give':
            if role in ctx.author.roles:
                await ctx.send(embed=discord_common.embed_neutral(f'You are already subscribed to {what} reminders'))
                return
            await ctx.author.add_roles(role, reason=f'User subscribed to {what} reminders')
            await ctx.send(embed=discord_common.embed_success(f'Successfully subscribed to {what} reminders'))
        elif action == 'remove':
            if role not in ctx.author.roles:
                await ctx.send(embed=discord_common.embed_neutral(f'You are not subscribed to {what} reminders'))
                return
            await ctx.author.remove_roles(role, reason=f'User unsubscribed from {what} reminders')
            await ctx.send(embed=discord_common.embed_success(f'Successfully unsubscribed from {what} reminders'))
        else:
            raise HandleCogError(f'Invalid action {action}')

    @commands.command(brief='Grants or removes the specified pingable role',
                      usage='[give/remove] [vc/duel]')
    async def role(self, ctx, action: str, which: str):
        """e.g. ;role remove duel"""
        if which == 'vc':
            await self._generic_remind(ctx, action, 'Virtual Contestant', 'vc')
        elif which == 'duel':
            await self._generic_remind(ctx, action, 'Duelist', 'duel')
        else:
            raise HandleCogError(f'Invalid role {which}')
