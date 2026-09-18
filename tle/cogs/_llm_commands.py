"""Discord command surface for the LLM cog."""
import logging
from datetime import datetime, timezone

import discord
from discord.ext import commands

from tle import constants
from tle.util import discord_common, llm_models
from tle.cogs import _llm_access as llm_access
from tle.cogs import _llm_ask as llm_ask
from tle.cogs import _llm_cooldown as llm_cooldown
from tle.cogs import _llm_format as llm_format
from tle.cogs import _llm_help as llm_help
from tle.cogs import _llm_limits as llm_limits
from tle.cogs import _llm_status as llm_status

logger = logging.getLogger(__name__)
_MIN_KEY_LENGTH = 20


class LlmCommandsMixin:
    """Public and owner-only prefix commands inherited by ``Llm``."""

    @commands.group(
        brief='Ask Gemini or Grok a question', invoke_without_command=True,
        aliases=('ai',), extras={
            'compact_help': llm_help.GROUP_HELP,
            'compact_command_help': llm_help.command_help,
        })
    async def llm(self, ctx, *, question: str = None):
        """Ask Gemini by default, or select Gemini/Grok explicitly."""
        await llm_ask.ask(self, ctx, _unwrap_quoted_request(question))

    @llm.command(brief='List selectable models and reasoning tiers')
    async def models(self, ctx):
        ladder = ', '.join(
            f'`{llm_format.safe_display(name)}`'
            for name in constants.LLM_MODELS)
        grok_ladder = ' → '.join(
            f'`{llm_format.safe_display(name)}`'
            for name in constants.XAI_MODELS)
        await ctx.send(embed=discord.Embed(
            title='Selectable models',
            description=(
                f'{llm_models.describe_catalog()}\n\n'
                f'{llm_models.describe_tiers()}\n\n'
                'Gemini: default, `+gemini`, or `@gemini <question>`.\n'
                f'Grok: `+grok` or `@grok <question>` uses '
                f'{grok_ladder}.\n\n'
                f'Prefix a question to pick one, e.g. '
                f'`;llm 3.5f-h why is this TLE?`\n'
                f'Left alone, the ladder is tried in order: {ladder}.\n\n'
                'Put an explicit provider first, then context/model controls: '
                '`;ai +gemini +context 3.5f question` or '
                '`;ai +grok +context question`.'),
            color=discord_common._ALERT_AMBER))

    @llm.command(brief='Add Gemini API keys (bot owner only)')
    async def keys(self, ctx, *api_keys: str):
        await self._add_provider_keys(ctx, api_keys, provider='gemini')

    @llm.command(brief='List stored Gemini keys (bot owner only)')
    async def keylist(self, ctx):
        if not await self._require_global_owner(ctx):
            return
        rows = self._llm_db().llm_get_keys(active_only=True)
        await ctx.send(embed=discord.Embed(
            title='Stored Gemini keys',
            description=llm_format.format_key_rows(rows),
            color=discord_common._ALERT_AMBER))

    @llm.command(brief='Forget a stored Gemini key (bot owner only)')
    async def keyforget(self, ctx, key_id: int):
        if await self._require_global_owner(ctx):
            await self._forget_provider_key(ctx, key_id, provider='gemini')

    @llm.command(brief='Show Gemini key-pool state (bot owner only)')
    async def keystatus(self, ctx):
        if not await self._require_global_owner(ctx):
            return
        description = llm_format.format_pool_status(self._get_pool().status())
        summary = self._llm_db().llm_provider_summary(
            'gemini', llm_ask.today())
        top = self._llm_db().llm_provider_top_users(
            'gemini', llm_ask.today())
        description += '\n\n' + llm_status.format_provider_summary(
            summary, top)
        await ctx.send(embed=discord.Embed(
            title='Gemini key pool', description=description,
            color=discord_common._ALERT_AMBER))

    @llm.command(brief='Show Grok health and spend (bot owner only)')
    async def grokstatus(self, ctx):
        if not await self._require_global_owner(ctx):
            return
        database = self._llm_db()
        health = llm_format.format_pool_status(
            self._get_xai_pool().status(), add_hint='XAI_API_KEY')
        summary = database.llm_provider_summary('xai', llm_ask.today())
        top = database.llm_provider_top_users('xai', llm_ask.today())
        report = llm_status.format_provider_summary(
            summary, top, show_cost=True)
        ledger = llm_status.format_xai_ledger(
            database.llm_xai_daily_summary())
        await ctx.send(embed=discord.Embed(
            title='Grok provider health',
            description=f'{health}\n\n{report}\n{ledger}',
            color=discord_common._ALERT_AMBER))

    @llm.command(brief='Reset today\'s Grok limits (admin/mod only)')
    async def grokreset(self, ctx):
        if not await self._require_guild_moderator(ctx):
            return
        cleared = self._llm_db().llm_reset_xai_daily_limits()
        logger.warning(
            'Grok daily limits reset by user=%s guild=%s; cleared=%s',
            ctx.author.id, ctx.guild.id, cleared)
        await ctx.send(embed=discord_common.embed_success(
            'Grok usage limits were reset bot-wide for the current UTC day. '
            'Provider telemetry was kept.'))

    @llm.command(brief='Reset provider health circuits (bot owner only)')
    async def healthreset(self, ctx, provider: str, key_id: int = None,
                          model: str = None):
        if not await self._require_global_owner(ctx):
            return
        provider = provider.casefold()
        if provider in ('grok', 'xai'):
            pool, label = self._get_xai_pool(), 'Grok'
        elif provider == 'gemini':
            pool, label = self._get_pool(), 'Gemini'
        else:
            await ctx.send(embed=discord_common.embed_alert(
                'Provider must be `gemini` or `grok`.'))
            return
        cleared = pool.reset_health(key_id=key_id, model=model)
        note = (' Persisted daily Gemini quota state was left intact.'
                if provider == 'gemini' else '')
        await ctx.send(embed=discord_common.embed_success(
            f'Reset {cleared} reversible {label} health state entries.' + note))

    @llm.command(brief='Show or set LLM context privacy')
    async def privacy(self, ctx, mode: str = None, scope: str = 'guild'):
        """Use ``;llm privacy <auto|explicit|off> [guild|channel]``."""
        if mode is None:
            policy, source = self._context_policy(ctx, with_source=True)
            await ctx.send(embed=discord_common.embed_neutral(
                f'Context policy: `{policy}` ({source}).\n'
                '`auto` may select recent chat, `explicit` requires '
                '`+context` or a reply, and `off` sends no surrounding chat.'))
            return
        mode, scope = mode.casefold(), scope.casefold()
        if mode not in ('auto', 'explicit', 'off', 'inherit'):
            await ctx.send(embed=discord_common.embed_alert(
                'Mode must be `auto`, `explicit`, `off`, or `inherit`.'))
            return
        if scope not in ('guild', 'channel'):
            await ctx.send(embed=discord_common.embed_alert(
                'Scope must be `guild` or `channel`.'))
            return
        if mode == 'inherit' and scope != 'channel':
            await ctx.send(embed=discord_common.embed_alert(
                '`inherit` only applies to a channel override.'))
            return
        if not self._is_privileged(ctx.author):
            await ctx.send(embed=discord_common.embed_alert(
                'Only this guild’s admins or moderators can change privacy.'))
            return
        self._set_context_policy(ctx, mode, scope=scope)
        await ctx.send(embed=discord_common.embed_success(
            f'LLM context policy for this {scope} is now `{mode}`.'))

    @llm.command(brief='Ban a user from LLM requests', usage='@user|user_id')
    async def ban(self, ctx, target: str = None):
        if not await self._require_guild_moderator(ctx):
            return
        identity = llm_access.user_target(ctx, target)
        if identity is None:
            await ctx.send(embed=discord_common.embed_alert(
                'Usage: `;llm ban @user` or `;llm ban <user_id>`.'))
            return
        user_id, label = identity
        added = self._llm_db().llm_ban_user(
            ctx.guild.id, user_id, banned_by=ctx.author.id)
        if added:
            message = f'`{label}` can no longer use LLM requests here.'
            embed = discord_common.embed_success(message)
        else:
            embed = discord_common.embed_neutral(
                f'`{label}` is already banned from LLM requests here.')
        await ctx.send(embed=embed)

    @llm.command(brief='Unban a user from LLM requests', usage='@user|user_id')
    async def unban(self, ctx, target: str = None):
        if not await self._require_guild_moderator(ctx):
            return
        identity = llm_access.user_target(ctx, target)
        if identity is None:
            await ctx.send(embed=discord_common.embed_alert(
                'Usage: `;llm unban @user` or `;llm unban <user_id>`.'))
            return
        user_id, label = identity
        removed = self._llm_db().llm_unban_user(ctx.guild.id, user_id)
        if removed:
            embed = discord_common.embed_success(
                f'`{label}` can use LLM requests here again.')
        else:
            embed = discord_common.embed_neutral(
                f'`{label}` is not banned from LLM requests here.')
        await ctx.send(embed=embed)

    @llm.command(brief='Show users banned from LLM requests')
    async def banlist(self, ctx):
        if not await self._require_guild_moderator(ctx):
            return
        rows = self._llm_db().llm_get_banned_users(ctx.guild.id)
        if not rows:
            await ctx.send(embed=discord_common.embed_neutral(
                'No users are banned from LLM requests in this server.'))
            return
        getter = getattr(ctx.guild, 'get_member', None)
        lines = []
        for row in rows[:50]:
            member = getter(int(row.user_id)) if getter is not None else None
            label = (llm_access.member_label(member) if member is not None
                     else f'User {row.user_id}')
            lines.append(f'`{label}` — `{row.user_id}`')
        if len(rows) > 50:
            lines.append(f'*…and {len(rows) - 50} more.*')
        await ctx.send(embed=discord.Embed(
            title='LLM request ban list', description='\n'.join(lines),
            color=discord_common._ALERT_AMBER))

    @llm.command(brief='Disable LLM requests by server or local scope')
    async def disable(self, ctx, *arguments: str):
        await self._set_llm_disabled(ctx, arguments, disabled=True)

    @llm.command(brief='Enable LLM requests by server or local scope')
    async def enable(self, ctx, *arguments: str):
        await self._set_llm_disabled(ctx, arguments, disabled=False)

    @llm.command(brief='Set an exact, channel-family, or server cooldown')
    async def cooldown(self, ctx, *arguments: str):
        await llm_cooldown.configure(self, ctx, arguments)

    @llm.command(brief='Set the regular-user Grok allowance',
                 aliases=('groklimit',))
    async def ratelimit(self, ctx, *arguments: str):
        await llm_limits.configure(self, ctx, arguments)

    @llm.command(brief='Add xAI API keys (bot owner only)',
                 aliases=('xkeys', 'xaikeys'))
    async def grokkeys(self, ctx, *api_keys: str):
        await self._add_provider_keys(ctx, api_keys, provider='xai')

    @llm.command(brief='List stored xAI keys (bot owner only)',
                 aliases=('xkeylist',))
    async def grokkeylist(self, ctx):
        if not await self._require_global_owner(ctx):
            return
        rows = self._llm_db().llm_get_keys(active_only=True, provider='xai')
        await ctx.send(embed=discord.Embed(
            title='Stored xAI keys',
            description=llm_format.format_key_rows(rows),
            color=discord_common._ALERT_AMBER))

    @llm.command(brief='Forget a stored xAI key (bot owner only)',
                 aliases=('xkeyforget',))
    async def grokkeyforget(self, ctx, key_id: int):
        if await self._require_global_owner(ctx):
            await self._forget_provider_key(ctx, key_id, provider='xai')

    def _llm_db(self):
        return llm_ask.db()

    async def _require_guild_moderator(self, ctx):
        if self._is_privileged(ctx.author):
            return True
        if self.bot is not None:
            try:
                if await self.bot.is_owner(ctx.author):
                    return True
            except Exception:  # noqa: BLE001 - fail closed
                logger.exception('Could not verify LLM access-control owner')
        await ctx.send(embed=discord_common.embed_alert(
            'Only this guild’s admins or moderators can manage LLM access.'))
        return False

    async def _set_llm_disabled(self, ctx, arguments, *, disabled):
        if not await self._require_guild_moderator(ctx):
            return
        resolved = llm_access.access_scope(arguments)
        action = 'disable' if disabled else 'enable'
        if resolved is None:
            await ctx.send(embed=discord_common.embed_alert(
                f'Usage: `;llm {action}`, `;llm {action} here`, or '
                f'`;llm {action} here +threads`.'))
            return

        channel_id = llm_access.scope_channel_id(ctx.channel)
        family_id = llm_access.family_channel_id(ctx.channel)
        database = self._llm_db()
        llm_access.set_disabled(
            database, ctx.guild.id, channel_id, family_id,
            disabled=disabled, scope=resolved)

        state = 'disabled' if disabled else 'enabled'
        if resolved == 'guild':
            message = (f'LLM requests are now {state} for every channel and '
                       'thread in this server. Previous local overrides were '
                       'cleared.')
        elif resolved == 'family':
            message = (f'LLM requests are now {state} for this channel and '
                       'all of its threads. Exact local overrides in this '
                       'channel family were cleared.')
        else:
            local_label = ('thread' if llm_access.is_thread_channel(ctx.channel)
                           else 'channel')
            message = f'LLM requests are now {state} for this {local_label}.'
        await ctx.send(embed=discord_common.embed_success(message))

    async def _require_global_owner(self, ctx, *, deleted=None,
                                    has_secret=False):
        # ``bot is None`` occurs only in isolated unit tests. A loaded cog has a
        # bot and always takes the strict owner path.
        allowed = self.bot is None
        if self.bot is not None:
            try:
                allowed = await self.bot.is_owner(ctx.author)
            except Exception:  # noqa: BLE001 - fail closed
                logger.exception('Could not verify LLM credential owner')
                allowed = False
        if allowed:
            return True

        warning = 'Only the bot owner can manage the global LLM credentials.'
        if has_secret:
            if deleted:
                warning += (' I deleted the message, but rotate that key if '
                            'anyone may have seen it.')
            else:
                warning += (' I could not delete the message—delete it now '
                            'and rotate the key.')
        await ctx.send(embed=discord_common.embed_alert(warning))
        return False

    async def _add_provider_keys(self, ctx, api_keys, *, provider):
        deleted = await _delete_quietly(ctx.message)
        has_secret = any(looks_like_api_key(value) for value in api_keys)
        if not await self._require_global_owner(
                ctx, deleted=deleted, has_secret=has_secret):
            return
        command = 'grokkeys' if provider == 'xai' else 'keys'
        if not api_keys:
            await ctx.send(embed=discord_common.embed_alert(
                f'Usage: `;llm {command} <key1> [key2 ...]`'))
            return

        fields = ('added', 'reactivated', 'duplicate', 'provider_conflict',
                  'wrong_provider', 'rejected')
        counts = {field: 0 for field in fields}
        for raw_key in api_keys:
            api_key = _strip_wrapping(raw_key)
            if len(api_key) < _MIN_KEY_LENGTH:
                counts['rejected'] += 1
                continue
            valid = (_is_xai_key(api_key) if provider == 'xai'
                     else _is_gemini_key(api_key))
            if not valid:
                other = (_is_gemini_key(api_key) if provider == 'xai'
                         else _is_xai_key(api_key))
                counts['wrong_provider' if other else 'rejected'] += 1
                continue
            label = (f'owner-{provider}-{ctx.author.id}-'
                     f'{datetime.now(timezone.utc):%Y%m%d}')
            result = self._llm_db().llm_add_key(
                api_key, label=label, guild_id=ctx.guild.id,
                added_by=ctx.author.id, provider=provider)
            counts[result] += 1

        pool = self._get_xai_pool() if provider == 'xai' else self._get_pool()
        pool.reload()
        noun = 'xAI key(s)' if provider == 'xai' else 'key(s)'
        parts = [f"{counts['added']} {noun} added"]
        wrong_provider = ('not shaped like an xAI key'
                          if provider == 'xai'
                          else 'shaped for xAI; use `;llm grokkeys`')
        labels = (
            ('reactivated', 'reactivated'),
            ('duplicate', 'already stored'),
            ('provider_conflict', 'assigned to the other provider; rotate it'),
            ('wrong_provider', wrong_provider),
            ('rejected', 'rejected as too short or invalid'),
        )
        parts.extend(f'{counts[field]} {label}' for field, label in labels
                     if counts[field])
        summary = ', '.join(parts) + '.'
        if not deleted:
            summary += ('\n\n\N{WARNING SIGN} I could not delete your message—'
                        'delete it yourself and rotate those keys.')
        logger.info('LLM %s keys updated by owner %s: %s',
                    provider, ctx.author.id, counts)
        await ctx.send(embed=discord_common.embed_success(summary))

    async def _forget_provider_key(self, ctx, key_id, *, provider):
        if self._llm_db().llm_forget_key(key_id, provider=provider):
            pool = self._get_xai_pool() if provider == 'xai' else self._get_pool()
            pool.reload()
            await ctx.send(embed=discord_common.embed_success(
                f'{provider} key #{key_id} removed from the active pool. '
                'Revoke it at the provider too; older backups may retain it.'))
        else:
            label = 'xAI key' if provider == 'xai' else 'key'
            await ctx.send(embed=discord_common.embed_alert(
                f'No active {label} #{key_id}.'))


def _strip_wrapping(token):
    return (token or '').strip().strip('`<>').strip()


def _unwrap_quoted_request(question):
    """Remove one whole-message quote pair used to escape subcommand names."""
    if question is None:
        return None
    text = question.strip()
    quote_pairs = {'"': '"', "'": "'", '“': '”', '‘': '’'}
    closing = quote_pairs.get(text[:1])
    if closing is not None and len(text) >= 2 and text.endswith(closing):
        return text[1:-1].strip()
    return question


def _is_xai_key(token):
    return (token or '').startswith('xai-')


def _is_gemini_key(token):
    return (token or '').startswith('AIza')


def looks_like_api_key(token):
    value = _strip_wrapping(token)
    return (len(value) >= _MIN_KEY_LENGTH
            and all(char.isalnum() or char in '_-' for char in value))


async def _delete_quietly(message):
    try:
        await message.delete()
        return True
    except Exception:  # noqa: BLE001 - missing permission/already deleted
        logger.warning('Could not delete an ;llm API-key message')
        return False


llm_help.apply_metadata(LlmCommandsMixin.llm)
