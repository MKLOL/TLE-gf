"""Discord command surface for the LLM cog."""
import logging
from datetime import datetime, timezone

import discord
from discord.ext import commands

from tle import constants
from tle.util import discord_common, llm_models
from tle.cogs import _llm_ask as llm_ask
from tle.cogs import _llm_format as llm_format

logger = logging.getLogger(__name__)
_MIN_KEY_LENGTH = 20


class LlmCommandsMixin:
    """Public and owner-only prefix commands inherited by ``Llm``."""

    @commands.group(brief='Ask Gemini or Grok a question',
                    invoke_without_command=True)
    async def llm(self, ctx, *, question: str = None):
        """Ask Gemini, or use a leading ``+grok`` to ask Grok."""
        await llm_ask.ask(self, ctx, question)

    @llm.command(brief='List selectable models and reasoning tiers')
    async def models(self, ctx):
        ladder = ', '.join(f'`{name}`' for name in constants.LLM_MODELS)
        await ctx.send(embed=discord.Embed(
            title='Selectable models',
            description=(
                f'{llm_models.describe_catalog()}\n\n'
                f'{llm_models.describe_tiers()}\n\n'
                f'Grok: `+grok` uses `{constants.XAI_MODEL}` and is also '
                f'available as `@grok <question>`.\n\n'
                f'Prefix a question to pick one, e.g. '
                f'`;llm 3.5f-h why is this TLE?`\n'
                f'Left alone, the ladder is tried in order: {ladder}.'),
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
        top = self._llm_db().llm_top_users(ctx.guild.id, llm_ask.today())
        description += '\n\n' + llm_format.format_usage(top)
        await ctx.send(embed=discord.Embed(
            title='Gemini key pool', description=description,
            color=discord_common._ALERT_AMBER))

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
            if _is_xai_key(api_key) != (provider == 'xai'):
                counts['wrong_provider'] += 1
                continue
            label = (f'{provider}-owner-{ctx.author.id}-'
                     f'{datetime.now(timezone.utc):%Y%m%d}')
            result = self._llm_db().llm_add_key(
                api_key, label=label, guild_id=ctx.guild.id,
                added_by=ctx.author.id, provider=provider)
            counts[result] += 1

        pool = self._get_xai_pool() if provider == 'xai' else self._get_pool()
        pool.reload()
        noun = 'xAI key(s)' if provider == 'xai' else 'key(s)'
        parts = [f"{counts['added']} {noun} added"]
        labels = (
            ('reactivated', 'reactivated'),
            ('duplicate', 'already stored'),
            ('provider_conflict', 'assigned to the other provider; rotate it'),
            ('wrong_provider', 'shaped for the other provider'),
            ('rejected', 'rejected as too short'),
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
                f'{provider} key #{key_id} securely removed from the pool.'))
        else:
            label = 'xAI key' if provider == 'xai' else 'key'
            await ctx.send(embed=discord_common.embed_alert(
                f'No active {label} #{key_id}.'))


def _strip_wrapping(token):
    return (token or '').strip().strip('`<>').strip()


def _is_xai_key(token):
    return (token or '').startswith('xai-')


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
