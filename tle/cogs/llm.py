"""``;llm`` — Gemini and Grok with bounded Discord context."""
import asyncio
import logging
import re
import time
from collections import namedtuple
from datetime import datetime, timedelta, timezone

import aiohttp
import discord
from discord.ext import commands

from tle import constants
from tle.util import discord_common, xai_api
from tle.util.llm_keypool import KeyPool
from tle.cogs import _llm_ask as llm_ask
from tle.cogs._llm_commands import (
    LlmCommandsMixin, _delete_quietly, _is_gemini_key, _is_xai_key,
    looks_like_api_key,
)
from tle.cogs._llm_runtime import RequestRuntime

logger = logging.getLogger(__name__)
_GROK_TRIGGER = re.compile(r'^\s*@grok(?:\s+|$)', re.IGNORECASE)
_MIN_KEY_LENGTH = 20
_EnvironmentKey = namedtuple('EnvironmentKey', 'id api_key label')

# Compatibility exports used by extensions/tests.
LlmNotReadyError = llm_ask.LlmNotReadyError
_db = llm_ask.db
_today = llm_ask.today


class Llm(LlmCommandsMixin, commands.Cog):
    """Provider pools/lifecycle; command decorators live in the mixin."""

    def __init__(self, bot):
        self.bot = bot
        self.logger = logger
        self._pool = None
        self._xai_pool = None
        self._session = None
        self._maintained_db = None
        self._runtime = RequestRuntime(
            {'gemini': constants.LLM_GEMINI_CONCURRENCY,
             'xai': constants.LLM_XAI_CONCURRENCY},
            queue_timeout=constants.LLM_QUEUE_TIMEOUT_SECONDS,
            request_timeout=constants.LLM_REQUEST_TIMEOUT_SECONDS)

    def cog_unload(self):
        if self._session is None or self._session.closed:
            return
        try:
            asyncio.get_running_loop().create_task(self._session.close())
        except RuntimeError:
            logger.warning('No running loop at cog_unload; aiohttp session '
                           'left for interpreter shutdown to reap')

    async def cog_check(self, ctx):
        """Keep global provider credentials unavailable in direct messages."""
        if getattr(ctx, 'guild', None) is None:
            error = getattr(commands, 'NoPrivateMessage',
                            commands.CheckFailure)
            raise error()
        return True

    def _get_pool(self):
        database = _db()
        if self._pool is None or self._pool.db is not database:
            environment = self._environment_keys(
                constants.GEMINI_API_KEYS, provider='gemini')
            self._erase_persisted_environment_keys(
                database, environment, provider='gemini')
            self._pool = KeyPool(
                database, constants.LLM_MODELS,
                ephemeral_keys=environment)
            self._maintain_llm_state(database)
        return self._pool

    def _get_xai_pool(self):
        database = _db()
        if (self._xai_pool is None or self._xai_pool.db is not database
                or self._xai_pool.models != list(constants.XAI_MODELS)):
            environment = self._environment_keys(
                constants.XAI_API_KEYS, provider='xai')
            self._erase_persisted_environment_keys(
                database, environment, provider='xai')
            self._xai_pool = xai_api.XaiKeyPool(
                database, constants.XAI_MODELS,
                ephemeral_keys=environment)
            self._maintain_llm_state(database)
        return self._xai_pool

    @staticmethod
    def _environment_keys(raw, *, provider):
        """Build stable process-only rows without persisting secret material."""
        rows = []
        base = -2000 if provider == 'xai' else -1000
        for index, key in enumerate(
                part.strip() for part in (raw or '').split(',')):
            valid_provider = (_is_xai_key(key) if provider == 'xai'
                              else _is_gemini_key(key))
            if len(key) < _MIN_KEY_LENGTH or not valid_provider:
                continue
            rows.append(_EnvironmentKey(
                base - index, key, f'{provider}-environment-{index + 1}'))
        if rows:
            logger.info('Loaded %d process-only %s key(s)', len(rows), provider)
        return rows

    @staticmethod
    def _erase_persisted_environment_keys(database, environment, *, provider):
        """Remove copies written by older releases before keys became ephemeral."""
        material = {row.api_key for row in environment}
        if not material:
            return
        for row in database.llm_get_keys(active_only=True, provider=provider):
            if row.api_key in material:
                database.llm_forget_key(row.id, provider=provider)
                logger.info('Erased legacy persisted copy of an environment '
                            '%s key id=%s', provider, row.id)

    def _maintain_llm_state(self, database):
        if self._maintained_db is database:
            return
        self._maintained_db = database
        database.llm_purge_expired_buckets(now=time.time())
        cutoff = time.time() - constants.LLM_TELEMETRY_RETENTION_DAYS * 86400
        database.llm_purge_request_usage(cutoff)
        before_day = (datetime.now(timezone.utc) - timedelta(
            days=constants.LLM_TELEMETRY_RETENTION_DAYS)).strftime('%Y-%m-%d')
        database.llm_purge_old_usage(before_day)

    @staticmethod
    def _context_config_key(ctx, scope):
        if scope == 'channel':
            return f'llm_context_channel:{ctx.channel.id}'
        return 'llm_context'

    def _context_policy(self, ctx, *, with_source=False):
        """Resolve channel override, guild policy, then environment default."""
        database = _db()
        getter = getattr(database, 'get_guild_config', None)
        channel_id = getattr(getattr(ctx, 'channel', None), 'id', None)
        candidates = []
        if channel_id is not None:
            candidates.append((f'llm_context_channel:{channel_id}',
                               'channel override'))
        candidates.append((self._context_config_key(ctx, 'guild'),
                           'guild setting'))
        if getter is not None:
            for key, source in candidates:
                value = getter(ctx.guild.id, key)
                if value in ('auto', 'explicit', 'off'):
                    return (value, source) if with_source else value
        default = 'auto' if constants.LLM_CONTEXT_ENABLED else 'off'
        return (default, 'environment default') if with_source else default

    def _set_context_policy(self, ctx, mode, *, scope):
        key = self._context_config_key(ctx, scope)
        if mode == 'inherit':
            _db().delete_guild_config(ctx.guild.id, key)
        else:
            _db().set_guild_config(ctx.guild.id, key, mode)

    def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def cog_command_error(self, ctx, error):
        """Handle startup and legacy role-check failures without secret logs."""
        cause = getattr(error, 'original', error)
        if isinstance(cause, LlmNotReadyError):
            error.handled = True
            await ctx.send(embed=discord_common.embed_alert(str(cause)))
            return
        if not isinstance(error, commands.MissingAnyRole):
            return
        error.handled = True
        content = getattr(ctx.message, 'content', '') or ''
        secrets = [word for word in content.split()
                   if looks_like_api_key(word)]
        if not secrets:
            await ctx.send(embed=discord_common.embed_alert(
                'That reads as an owner-only key command, not a question. '
                'Rephrase it, e.g. `;llm what are API keys?`'))
            return
        deleted = await _delete_quietly(ctx.message)
        warning = ('Local moderators cannot manage global API keys; only the '
                   'bot owner can. I deleted the message; revoke that key.'
                   if deleted else
                   'Local moderators cannot manage global keys; only the bot '
                   'owner can. I could not delete it—delete it yourself and '
                   'revoke that key now.')
        await ctx.send(embed=discord_common.embed_alert(warning))

    @staticmethod
    def _is_privileged(member):
        return any(role.name in (constants.TLE_ADMIN, constants.TLE_MODERATOR)
                   for role in getattr(member, 'roles', []))

    @commands.Cog.listener()
    async def on_message(self, message):
        """Handle the requested literal ``@grok <text>`` trigger."""
        if (getattr(message, 'guild', None) is None
                or getattr(getattr(message, 'author', None), 'bot', False)):
            return
        match = _GROK_TRIGGER.match(getattr(message, 'content', '') or '')
        if match is None:
            return
        ctx = await self.bot.get_context(message)
        question = message.content[match.end():].strip() or None
        try:
            can_run = getattr(self.bot, 'can_run', None)
            if can_run is not None and not await can_run(ctx):
                return
            await llm_ask.ask_grok(self, ctx, question)
        except discord_common.FeatureDisabledSilent:
            return
        except commands.CheckFailure:
            return
        except LlmNotReadyError as err:
            await ctx.send(embed=discord_common.embed_alert(str(err)))
        except Exception:  # noqa: BLE001 - listeners have no command handler
            logger.exception('Unhandled @grok listener failure')
            await ctx.send(embed=discord_common.embed_alert(
                'Grok hit an unexpected error. Try again shortly.'))

    def _bot_user_id(self):
        user = getattr(self.bot, 'user', None)
        return getattr(user, 'id', None)

    @staticmethod
    def _describe_failure(err):
        return llm_ask.describe_gemini_failure(err)

    @staticmethod
    async def _resolve_reference(ctx):
        reference = getattr(ctx.message, 'reference', None)
        if reference is None:
            return None
        resolved = getattr(reference, 'resolved', None)
        if isinstance(resolved, discord.Message):
            return resolved
        message_id = getattr(reference, 'message_id', None)
        if message_id is None:
            return None
        try:
            return await ctx.channel.fetch_message(message_id)
        except Exception:  # noqa: BLE001 - deleted/inaccessible message
            return None


async def setup(bot):
    await bot.add_cog(Llm(bot))
