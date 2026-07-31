"""``;llm`` — Gemini and Grok with bounded Discord context."""
import asyncio
import logging
import re

import aiohttp
import discord
from discord.ext import commands

from tle import constants
from tle.util import discord_common, xai_api
from tle.util.llm_keypool import KeyPool
from tle.cogs import _llm_ask as llm_ask
from tle.cogs._llm_commands import (
    LlmCommandsMixin, _delete_quietly, _is_xai_key, looks_like_api_key,
)

logger = logging.getLogger(__name__)
_GROK_TRIGGER = re.compile(r'^\s*@grok(?:\s+|$)', re.IGNORECASE)
_MIN_KEY_LENGTH = 20

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
        self._bootstrapped = False
        self._xai_bootstrapped = False

    def cog_unload(self):
        if self._session is None or self._session.closed:
            return
        try:
            asyncio.get_running_loop().create_task(self._session.close())
        except RuntimeError:
            logger.warning('No running loop at cog_unload; aiohttp session '
                           'left for interpreter shutdown to reap')

    def _get_pool(self):
        database = _db()
        if self._pool is None or self._pool.db is not database:
            self._pool = KeyPool(database, constants.LLM_MODELS)
            self._bootstrap_env_keys()
            self._pool.reload()
        return self._pool

    def _get_xai_pool(self):
        database = _db()
        if (self._xai_pool is None or self._xai_pool.db is not database
                or self._xai_pool.model != constants.XAI_MODEL):
            self._xai_pool = xai_api.XaiKeyPool(database, constants.XAI_MODEL)
            self._bootstrap_xai_env_keys()
            self._xai_pool.reload()
        return self._xai_pool

    def _bootstrap_env_keys(self):
        """Import environment Gemini keys once (made memory-only later)."""
        if self._bootstrapped:
            return
        self._bootstrapped = True
        added = 0
        for index, key in enumerate(
                part.strip() for part in constants.GEMINI_API_KEYS.split(',')):
            if len(key) < _MIN_KEY_LENGTH or _is_xai_key(key):
                continue
            result = _db().llm_add_key(
                key, label=f'env-{index + 1}', provider='gemini')
            added += result in ('added', 'reactivated')
        if added:
            logger.info('Loaded %d Gemini key(s) from GEMINI_API_KEYS', added)

    def _bootstrap_xai_env_keys(self):
        """Import environment xAI keys once (made memory-only later)."""
        if self._xai_bootstrapped:
            return
        self._xai_bootstrapped = True
        added = 0
        for index, key in enumerate(
                part.strip() for part in constants.XAI_API_KEYS.split(',')):
            if len(key) < _MIN_KEY_LENGTH or not _is_xai_key(key):
                continue
            result = _db().llm_add_key(
                key, label=f'xai-env-{index + 1}', provider='xai')
            added += result in ('added', 'reactivated')
        if added:
            logger.info('Loaded %d xAI key(s) from XAI_API_KEY(S)', added)

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
            await llm_ask.ask_grok(self, ctx, question)
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
