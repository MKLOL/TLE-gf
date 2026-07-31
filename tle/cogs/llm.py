"""``;llm`` — ask Google Gemini a question, with channel context when needed.

There is deliberately no per-user cap or cooldown: the shared free-tier
allowance is the only limit. Calls are still *counted* per user so `;llm
keystatus` can show moderators who is consuming it.

Key management is mod-gated and self-redacting: the command that adds keys
deletes the invoking message, and no code path in this cog ever prints key
material back to a channel or a log line.
"""
import asyncio
import logging
import re
from datetime import datetime, timezone

import aiohttp
import discord
from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, gemini_api, llm_models
from tle.util.llm_keypool import KeyPool
from tle.cogs import _llm_context as llm_context
from tle.cogs import _llm_format as llm_format
from tle.cogs import _llm_pipeline as llm_pipeline

logger = logging.getLogger(__name__)

# A key that does not even look like a credential is almost certainly a typo
# or a stray word, and should not be stored.
_MIN_KEY_LENGTH = 20
# API keys are long runs of URL-safe characters. Ordinary English words are
# not, which lets `;llm keys are overrated` be told apart from a real paste.
_KEY_SHAPED = re.compile(r'^[A-Za-z0-9_\-]{%d,}$' % _MIN_KEY_LENGTH)


class LlmNotReadyError(commands.CommandError):
    """The database is not connected yet.

    ``cf_common.initialize`` fetches Codeforces data *before* it assigns
    ``user_db``, and the bot accepts commands throughout — so for the first
    seconds after a restart ``cf_common.user_db`` is None. Every command in
    this cog needs it, so the window is worth naming rather than raising
    AttributeError from three frames deep.
    """


def _db():
    """The user database, or raise if the bot has not finished starting.

    Always looked up fresh: caching the value risks capturing the ``None``
    that exists during startup and holding it for the life of the process.
    """
    database = cf_common.user_db
    if database is None:
        raise LlmNotReadyError(
            'The bot is still starting up and the database is not connected '
            'yet. Try again in a few seconds.')
    return database


def looks_like_api_key(token):
    """True if a message token could plausibly be a live credential."""
    return bool(_KEY_SHAPED.match(_strip_wrapping(token)))


def _strip_wrapping(token):
    """Drop the backticks/angle brackets people wrap secrets in."""
    return (token or '').strip().strip('`<>').strip()


class Llm(commands.Cog):
    """Gemini-backed question answering with a rotating pool of API keys."""

    def __init__(self, bot):
        self.bot = bot
        self._pool = None
        self._session = None
        self._bootstrapped = False

    def cog_unload(self):
        if self._session is None or self._session.closed:
            return
        try:
            asyncio.get_running_loop().create_task(self._session.close())
        except RuntimeError:
            # Unloaded outside the event loop (shutdown, tests) — nothing to
            # schedule the close on. Leaking a closed-anyway session at exit
            # beats raising from cog_unload.
            logger.warning('No running loop at cog_unload; aiohttp session '
                           'left for interpreter shutdown to reap')

    # ── Lazy setup ──────────────────────────────────────────────────────
    # The database is not connected when cogs load (cf_common.initialize runs
    # on_ready), so the pool is built on first use rather than in __init__.

    def _get_pool(self):
        database = _db()
        # Rebuild if the connection object changed under us, so a pool built
        # against a stale (or absent) database is never reused.
        if self._pool is None or self._pool.db is not database:
            self._pool = KeyPool(database, constants.LLM_MODELS)
            self._bootstrap_env_keys()
            self._pool.reload()
        return self._pool

    def _bootstrap_env_keys(self):
        """Merge any keys supplied via GEMINI_API_KEYS into the database."""
        if self._bootstrapped:
            return
        self._bootstrapped = True
        raw = (constants.GEMINI_API_KEYS or '').strip()
        if not raw:
            return
        added = 0
        for index, key in enumerate(part.strip() for part in raw.split(',')):
            if len(key) < _MIN_KEY_LENGTH:
                continue
            if _db().llm_add_key(key, label=f'env-{index + 1}') != 'duplicate':
                added += 1
        if added:
            logger.info('Loaded %d Gemini key(s) from GEMINI_API_KEYS', added)

    def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    # ── Gating ──────────────────────────────────────────────────────────

    async def cog_command_error(self, ctx, error):
        """Turn this cog's expected failures into replies, not tracebacks.

        Two cases. First, the bot not being ready: every command here needs the
        database, and it is None for the first seconds after a restart.

        Second, a failed permission check on the key commands. Without this, a
        non-moderator running ``;llm keys <key>`` gets nothing: the role check
        fails *before* the command body runs, so the message is never deleted,
        and ``MissingAnyRole`` is a ``CheckFailure`` that the global handler
        only logs. Someone who did not realise they lack the role would paste a
        live key and watch nothing happen.

        ``;llm keys`` also swallows ordinary sentences — "keys", "keylist" and
        "keystatus" are English words — so a message with no key-shaped token
        gets an explanation instead of a deletion.
        """
        cause = getattr(error, 'original', error)
        if isinstance(cause, LlmNotReadyError):
            error.handled = True
            await ctx.send(embed=discord_common.embed_alert(str(cause)))
            return

        if not isinstance(error, commands.MissingAnyRole):
            return
        error.handled = True

        content = getattr(ctx.message, 'content', '') or ''
        secrets = [token for token in content.split() if looks_like_api_key(token)]

        if not secrets:
            await ctx.send(embed=discord_common.embed_alert(
                'That reads as the moderator-only key command, not a question. '
                'Rephrase it — e.g. `;llm what are API keys?`'))
            return

        deleted = await _delete_quietly(ctx.message)
        logger.warning('Non-moderator %s attempted ;llm keys in guild %s '
                       '(deleted=%s)', ctx.author.id, ctx.guild.id, deleted)
        warning = ('\N{WARNING SIGN} Only moderators can add API keys — and '
                   'that message looked like it contained one.')
        warning += ('\n\nI deleted it, but treat the key as compromised and '
                    'revoke it.' if deleted else
                    '\n\n**I could not delete it — delete it yourself and '
                    'revoke that key now.**')
        await ctx.send(embed=discord_common.embed_alert(warning))

    @staticmethod
    def _is_privileged(member):
        return any(role.name in (constants.TLE_ADMIN, constants.TLE_MODERATOR)
                   for role in getattr(member, 'roles', []))

    # ── Main command ────────────────────────────────────────────────────

    @commands.group(brief='Ask Gemini a question', invoke_without_command=True)
    async def llm(self, ctx, *, question: str = None):
        """Ask an LLM a question.

        Reply to a message with `;llm` to ask about that message; attached
        images are sent along, so a screenshot works. Questions about the
        ongoing conversation pull in recent channel history automatically.

        Start the question with a model name to pick one, optionally with a
        reasoning tier — `;llm models` lists them.

        Usage:
          ;llm <question>          — ask anything
          ;llm                     — (as a reply) explain that message
          ;llm 3.5f <question>     — pick the model
          ;llm 3.5f-h <question>   — and the reasoning tier
        """
        referenced = await self._resolve_reference(ctx)
        if question is None and referenced is None:
            await ctx.send_help(ctx.command)
            return

        try:
            spec, tier, question = llm_models.split_selector(question)
        except ValueError as err:
            await ctx.send(embed=discord_common.embed_alert(str(err)))
            return
        if spec is not None and not question and referenced is None:
            await ctx.send(embed=discord_common.embed_alert(
                f'`{spec.aliases[0]}` selected, but no question followed it.'))
            return

        if question and len(question) > constants.LLM_MAX_PROMPT_CHARS:
            await ctx.send(embed=discord_common.embed_alert(
                f'Question too long (max {constants.LLM_MAX_PROMPT_CHARS} characters).'))
            return

        pool = self._get_pool()
        if pool.key_count() == 0:
            await ctx.send(embed=discord_common.embed_alert(
                'No Gemini API keys are configured. A moderator can add some '
                'with `;llm keys <key> [key ...]`.'))
            return

        models = [spec.model_id] if spec is not None else None

        stats, failure, mode, window = {}, None, llm_context.MODE_DIRECT, []
        try:
            async with ctx.typing():
                mode = await llm_pipeline.classify(
                    pool, question, referenced is not None,
                    session=self._get_session(), stats=stats,
                    author_name=getattr(ctx.author, 'display_name', None),
                    author_id=getattr(ctx.author, 'id', None),
                    sent_at=getattr(ctx.message, 'created_at', None))
                window = await llm_pipeline.gather(
                    ctx, mode, referenced, bot_user_id=self._bot_user_id())
                prompt = llm_pipeline.build_prompt(
                    question, referenced, window, mode=mode)
                image_messages = [referenced]
                image_messages += [message for message in window
                                   if message is not referenced and
                                   message is not ctx.message]
                image_messages.append(ctx.message)
                attachments = llm_context.select_image_attachments(
                    image_messages,
                    constants.LLM_MAX_IMAGES, constants.LLM_MAX_IMAGE_BYTES,
                    max_total_bytes=constants.LLM_MAX_TOTAL_IMAGE_BYTES)
                images = await llm_context.read_images(attachments)
                answer, lease = await gemini_api.complete(
                    pool, prompt, images=images,
                    system_instruction=llm_context.SYSTEM_INSTRUCTION,
                    max_output_tokens=constants.LLM_MAX_OUTPUT_TOKENS,
                    session=self._get_session(), stats=stats,
                    models=models, tier=tier,
                    tools=[{'url_context': {}}, {'google_search': {}}])
        except gemini_api.GeminiError as err:
            failure = err

        if failure is not None:
            # Usage is recorded, not enforced — but it should still reflect
            # requests that actually reached Google, so `;llm keystatus` shows
            # true consumption rather than only successful calls.
            if stats.get('attempts'):
                _db().llm_bump_usage(
                    ctx.guild.id, ctx.author.id, _today())
            if isinstance(failure, (gemini_api.ModelUnavailableError,)):
                logger.error('Gemini model misconfigured: %s', failure)
            elif not isinstance(failure, (gemini_api.NoCapacityError,
                                          gemini_api.BlockedError,
                                          gemini_api.NoKeysError)):
                logger.exception('Gemini request failed', exc_info=failure)
            await ctx.send(embed=discord_common.embed_alert(
                self._describe_failure(failure)))
            return

        _db().llm_bump_usage(ctx.guild.id, ctx.author.id, _today())
        tier_note = f'{lease.model} ({tier})' if tier else lease.model
        for embed in llm_format.build_answer_embeds(
                answer, tier_note, author=ctx.author,
                footer_extra=llm_pipeline.describe_mode(mode, window)):
            await ctx.send(embed=embed)

    def _bot_user_id(self):
        """The bot's own user id, so its answers stay out of the transcript."""
        user = getattr(self.bot, 'user', None)
        return getattr(user, 'id', None)

    @staticmethod
    def _describe_failure(err):
        """User-facing text for a failed request — never raw upstream HTML."""
        if isinstance(err, gemini_api.NoCapacityError):
            if err.attempts_exhausted:
                return ('Gemini failed on every key I tried. Give it a moment '
                        'and ask again.')
            if err.retry_after:
                return (f'All Gemini keys are out of quota right now. Try again '
                        f'in {llm_format.format_duration(err.retry_after)}.')
            return 'All Gemini keys are out of quota right now. Try again later.'
        if isinstance(err, gemini_api.BlockedError):
            return str(err)
        if isinstance(err, gemini_api.ModelUnavailableError):
            return ('The configured Gemini model is unavailable. A moderator '
                    'should check `LLM_MODELS`.')
        if isinstance(err, gemini_api.NoKeysError):
            return ('No Gemini API keys are configured. A moderator can add '
                    'some with `;llm keys <key> [key ...]`.')
        return f'Gemini request failed: {gemini_api.truncate_error(err)}'

    @staticmethod
    async def _resolve_reference(ctx):
        """The message this command is replying to, or None."""
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
        except Exception:  # noqa: BLE001 — deleted or inaccessible message
            return None

    @llm.command(brief='List selectable models and reasoning tiers')
    async def models(self, ctx):
        """Show the models you can put in front of a question.

        Usage:
          ;llm 3.5f <question>
          ;llm 3.5f-h <question>   — pick the reasoning tier too
        """
        ladder = ', '.join(f'`{name}`' for name in constants.LLM_MODELS)
        await ctx.send(embed=discord.Embed(
            title='Selectable models',
            description=(
                f'{llm_models.describe_catalog()}\n\n'
                f'{llm_models.describe_tiers()}\n\n'
                f'Prefix a question to pick one, e.g. '
                f'`;llm 3.5f-h why is this TLE?`\n'
                f'Left alone, the ladder is tried in order: {ladder}.'),
            color=discord_common._ALERT_AMBER))

    # ── Key management (moderators only) ────────────────────────────────

    @llm.command(brief='Add Gemini API keys (mod only)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def keys(self, ctx, *api_keys: str):
        """Store one or more Gemini API keys for the bot to rotate through.

        The invoking message is deleted immediately so the keys do not linger
        in channel history. Keys are still visible to anyone watching the
        channel as you send them and to Discord itself — prefer setting
        `GEMINI_API_KEYS` in the environment where you can.

        Usage:
          ;llm keys <key1> [key2 ...]
        """
        deleted = await _delete_quietly(ctx.message)

        if not api_keys:
            await ctx.send(embed=discord_common.embed_alert(
                'Usage: `;llm keys <key1> [key2 ...]`'))
            return

        counts = {'added': 0, 'reactivated': 0, 'duplicate': 0, 'rejected': 0}
        for api_key in api_keys:
            api_key = _strip_wrapping(api_key)
            if len(api_key) < _MIN_KEY_LENGTH:
                counts['rejected'] += 1
                continue
            label = f'{ctx.author.display_name}-{datetime.now(timezone.utc):%Y%m%d}'
            counts[_db().llm_add_key(
                api_key, label=label, guild_id=ctx.guild.id,
                added_by=ctx.author.id)] += 1

        self._get_pool().reload()

        parts = [f"{counts['added']} key(s) added"]
        if counts['reactivated']:
            parts.append(f"{counts['reactivated']} reactivated")
        if counts['duplicate']:
            parts.append(f"{counts['duplicate']} already stored")
        if counts['rejected']:
            parts.append(f"{counts['rejected']} rejected as too short")
        summary = ', '.join(parts) + '.'
        if not deleted:
            summary += ('\n\n\N{WARNING SIGN} I could not delete your message — '
                        'delete it yourself, and rotate those keys if anyone saw it.')
        logger.info('LLM keys updated by %s in guild %s: %s',
                    ctx.author.id, ctx.guild.id, counts)
        await ctx.send(embed=discord_common.embed_success(summary))

    @llm.command(brief='List stored API keys, redacted (mod only)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def keylist(self, ctx):
        """Show which keys are stored. Values are always redacted."""
        rows = _db().llm_get_keys(active_only=True)
        await ctx.send(embed=discord.Embed(
            title='Stored Gemini keys',
            description=llm_format.format_key_rows(rows),
            color=discord_common._ALERT_AMBER))

    @llm.command(brief='Forget a stored API key (mod only)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def keyforget(self, ctx, key_id: int):
        """Remove a key from the pool by its id (see `;llm keylist`)."""
        if _db().llm_forget_key(key_id):
            self._get_pool().reload()
            await ctx.send(embed=discord_common.embed_success(
                f'Key #{key_id} removed from the pool.'))
        else:
            await ctx.send(embed=discord_common.embed_alert(
                f'No active key #{key_id}.'))

    @llm.command(brief='Show per-key quota state (mod only)')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def keystatus(self, ctx):
        """Show which key × model buckets are ready, cooling, or spent.

        Also shows who has been consuming today's shared allowance. Nothing is
        capped per user, so this is the only visibility into that.
        """
        pool = self._get_pool()
        description = llm_format.format_pool_status(pool.status())
        top = _db().llm_top_users(ctx.guild.id, _today())
        description += '\n\n' + llm_format.format_usage(top)
        await ctx.send(embed=discord.Embed(
            title='Gemini key pool',
            description=description,
            color=discord_common._ALERT_AMBER))


def _today():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


async def _delete_quietly(message):
    """Best-effort delete. Returns True if the message is gone."""
    try:
        await message.delete()
        return True
    except Exception:  # noqa: BLE001 — missing permission, already deleted, DM
        logger.warning('Could not delete a ;llm keys message')
        return False


async def setup(bot):
    await bot.add_cog(Llm(bot))
