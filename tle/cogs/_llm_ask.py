"""Shared request flow for the public ``;llm`` ask command."""
from datetime import datetime, timezone

from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, gemini_api, llm_models
from tle.cogs import _llm_context as llm_context
from tle.cogs import _llm_format as llm_format
from tle.cogs import _llm_pipeline as llm_pipeline


class LlmNotReadyError(commands.CommandError):
    """Raised while the bot database is not connected yet."""


def db():
    """Return the live user database, or name the startup race clearly."""
    database = cf_common.user_db
    if database is None:
        raise LlmNotReadyError(
            'The bot is still starting up and the database is not connected '
            'yet. Try again in a few seconds.')
    return database


def today():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


async def ask_gemini(cog, ctx, question):
    """Run the existing Gemini request path for one Discord context."""
    referenced = await cog._resolve_reference(ctx)
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

    pool = cog._get_pool()
    if pool.key_count() == 0:
        await ctx.send(embed=discord_common.embed_alert(
            'No Gemini API keys are configured. A moderator can add some '
            'with `;llm keys <key> [key ...]`.'))
        return

    models = [spec.model_id] if spec is not None else None
    attachments = llm_context.select_image_attachments(
        [referenced, ctx.message],
        constants.LLM_MAX_IMAGES, constants.LLM_MAX_IMAGE_BYTES,
        max_total_bytes=constants.LLM_MAX_TOTAL_IMAGE_BYTES)

    stats, failure, mode, window = {}, None, llm_context.MODE_DIRECT, []
    try:
        async with ctx.typing():
            mode = await llm_pipeline.classify(
                pool, question, referenced is not None,
                session=cog._get_session(), stats=stats,
                author_name=getattr(ctx.author, 'display_name', None),
                author_id=getattr(ctx.author, 'id', None),
                sent_at=getattr(ctx.message, 'created_at', None))
            window = await llm_pipeline.gather(
                ctx, mode, referenced, bot_user_id=cog._bot_user_id())
            prompt = llm_pipeline.build_prompt(question, referenced, window)
            images = await llm_context.read_images(attachments)
            answer, lease = await gemini_api.complete(
                pool, prompt, images=images,
                system_instruction=llm_context.SYSTEM_INSTRUCTION,
                max_output_tokens=constants.LLM_MAX_OUTPUT_TOKENS,
                session=cog._get_session(), stats=stats,
                models=models, tier=tier)
    except gemini_api.GeminiError as err:
        failure = err

    if failure is not None:
        if stats.get('attempts'):
            db().llm_bump_usage(ctx.guild.id, ctx.author.id, today())
        if isinstance(failure, gemini_api.ModelUnavailableError):
            cog.logger.error('Gemini model misconfigured: %s', failure)
        elif not isinstance(failure, (gemini_api.NoCapacityError,
                                      gemini_api.BlockedError,
                                      gemini_api.NoKeysError)):
            cog.logger.exception('Gemini request failed', exc_info=failure)
        await ctx.send(embed=discord_common.embed_alert(
            describe_gemini_failure(failure)))
        return

    db().llm_bump_usage(ctx.guild.id, ctx.author.id, today())
    tier_note = f'{lease.model} ({tier})' if tier else lease.model
    for embed in llm_format.build_answer_embeds(
            answer, tier_note, author=ctx.author,
            footer_extra=llm_pipeline.describe_mode(mode, window)):
        await ctx.send(embed=embed)


def describe_gemini_failure(err):
    """User-facing text for a failed request; never raw upstream HTML."""
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
