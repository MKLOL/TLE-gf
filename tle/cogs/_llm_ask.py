"""Shared guarded request flow for commands and literal provider triggers."""
from datetime import datetime, timezone
import logging
import secrets
import time

from discord.ext import commands

from tle import constants
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, gemini_api, llm_models, xai_api
from tle.cogs import _llm_access as llm_access
from tle.cogs import _llm_accounting as accounting
from tle.cogs import _llm_context as llm_context
from tle.cogs import _llm_entrypoints as llm_entrypoints
from tle.cogs import _llm_format as llm_format
from tle.cogs import _llm_history as llm_history
from tle.cogs import _llm_identity as llm_identity
from tle.cogs import _llm_limits as llm_limits
from tle.cogs import _llm_pipeline as llm_pipeline
from tle.cogs import _llm_profiles as llm_profiles
from tle.cogs._llm_failures import (
    describe_gemini_failure, describe_xai_failure,
)
from tle.cogs._llm_runtime import (
    ProviderQueueError, RequestBusyError, RequestDeadlineError,
)

logger = logging.getLogger(__name__)
split_provider = llm_entrypoints.split_provider


class LlmNotReadyError(commands.CommandError):
    """Raised while the bot database is not connected yet."""


class _ContextDisabledError(Exception):
    pass


def db():
    database = cf_common.user_db
    if database is None:
        raise LlmNotReadyError(
            'The bot is still starting up and the database is not connected '
            'yet. Try again in a few seconds.')
    return database


def today():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


async def ask(cog, ctx, question):
    if getattr(ctx, 'guild', None) is None:
        await ctx.send(embed=discord_common.embed_alert(
            'LLM requests are only available inside a server.'))
        return
    provider, question, explicit = llm_entrypoints.parse_provider(question)
    if provider == 'grok':
        await ask_grok(cog, ctx, question)
    else:
        await ask_gemini(cog, ctx, question, explicit=explicit)


async def ask_gemini(cog, ctx, question, *, explicit=False):
    if not await llm_access.allow_request_or_notify(db(), ctx):
        return
    referenced = await cog._resolve_reference(ctx)
    controls = llm_context.parse_context_controls(
        question, max_messages=constants.LLM_CONTEXT_MESSAGES)
    if controls.error:
        await ctx.send(embed=discord_common.embed_alert(controls.error))
        return
    question = controls.question
    if (question is None and referenced is None
            and controls.mode != llm_context.MODE_CONTEXT):
        if explicit:
            await ctx.send(embed=discord_common.embed_alert(
                llm_entrypoints.usage('gemini')))
        else:
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
    if not await _valid_question(question, ctx):
        return
    question = llm_history.redact_secrets(question) if question else None

    pool = cog._get_pool()
    if pool.key_count() == 0:
        await ctx.send(embed=discord_common.embed_alert(
            'No Gemini API keys are configured. The bot owner should set '
            '`GEMINI_API_KEYS` and restart, or use `;llm keys` in a private '
            'owner-only channel.'))
        return
    configured_models = list(constants.LLM_MODELS)
    models = [spec.model_id] if spec else None
    if not configured_models and spec is None:
        await ctx.send(embed=discord_common.embed_alert(
            'No Gemini models are enabled in `LLM_MODELS`.'))
        return
    if _context_forbidden(cog, ctx, controls):
        await _send_context_disabled(ctx)
        return

    attachments = llm_context.select_image_attachments(
        [referenced, ctx.message], constants.LLM_MAX_IMAGES,
        constants.LLM_MAX_IMAGE_BYTES,
        max_total_bytes=constants.LLM_MAX_TOTAL_IMAGE_BYTES)
    router_stats, answer_stats = (
        accounting.new_stage_stats(), accounting.new_stage_stats())
    started = time.monotonic()
    mode, window, lease = llm_context.MODE_DIRECT, [], None

    async def operation():
        nonlocal mode, window, lease
        llm_access.raise_if_request_blocked(db(), ctx)
        mode, window, explicit = await _prepare_context(
            cog, ctx, 'gemini', pool, question, referenced, attachments,
            controls, router_stats)
        prompt = llm_pipeline.build_prompt(
            question, referenced, window, mode=mode)
        answer_attachments = _answer_attachments(
            referenced, ctx.message, window)
        images = await llm_context.read_images(answer_attachments)
        answer, lease = await gemini_api.complete(
            pool, prompt, images=images,
            system_instruction=llm_context.SYSTEM_INSTRUCTION,
            max_output_tokens=constants.LLM_MAX_OUTPUT_TOKENS,
            session=cog._get_session(), stats=answer_stats,
            models=models, tier=tier, tools=[{'url_context': {}}])
        return answer, explicit

    try:
        async with ctx.typing():
            answer, explicit = await cog._runtime.run(
                'gemini', ctx.author.id, operation)
    except llm_access.LlmAccessDeniedError as err:
        await ctx.send(embed=discord_common.embed_alert(str(err)))
        return
    except (RequestBusyError, ProviderQueueError, RequestDeadlineError) as err:
        _record(cog, ctx, 'gemini', _runtime_outcome(err), started,
                _primary_model(models, configured_models), router_stats,
                answer_stats, mode, window)
        await _handle_runtime_error(ctx, err)
        return
    except gemini_api.GeminiError as err:
        _record(cog, ctx, 'gemini', 'failed', started,
                _primary_model(models, configured_models),
                router_stats, answer_stats, mode, window)
        if isinstance(err, gemini_api.ModelUnavailableError):
            cog.logger.error('Gemini model ladder unavailable: %s', err)
        elif not isinstance(err, (gemini_api.NoCapacityError,
                                  gemini_api.BlockedError,
                                  gemini_api.NoKeysError)):
            cog.logger.error('Gemini request failed', exc_info=True)
        await ctx.send(embed=discord_common.embed_alert(
            describe_gemini_failure(err)))
        return
    except Exception:  # noqa: BLE001 - guarantee a user-visible failure
        await _unexpected(cog, ctx, 'gemini', started, router_stats,
                          answer_stats, mode, window,
                          _primary_model(models, configured_models))
        return

    _record(cog, ctx, 'gemini', 'success', started, lease.model,
            router_stats, answer_stats, mode, window)
    tier_note = f'{lease.model} ({tier})' if tier else lease.model
    footer = llm_pipeline.describe_mode(
        mode, window, explicit=explicit,
        has_reference=referenced is not None)
    embeds = llm_format.build_answer_embeds(
        answer, tier_note, author=ctx.author, footer_extra=footer)
    await _send_answer_embeds(ctx, embeds)


async def ask_grok(cog, ctx, question):
    if not await llm_access.allow_request_or_notify(db(), ctx):
        return
    referenced = await cog._resolve_reference(ctx)
    controls = llm_context.parse_context_controls(
        question, max_messages=constants.LLM_CONTEXT_MESSAGES)
    if controls.error:
        await ctx.send(embed=discord_common.embed_alert(controls.error))
        return
    question = controls.question
    if (question is None and referenced is None
            and controls.mode != llm_context.MODE_CONTEXT):
        await ctx.send(embed=discord_common.embed_alert(
            llm_entrypoints.usage('grok')))
        return
    if not await _valid_question(question, ctx):
        return
    question = llm_history.redact_secrets(question) if question else None
    if _context_forbidden(cog, ctx, controls):
        await _send_context_disabled(ctx)
        return

    pool = cog._get_xai_pool()
    if pool.key_count() == 0:
        await ctx.send(embed=discord_common.embed_alert(
            'No xAI API keys are configured. The bot owner should set '
            '`XAI_API_KEY` and restart, or use `;llm grokkeys` in a private '
            'owner-only channel.'))
        return
    attachments = llm_context.select_image_attachments(
        [referenced, ctx.message], constants.LLM_MAX_IMAGES,
        constants.LLM_MAX_IMAGE_BYTES,
        max_total_bytes=constants.LLM_MAX_TOTAL_IMAGE_BYTES,
        mime_check=xai_api.is_supported_image)
    router_stats, answer_stats = (
        accounting.new_stage_stats(), accounting.new_stage_stats())
    started = time.monotonic()
    mode, window, lease = llm_context.MODE_DIRECT, [], None
    reservation_id = None

    async def operation():
        nonlocal mode, window, lease, reservation_id
        llm_access.raise_if_request_blocked(db(), ctx)
        user_rate = llm_limits.resolve(db(), ctx.guild.id)
        reservation_id = db().llm_reserve_xai_request(
            ctx.author.id, user_limit=max(1, user_rate.requests),
            window_seconds=user_rate.window_seconds,
            daily_limit=constants.XAI_DAILY_REQUEST_LIMIT,
            guild_id=ctx.guild.id, model=constants.XAI_MODELS[0],
            reserved_microusd=accounting.xai_reservation_microusd(),
            daily_budget_microusd=accounting.daily_budget_microusd(),
            return_id=True,
            enforce_user_limit=(
                user_rate.enabled and not cog._is_privileged(ctx.author)))
        if isinstance(reservation_id, str):
            raise llm_limits.GrokGuardError(
                str(reservation_id),
                getattr(reservation_id, 'retry_at', None), user_rate)
        mode, window, explicit = await _prepare_context(
            cog, ctx, 'xai', pool, question, referenced, attachments,
            controls, router_stats)
        profiles = llm_profiles.build_profiles(
            db(), ctx.guild.id, ctx.author, [referenced, *window],
            focused=referenced)
        routing = ''
        requester_name = str(
            getattr(ctx.author, 'display_name', '') or '')
        if (referenced is not None or window
                or requester_name.casefold() == 'fffff'):
            routing = llm_identity.build_request_routing(
                ctx.author, ctx.message, referenced)
        prompt = llm_pipeline.build_prompt(
            question, referenced, window, mode=mode, profiles=profiles,
            routing=routing, requester_id=ctx.author.id)
        images = await llm_context.read_images(attachments)
        answer, lease = await xai_api.complete(
            pool, prompt, images=images,
            system_instruction=llm_context.GROK_SYSTEM_INSTRUCTION,
            max_output_tokens=constants.XAI_MAX_OUTPUT_TOKENS,
            reasoning_effort='low', session=cog._get_session(),
            stats=answer_stats, models=constants.XAI_MODELS)
        return answer, explicit

    try:
        async with ctx.typing():
            answer, explicit = await cog._runtime.run(
                'xai', ctx.author.id, operation)
    except llm_access.LlmAccessDeniedError as err:
        await ctx.send(embed=discord_common.embed_alert(str(err)))
        return
    except llm_limits.GrokGuardError as err:
        _record(cog, ctx, 'xai', 'guarded', started,
                constants.XAI_MODELS[0], router_stats, answer_stats,
                mode, window)
        await ctx.send(embed=discord_common.embed_alert(
            llm_limits.guard_message(err)))
        return
    except (RequestBusyError, ProviderQueueError, RequestDeadlineError) as err:
        await _finalize_xai(reservation_id, router_stats, answer_stats,
                            outcome='timeout')
        cost = accounting.xai_cost_microusd(router_stats, answer_stats)
        _record(cog, ctx, 'xai', _runtime_outcome(err), started,
                getattr(lease, 'model', None) or constants.XAI_MODELS[0],
                router_stats, answer_stats, mode, window, cost=cost)
        await _handle_runtime_error(ctx, err)
        return
    except xai_api.XaiError as err:
        cost = accounting.xai_cost_microusd(router_stats, answer_stats)
        await _finalize_xai(reservation_id, router_stats, answer_stats,
                            outcome='failed', model=getattr(lease, 'model', None))
        _record(cog, ctx, 'xai', 'failed', started,
                getattr(lease, 'model', None) or constants.XAI_MODELS[0],
                router_stats, answer_stats, mode, window, cost=cost)
        if isinstance(err, xai_api.ModelUnavailableError):
            cog.logger.error('xAI model ladder unavailable: %s', err)
        elif not isinstance(err, (xai_api.NoKeysError, xai_api.RateLimitError,
                                  xai_api.AccessDeniedError,
                                  xai_api.AuthenticationError,
                                  xai_api.NoCapacityError,
                                  xai_api.BlockedError)):
            cog.logger.error('xAI request failed', exc_info=True)
        await ctx.send(embed=discord_common.embed_alert(
            describe_xai_failure(err)))
        return
    except Exception:  # noqa: BLE001 - guarantee a user-visible failure
        await _finalize_xai(reservation_id, router_stats, answer_stats,
                            outcome='unexpected')
        await _unexpected(cog, ctx, 'xai', started, router_stats,
                          answer_stats, mode, window,
                          constants.XAI_MODELS[0])
        return

    cost = accounting.xai_cost_microusd(router_stats, answer_stats)
    await _finalize_xai(reservation_id, router_stats, answer_stats,
                        outcome='success', model=lease.model)
    _record(cog, ctx, 'xai', 'success', started, lease.model,
            router_stats, answer_stats, mode, window, cost=cost)
    footer = llm_pipeline.describe_mode(
        mode, window, explicit=explicit,
        has_reference=referenced is not None)
    embeds = llm_format.build_answer_embeds(
        answer, lease.model, author=ctx.author, footer_extra=footer)
    await _send_answer_embeds(ctx, embeds)


async def _send_answer_embeds(ctx, embeds):
    """Reply with the first answer page; send later pages without references."""
    for index, embed in enumerate(embeds):
        if index == 0:
            await ctx.send(
                embed=embed, reference=ctx.message, mention_author=False)
        else:
            await ctx.send(embed=embed)


async def _prepare_context(cog, ctx, provider, pool, question, referenced,
                           attachments, controls, router_stats):
    policy = cog._context_policy(ctx)
    explicit = controls.mode is not None or policy != 'auto'
    if policy == 'off':
        mode, force_direct = llm_context.MODE_DIRECT, True
    elif controls.mode is not None:
        mode = llm_context.apply_mode_override(
            llm_context.MODE_DIRECT, controls, is_reply=referenced is not None)
        force_direct = controls.mode == llm_context.MODE_DIRECT
    elif policy == 'explicit':
        mode = (llm_context.MODE_REPLY_CHAIN if referenced is not None
                else llm_context.MODE_DIRECT)
        force_direct = referenced is None
    else:
        classifier = (llm_pipeline.classify_grok if provider == 'xai'
                      else llm_pipeline.classify)
        mode = await classifier(
            pool, question, referenced is not None,
            session=cog._get_session(), stats=router_stats,
            author_name=getattr(ctx.author, 'display_name', None),
            author_id=getattr(ctx.author, 'id', None),
            sent_at=getattr(ctx.message, 'created_at', None),
            has_current_images=bool(attachments))
        force_direct = False
    window = await llm_pipeline.gather(
        ctx, mode, referenced, bot_user_id=cog._bot_user_id(),
        message_limit=controls.message_limit, force_direct=force_direct)
    return mode, window, explicit


def _answer_attachments(referenced, current, window):
    messages = [referenced, current]
    messages.extend(message for message in window
                    if message is not referenced and message is not current)
    return llm_context.select_image_attachments(
        messages, constants.LLM_MAX_IMAGES, constants.LLM_MAX_IMAGE_BYTES,
        max_total_bytes=constants.LLM_MAX_TOTAL_IMAGE_BYTES)


def _context_forbidden(cog, ctx, controls):
    return (cog._context_policy(ctx) == 'off'
            and controls.mode == llm_context.MODE_CONTEXT)


def _primary_model(models, configured):
    candidates = models if models is not None else configured
    return candidates[0] if candidates else None


async def _send_context_disabled(ctx):
    await ctx.send(embed=discord_common.embed_alert(
        'Channel-history forwarding is disabled here. Ask without `+context` '
        'or have a guild moderator change `;llm privacy`.'))


async def _valid_question(question, ctx):
    if question and len(question) > constants.LLM_MAX_PROMPT_CHARS:
        await ctx.send(embed=discord_common.embed_alert(
            f'Question too long (max '
            f'{constants.LLM_MAX_PROMPT_CHARS} characters).'))
        return False
    return True


async def _handle_runtime_error(ctx, error):
    if isinstance(error, RequestBusyError):
        text = 'You already have an LLM request running. Let it finish first.'
    elif isinstance(error, ProviderQueueError):
        text = 'The LLM queue is busy right now. Try again shortly.'
    else:
        text = 'The LLM request timed out before it could finish. Try again.'
    await ctx.send(embed=discord_common.embed_alert(text))


async def _finalize_xai(reservation_id, router_stats, answer_stats, *,
                        outcome, model=None):
    if not isinstance(reservation_id, int):
        return
    cost = accounting.xai_cost_microusd(router_stats, answer_stats)
    actual = (cost if accounting.has_xai_cost_observation(
        router_stats, answer_stats) else None)
    try:
        db().llm_finalize_xai_request(
            reservation_id, actual_microusd=actual,
            outcome=outcome, model=model)
    except Exception:
        logger.exception('Could not finalize xAI reservation id=%s',
                         reservation_id)


def _runtime_outcome(error):
    if isinstance(error, RequestBusyError):
        return 'busy'
    if isinstance(error, ProviderQueueError):
        return 'queue_timeout'
    return 'deadline'


def _record(cog, ctx, provider, outcome, started, model, router_stats,
            answer_stats, mode, window, *, cost=0):
    try:
        accounting.record(
            db(), ctx, provider, outcome, started, model=model,
            router_stats=router_stats, answer_stats=answer_stats,
            mode=mode, window=window, cost_microusd=cost)
    except Exception:
        cog.logger.exception('Could not record %s LLM telemetry', provider)


async def _unexpected(cog, ctx, provider, started, router_stats,
                      answer_stats, mode, window, model):
    trace_id = secrets.token_hex(4)
    _record(cog, ctx, provider, 'unexpected', started, model,
            router_stats, answer_stats, mode, window)
    cog.logger.exception('Unexpected %s LLM failure (reference %s)',
                         provider, trace_id)
    await ctx.send(embed=discord_common.embed_alert(
        f'The LLM hit an unexpected error. Reference `{trace_id}`.'))
