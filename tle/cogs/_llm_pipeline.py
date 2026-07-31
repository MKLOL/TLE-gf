"""Two-stage request pipeline for ``;llm``: route, then answer.

Stage one resolves replies and obvious history requests locally, then asks a
cheap model only for genuinely ambiguous questions. Stage two collects the
chosen conversation window and builds the final prompt. Keeping this out of
the cog leaves the cog to commands and Discord I/O.

When needed, Gemini routing is charged to the *cheapest* model in the ladder;
Grok routes through xAI with reasoning disabled and a tiny output cap.
"""
import logging

from tle import constants
from tle.util import gemini_api, llm_models, xai_api
from tle.cogs import _llm_context as llm_context
from tle.cogs import _llm_history as llm_history

logger = logging.getLogger(__name__)

# Deliberately roomy. Reasoning tokens are drawn from the same budget as the
# answer, so a tight cap here (this was 16) is spent thinking and returns an
# empty response — which classify() then reads as a failure and downgrades to
# `direct`, silently disabling channel context for every question ever asked.
# Thinking is set to the model's lowest tier as well, so the cap is slack
# rather than load-bearing.
_CLASSIFIER_MAX_TOKENS = 512
_GROK_CLASSIFIER_MAX_TOKENS = 32

# Force a valid label instead of hoping for one bare word. Same approach as
# MKLOL/TLE-gf#10, which uses responseSchema on its classifier.
_CLASSIFIER_SCHEMA = {
    'type': 'STRING',
    'enum': [llm_context.MODE_DIRECT, llm_context.MODE_CONTEXT],
}


def _local_choice(question, is_reply, has_current_images):
    """Resolve structural/high-confidence routes before paying a provider."""
    hint = llm_context.local_mode_hint(
        question, is_reply=is_reply,
        has_current_images=has_current_images)
    if hint == llm_context.MODE_REPLY_CHAIN:
        return hint
    if not constants.LLM_CONTEXT_ENABLED:
        return llm_context.MODE_DIRECT
    return hint


async def classify(pool, question, is_reply, session=None, stats=None,
                   author_name=None, author_id=None, sent_at=None,
                   has_current_images=False):
    """Decide whether this question needs channel history.

    Falls back to ``direct`` on any failure: routing is an optimisation, and
    losing it should degrade the answer, not block it. A quota failure here is
    deliberately swallowed so the answer call still gets its chance.
    """
    local = _local_choice(question, is_reply, has_current_images)
    if local is not None:
        logger.info(';llm routed locally to %s (is_reply=%s)', local, is_reply)
        return local

    # LLM_MODELS is ordered cheapest-first, so the router takes the head of the
    # ladder. (This read `[-1:]` — the last entry, i.e. the most expensive
    # model — which billed every routing call at the top rate.)
    cheapest = pool.models[:1] if pool.models else None
    try:
        raw, _ = await gemini_api.complete(
            pool,
            llm_context.build_classifier_prompt(
                question, is_reply, author_name=author_name,
                author_id=author_id, sent_at=sent_at,
                has_current_images=has_current_images),
            system_instruction=llm_context.CLASSIFIER_INSTRUCTION,
            max_output_tokens=_CLASSIFIER_MAX_TOKENS,
            temperature=0,
            session=session,
            models=cheapest,
            stats=stats,
            max_attempts=2,
            tier=llm_models.LEAST,
            response_mime_type='application/json',
            response_schema=_CLASSIFIER_SCHEMA)
    except gemini_api.GeminiError as err:
        # Logged at WARNING, not INFO: a router that always fails looks exactly
        # like a bot that never uses context, and the previous INFO line was
        # invisible at the default log level.
        logger.warning(';llm router failed (%s) — answering without context',
                       err)
        return llm_context.MODE_DIRECT

    mode = llm_context.parse_mode(raw, is_reply)
    logger.info(';llm routed to %s (raw=%r, is_reply=%s)', mode, raw, is_reply)
    return mode


async def classify_grok(pool, question, is_reply, session=None, stats=None,
                        author_name=None, author_id=None, sent_at=None,
                        has_current_images=False):
    """xAI-backed equivalent of :func:`classify` for the Grok route."""
    local = _local_choice(question, is_reply, has_current_images)
    if local is not None:
        logger.info('@grok routed locally to %s (is_reply=%s)', local, is_reply)
        return local
    try:
        raw, _ = await xai_api.complete(
            pool,
            llm_context.build_classifier_prompt(
                question, is_reply, author_name=author_name,
                author_id=author_id, sent_at=sent_at,
                has_current_images=has_current_images),
            system_instruction=llm_context.CLASSIFIER_INSTRUCTION,
            max_output_tokens=_GROK_CLASSIFIER_MAX_TOKENS,
            temperature=0,
            reasoning_effort='none',
            session=session,
            stats=stats,
            max_attempts=2)
    except xai_api.XaiError as err:
        logger.warning('@grok router failed (%s) — answering without context',
                       err)
        return llm_context.MODE_DIRECT

    mode = llm_context.parse_mode(raw, is_reply)
    logger.info('@grok routed to %s (raw=%r, is_reply=%s)', mode, raw, is_reply)
    return mode


async def gather(ctx, mode, referenced, bot_user_id=None):
    """Collect the message window a mode calls for.

    A reply is selected structurally before provider routing and always gets
    its surrounding exchange. Reading history costs Discord I/O and bounded
    input tokens, not another inference call.
    """
    if referenced is not None:
        window = await llm_history.collect_reply_window(
            ctx.channel, referenced,
            before_count=constants.LLM_REPLY_BEFORE,
            after_count=constants.LLM_REPLY_AFTER,
            window_seconds=constants.LLM_CONTEXT_WINDOW_SECONDS,
            bot_user_id=bot_user_id, until=ctx.message)
    elif mode == llm_context.MODE_CONTEXT:
        window = await llm_history.collect_recent(
            ctx.channel, before=ctx.message,
            limit=constants.LLM_CONTEXT_MESSAGES,
            window_seconds=constants.LLM_CONTEXT_WINDOW_SECONDS,
            bot_user_id=bot_user_id)
    else:
        return []

    if not window:
        logger.warning(';llm gathered no context for mode=%s (is_reply=%s) — '
                       'check Read Message History and '
                       'LLM_CONTEXT_WINDOW_SECONDS',
                       mode, referenced is not None)
    else:
        logger.info(';llm gathered %d message(s) for mode=%s',
                    len(window), mode)
    return window


def build_prompt(question, referenced, window):
    """Final prompt for the answer call.

    Three shapes, cheapest context first: a bare question, a quoted single
    message, or a transcript window.
    """
    ref_author = getattr(getattr(referenced, 'author', None), 'display_name', None)
    ref_content = getattr(referenced, 'content', None)

    if window:
        transcript = llm_history.format_transcript(window, focus=referenced)
        if transcript.strip():
            return llm_context.build_context_prompt(
                question, transcript, is_reply=referenced is not None,
                ref_author=ref_author, ref_content=ref_content)

    if referenced is not None:
        return llm_context.build_reply_prompt(
            question, ref_author=ref_author, ref_content=ref_content,
            ref_has_attachments=bool(getattr(referenced, 'attachments', None)))

    return llm_context.build_question_prompt(question)


def describe_mode(mode, window):
    """Short footer note about what context was used, or None."""
    if not window:
        return None
    return f'{len(window)} messages of context'
