"""Two-stage request pipeline for ``;llm``: route, then answer.

Stage one asks a cheap model whether the question needs the surrounding
conversation. Stage two collects that conversation if so, and builds the final
prompt. Keeping this out of the cog leaves the cog to commands and Discord I/O.

The routing call is charged to the *cheapest* model in the ladder regardless of
which model the user picked for the answer, and capped to a handful of output
tokens — the shared free-tier quota should not be spent twice at the same rate
just to decide whether history is needed.
"""
import logging

from tle import constants
from tle.util import gemini_api, llm_models
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

# Force a valid label instead of hoping for one bare word. Same approach as
# MKLOL/TLE-gf#10, which uses responseSchema on its classifier.
_CLASSIFIER_SCHEMA = {
    'type': 'STRING',
    'enum': [llm_context.MODE_DIRECT, llm_context.MODE_CONTEXT,
             llm_context.MODE_REPLY_CHAIN],
}


async def classify(pool, question, is_reply, session=None, stats=None,
                   author_name=None, author_id=None, sent_at=None):
    """Decide whether this question needs channel history.

    Falls back to ``requires_context`` on any failure: if the router cannot
    decide, collecting context is the safer answer path. A quota failure here
    is deliberately swallowed so the answer call still gets its chance.
    """
    if not constants.LLM_CONTEXT_ENABLED:
        return llm_context.MODE_DIRECT

    # LLM_MODELS is ordered cheapest-first, so the router takes the head of the
    # ladder. (This read `[-1:]` — the last entry, i.e. the most expensive
    # model — which billed every routing call at the top rate.)
    cheapest = pool.models[:1] if pool.models else None
    try:
        raw, _ = await gemini_api.complete(
            pool,
            llm_context.build_classifier_prompt(
                question, is_reply, author_name=author_name,
                author_id=author_id, sent_at=sent_at),
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
        logger.warning(';llm router failed (%s) — answering with context',
                       err)
        return llm_context.MODE_CONTEXT

    mode = llm_context.parse_mode(raw, is_reply)
    logger.info(';llm routed to %s (raw=%r, is_reply=%s)', mode, raw, is_reply)
    return mode


async def gather(ctx, mode, referenced, bot_user_id=None):
    """Collect the message window a mode calls for.

    A reply always gets its surrounding exchange, whatever the router decided.
    Someone who took the trouble to reply to a message wants that message's
    context, and reading channel history costs a Discord call, not an API one —
    so there is nothing to save by trusting the router here.
    """
    if referenced is not None:
        window = await llm_history.collect_reply_window(
            ctx.channel, referenced,
            before_count=constants.LLM_REPLY_BEFORE,
            after_count=constants.LLM_REPLY_AFTER,
            window_seconds=constants.LLM_CONTEXT_WINDOW_SECONDS,
            bot_user_id=bot_user_id)
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


def build_prompt(question, referenced, window, mode=llm_context.MODE_DIRECT):
    """Final prompt for the answer call.

    Three shapes, cheapest context first: a bare question, a quoted single
    message, or a transcript window.
    """
    ref_author = getattr(getattr(referenced, 'author', None), 'display_name', None)
    # Discord stores embed output separately from message.content. The LLM
    # cog sends answers in embeds, so a reply to one must quote the rendered
    # embed text as well as ordinary message content.
    ref_content = llm_history.message_text(referenced)

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

    return llm_context.build_question_prompt(
        question, context_requested=mode == llm_context.MODE_CONTEXT)


def describe_mode(mode, window):
    """Short footer note about what context was used, or None."""
    if mode == llm_context.MODE_DIRECT or not window:
        return None
    return f'{len(window)} messages of context'
