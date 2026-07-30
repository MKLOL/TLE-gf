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
from tle.util import gemini_api
from tle.cogs import _llm_context as llm_context
from tle.cogs import _llm_history as llm_history

logger = logging.getLogger(__name__)

_CLASSIFIER_MAX_TOKENS = 16


async def classify(pool, question, is_reply, session=None, stats=None):
    """Decide whether this question needs channel history.

    Falls back to ``direct`` on any failure: routing is an optimisation, and
    losing it should degrade the answer, not block it. A quota failure here is
    deliberately swallowed so the answer call still gets its chance.
    """
    if not constants.LLM_CONTEXT_ENABLED:
        return llm_context.MODE_DIRECT

    cheapest = pool.models[-1:] if pool.models else None
    try:
        raw, _ = await gemini_api.complete(
            pool,
            llm_context.build_classifier_prompt(question, is_reply),
            system_instruction=llm_context.CLASSIFIER_INSTRUCTION,
            max_output_tokens=_CLASSIFIER_MAX_TOKENS,
            temperature=0,
            session=session,
            models=cheapest,
            stats=stats,
            max_attempts=2)
    except gemini_api.GeminiError as err:
        logger.info(';llm classifier unavailable (%s), answering directly', err)
        return llm_context.MODE_DIRECT
    return llm_context.parse_mode(raw, is_reply)


async def gather(ctx, mode, referenced, bot_user_id=None):
    """Collect the message window a mode calls for. Empty for ``direct``."""
    if mode == llm_context.MODE_REPLY_CHAIN and referenced is not None:
        return await llm_history.collect_reply_window(
            ctx.channel, referenced,
            before_count=constants.LLM_REPLY_BEFORE,
            after_count=constants.LLM_REPLY_AFTER,
            window_seconds=constants.LLM_CONTEXT_WINDOW_SECONDS,
            bot_user_id=bot_user_id)
    if mode == llm_context.MODE_CONTEXT:
        return await llm_history.collect_recent(
            ctx.channel, before=ctx.message,
            limit=constants.LLM_CONTEXT_MESSAGES,
            window_seconds=constants.LLM_CONTEXT_WINDOW_SECONDS,
            bot_user_id=bot_user_id)
    return []


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
    if mode == llm_context.MODE_DIRECT or not window:
        return None
    return f'{len(window)} messages of context'
