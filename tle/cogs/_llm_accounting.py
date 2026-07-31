"""Prompt-free accounting helpers for the Discord LLM pipeline."""
import math
import time

from tle import constants


def new_stage_stats():
    return {'attempts': 0, 'input_tokens': 0,
            'output_tokens': 0, 'total_tokens': 0}


def xai_cost_microusd(*stats_groups):
    """Use xAI's billed cost when returned, with token-price fallback."""
    total = 0
    for group in stats_groups:
        if isinstance(group, dict) and 'cost_microusd' in group:
            total += _integer(group, 'cost_microusd')
            continue
        total += int(math.ceil(
            _integer(group, 'input_tokens')
            * constants.XAI_INPUT_USD_PER_MILLION
            + _integer(group, 'output_tokens')
            * constants.XAI_OUTPUT_USD_PER_MILLION))
    return total


def xai_reservation_microusd():
    """Conservative pre-call hold covering router plus capped answer output."""
    input_tokens = constants.XAI_REQUEST_RESERVE_INPUT_TOKENS
    output_tokens = (constants.XAI_MAX_OUTPUT_TOKENS
                     + constants.XAI_ROUTER_MAX_OUTPUT_TOKENS)
    return int(math.ceil(
        input_tokens * constants.XAI_INPUT_USD_PER_MILLION
        + output_tokens * constants.XAI_OUTPUT_USD_PER_MILLION))


def daily_budget_microusd():
    return int(math.floor(constants.XAI_DAILY_BUDGET_USD * 1_000_000))


def has_xai_cost_observation(*stats_groups):
    """True when exact cost or enough token usage exists to reconcile."""
    return any(
        isinstance(group, dict) and (
            'cost_microusd' in group
            or _integer(group, 'input_tokens')
            or _integer(group, 'output_tokens'))
        for group in stats_groups)


def record(database, ctx, provider, outcome, started_at, *, model=None,
           router_stats=None, answer_stats=None, mode=None, window=None,
           cost_microusd=0):
    """Persist one request without accepting prompt/answer arguments."""
    router_stats = router_stats or {}
    answer_stats = answer_stats or {}
    input_tokens = (_integer(router_stats, 'input_tokens')
                    + _integer(answer_stats, 'input_tokens'))
    output_tokens = (_integer(router_stats, 'output_tokens')
                     + _integer(answer_stats, 'output_tokens'))
    total_tokens = (_integer(router_stats, 'total_tokens')
                    + _integer(answer_stats, 'total_tokens'))
    database.llm_record_request(
        ctx.guild.id, ctx.author.id, provider, _today(), outcome,
        model=model,
        router_attempts=_integer(router_stats, 'attempts'),
        answer_attempts=_integer(answer_stats, 'attempts'),
        input_tokens=input_tokens, output_tokens=output_tokens,
        total_tokens=total_tokens,
        latency_ms=max(0, int((time.monotonic() - started_at) * 1000)),
        cost_microusd=cost_microusd,
        context_mode=mode,
        context_messages=len(window or []))
    if outcome == 'success' or (
            _integer(router_stats, 'attempts')
            + _integer(answer_stats, 'attempts')):
        database.llm_bump_usage(ctx.guild.id, ctx.author.id, _today())


def _today():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def _integer(mapping, key):
    try:
        return max(0, int(mapping.get(key, 0) or 0))
    except (AttributeError, TypeError, ValueError):
        return 0
