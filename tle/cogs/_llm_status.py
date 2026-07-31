"""Owner-only provider telemetry formatting without prompt or key data."""


def format_provider_summary(summary, top_users, *, show_cost=False):
    calls = int(_value(summary, 'calls'))
    successes = int(_value(summary, 'successes'))
    router_attempts = int(_value(summary, 'router_attempts'))
    answer_attempts = int(_value(summary, 'answer_attempts'))
    input_tokens = int(_value(summary, 'input_tokens'))
    output_tokens = int(_value(summary, 'output_tokens'))
    total_tokens = int(_value(summary, 'total_tokens'))
    latency = _value(summary, 'average_latency_ms')

    rate = (100 * successes / calls) if calls else 0
    lines = [
        f'**Today:** {calls} invocation(s), {successes} successful '
        f'({rate:.0f}%)',
        f'**Provider attempts:** {router_attempts} router, '
        f'{answer_attempts} answer',
        f'**Tokens:** {input_tokens:,} in, {output_tokens:,} out, '
        f'{total_tokens:,} total',
        f'**Average latency:** {latency:,.0f} ms',
    ]
    if show_cost:
        lines.append(
            f'**Recorded request cost:** '
            f'{format_microusd(_value(summary, "cost_microusd"))}')
    if top_users:
        lines.append('**Top users:** ' + ', '.join(
            _format_user(row, show_cost=show_cost) for row in top_users))
    return '\n'.join(lines)


def format_xai_ledger(summary):
    """Show actual/guarded spend without revealing configured thresholds."""
    calls = _value(summary, 'calls')
    actual = _value(summary, 'actual_microusd')
    guarded = _value(summary, 'guarded_microusd')
    return (
        f'**Credit guard today:** {calls} reservation(s), '
        f'{format_microusd(actual)} reconciled, '
        f'{format_microusd(guarded)} committed at most'
    )


def format_microusd(value):
    dollars = max(0, int(value or 0)) / 1_000_000
    if dollars == 0:
        return '$0.00'
    if dollars < 0.01:
        return f'${dollars:.4f}'
    return f'${dollars:.2f}'


def _format_user(row, *, show_cost):
    text = f'<@{row.user_id}> {int(row.calls)}'
    if show_cost:
        text += f' ({format_microusd(row.cost_microusd)})'
    return text


def _value(row, field):
    try:
        return max(0, float(getattr(row, field) or 0))
    except (AttributeError, TypeError, ValueError):
        return 0
