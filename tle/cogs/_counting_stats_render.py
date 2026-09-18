"""Discord-independent text rendering for structured counting statistics."""

import math
import re


_BASE_LABELS = {10: 'DEC', 2: 'BIN', 16: 'HEX'}
_MAX_RENDERED_NAME = 28


def format_counting_stats(stats, *, max_description_chars=512,
                          max_field_chars=1024):
    """Return ``(description, fields)`` ready to apply to an embed.

    Each field is a ``(name, value, inline)`` tuple.  The return value has no
    discord.py dependency, so the cog only needs to loop over the field tuples.
    """
    accuracy = _format_percent(stats.accuracy_percent)
    description = _clip(
        (f'**Current count:** {stats.current_count:,}\n'
         f'{stats.total_successes:,}/{stats.total_attempts:,} correct • '
         f'{accuracy} accuracy • {stats.unique_counters:,} unique counters'),
        max_description_chars)
    fields = (
        ('🏆 Top counters', _format_author_totals(
            stats.top_success_authors, empty='No successes yet'), True),
        ('💥 Most misses', _format_author_totals(
            stats.most_misses, empty='Nobody 🎯'), True),
        ('🔢 Base usage', ' • '.join(
            f'{_BASE_LABELS.get(item.radix, f"base {item.radix}")} '
            f'{item.count:,}' for item in stats.base_usage), True),
        ('⏱️ Pace', _format_pace(stats.gaps), False),
        ('🔥 Current same-user streak', _format_streak(
            stats.current_same_user_streak), False),
        ('🔥 Longest same-user streak', _format_streak(
            stats.longest_same_user_streak), False),
    )
    return description, tuple(
        (name, _clip(value, max_field_chars), inline)
        for name, value, inline in fields)


def render_counting_stats(stats, *, max_chars=1800):
    """Render one compact description, never exceeding ``max_chars``."""
    max_chars = max(0, _safe_int(max_chars, 1800))
    if max_chars == 0:
        return ''
    accuracy = _format_percent(stats.accuracy_percent)
    lines = [
        f'**Current count:** {stats.current_count:,}',
        (f'**Scoreboard:** {stats.total_successes:,}/'
         f'{stats.total_attempts:,} correct • {accuracy} accuracy • '
         f'{stats.unique_counters:,} unique counters'),
        '**Top counters:** ' + _format_author_totals(
            stats.top_success_authors, empty='No successes yet'),
        '**Most misses:** ' + _format_author_totals(
            stats.most_misses, empty='Nobody 🎯'),
        '**Base usage:** ' + ' • '.join(
            f'{_BASE_LABELS.get(item.radix, f"base {item.radix}")} '
            f'{item.count:,}' for item in stats.base_usage),
        '**Pace:** ' + _format_pace(stats.gaps),
        '**Current same-user streak:** ' + _format_streak(
            stats.current_same_user_streak),
        '**Same-user streak:** ' + _format_streak(
            stats.longest_same_user_streak),
    ]
    return _join_bounded_lines(lines, max_chars)


def format_duration(seconds):
    """Format a non-negative duration compactly for a Discord embed."""
    try:
        seconds = float(seconds)
    except (TypeError, ValueError, OverflowError):
        return '—'
    if not math.isfinite(seconds):
        return '—'
    seconds = max(0.0, seconds)
    if seconds < 1:
        number = f'{seconds:.2f}'.rstrip('0').rstrip('.')
        return f'{number}s'
    rounded = int(round(seconds))
    if rounded < 60:
        return f'{rounded}s'
    minutes, secs = divmod(rounded, 60)
    if minutes < 60:
        return f'{minutes}m {secs:02d}s'
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f'{hours}h {minutes:02d}m'
    days, hours = divmod(hours, 24)
    return f'{days}d {hours}h'


def _format_percent(value):
    if math.isclose(value, round(value), abs_tol=0.05):
        return f'{round(value):.0f}%'
    return f'{value:.1f}%'


def _format_author_totals(rows, *, empty):
    if not rows:
        return empty
    medals = ('🥇', '🥈', '🥉')
    return ' • '.join(
        f'{medals[index] + " " if index < len(medals) else ""}'
        f'{_render_name(row.author_name)} — {row.count:,}'
        for index, row in enumerate(rows))


def _format_pace(gaps):
    if gaps.sample_count == 0:
        return 'Not enough consecutive successes yet'
    return (
        f'fastest {format_duration(gaps.fastest.seconds)}'
        f'{_gap_numbers(gaps.fastest)} • '
        f'average {format_duration(gaps.average_seconds)} • '
        f'longest {format_duration(gaps.longest.seconds)}'
        f'{_gap_numbers(gaps.longest)}')


def _gap_numbers(gap):
    if gap.from_number is None or gap.to_number is None:
        return ''
    return f' (#{gap.from_number}→#{gap.to_number})'


def _format_streak(streak):
    if streak is None:
        return 'No successes yet'
    span = ''
    if streak.start_number is not None and streak.end_number is not None:
        span = f' (#{streak.start_number}–#{streak.end_number})'
    return f'{_render_name(streak.author_name)} — {streak.length:,}{span}'


def _render_name(value):
    name = ' '.join(str(value or '').split())[:_MAX_RENDERED_NAME] or 'Unknown'
    name = ''.join(char for char in name if char.isprintable())
    name = name.replace('@', '@\u200b')
    return re.sub(r'([\\`*_{}\[\]()<>#+.!|~>-])', r'\\\1', name)


def _join_bounded_lines(lines, max_chars):
    output = []
    used = 0
    for line in lines:
        extra = len(line) + (1 if output else 0)
        if used + extra <= max_chars:
            output.append(line)
            used += extra
            continue
        if output and used + 2 <= max_chars:
            output.append('…')
        elif not output:
            return line[:max_chars]
        break
    return '\n'.join(output)


def _clip(value, limit):
    limit = max(0, _safe_int(limit, 0))
    text = str(value)
    if len(text) <= limit:
        return text
    return text[:max(0, limit - 1)] + ('…' if limit else '')


def _safe_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default
