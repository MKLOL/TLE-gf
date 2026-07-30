"""Pure formatting helpers for the ``;llm`` cog.

Kept free of network and database access so they can be tested directly.
"""
import discord

_EMBED_DESC_LIMIT = 4000
_ANSWER_COLOR = 0x4285F4  # Google blue

# Discord rejects an embed description over 4096 characters, and answers can
# run long, so they are split across several embeds on paragraph boundaries.
_MAX_EMBED_PAGES = 4


def redact_key(api_key):
    """Render a key safe to display: keep enough to identify, drop the secret.

    Short strings are redacted entirely rather than mostly revealed.
    """
    if not api_key:
        return '(empty)'
    api_key = str(api_key)
    if len(api_key) < 16:
        return '*' * len(api_key)
    return f'{api_key[:6]}…{api_key[-4:]}'


def format_duration(seconds):
    """Human-readable wait, e.g. '45s', '12min', '3h 20min'."""
    if seconds is None:
        return 'unknown'
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f'{seconds}s'
    minutes = seconds // 60
    if minutes < 60:
        return f'{minutes}min'
    hours, minutes = divmod(minutes, 60)
    return f'{hours}h {minutes}min' if minutes else f'{hours}h'


def split_for_embed(text, limit=_EMBED_DESC_LIMIT, max_pages=_MAX_EMBED_PAGES):
    """Split an answer into embed-sized chunks, preferring clean breaks.

    Tries paragraph breaks first, then line breaks, and only cuts mid-line as
    a last resort. Output beyond ``max_pages`` is dropped with a marker so a
    runaway answer cannot spam a channel.
    """
    text = (text or '').strip()
    if not text:
        return ['*(empty answer)*']

    chunks = []
    remaining = text
    while remaining and len(chunks) < max_pages:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        window = remaining[:limit]
        cut = window.rfind('\n\n')
        if cut < limit // 2:
            cut = window.rfind('\n')
        if cut < limit // 2:
            cut = window.rfind(' ')
        if cut <= 0:
            cut = limit
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()

    if remaining and len(chunks) == max_pages:
        marker = '\n\n*(answer truncated)*'
        # Trim before appending, or the marker itself overflows the limit on a
        # chunk that already filled it.
        head = chunks[-1][:max(0, limit - len(marker))].rstrip()
        chunks[-1] = head + marker
    return chunks


def build_answer_embeds(answer, model, author=None, footer_extra=None):
    """Build the embed(s) carrying an answer back to the channel."""
    chunks = split_for_embed(answer)
    embeds = []
    for index, chunk in enumerate(chunks):
        embed = discord.Embed(description=chunk, color=_ANSWER_COLOR)
        if index == len(chunks) - 1:
            footer = model
            if footer_extra:
                footer = f'{model} • {footer_extra}'
            embed.set_footer(text=footer)
        embeds.append(embed)
    if embeds and author is not None:
        name = getattr(author, 'display_name', None) or str(author)
        avatar = getattr(getattr(author, 'display_avatar', None), 'url', None)
        try:
            embeds[0].set_author(name=f'Asked by {name}', icon_url=avatar)
        except TypeError:  # older/stubbed Embed without icon_url
            embeds[0].set_author(name=f'Asked by {name}')
    return embeds


def format_key_rows(keys):
    """Render stored keys for ``;llm keylist`` — redacted, never raw."""
    if not keys:
        return '*No API keys stored.*'
    lines = []
    for row in keys:
        label = f' `{row.label}`' if getattr(row, 'label', None) else ''
        added_by = getattr(row, 'added_by', None)
        who = f' — added by <@{added_by}>' if added_by else ''
        lines.append(f'**#{row.id}**{label} `{redact_key(row.api_key)}`{who}')
    return '\n'.join(lines)


def format_usage(top):
    """Render today's heaviest ``;llm`` users. ``top`` is ``(rows, total)``."""
    rows, total = top
    if not total:
        return '**Today:** no calls yet.'
    lines = [f'**Today:** {total} call(s)']
    lines += [f'<@{row.user_id}> — {row.count}' for row in rows]
    return '\n'.join(lines)


def format_pool_status(status_rows):
    """Render per-bucket pool state for ``;llm keystatus``."""
    if not status_rows:
        return '*No API keys stored — add some with `;llm keys <key> ...`.*'
    by_key = {}
    for row in status_rows:
        by_key.setdefault(row['key_id'], []).append(row)
    lines = []
    for key_id, rows in sorted(by_key.items()):
        label = rows[0].get('label')
        header = f'**Key #{key_id}**' + (f' `{label}`' if label else '')
        lines.append(header)
        for row in rows:
            if row['state'] == 'ready':
                mark = '\N{LARGE GREEN CIRCLE}'
                detail = 'ready'
            elif row['state'] == 'daily quota spent':
                mark = '\N{LARGE RED CIRCLE}'
                detail = f"daily quota spent, back in {format_duration(row['wait'])}"
            else:
                mark = '\N{LARGE YELLOW CIRCLE}'
                detail = f"cooling down, {format_duration(row['wait'])}"
            lines.append(f'{mark} `{row["model"]}` — {detail}')
    return '\n'.join(lines)
