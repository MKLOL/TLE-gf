"""Extract the human-readable text of Discord messages and embeds."""


def _embed_text(embed):
    """Return the visible title, body, fields, footer, and fallback URL."""
    pieces = []
    author = getattr(getattr(embed, 'author', None), 'name', None)
    if author:
        pieces.append(f'Embed author: {author}')
    title = getattr(embed, 'title', None)
    if title:
        pieces.append(f'Embed title: {title}')
    description = getattr(embed, 'description', None)
    if description:
        pieces.append(description)
    for field in getattr(embed, 'fields', None) or []:
        name = getattr(field, 'name', None)
        value = getattr(field, 'value', None)
        if name and value:
            pieces.append(f'{name}: {value}')
        elif value or name:
            pieces.append(value or name)
    footer = getattr(getattr(embed, 'footer', None), 'text', None)
    if footer:
        pieces.append(f'Embed footer: {footer}')
    url = getattr(embed, 'url', None)
    if url and not pieces:
        pieces.append(f'Embed URL: {url}')
    return '\n'.join(str(piece).strip() for piece in pieces
                     if str(piece).strip())


def message_text(message):
    """Return visible message text, including embeds and attachment names."""
    pieces = []
    content = (getattr(message, 'content', '') or '').strip()
    if content:
        pieces.append(content)
    for embed in getattr(message, 'embeds', None) or []:
        text = _embed_text(embed)
        if text:
            pieces.append(text)
    attachments = getattr(message, 'attachments', None) or []
    names = [getattr(item, 'filename', None) for item in attachments]
    names = [name for name in names if name]
    if attachments:
        pieces.append(f'[attached: {", ".join(names or ["file"])}]')
    return '\n'.join(pieces)
