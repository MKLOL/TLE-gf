"""Image selection for the shared starboard and pillboard renderer."""

import re


CARRIED_EMBED_TYPES = ('rich', 'link', 'article')


def carried_image_urls(embeds):
    """Images already displayed by cards that are copied in full."""
    return {
        getattr(getattr(embed, field, None), 'url', None)
        for embed in embeds if embed.type in CARRIED_EMBED_TYPES
        for field in ('image', 'thumbnail')
    } - {None}


def preview_image_urls(embeds):
    """Extract auto-preview images without repeating carried card media."""
    carried_urls = carried_image_urls(embeds)
    image_urls = []
    for embed in embeds:
        if embed.type in CARRIED_EMBED_TYPES:
            continue
        if embed.type == 'image' and embed.url:
            if embed.url not in carried_urls:
                image_urls.append(embed.url)
            continue
        if image_urls:
            # Keep galleries of plain images separate from other previews.
            break
        thumb_url = getattr(embed.thumbnail, 'url', None) or ''
        if embed.type == 'gifv':
            # Tenor's AAAAe thumbnail is static; AAAAC selects the GIF.
            chosen = re.sub(
                r'(media\.tenor\.com/[^/]+?)AAAA[a-zA-Z0-9](/[^.]+)\.\w+$',
                r'\1AAAAC\2.gif',
                thumb_url,
            )
            if chosen and chosen not in carried_urls:
                image_urls.append(chosen)
            break
        if thumb_url and thumb_url not in carried_urls:
            image_urls.append(thumb_url)
            break
    return image_urls
