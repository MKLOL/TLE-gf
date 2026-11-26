"""Sticker media shared by board posts, forwards, and reply previews."""


def sticker_media(message):
    """Return image URLs and a label for stickers embeds cannot display.

    discord.py exposes message sticker_items as StickerItems in .stickers.
    PNG (1), APNG (2), and GIF (4) have image URLs; Lottie (3) is JSON.
    Keep unsupported stickers named so the original-message link remains
    useful without attempting to embed JSON as an image.
    """
    images, labels = [], []
    for sticker in getattr(message, 'stickers', ()):
        format_value = getattr(sticker.format, 'value', sticker.format)
        if format_value in (1, 2, 4):
            images.append(sticker.url)
        else:
            name = sticker.name.replace('`', '\u02cb')
            labels.append(f'`{name}`')
    return images, ', '.join(labels)
