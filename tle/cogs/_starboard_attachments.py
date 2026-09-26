"""Attachment helpers shared by starboard rendering paths."""

# Discord's AttachmentFlags.IS_SPOILER bit (the API sends this as flags=8).
_ATTACHMENT_SPOILER_FLAG = 1 << 3


def _attachment_is_spoiler(attachment):
    """Return whether Discord marked an attachment as a spoiler."""
    filename = getattr(attachment, 'filename', '') or ''
    if filename.startswith('SPOILER_'):
        return True

    flags = getattr(attachment, 'flags',
                    getattr(attachment, '_flags', None))
    if getattr(flags, 'spoiler', False):
        return True
    value = getattr(flags, 'value', flags)
    try:
        if int(value) & _ATTACHMENT_SPOILER_FLAG:
            return True
    except (TypeError, ValueError):
        pass

    checker = getattr(attachment, 'is_spoiler', None)
    if checker is None:
        return False
    try:
        return bool(checker())
    except Exception:
        return False
