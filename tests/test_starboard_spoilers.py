"""Spoiler attachment rendering for starboard and pillboard posts."""

from tests.starboard_test_utils import _FakeAttachment, _FakeMessage, _run
from tle.cogs.starboard import Starboard


def test_spoiler_attachment_flag_wins_without_filename_prefix():
    """Discord's IS_SPOILER flag is authoritative on the wire."""
    class FlaggedAttachment(_FakeAttachment):
        flags = 1 << 3

        def is_spoiler(self):
            return False

    att = FlaggedAttachment('PXL_20260911_162227547.jpg')
    _content, embeds, files = _run(
        Starboard.build_starboard_message(
            _FakeMessage(attachments=[att]),
            '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
    assert all(embed.image_url is None for embed in embeds)
    assert len(files) == 1
    assert files[0].spoiler is True
