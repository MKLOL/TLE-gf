"""Tests for build_starboard_message: embeds, attachments, replies, galleries."""
from datetime import datetime

from tests.starboard_test_utils import (
    _run,
    _FakeAuthor,
    _FakeReference,
    _FakeAttachment,
    _FakeMessage,
)
from tle.cogs.starboard import Starboard, _REPLY_EMBED_COLOR


class TestBuildStarboardMessage:
    """Tests for the new build_starboard_message method."""

    def test_returns_content_embeds_files(self):
        msg = _FakeMessage()
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert isinstance(content, str)
        assert isinstance(embeds, list)
        assert isinstance(files, list)

    def test_content_has_count_and_jump_url(self):
        msg = _FakeMessage()
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 7, 0xffaa10))
        assert '**7**' in content
        assert 'https://discord.com/channels/111/222/333' in content

    def test_main_embed_uses_set_author_with_jump_url(self):
        msg = _FakeMessage()
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        main_embed = embeds[-1]
        assert main_embed.author_data is not None
        assert main_embed.author_data['name'] == 'TestUser'
        assert main_embed.author_data['icon_url'] == 'https://cdn.example.com/avatar.png'
        assert main_embed.author_data['url'] == 'https://discord.com/channels/111/222/333'

    def test_main_embed_has_description_not_fields(self):
        msg = _FakeMessage(content='Some text')
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        main_embed = embeds[-1]
        assert main_embed.description == 'Some text'
        # No Channel/Jump to/Content fields like the old format
        field_names = [f['name'] for f in main_embed.fields]
        assert 'Channel' not in field_names
        assert 'Jump to' not in field_names
        assert 'Content' not in field_names

    def test_no_description_when_empty_content(self):
        msg = _FakeMessage(content='')
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        main_embed = embeds[-1]
        assert main_embed.description is None

    def test_color_passed_through(self):
        msg = _FakeMessage()
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0x00ff00))
        main_embed = embeds[-1]
        assert main_embed.color == 0x00ff00

    def test_image_attachment_set_on_embed(self):
        att = _FakeAttachment('photo.png')
        msg = _FakeMessage(attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        main_embed = embeds[-1]
        assert main_embed.image_url == 'https://cdn.example.com/file'

    def test_spoiler_image_not_embedded(self):
        """Spoiler images must not be set_image'd — that strips the spoiler."""
        att = _FakeAttachment('SPOILER_photo.png')
        msg = _FakeMessage(attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        for embed in embeds:
            assert embed.image_url is None

    def test_spoiler_image_uploaded_as_file(self):
        """Spoiler images are re-uploaded as file attachments to preserve the spoiler."""
        att = _FakeAttachment('SPOILER_photo.png')
        msg = _FakeMessage(attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert len(files) == 1
        assert files[0] == 'File:SPOILER_photo.png'

    def test_spoiler_image_to_file_called_with_spoiler_true(self):
        """to_file() must be called with spoiler=True — discord.py 2.4's
        to_file() defaults spoiler=False and would otherwise strip it."""
        att = _FakeAttachment('SPOILER_photo.png')
        msg = _FakeMessage(attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert files[0].spoiler is True

    def test_spoiler_filename_wins_when_attachment_helper_is_incomplete(self):
        """Snapshot-like attachments still preserve Discord's wire marker."""
        class SnapshotAttachment(_FakeAttachment):
            def is_spoiler(self):
                return False

        att = SnapshotAttachment('SPOILER_photo.png')
        msg = _FakeMessage(attachments=[att])
        _content, _embeds, files = _run(
            Starboard.build_starboard_message(
                msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert files[0].spoiler is True

    def test_non_spoiler_video_to_file_not_spoilered(self):
        """A normal video must not be spoilered when re-uploaded."""
        att = _FakeAttachment('clip.mp4', url='https://cdn.example.com/clip.mp4')
        msg = _FakeMessage(attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert files[0].spoiler is False

    def test_spoiler_image_author_in_content(self):
        """Spoiler image messages put author in content like videos do."""
        att = _FakeAttachment('SPOILER_photo.png')
        msg = _FakeMessage(attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert 'TestUser' in content

    def test_mixed_spoiler_and_normal_image(self):
        """Normal images embed; spoiler images go to files."""
        normal = _FakeAttachment('cat.jpg', url='https://cdn.example.com/cat.jpg')
        spoiler = _FakeAttachment('SPOILER_secret.png', url='https://cdn.example.com/secret.png')
        msg = _FakeMessage(attachments=[normal, spoiler])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        main_embed = embeds[-1]
        assert main_embed.image_url == 'https://cdn.example.com/cat.jpg'
        assert len(files) == 1
        assert files[0] == 'File:SPOILER_secret.png'

    def test_video_as_file_attachment(self):
        """Videos are uploaded as file attachments for native playback."""
        att = _FakeAttachment('clip.mp4', url='https://cdn.example.com/clip.mp4')
        msg = _FakeMessage(attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert len(files) == 1
        assert files[0] == 'File:clip.mp4'

    def test_video_author_in_content_header(self):
        """For video messages, author name goes in content (above file attachment)."""
        att = _FakeAttachment('clip.mp4', url='https://cdn.example.com/clip.mp4')
        msg = _FakeMessage(attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert 'TestUser' in content

    def test_video_only_no_empty_embed(self):
        """Video-only messages (no text) should not have a main embed."""
        att = _FakeAttachment('clip.mp4', url='https://cdn.example.com/clip.mp4')
        msg = _FakeMessage(content='', attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert len(embeds) == 0

    def test_video_with_text_has_embed(self):
        """Video + text content should still have a text embed."""
        att = _FakeAttachment('clip.mp4', url='https://cdn.example.com/clip.mp4')
        msg = _FakeMessage(content='Check this out', attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert len(embeds) == 1
        assert embeds[0].description == 'Check this out'

    def test_audio_as_file_attachment(self):
        """Audio files are uploaded as file attachments for native playback."""
        att = _FakeAttachment('song.mp3', url='https://cdn.example.com/song.mp3')
        msg = _FakeMessage(attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert len(files) == 1
        assert files[0] == 'File:song.mp3'

    def test_audio_author_in_content_header(self):
        """For audio messages, author name goes in content (above file attachment)."""
        att = _FakeAttachment('song.ogg', url='https://cdn.example.com/song.ogg')
        msg = _FakeMessage(attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert 'TestUser' in content

    def test_audio_only_no_empty_embed(self):
        """Audio-only messages (no text) should not have a main embed."""
        att = _FakeAttachment('song.flac', url='https://cdn.example.com/song.flac')
        msg = _FakeMessage(content='', attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert len(embeds) == 0

    def test_audio_with_text_has_embed(self):
        """Audio + text content should still have a text embed."""
        att = _FakeAttachment('song.wav', url='https://cdn.example.com/song.wav')
        msg = _FakeMessage(content='Listen to this', attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert len(embeds) == 1
        assert embeds[0].description == 'Listen to this'

    def test_other_attachment_as_field_link(self):
        att = _FakeAttachment('document.pdf')
        msg = _FakeMessage(attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        main_embed = embeds[-1]
        field_names = [f['name'] for f in main_embed.fields]
        assert 'Attachment' in field_names

    def test_rich_embeds_carried_over(self):
        """Rich embeds from the original message (e.g. bot embeds) should be included."""
        class FakeRichEmbed:
            type = 'rich'
            title = 'B. Count Pairs'
            image = None
            thumbnail = None
            url = None
        msg = _FakeMessage(content='Challenge problem for kindmango', embeds=[FakeRichEmbed()])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 3, 0xffaa10))
        # Main embed + the carried-over rich embed
        assert len(embeds) == 2
        assert embeds[1].title == 'B. Count Pairs'

    def test_link_embeds_carried_over(self):
        """Link preview embeds (e.g. Codeforces blog posts) should be included."""
        class FakeLinkEmbed:
            type = 'link'
            title = 'Codeforces Round 1084 (Div. 3)'
            image = None
            thumbnail = None
            url = 'https://codeforces.com/blog/entry/151519'
        msg = _FakeMessage(content='https://codeforces.com/blog/entry/151519', embeds=[FakeLinkEmbed()])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 9, 0xffaa10))
        assert len(embeds) == 2  # Main embed + link embed
        assert embeds[1].title == 'Codeforces Round 1084 (Div. 3)'

    def test_image_embeds_not_carried_over(self):
        """Auto-generated image embeds should not be carried over."""
        class FakeImageEmbed:
            type = 'image'
            url = 'https://example.com/image.png'
            image = None
            thumbnail = None
        msg = _FakeMessage(content='Some text', embeds=[FakeImageEmbed()])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 3, 0xffaa10))
        assert len(embeds) == 1  # Only main embed

    def test_gifv_embed_swaps_tenor_format_code(self):
        """Tenor GIFs: swap AAAAe format code to AAAAC and .png to .gif."""
        class FakeVideo:
            url = 'https://media.tenor.com/-92EuoZ_JUUAAAPo/luigi-discord-mod.mp4'
        class FakeThumbnail:
            url = 'https://media.tenor.com/-92EuoZ_JUUAAAAe/luigi-discord-mod.png'
            proxy_url = 'https://images-ext-1.discordapp.net/external/HASH/https/media.tenor.com/-92EuoZ_JUUAAAAe/luigi-discord-mod.png'
        class FakeGifvEmbed:
            type = 'gifv'
            url = 'https://tenor.com/view/luigi-discord-mod-post-luigi-gif-25261697'
            video = FakeVideo()
            image = None
            thumbnail = FakeThumbnail()
        msg = _FakeMessage(content='', embeds=[FakeGifvEmbed()])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        main_embed = embeds[0]
        assert main_embed.image_url == 'https://media.tenor.com/-92EuoZ_JUUAAAAC/luigi-discord-mod.gif'

    def test_gifv_embed_falls_back_to_static_thumbnail(self):
        """When thumbnail URL doesn't match Tenor pattern, use it as-is."""
        class FakeVideo:
            url = 'https://example.com/video.mp4'
        class FakeThumbnail:
            url = 'https://example.com/some-thumbnail.png'
            proxy_url = None
        class FakeGifvEmbed:
            type = 'gifv'
            url = 'https://example.com/gif-page'
            video = FakeVideo()
            image = None
            thumbnail = FakeThumbnail()
        msg = _FakeMessage(content='', embeds=[FakeGifvEmbed()])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        main_embed = embeds[0]
        # Falls back to static thumbnail — at least shows something
        assert main_embed.image_url == 'https://example.com/some-thumbnail.png'

    def test_gifv_embed_not_carried_over(self):
        """gifv auto-embeds should not be carried over (same as image/video)."""
        class FakeVideo:
            url = 'https://media.tenor.com/-92EuoZ_JUUAAAPo/luigi-discord-mod.mp4'
        class FakeThumbnail:
            url = 'https://media.tenor.com/-92EuoZ_JUUAAAAe/luigi-discord-mod.png'
            proxy_url = 'https://images-ext-1.discordapp.net/external/HASH/https/media.tenor.com/-92EuoZ_JUUAAAAe/luigi-discord-mod.png'
        class FakeGifvEmbed:
            type = 'gifv'
            url = 'https://tenor.com/view/luigi-discord-mod-post-luigi-gif-25261697'
            video = FakeVideo()
            image = None
            thumbnail = FakeThumbnail()
        msg = _FakeMessage(content='Check this out', embeds=[FakeGifvEmbed()])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        # Only main embed — gifv should not be carried over
        assert len(embeds) == 1

    def test_embeds_capped_at_10(self):
        """Discord allows max 10 embeds per message."""
        class FakeRichEmbed:
            type = 'rich'
            title = 'embed'
            image = None
            thumbnail = None
            url = None
        # 15 rich embeds + main embed would be 16 without the cap
        msg = _FakeMessage(content='text', embeds=[FakeRichEmbed() for _ in range(15)])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 3, 0xffaa10))
        assert len(embeds) == 10

    def test_embeds_exactly_10_not_truncated(self):
        """Exactly 10 embeds should not be truncated."""
        class FakeRichEmbed:
            type = 'rich'
            title = 'embed'
            image = None
            thumbnail = None
            url = None
        # 9 rich embeds + 1 main embed = exactly 10
        msg = _FakeMessage(content='text', embeds=[FakeRichEmbed() for _ in range(9)])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 3, 0xffaa10))
        assert len(embeds) == 10

    def test_embeds_cap_with_reply(self):
        """Reply embed + main embed + carried-over should still respect the 10-embed cap."""
        class FakeRichEmbed:
            type = 'rich'
            title = 'embed'
            image = None
            thumbnail = None
            url = None
        ref_msg = _FakeMessage(content='Parent')
        ref = _FakeReference(message_id=444, resolved=ref_msg)
        # reply(1) + main(1) + 12 carried-over = 14 without cap -> should be 10
        msg = _FakeMessage(content='Child', reference=ref,
                           embeds=[FakeRichEmbed() for _ in range(12)])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 3, 0xffaa10))
        assert len(embeds) == 10
        # Reply embed should be first (not truncated)
        assert embeds[0].author_data['name'] == 'Replying to TestUser'

    def test_no_reply_embed_without_reference(self):
        msg = _FakeMessage()
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert len(embeds) == 1  # Only main embed

    def test_reply_embed_present_with_resolved_reference(self):
        ref_author = _FakeAuthor()
        ref_author.display_name = 'ReplyTarget'
        ref_msg = _FakeMessage(content='Original message')
        ref_msg.author = ref_author
        ref_msg.created_at = datetime(2025, 1, 1)

        ref = _FakeReference(message_id=444, resolved=ref_msg)
        msg = _FakeMessage(content='My reply', reference=ref)
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert len(embeds) == 2  # Reply embed + main embed
        reply_embed = embeds[0]
        assert reply_embed.author_data['name'] == 'Replying to ReplyTarget'
        assert reply_embed.description == 'Original message'
        assert reply_embed.color == _REPLY_EMBED_COLOR

    def test_reply_embed_comes_before_main(self):
        ref_msg = _FakeMessage(content='Parent msg')
        ref = _FakeReference(message_id=444, resolved=ref_msg)
        msg = _FakeMessage(content='Child msg', reference=ref)
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        # Reply embed first, main embed second
        assert embeds[0].description == 'Parent msg'
        assert embeds[1].description == 'Child msg'

    def test_reply_embed_skipped_when_ref_msg_empty(self):
        """Reply to a message with no text, no attachments, no embeds should skip reply embed."""
        ref_msg = _FakeMessage(content='', attachments=[], embeds=[])
        ref = _FakeReference(message_id=444, resolved=ref_msg)
        msg = _FakeMessage(content='Replying to nothing', reference=ref)
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        # Only the main embed, no empty reply embed
        assert len(embeds) == 1
        assert embeds[0].description == 'Replying to nothing'

    def test_reply_embed_present_when_ref_has_text(self):
        """Reply embed should still appear when ref_msg has text."""
        ref_msg = _FakeMessage(content='I have text')
        ref = _FakeReference(message_id=444, resolved=ref_msg)
        msg = _FakeMessage(content='Reply', reference=ref)
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert len(embeds) == 2
        assert embeds[0].description == 'I have text'

    def test_reply_embed_present_when_ref_has_only_image(self):
        """Reply embed should appear when ref_msg has only an image attachment."""
        ref_msg = _FakeMessage(
            content='',
            attachments=[_FakeAttachment('photo.png', url='https://cdn.example.com/photo.png')],
        )
        ref = _FakeReference(message_id=444, resolved=ref_msg)
        msg = _FakeMessage(content='Nice pic', reference=ref)
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert len(embeds) == 2
        assert embeds[0].image_url == 'https://cdn.example.com/photo.png'

    def test_reply_spoiler_image_not_embedded(self):
        """Spoiler images in replied-to messages must not be set_image'd."""
        ref_msg = _FakeMessage(
            content='',
            attachments=[_FakeAttachment('SPOILER_secret.png', url='https://cdn.example.com/secret.png')],
        )
        ref = _FakeReference(message_id=444, resolved=ref_msg)
        msg = _FakeMessage(content='Replying', reference=ref)
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        # Reply embed should exist (spoiler image is listed as an attachment field)
        # but it must NOT have the image embedded
        for embed in embeds:
            assert embed.image_url is None

    def test_reply_embed_present_when_ref_has_only_video(self):
        """Reply embed should appear when ref_msg has only a video (shown as field)."""
        ref_msg = _FakeMessage(
            content='',
            attachments=[_FakeAttachment('clip.mp4', url='https://cdn.example.com/clip.mp4')],
        )
        ref = _FakeReference(message_id=444, resolved=ref_msg)
        msg = _FakeMessage(content='Cool clip', reference=ref)
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert len(embeds) == 2
        reply_embed = embeds[0]
        field_names = [f['name'] for f in reply_embed.fields]
        assert 'Attachment' in field_names

    def test_reply_embed_shows_video_attachment_as_field(self):
        """Video in the replied-to message should appear as an Attachment field."""
        ref_msg = _FakeMessage(
            content='Check this clip',
            attachments=[_FakeAttachment('clip.mp4', url='https://cdn.example.com/clip.mp4')],
        )
        ref = _FakeReference(message_id=444, resolved=ref_msg)
        msg = _FakeMessage(content='Nice!', reference=ref)
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        reply_embed = embeds[0]
        field_names = [f['name'] for f in reply_embed.fields]
        assert 'Attachment' in field_names
        assert '`clip.mp4`' in reply_embed.fields[0]['value']

    def test_reply_embed_shows_other_attachment_as_field(self):
        """Non-image attachment (PDF, ZIP) in replied-to message should appear as field."""
        ref_msg = _FakeMessage(
            content='Here is the doc',
            attachments=[_FakeAttachment('notes.pdf')],
        )
        ref = _FakeReference(message_id=444, resolved=ref_msg)
        msg = _FakeMessage(content='Thanks', reference=ref)
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 3, 0xffaa10))
        reply_embed = embeds[0]
        field_names = [f['name'] for f in reply_embed.fields]
        assert 'Attachment' in field_names
        assert '`notes.pdf`' in reply_embed.fields[0]['value']

    def test_reply_embed_video_and_image(self):
        """Reply to message with both image and video: image in embed, video as field."""
        ref_msg = _FakeMessage(
            content='',
            attachments=[
                _FakeAttachment('photo.png', url='https://cdn.example.com/photo.png'),
                _FakeAttachment('clip.mp4', url='https://cdn.example.com/clip.mp4'),
            ],
        )
        ref = _FakeReference(message_id=444, resolved=ref_msg)
        msg = _FakeMessage(content='Wow', reference=ref)
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        reply_embed = embeds[0]
        assert reply_embed.image_url == 'https://cdn.example.com/photo.png'
        field_names = [f['name'] for f in reply_embed.fields]
        assert 'Attachment' in field_names

    def test_long_content_truncated(self):
        msg = _FakeMessage(content='x' * 5000)
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        main_embed = embeds[-1]
        assert len(main_embed.description) == 4096
        assert main_embed.description.endswith('...')

    def test_no_footer_set(self):
        """New format uses set_author, not footer."""
        msg = _FakeMessage()
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        main_embed = embeds[-1]
        assert main_embed.footer is None

    def test_timestamp_set(self):
        msg = _FakeMessage()
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        main_embed = embeds[-1]
        assert main_embed.timestamp == datetime(2025, 1, 1)
