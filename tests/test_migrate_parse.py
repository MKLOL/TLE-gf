"""Tests for migration helper functions: parsing, serialization, fallback building."""
import json

import pytest

import discord
from tle.cogs._migrate_helpers import (
    parse_old_bot_message,
    serialize_embed_fallback,
    build_fallback_message,
)


# =====================================================================
# parse_old_bot_message
# =====================================================================


class TestParseOldBotMessage:
    def test_standard_format(self):
        content = '⭐ **5** | https://discord.com/channels/111/222/333'
        result = parse_old_bot_message(content)
        assert result == ('⭐', 5, 111, 222, 333)

    def test_custom_emoji(self):
        content = '<:pill:123456> **3** | https://discord.com/channels/111/222/333'
        result = parse_old_bot_message(content)
        assert result == ('<:pill:123456>', 3, 111, 222, 333)

    def test_unicode_emoji(self):
        content = '🍫 **10** | https://discord.com/channels/111/222/333'
        result = parse_old_bot_message(content)
        assert result == ('🍫', 10, 111, 222, 333)

    def test_large_count(self):
        content = '⭐ **999** | https://discord.com/channels/111/222/333'
        result = parse_old_bot_message(content)
        assert result[1] == 999

    def test_count_one(self):
        content = '⭐ **1** | https://discord.com/channels/111/222/333'
        result = parse_old_bot_message(content)
        assert result[1] == 1

    def test_discordapp_url(self):
        content = '⭐ **5** | https://discordapp.com/channels/111/222/333'
        result = parse_old_bot_message(content)
        assert result == ('⭐', 5, 111, 222, 333)

    def test_no_match_random_text(self):
        assert parse_old_bot_message('just some random text') is None

    def test_no_match_missing_count(self):
        assert parse_old_bot_message('⭐ | https://discord.com/channels/111/222/333') is None

    def test_no_match_missing_url(self):
        assert parse_old_bot_message('⭐ **5**') is None

    def test_empty_string(self):
        assert parse_old_bot_message('') is None

    def test_none_input(self):
        assert parse_old_bot_message(None) is None

    def test_author_in_content(self):
        content = '💊 **5** · SomeUser | https://discord.com/channels/111/222/333'
        result = parse_old_bot_message(content)
        assert result == ('💊', 5, 111, 222, 333)

    def test_real_snowflake_ids(self):
        content = '⭐ **7** | https://discord.com/channels/1273752315022540861/1274019679425265685/1276961610195537991'
        result = parse_old_bot_message(content)
        assert result[2] == 1273752315022540861
        assert result[3] == 1274019679425265685
        assert result[4] == 1276961610195537991

    def test_whitespace_around_pipe(self):
        content = '⭐ **5**  |  https://discord.com/channels/111/222/333'
        result = parse_old_bot_message(content)
        assert result == ('⭐', 5, 111, 222, 333)

    def test_multiple_word_emoji(self):
        """Custom emoji with underscores."""
        content = '<:chocolate_bar:789> **2** | https://discord.com/channels/111/222/333'
        result = parse_old_bot_message(content)
        assert result == ('<:chocolate_bar:789>', 2, 111, 222, 333)


# =====================================================================
# serialize_embed_fallback
# =====================================================================


class _FakeEmbed:
    def __init__(self, title=None, description=None, color=None, image=None,
                 author_data=None, fields=None, footer=None, timestamp=None):
        self.title = title
        self.description = description
        self.color = color
        self.image = image
        self.author_data = author_data
        self.fields = fields or []
        self.footer = footer
        self.timestamp = timestamp


class _FakeImage:
    def __init__(self, url):
        self.url = url


class _FakeMsg:
    def __init__(self, content='', embeds=None):
        self.content = content
        self.embeds = embeds or []


class TestSerializeEmbedFallback:
    def test_basic_embed_roundtrip(self):
        embed = _FakeEmbed(title='Test', description='Hello', color=0xff0000)
        msg = _FakeMsg(content='Some text', embeds=[embed])
        result = serialize_embed_fallback(msg)
        data = json.loads(result)
        assert data['content'] == 'Some text'
        assert data['embeds'][0]['title'] == 'Test'
        assert data['embeds'][0]['description'] == 'Hello'
        assert data['embeds'][0]['color'] == 0xff0000

    def test_embed_with_image(self):
        embed = _FakeEmbed(image=_FakeImage('https://example.com/img.png'))
        msg = _FakeMsg(embeds=[embed])
        result = serialize_embed_fallback(msg)
        data = json.loads(result)
        assert data['embeds'][0]['image_url'] == 'https://example.com/img.png'

    def test_embed_with_author(self):
        embed = _FakeEmbed(author_data={'name': 'Bob', 'icon_url': 'https://x.com/a.png', 'url': None})
        msg = _FakeMsg(embeds=[embed])
        result = serialize_embed_fallback(msg)
        data = json.loads(result)
        assert data['embeds'][0]['author']['name'] == 'Bob'

    def test_embed_with_fields(self):
        embed = _FakeEmbed(fields=[
            {'name': 'Field1', 'value': 'Val1', 'inline': True},
            {'name': 'Field2', 'value': 'Val2', 'inline': False},
        ])
        msg = _FakeMsg(embeds=[embed])
        result = serialize_embed_fallback(msg)
        data = json.loads(result)
        assert len(data['embeds'][0]['fields']) == 2
        assert data['embeds'][0]['fields'][0]['name'] == 'Field1'

    def test_empty_embeds(self):
        msg = _FakeMsg(content='', embeds=[])
        result = serialize_embed_fallback(msg)
        data = json.loads(result)
        assert data == {}

    def test_multiple_embeds(self):
        e1 = _FakeEmbed(title='First')
        e2 = _FakeEmbed(title='Second')
        msg = _FakeMsg(embeds=[e1, e2])
        result = serialize_embed_fallback(msg)
        data = json.loads(result)
        assert len(data['embeds']) == 2
        assert data['embeds'][1]['title'] == 'Second'

    def test_color_serialized_as_int(self):
        """H2: embed.color must be serialized as int, not a discord.Color object."""
        # Simulate a discord.Color-like object that has int() support
        class FakeColor:
            def __init__(self, value):
                self._value = value
            def __int__(self):
                return self._value
            def __bool__(self):
                return True
        embed = _FakeEmbed(color=FakeColor(0xff0000))
        msg = _FakeMsg(embeds=[embed])
        result = serialize_embed_fallback(msg)
        data = json.loads(result)  # Would crash before fix if Color isn't JSON-serializable
        assert data['embeds'][0]['color'] == 0xff0000

    def test_color_none_omitted(self):
        embed = _FakeEmbed(color=None)
        msg = _FakeMsg(embeds=[embed])
        result = serialize_embed_fallback(msg)
        data = json.loads(result)
        assert data == {}  # No data to serialize


# =====================================================================
# build_fallback_message
# =====================================================================


class _FakeEntry:
    def __init__(self, star_count=5, original_msg_id='333',
                 guild_id=None, source_channel_id=None):
        self.star_count = star_count
        self.original_msg_id = original_msg_id
        self.guild_id = guild_id
        self.source_channel_id = source_channel_id


class TestBuildFallbackMessage:
    def test_reconstructs_content_line(self):
        entry = _FakeEntry(star_count=5, original_msg_id='333')
        fallback = json.dumps({'content': 'Hello world'})
        content, embeds = build_fallback_message(entry, fallback, '⭐')
        assert '⭐' in content
        assert '**5**' in content
        assert '333' in content

    def test_reconstructs_embed_from_json(self):
        entry = _FakeEntry()
        fallback = json.dumps({
            'content': 'Test message',
            'embeds': [{'author': {'name': 'Bob', 'icon_url': None, 'url': None}}],
        })
        content, embeds = build_fallback_message(entry, fallback, '⭐')
        assert len(embeds) == 1
        assert embeds[0].author_data['name'] == 'Bob'

    def test_handles_image(self):
        entry = _FakeEntry()
        fallback = json.dumps({
            'embeds': [{'image_url': 'https://example.com/img.png'}],
        })
        content, embeds = build_fallback_message(entry, fallback, '⭐')
        assert len(embeds) == 1
        assert embeds[0].image_url == 'https://example.com/img.png'

    def test_handles_empty_fallback(self):
        entry = _FakeEntry()
        content, embeds = build_fallback_message(entry, '{}', '⭐')
        assert '⭐' in content
        assert '**5**' in content
        assert embeds == []

    def test_handles_none_fallback(self):
        entry = _FakeEntry()
        content, embeds = build_fallback_message(entry, None, '⭐')
        assert '⭐' in content
        assert embeds == []

    def test_zero_star_count(self):
        entry = _FakeEntry(star_count=0)
        content, embeds = build_fallback_message(entry, '{}', '⭐')
        assert '**0**' in content

    def test_jump_url_uses_real_ids_when_available(self):
        """Crawled entries should have a proper jump URL, not channels/0/0/."""
        entry = _FakeEntry(guild_id='111', source_channel_id='222')
        content, embeds = build_fallback_message(entry, '{}', '⭐')
        assert 'discord.com/channels/111/222/333' in content

    def test_jump_url_falls_back_to_zero_when_no_ids(self):
        """Deleted entries with no source_channel_id get 0 placeholders."""
        entry = _FakeEntry()  # no guild_id or source_channel_id
        content, embeds = build_fallback_message(entry, '{}', '⭐')
        assert 'discord.com/channels/0/0/333' in content

    def test_jump_url_partial_ids(self):
        """Entry with guild_id but no source_channel_id."""
        entry = _FakeEntry(guild_id='111')
        content, embeds = build_fallback_message(entry, '{}', '⭐')
        assert 'discord.com/channels/111/0/333' in content
