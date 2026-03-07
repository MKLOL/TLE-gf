"""Cog-level tests for starboard: jump URL parsing, build_starboard_message,
default emoji parameters, argument parsing, and snowflake time filtering.
"""
import asyncio
import inspect
import sqlite3
import time as time_mod
from datetime import datetime, timezone

import pytest

from tests.test_starboard_db import FakeUserDb
from tle.cogs._starboard_helpers import _parse_jump_url
from tle.cogs.starboard import (
    Starboard,
    _starboard_content,
    _parse_starboard_args,
    _NO_TIME_BOUND,
    _REPLY_EMBED_COLOR,
)
from tle.constants import _DEFAULT_STAR
from tle.util.db.starboard_db import snowflake_to_unix_sql, DISCORD_EPOCH_MS, SNOWFLAKE_TIMESTAMP_DIVISOR

GUILD_A = 111111111111111111
STAR = '\N{WHITE MEDIUM STAR}'
FIRE = '\N{FIRE}'


@pytest.fixture
def db():
    d = FakeUserDb()
    yield d
    d.close()


# =====================================================================
# Jump URL parsing for backfill optimization
# =====================================================================


class TestParseJumpUrl:
    def test_standard_discord_url(self):
        text = '[Original](https://discord.com/channels/111/222/333)'
        result = _parse_jump_url(text)
        assert result == (111, 222, 333)

    def test_discordapp_url(self):
        text = '[Original](https://discordapp.com/channels/111/222/333)'
        result = _parse_jump_url(text)
        assert result == (111, 222, 333)

    def test_real_snowflake_ids(self):
        text = '[Original](https://discord.com/channels/1273752315022540861/1274019679425265685/1276961610195537991)'
        result = _parse_jump_url(text)
        assert result == (1273752315022540861, 1274019679425265685, 1276961610195537991)

    def test_extracts_channel_id(self):
        """The channel_id (second element) is what the backfill needs."""
        text = '[Original](https://discord.com/channels/111/999888777/333)'
        result = _parse_jump_url(text)
        _, channel_id, _ = result
        assert channel_id == 999888777

    def test_no_url_returns_none(self):
        assert _parse_jump_url('no url here') is None

    def test_empty_string_returns_none(self):
        assert _parse_jump_url('') is None

    def test_partial_url_returns_none(self):
        assert _parse_jump_url('https://discord.com/channels/111/222') is None

    def test_wrong_domain_returns_none(self):
        assert _parse_jump_url('https://example.com/channels/111/222/333') is None

    def test_url_embedded_in_markdown(self):
        """The real embed field value has markdown link syntax."""
        text = '[Original](https://discord.com/channels/111/222/333)'
        result = _parse_jump_url(text)
        assert result == (111, 222, 333)

    def test_url_with_extra_text(self):
        text = 'Check this out: https://discord.com/channels/111/222/333 cool right?'
        result = _parse_jump_url(text)
        assert result == (111, 222, 333)


# =====================================================================
# get_starboard_emojis_for_guild now includes channel_id
# =====================================================================

class TestGetEmojisIncludesChannelId:
    def test_channel_id_returned(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.set_starboard_channel(GUILD_A, STAR, 999888)

        emojis = db.get_starboard_emojis_for_guild(GUILD_A)
        assert len(emojis) == 1
        assert emojis[0].channel_id == '999888'

    def test_channel_id_none_when_not_set(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)

        emojis = db.get_starboard_emojis_for_guild(GUILD_A)
        assert len(emojis) == 1
        assert emojis[0].channel_id is None

    def test_multiple_emojis_different_channels(self, db):
        db.add_starboard_emoji(GUILD_A, STAR, 3, 0xffaa10)
        db.add_starboard_emoji(GUILD_A, FIRE, 5, 0xff0000)
        db.set_starboard_channel(GUILD_A, STAR, 100)
        db.set_starboard_channel(GUILD_A, FIRE, 200)

        emojis = db.get_starboard_emojis_for_guild(GUILD_A)
        by_emoji = {e.emoji: e for e in emojis}
        assert by_emoji[STAR].channel_id == '100'
        assert by_emoji[FIRE].channel_id == '200'


# =====================================================================
# _starboard_content helper
# =====================================================================


class TestStarboardContent:
    def test_format(self):
        result = _starboard_content('\N{WHITE MEDIUM STAR}', 5, 222, 'https://discord.com/channels/1/2/3')
        assert '\N{WHITE MEDIUM STAR}' in result
        assert '**5**' in result
        assert '<#222>' in result
        assert '[\U0001f4ac](https://discord.com/channels/1/2/3)' in result

    def test_pipe_separator(self):
        result = _starboard_content('\N{FIRE}', 3, 999, 'https://example.com')
        assert '|' in result


# =====================================================================
# build_starboard_message tests
# =====================================================================

import discord


class _FakeDisplayAvatar:
    url = 'https://cdn.example.com/avatar.png'


class _FakeAuthor:
    display_name = 'TestUser'
    display_avatar = _FakeDisplayAvatar()
    def __str__(self):
        return 'TestUser#1234'


class _FakeChannel:
    id = 222
    mention = '#general'

    async def fetch_message(self, msg_id):
        raise discord.NotFound()


class _FakeReference:
    def __init__(self, message_id=None, resolved=None):
        self.message_id = message_id
        self.resolved = resolved


class _FakeAttachment:
    def __init__(self, filename, url='https://cdn.example.com/file'):
        self.filename = filename
        self.url = url

    async def to_file(self):
        return f'File:{self.filename}'


class _FakeMessage:
    """Minimal message mock for build_starboard_message tests."""
    def __init__(self, content='Hello world', embeds=None, attachments=None, reference=None):
        self.content = content
        self.embeds = embeds or []
        self.attachments = attachments or []
        self.created_at = datetime(2025, 1, 1)
        self.channel = _FakeChannel()
        self.jump_url = 'https://discord.com/channels/111/222/333'
        self.author = _FakeAuthor()
        self.reference = reference
        self.type = discord.MessageType.default


def _run(coro):
    """Run an async coroutine synchronously for testing."""
    return asyncio.run(coro)


class TestBuildStarboardMessage:
    """Tests for the new build_starboard_message method."""

    def test_returns_content_embeds_files(self):
        msg = _FakeMessage()
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert isinstance(content, str)
        assert isinstance(embeds, list)
        assert isinstance(files, list)

    def test_content_has_count_and_channel(self):
        msg = _FakeMessage()
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 7, 0xffaa10))
        assert '**7**' in content
        assert '<#222>' in content

    def test_main_embed_uses_set_author(self):
        msg = _FakeMessage()
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        main_embed = embeds[-1]
        assert main_embed.author_data is not None
        assert main_embed.author_data['name'] == 'TestUser'
        assert main_embed.author_data['icon_url'] == 'https://cdn.example.com/avatar.png'

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
        assert len(files) == 0

    def test_video_attachment_added_as_file(self):
        att = _FakeAttachment('clip.mp4')
        msg = _FakeMessage(attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        assert len(files) == 1
        assert files[0] == 'File:clip.mp4'

    def test_other_attachment_as_field_link(self):
        att = _FakeAttachment('document.pdf')
        msg = _FakeMessage(attachments=[att])
        content, embeds, files = _run(Starboard.build_starboard_message(msg, '\N{WHITE MEDIUM STAR}', 5, 0xffaa10))
        main_embed = embeds[-1]
        field_names = [f['name'] for f in main_embed.fields]
        assert 'Attachment' in field_names
        assert len(files) == 0

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


# =====================================================================
# Default emoji parameter on all starboard commands
# =====================================================================


def _unwrap(attr):
    """Get the original function from a stubbed command decorator."""
    while hasattr(attr, '__wrapped__'):
        attr = attr.__wrapped__
    return attr


class TestDefaultEmojiParameter:
    """All starboard commands should default the emoji parameter to the star emoji."""

    # Commands with an explicit emoji parameter (admin/config commands)
    _COMMANDS_WITH_EMOJI_PARAM = [
        'add', 'delete', 'edit_threshold', 'edit_color',
        'here', 'clear', 'remove',
    ]

    # Commands using *args + _parse_starboard_args (leaderboard/top commands)
    _COMMANDS_WITH_ARGS = [
        'leaderboard', 'star_leaderboard', 'star_givers', 'top',
    ]

    @pytest.mark.parametrize('method_name', _COMMANDS_WITH_EMOJI_PARAM)
    def test_emoji_defaults_to_star(self, method_name):
        method = _unwrap(getattr(Starboard, method_name))
        sig = inspect.signature(method)
        assert 'emoji' in sig.parameters, f'{method_name} missing emoji parameter'
        param = sig.parameters['emoji']
        assert param.default == _DEFAULT_STAR, (
            f'{method_name}: emoji default is {param.default!r}, expected {_DEFAULT_STAR!r}'
        )

    @pytest.mark.parametrize('method_name', _COMMANDS_WITH_ARGS)
    def test_args_commands_use_varargs(self, method_name):
        """Leaderboard/top commands use *args and parse emoji via _parse_starboard_args."""
        method = _unwrap(getattr(Starboard, method_name))
        sig = inspect.signature(method)
        assert 'args' in sig.parameters, f'{method_name} should accept *args'

    def test_edit_threshold_required_arg_before_emoji(self):
        """threshold should come before the optional emoji."""
        sig = inspect.signature(_unwrap(Starboard.edit_threshold))
        params = list(sig.parameters.keys())
        assert params.index('threshold') < params.index('emoji')

    def test_edit_color_required_arg_before_emoji(self):
        """color should come before the optional emoji."""
        sig = inspect.signature(_unwrap(Starboard.edit_color))
        params = list(sig.parameters.keys())
        assert params.index('color') < params.index('emoji')

    def test_remove_required_arg_before_emoji(self):
        """original_message_id should come before the optional emoji."""
        sig = inspect.signature(_unwrap(Starboard.remove))
        params = list(sig.parameters.keys())
        assert params.index('original_message_id') < params.index('emoji')


# =====================================================================
# _parse_starboard_args
# =====================================================================


class TestParseStarboardArgs:
    """Test the argument parser for starboard leaderboard/top commands."""

    def test_no_args_defaults_to_star_all_time(self):
        emoji, dlo, dhi = _parse_starboard_args(())
        assert emoji == STAR
        assert dlo == 0
        assert dhi == _NO_TIME_BOUND

    def test_emoji_only(self):
        emoji, dlo, dhi = _parse_starboard_args(('\N{FIRE}',))
        assert emoji == '\N{FIRE}'
        assert dlo == 0
        assert dhi == _NO_TIME_BOUND

    def test_star_emoji_explicit(self):
        emoji, dlo, dhi = _parse_starboard_args(('\N{WHITE MEDIUM STAR}',))
        assert emoji == '\N{WHITE MEDIUM STAR}'
        assert dlo == 0
        assert dhi == _NO_TIME_BOUND

    def test_week_keyword_defaults_star(self):
        emoji, dlo, dhi = _parse_starboard_args(('week',))
        assert emoji == STAR
        assert dlo > 0
        assert dhi == _NO_TIME_BOUND

    def test_month_keyword_defaults_star(self):
        emoji, dlo, dhi = _parse_starboard_args(('month',))
        assert emoji == STAR
        assert dlo > 0
        assert dhi == _NO_TIME_BOUND

    def test_year_keyword_defaults_star(self):
        emoji, dlo, dhi = _parse_starboard_args(('year',))
        assert emoji == STAR
        assert dlo > 0
        assert dhi == _NO_TIME_BOUND

    def test_emoji_and_week(self):
        emoji, dlo, dhi = _parse_starboard_args(('\N{FIRE}', 'week'))
        assert emoji == '\N{FIRE}'
        assert dlo > 0

    def test_week_and_emoji_reversed_order(self):
        """Order shouldn't matter."""
        emoji, dlo, dhi = _parse_starboard_args(('week', '\N{FIRE}'))
        assert emoji == '\N{FIRE}'
        assert dlo > 0

    def test_week_sets_monday(self):
        emoji, dlo, dhi = _parse_starboard_args(('week',))
        monday = datetime.fromtimestamp(dlo)
        assert monday.weekday() == 0  # Monday
        assert monday.hour == 0
        assert monday.minute == 0
        assert monday.second == 0

    def test_month_sets_first_of_month(self):
        emoji, dlo, dhi = _parse_starboard_args(('month',))
        first = datetime.fromtimestamp(dlo)
        assert first.day == 1
        assert first.hour == 0
        assert first.minute == 0

    def test_year_sets_jan_first(self):
        emoji, dlo, dhi = _parse_starboard_args(('year',))
        jan1 = datetime.fromtimestamp(dlo)
        assert jan1.month == 1
        assert jan1.day == 1
        assert jan1.hour == 0

    def test_dge_date_arg(self):
        """d>=01012025 should set dlo to Jan 1 2025."""
        emoji, dlo, dhi = _parse_starboard_args(('d>=01012025',))
        assert emoji == STAR
        dt_obj = datetime.fromtimestamp(dlo)
        assert dt_obj.year == 2025
        assert dt_obj.month == 1
        assert dt_obj.day == 1

    def test_dlt_date_arg(self):
        """d<01022025 should set dhi to Feb 1 2025."""
        emoji, dlo, dhi = _parse_starboard_args(('d<01022025',))
        assert emoji == STAR
        dt_obj = datetime.fromtimestamp(dhi)
        assert dt_obj.year == 2025
        assert dt_obj.month == 2
        assert dt_obj.day == 1

    def test_dge_and_dlt_combined(self):
        emoji, dlo, dhi = _parse_starboard_args(('d>=01012025', 'd<01022025'))
        assert emoji == STAR
        assert dlo < dhi
        lo_dt = datetime.fromtimestamp(dlo)
        hi_dt = datetime.fromtimestamp(dhi)
        assert lo_dt.month == 1
        assert hi_dt.month == 2

    def test_emoji_with_dge_and_dlt(self):
        emoji, dlo, dhi = _parse_starboard_args(('\N{FIRE}', 'd>=01012025', 'd<01022025'))
        assert emoji == '\N{FIRE}'
        assert dlo > 0
        assert dhi < _NO_TIME_BOUND

    def test_year_only_format(self):
        """d>=2024 should parse as Jan 1 2024."""
        emoji, dlo, dhi = _parse_starboard_args(('d>=2024',))
        dt_obj = datetime.fromtimestamp(dlo)
        assert dt_obj.year == 2024
        assert dt_obj.month == 1
        assert dt_obj.day == 1

    def test_month_year_format(self):
        """d>=032025 should parse as March 2025."""
        emoji, dlo, dhi = _parse_starboard_args(('d>=032025',))
        dt_obj = datetime.fromtimestamp(dlo)
        assert dt_obj.year == 2025
        assert dt_obj.month == 3

    def test_keyword_case_insensitive(self):
        emoji, dlo, dhi = _parse_starboard_args(('Week',))
        assert emoji == STAR
        assert dlo > 0

    def test_keyword_uppercase(self):
        emoji, dlo, dhi = _parse_starboard_args(('MONTH',))
        assert emoji == STAR
        assert dlo > 0

    def test_timeline_keyword_not_treated_as_emoji(self):
        """'week' should not be stored as the emoji."""
        emoji, dlo, dhi = _parse_starboard_args(('week',))
        assert emoji != 'week'
        assert emoji == STAR

    def test_multiple_emojis_last_wins(self):
        """If multiple non-keyword args given, last one is the emoji."""
        emoji, dlo, dhi = _parse_starboard_args(('\N{FIRE}', '\N{HEAVY BLACK HEART}'))
        assert emoji == '\N{HEAVY BLACK HEART}'

    def test_week_dge_combined_uses_max_dlo(self):
        """d>= should take max with week's dlo."""
        # Use a date far in the future to ensure it overrides week
        emoji, dlo, dhi = _parse_starboard_args(('week', 'd>=01012030'))
        dt_obj = datetime.fromtimestamp(dlo)
        assert dt_obj.year == 2030

    def test_default_emoji_override(self):
        emoji, dlo, dhi = _parse_starboard_args(('week',), default_emoji='\N{FIRE}')
        assert emoji == '\N{FIRE}'


# =====================================================================
# DB time filtering via snowflake timestamps
# =====================================================================


def _make_snowflake(year, month, day):
    """Create a Discord snowflake ID from a date. Used in tests to create
    messages at known timestamps for time-range filtering."""
    dt_obj = datetime(year, month, day, tzinfo=timezone.utc)
    ts_ms = int(dt_obj.timestamp() * 1000)
    discord_epoch_ms = 1420070400000
    snowflake = (ts_ms - discord_epoch_ms) << 22
    return str(snowflake)


class TestSnowflakeTimeFiltering:
    """Test that DB leaderboard queries correctly filter by snowflake timestamp."""

    def _setup_messages(self, db):
        """Add messages at known dates for filtering tests."""
        db.add_starboard_emoji(GUILD_A, STAR, 1, 0xffaa10)
        # Jan 2024 message
        db.add_starboard_message_v1(
            _make_snowflake(2024, 1, 15), 'sb1', GUILD_A, STAR, author_id='user1')
        db.update_starboard_star_count(_make_snowflake(2024, 1, 15), STAR, 5)
        # June 2024 message
        db.add_starboard_message_v1(
            _make_snowflake(2024, 6, 15), 'sb2', GUILD_A, STAR, author_id='user1')
        db.update_starboard_star_count(_make_snowflake(2024, 6, 15), STAR, 3)
        # Dec 2024 message
        db.add_starboard_message_v1(
            _make_snowflake(2024, 12, 1), 'sb3', GUILD_A, STAR, author_id='user2')
        db.update_starboard_star_count(_make_snowflake(2024, 12, 1), STAR, 10)
        # Feb 2025 message
        db.add_starboard_message_v1(
            _make_snowflake(2025, 2, 10), 'sb4', GUILD_A, STAR, author_id='user2')
        db.update_starboard_star_count(_make_snowflake(2025, 2, 10), STAR, 7)

    def _setup_reactors(self, db):
        """Add reactors for star-givers filtering tests."""
        db.add_reactor(_make_snowflake(2024, 1, 15), STAR, 'reactor1')
        db.add_reactor(_make_snowflake(2024, 1, 15), STAR, 'reactor2')
        db.add_reactor(_make_snowflake(2025, 2, 10), STAR, 'reactor1')

    def _ts(self, year, month, day):
        """Get unix timestamp for a date."""
        return datetime(year, month, day, tzinfo=timezone.utc).timestamp()

    # --- get_starboard_leaderboard ---

    def test_leaderboard_no_filter(self, db):
        self._setup_messages(db)
        rows = db.get_starboard_leaderboard(GUILD_A, STAR)
        assert len(rows) == 2  # user1 and user2

    def test_leaderboard_dlo_filter(self, db):
        self._setup_messages(db)
        dlo = self._ts(2024, 7, 1)
        rows = db.get_starboard_leaderboard(GUILD_A, STAR, dlo=dlo)
        # Only Dec 2024 and Feb 2025 messages (user2 has both)
        assert len(rows) == 1
        assert rows[0].author_id == 'user2'
        assert rows[0].message_count == 2

    def test_leaderboard_dhi_filter(self, db):
        self._setup_messages(db)
        dhi = self._ts(2024, 7, 1)
        rows = db.get_starboard_leaderboard(GUILD_A, STAR, dhi=dhi)
        # Only Jan 2024 and June 2024 messages (user1 has both)
        assert len(rows) == 1
        assert rows[0].author_id == 'user1'

    def test_leaderboard_range_filter(self, db):
        self._setup_messages(db)
        dlo = self._ts(2024, 6, 1)
        dhi = self._ts(2024, 12, 31)
        rows = db.get_starboard_leaderboard(GUILD_A, STAR, dlo=dlo, dhi=dhi)
        # June 2024 (user1) and Dec 2024 (user2)
        assert len(rows) == 2

    def test_leaderboard_empty_range(self, db):
        self._setup_messages(db)
        dlo = self._ts(2023, 1, 1)
        dhi = self._ts(2023, 12, 31)
        rows = db.get_starboard_leaderboard(GUILD_A, STAR, dlo=dlo, dhi=dhi)
        assert len(rows) == 0

    # --- get_starboard_star_leaderboard ---

    def test_star_leaderboard_no_filter(self, db):
        self._setup_messages(db)
        rows = db.get_starboard_star_leaderboard(GUILD_A, STAR)
        assert len(rows) == 2

    def test_star_leaderboard_dlo_filter(self, db):
        self._setup_messages(db)
        dlo = self._ts(2024, 7, 1)
        rows = db.get_starboard_star_leaderboard(GUILD_A, STAR, dlo=dlo)
        assert len(rows) == 1
        assert rows[0].author_id == 'user2'
        assert rows[0].total_stars == 17  # 10 + 7

    def test_star_leaderboard_range(self, db):
        self._setup_messages(db)
        dlo = self._ts(2024, 1, 1)
        dhi = self._ts(2024, 7, 1)
        rows = db.get_starboard_star_leaderboard(GUILD_A, STAR, dlo=dlo, dhi=dhi)
        assert len(rows) == 1
        assert rows[0].author_id == 'user1'
        assert rows[0].total_stars == 8  # 5 + 3

    # --- get_top_starboard_messages ---

    def test_top_messages_no_filter(self, db):
        self._setup_messages(db)
        rows = db.get_top_starboard_messages(GUILD_A, STAR)
        assert len(rows) == 4

    def test_top_messages_dlo_filter(self, db):
        self._setup_messages(db)
        dlo = self._ts(2025, 1, 1)
        rows = db.get_top_starboard_messages(GUILD_A, STAR, dlo=dlo)
        assert len(rows) == 1
        assert rows[0].star_count == 7

    def test_top_messages_dhi_filter(self, db):
        self._setup_messages(db)
        dhi = self._ts(2024, 2, 1)
        rows = db.get_top_starboard_messages(GUILD_A, STAR, dhi=dhi)
        assert len(rows) == 1
        assert rows[0].star_count == 5

    def test_top_messages_range(self, db):
        self._setup_messages(db)
        dlo = self._ts(2024, 6, 1)
        dhi = self._ts(2025, 1, 1)
        rows = db.get_top_starboard_messages(GUILD_A, STAR, dlo=dlo, dhi=dhi)
        assert len(rows) == 2
        # Should be sorted by star_count DESC
        assert rows[0].star_count == 10
        assert rows[1].star_count == 3

    # --- get_star_givers_leaderboard ---

    def test_star_givers_no_filter(self, db):
        self._setup_messages(db)
        self._setup_reactors(db)
        rows = db.get_star_givers_leaderboard(GUILD_A, STAR)
        assert len(rows) == 2
        # reactor1 reacted on 2 messages, reactor2 on 1
        givers = {r.user_id: r.stars_given for r in rows}
        assert givers['reactor1'] == 2
        assert givers['reactor2'] == 1

    def test_star_givers_dlo_filter(self, db):
        self._setup_messages(db)
        self._setup_reactors(db)
        dlo = self._ts(2025, 1, 1)
        rows = db.get_star_givers_leaderboard(GUILD_A, STAR, dlo=dlo)
        # Only Feb 2025 message has reactor1
        assert len(rows) == 1
        assert rows[0].user_id == 'reactor1'
        assert rows[0].stars_given == 1

    def test_star_givers_dhi_filter(self, db):
        self._setup_messages(db)
        self._setup_reactors(db)
        dhi = self._ts(2024, 2, 1)
        rows = db.get_star_givers_leaderboard(GUILD_A, STAR, dhi=dhi)
        # Only Jan 2024 message has reactor1 and reactor2
        assert len(rows) == 2

    # --- Boundary / edge cases ---

    def test_dlo_zero_means_no_bound(self, db):
        """dlo=0 should not filter anything (same as no filter)."""
        self._setup_messages(db)
        rows_all = db.get_starboard_leaderboard(GUILD_A, STAR)
        rows_zero = db.get_starboard_leaderboard(GUILD_A, STAR, dlo=0)
        assert len(rows_all) == len(rows_zero)

    def test_dhi_sentinel_means_no_bound(self, db):
        """dhi=_NO_TIME_BOUND should not filter anything."""
        from tle.util.db.starboard_db import _NO_TIME_BOUND as DB_NO_TIME_BOUND
        self._setup_messages(db)
        rows_all = db.get_starboard_leaderboard(GUILD_A, STAR)
        rows_nobound = db.get_starboard_leaderboard(GUILD_A, STAR, dhi=DB_NO_TIME_BOUND)
        assert len(rows_all) == len(rows_nobound)

    def test_exact_boundary_dlo_inclusive(self, db):
        """dlo is inclusive (>=): a message at exactly dlo should be included."""
        db.add_starboard_emoji(GUILD_A, STAR, 1, 0xffaa10)
        exact_ts = self._ts(2024, 6, 15)
        db.add_starboard_message_v1(
            _make_snowflake(2024, 6, 15), 'sb1', GUILD_A, STAR, author_id='user1')
        db.update_starboard_star_count(_make_snowflake(2024, 6, 15), STAR, 5)
        rows = db.get_top_starboard_messages(GUILD_A, STAR, dlo=exact_ts)
        assert len(rows) == 1

    def test_exact_boundary_dhi_exclusive(self, db):
        """dhi is exclusive (<): a message at exactly dhi should be excluded."""
        db.add_starboard_emoji(GUILD_A, STAR, 1, 0xffaa10)
        exact_ts = self._ts(2024, 6, 15)
        db.add_starboard_message_v1(
            _make_snowflake(2024, 6, 15), 'sb1', GUILD_A, STAR, author_id='user1')
        db.update_starboard_star_count(_make_snowflake(2024, 6, 15), STAR, 5)
        rows = db.get_top_starboard_messages(GUILD_A, STAR, dhi=exact_ts)
        assert len(rows) == 0


# =====================================================================
# snowflake_to_unix_sql correctness
# =====================================================================


class TestSnowflakeToUnixSql:
    """Verify the SQL expression correctly extracts timestamps from Discord snowflakes."""

    def test_known_snowflake(self):
        """Test with a real Discord snowflake ID."""
        conn = sqlite3.connect(':memory:')
        # Known snowflake: 1276961610195537991 -> 2024-08-24 17:49:32 UTC
        expr = snowflake_to_unix_sql('val')
        row = conn.execute(f'SELECT {expr} FROM (SELECT 1276961610195537991 AS val)').fetchone()
        ts = row[0]
        dt_obj = datetime.fromtimestamp(ts, tz=timezone.utc)
        assert dt_obj.year == 2024
        assert dt_obj.month == 8
        assert dt_obj.day == 24

    def test_roundtrip_with_make_snowflake(self):
        """A snowflake created from a date should produce that same date back."""
        conn = sqlite3.connect(':memory:')
        sf = _make_snowflake(2025, 3, 1)
        expr = snowflake_to_unix_sql('val')
        row = conn.execute(f'SELECT {expr} FROM (SELECT ? AS val)', (sf,)).fetchone()
        ts = row[0]
        dt_obj = datetime.fromtimestamp(ts, tz=timezone.utc)
        assert dt_obj.year == 2025
        assert dt_obj.month == 3
        assert dt_obj.day == 1
