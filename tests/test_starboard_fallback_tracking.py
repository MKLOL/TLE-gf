"""Fallback proxy reactions, persistence, statistics, repairs and rendering."""
import asyncio
import sqlite3

from tests.starboard_fallback_utils import (
    board, GASP, ELEPHANT, MESSAGE, STAR_CHANNEL, PILL_CHANNEL,
)
from tests.starboard_test_utils import GUILD_A, GUILD_B, _FakeAttachment
from tle.cogs._starboard_render import _starboard_content
from tle.util.db.user_db_conn import namedtuple_factory
from tle.util.starboard_routing import STAR, PILL


def test_proxy_deduplicates_same_emoji_and_survives_physical_removal(board):
    h, db = board
    h.react(GASP, [1, 2, 3])
    asyncio.run(h.add(GASP, 3))
    post = h.channels[STAR_CHANNEL].sent[0]
    asyncio.run(h.add(GASP, 3, channel=STAR_CHANNEL, message=post.id))
    assert post.content.startswith(f'{GASP} **3** |')
    h.react(GASP, [1, 2])
    asyncio.run(h.remove(GASP, 3))
    assert post.content.startswith(f'{GASP} **3** |')
    asyncio.run(h.remove(GASP, 3, channel=STAR_CHANNEL, message=post.id))
    assert post.content == h.message.jump_url
    assert db.get_starboard_message_v1(MESSAGE, STAR).star_count == 2


def test_proxy_fallback_can_put_original_on_other_board(board):
    h, db = board
    h.react(GASP, [1, 2, 3])
    asyncio.run(h.add(GASP, 3))
    star_post = h.channels[STAR_CHANNEL].sent[0]
    for user in (1, 2, 3):
        asyncio.run(h.add('💀', user, channel=STAR_CHANNEL, message=star_post.id))
    assert not h.channels[PILL_CHANNEL].sent
    asyncio.run(h.add('💀', 4, channel=STAR_CHANNEL, message=star_post.id))
    pill_post = h.channels[PILL_CHANNEL].sent[0]
    assert pill_post.content == f'💀 **4** | {h.message.jump_url}'
    assert db.get_starboard_message_v1(MESSAGE, PILL).star_count == 4
    assert not db.get_starboard_entries_for_message(star_post.id)
    asyncio.run(h.remove('💀', 4, channel=STAR_CHANNEL, message=star_post.id))
    assert pill_post.content == h.message.jump_url


def test_stats_count_fallback_users_once_and_keep_sticky_self_reaction(board):
    h, db = board
    h.react(GASP, [1, 2, 42])
    h.react(ELEPHANT, [1, 3, 42])
    asyncio.run(h.add(GASP, 42))
    assert db.get_starboard_message_v1(MESSAGE, STAR).star_count == 4
    assert db.get_starboard_leaderboard(GUILD_A, STAR)[0].message_count == 1
    assert db.get_starboard_star_leaderboard(GUILD_A, STAR)[0].total_stars == 4
    assert {row.user_id: row.stars_given for row in db.get_star_givers_leaderboard(
        GUILD_A, STAR)} == {'1': 1, '2': 1, '3': 1, '42': 1}
    marks = db.get_narcissus_leaderboard(GUILD_A, STAR)
    assert [(row.user_id, row.self_stars, row.unreacted) for row in marks] == [('42', 1, 0)]
    h.react(GASP, [1, 2])
    h.react(ELEPHANT, [1, 3])
    asyncio.run(h.remove(GASP, 42))
    asyncio.run(h.remove(ELEPHANT, 42))
    marks = db.get_narcissus_leaderboard(GUILD_A, STAR)
    assert [(row.self_stars, row.unreacted) for row in marks] == [(1, 1)]


def test_repair_resyncs_fallbacks_but_keeps_proxy_rows(board):
    h, db = board
    h.react(GASP, [1, 2, 3])
    asyncio.run(h.add(GASP, 3))
    post = h.channels[STAR_CHANNEL].sent[0]
    asyncio.run(h.add(GASP, 4, channel=STAR_CHANNEL, message=post.id))
    h.react(GASP, [1])  # Two physical removals were missed while offline.
    counts, total = asyncio.run(h.cog._sync_board_reactions(
        h.message, GUILD_A, STAR, force=True))
    assert counts == [(GASP, 2)]
    assert total == 2
    assert db.get_proxy_reactors(MESSAGE, GASP) == ['4']


def test_reopened_database_and_new_cog_retain_fallback_identity(board, tmp_path):
    h, db = board
    h.react(GASP, [1, 2, 3])
    asyncio.run(h.add(GASP, 3))
    connection = sqlite3.connect(tmp_path / 'reopened.db')
    db.conn.backup(connection)
    db.conn.close()
    connection.row_factory = namedtuple_factory
    db.conn = connection
    h.restart()
    h.react(GASP, [1, 2, 3, 4])
    asyncio.run(h.add(GASP, 4))
    assert len(h.channels[STAR_CHANNEL].sent) == 1
    assert h.channels[STAR_CHANNEL].sent[0].content.startswith(f'{GASP} **4** |')


def test_other_guild_configuration_does_not_change_this_guild_routing(board):
    h, db = board
    db.add_starboard_emoji(GUILD_B, GASP, 100, 0)
    db.add_starboard_alias(GUILD_B, ELEPHANT, PILL)
    h.react(GASP, [1, 2, 3])
    h.react(ELEPHANT, [1, 2, 3])
    asyncio.run(h.add(GASP, 3))
    assert f'{GASP} **3**' in h.channels[STAR_CHANNEL].sent[0].content
    assert f'{ELEPHANT} **3**' in h.channels[STAR_CHANNEL].sent[0].content


def test_video_header_retains_author_and_separate_counts(board):
    h, _ = board
    h.message.attachments = [_FakeAttachment('clip.mp4', 'https://example.test/clip.mp4')]
    h.react(GASP, [1, 2, 3])
    h.react(STAR, [1, 2, 3])
    asyncio.run(h.add(GASP, 3))
    content = h.channels[STAR_CHANNEL].sent[0].content
    assert content.startswith(f'{STAR} **3** {GASP} **3** · **')
    assert h.message.author.display_name in content
    assert content.endswith(h.message.jump_url)


def test_large_header_keeps_complete_emojis_and_jump_url():
    counts = [(f'<:{"a" * 32}:{10**19 + i}>', 123) for i in range(30)]
    url = 'https://discord.com/channels/1/2/3'
    content = _starboard_content(STAR, 123, url, counts)
    assert len(content) < 2000
    assert content.endswith(url)
    assert 'more)' in content
    assert content.count('<:') == content.count('>')


def test_fast_update_path_keeps_fallback_header(board, monkeypatch):
    from tle.cogs import _starboard_core
    h, _ = board
    monkeypatch.setattr(_starboard_core, 'FULL_RE_RENDER', False)
    h.react(GASP, [1, 2, 3])
    asyncio.run(h.add(GASP, 3))
    h.react(GASP, [1, 2, 3, 4])
    asyncio.run(h.add(GASP, 4))
    assert h.channels[STAR_CHANNEL].sent[0].content.startswith(f'{GASP} **4** |')


def test_board_deletion_cleans_fallback_reactors_without_touching_other_board(board):
    h, db = board
    h.react(GASP, [1, 2, 3])
    h.react('💀', [1, 2, 3, 4])
    asyncio.run(h.add(GASP, 3))
    asyncio.run(h.add('💀', 4))
    db.remove_starboard_emoji(GUILD_A, STAR)
    assert db.get_reactors(MESSAGE, GASP) == []
    assert len(db.get_reactors(MESSAGE, '💀')) == 4
    assert db.get_starboard_message_v1(MESSAGE, PILL) is not None
