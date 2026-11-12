"""Independent fallback thresholds, mixed headers and configuration precedence."""
import asyncio

import pytest

from tests.starboard_test_utils import GUILD_A
from tests.starboard_fallback_utils import (
    board, GASP, ELEPHANT, MESSAGE, STAR_CHANNEL, PILL_CHANNEL,
)
from tle.util.starboard_routing import STAR, PILL, fallback_board


@pytest.mark.parametrize('emoji, expected', [
    ('<:chocoGasp:123>', STAR), ('🐘', STAR), ('💀', PILL), ('☠️', PILL),
    ('🤡', PILL), ('<a:brandNewClownDance:901>', PILL),
    ('<:spaceclown:901>', PILL), ('<:Robot_Dumb_42:902>', PILL),
    ('<:reverse_pill:903>', STAR), ('<:reverseSkull:904>', STAR),
    ('<:reversepill:905>', STAR), ('<:reverse_star:906>', None),
    ('<:not_pill:907>', PILL), ('<:anti_skull:908>', PILL),
    ('<:no_pill:909>', PILL), ('<:un_skull:910>', PILL),
    ('<:nostar:911>', STAR), ('<:catSob:912>', STAR),
    ('<:RoadTrip:913>', STAR), ('<:spill:914>', STAR),
    ('<:MysteryCreature:915>', STAR), ('<:Despair:916>', STAR),
    ('1️⃣', STAR), ('not-an-emoji', None),
    ('<:flag_us:920>', PILL), ('<a:FlagVN:921>', PILL),
    ('<:flagth:922>', PILL), ('<:reverse_flag:923>', STAR),
    ('🇻🇳', PILL), ('🇺🇸', PILL), ('🇹🇭', PILL), ('🇪🇺', PILL),
    ('🏳️', PILL), ('🏴', PILL), ('🏳️‍🌈', PILL), ('🏴‍☠️', PILL),
    ('🏁', PILL), ('🚩', PILL), ('🇻', STAR),
    ('<:lying:930>', PILL), ('<:toxicpie_lying_face:931>', PILL),
    ('<a:LyingFace:932>', PILL), ('<:lyingcat:933>', PILL),
    ('🤥', PILL), ('<:reverse_lying:934>', STAR),
    ('<:flying:935>', STAR), ('<:truthful_face:936>', STAR),
])
def test_generic_name_cues(emoji, expected):
    assert fallback_board(emoji) == expected


@pytest.mark.parametrize('reactions', [
    [(ELEPHANT, [1, 2]), (PILL, [3, 4])],
    [(ELEPHANT, [1, 2]), (GASP, [3, 4])],
    [(PILL, [1, 2]), ('💀', [3, 4])],
    [(STAR, [1, 2]), (GASP, [3, 4])],
])
def test_subthreshold_groups_never_pool(board, reactions):
    h, db = board
    for emoji, users in reactions:
        h.react(emoji, users)
        asyncio.run(h.add(emoji, users[-1]))
    assert not h.channels[STAR_CHANNEL].sent
    assert not h.channels[PILL_CHANNEL].sent
    assert not db.get_starboard_entries_for_message(MESSAGE)


def test_fallback_alone_preserves_emoji_and_posts_once(board):
    h, db = board
    h.react(GASP, [1, 2, 3])
    asyncio.run(h.add(GASP, 3))
    asyncio.run(h.add(GASP, 3))
    posts = h.channels[STAR_CHANNEL].sent
    assert len(posts) == 1
    assert posts[0].content == f'{GASP} **3** | {h.message.jump_url}'
    assert db.get_starboard_message_v1(MESSAGE, STAR).star_count == 3
    assert db.resolve_alias(GUILD_A, GASP) is None
    assert db.get_starboard_entry(GUILD_A, GASP) is None


def test_mixed_header_discovered_on_first_event_and_updated_in_place(board):
    h, db = board
    h.react(STAR, [1, 2, 3])
    h.react(GASP, [1, 4, 5])
    asyncio.run(h.add(STAR, 3))
    asyncio.run(h.add(GASP, 5))
    posts = h.channels[STAR_CHANNEL].sent
    assert len(posts) == 1
    assert posts[0].content == f'{STAR} **3** {GASP} **3** | {h.message.jump_url}'
    # Header counts are independent, while leaderboards deduplicate people.
    assert db.get_starboard_message_v1(MESSAGE, STAR).star_count == 5


@pytest.mark.parametrize('emoji', ['💀', '🇻🇳', '<:flagth:922>', '<:lying:930>', '🤥'])
def test_pill_fallback_needs_four_and_posts_to_pillboard(board, emoji):
    h, db = board
    h.react(emoji, [1, 2, 3])
    asyncio.run(h.add(emoji, 3))
    assert not h.channels[PILL_CHANNEL].sent
    h.react(emoji, [1, 2, 3, 4])
    asyncio.run(h.add(emoji, 4))
    assert h.channels[PILL_CHANNEL].sent[0].content.startswith(f'{emoji} **4** |')
    assert not h.channels[STAR_CHANNEL].sent
    assert db.get_starboard_message_v1(MESSAGE, PILL).star_count == 4


def test_reverse_only_routes_to_starboard(board):
    h, _ = board
    reverse = '<:reverse_pill:99>'
    h.react(reverse, [1, 2, 3])
    asyncio.run(h.add(reverse, 3))
    assert h.channels[STAR_CHANNEL].sent[0].content.startswith(reverse)
    assert not h.channels[PILL_CHANNEL].sent


def test_existing_aliases_still_pool_but_fallbacks_do_not(board):
    h, db = board
    db.add_starboard_alias(GUILD_A, '👍', STAR)
    h.react(STAR, [1, 2])
    h.react('👍', [2, 3])
    asyncio.run(h.add('👍', 3))
    assert h.channels[STAR_CHANNEL].sent[0].content.startswith('⭐ **3** |')
    assert db.get_starboard_message_v1(MESSAGE, STAR).star_count == 3


def test_explicit_alias_overrides_heuristic(board):
    h, db = board
    db.add_starboard_alias(GUILD_A, GASP, PILL)
    h.react(GASP, [1, 2, 3, 4])
    asyncio.run(h.add(GASP, 4))
    assert not h.channels[STAR_CHANNEL].sent
    assert h.channels[PILL_CHANNEL].sent[0].content.startswith('💊 **4** |')


def test_explicit_emoji_without_channel_does_not_fall_through(board):
    h, db = board
    db.add_starboard_emoji(GUILD_A, GASP, 3, 0)
    h.react(GASP, [1, 2, 3, 4])
    asyncio.run(h.add(GASP, 4))
    assert not h.channels[STAR_CHANNEL].sent
    # Also exclude it from discovery when a different reaction arrives.
    h.react(STAR, [5])
    asyncio.run(h.add(STAR, 5))
    assert not h.channels[STAR_CHANNEL].sent


def test_missing_destination_does_not_redirect_to_other_board(board):
    h, db = board
    db.clear_starboard_channel(GUILD_A, PILL)
    h.react('💀', [1, 2, 3, 4])
    asyncio.run(h.add('💀', 4))
    assert not h.channels[STAR_CHANNEL].sent
    assert not h.channels[PILL_CHANNEL].sent


def test_reads_configured_threshold_instead_of_hardcoding(board):
    h, db = board
    db.update_starboard_threshold(GUILD_A, STAR, 5)
    h.react(GASP, [1, 2, 3, 4])
    asyncio.run(h.add(GASP, 4))
    assert not h.channels[STAR_CHANNEL].sent
    h.react(GASP, [1, 2, 3, 4, 5])
    asyncio.run(h.add(GASP, 5))
    assert len(h.channels[STAR_CHANNEL].sent) == 1


def test_removals_and_restart_keep_separate_counts(board):
    h, db = board
    h.react(GASP, [1, 2, 3])
    asyncio.run(h.add(GASP, 3))
    h.restart()
    h.react(GASP, [1, 2])
    asyncio.run(h.remove(GASP, 3))
    assert h.channels[STAR_CHANNEL].sent[0].content == h.message.jump_url
    assert db.get_starboard_message_v1(MESSAGE, STAR).star_count == 2
    h.react(GASP, [1])
    asyncio.run(h.remove(GASP, 2))
    assert h.channels[STAR_CHANNEL].sent[0].content == h.message.jump_url


def test_concurrent_qualifying_emojis_create_one_post(board):
    h, _ = board
    h.react(GASP, [1, 2, 3])
    h.react(ELEPHANT, [4, 5, 6])

    async def run():
        await asyncio.gather(h.add(GASP, 3), h.add(ELEPHANT, 6))

    asyncio.run(run())
    assert len(h.channels[STAR_CHANNEL].sent) == 1


def test_fallback_in_board_channel_does_not_repost_board_chatter(board):
    h, db = board
    asyncio.run(h.add(GASP, 3, channel=STAR_CHANNEL, message=900))
    assert not h.channels[STAR_CHANNEL].sent
    assert db.get_reactor_count(900, GASP) == 0
