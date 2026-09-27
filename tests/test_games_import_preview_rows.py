"""Reviewable Discord matches and privacy-safe ownership labels in previews."""
import json

import pytest

from tests.games_helpers import games, preview
from tle.cogs._minigame_queens_cog import _QUEENS_ANONYMOUS_LINK_MARKER


@pytest.mark.parametrize('registered', [False, True])
@pytest.mark.parametrize('named', [False, True])
def test_preview_distinguishes_own_rows_from_friends_and_unassigned_names(games, registered, named):
    if not registered:
        games.db.delete_minigame_player_link(1, 'linkedin', 10)
    own = 'Viewer LinkedIn\nYou' if named else 'You'
    before = games.db.conn.total_changes
    data = preview(games, leaderboard=(
        f'Alice LinkedIn\n0:10\n{own}\n0:12\nUnknown Player\n0:20'))
    assert games.db.conn.total_changes == before
    friend, owner, unknown = data['rows']
    assert (friend['name'], friend['discord_name'], friend['registered'], friend['is_own']) == (
        'Alice LinkedIn', 'Alice', True, False)
    expected_owner_name = 'Importer' if registered else ('Viewer LinkedIn' if named else 'You')
    assert (owner['name'], owner['discord_name'], owner['registered'], owner['is_own']) == (
        expected_owner_name, 'Mod' if registered else None, registered, True)
    assert (unknown['name'], unknown['discord_name'], unknown['registered'], unknown['is_own']) == (
        'Unknown Player', None, False, False)
    assert all(type(row['is_own']) is bool for row in data['rows'])
    serialized = json.dumps(data)
    assert 'source_user_id' not in serialized and 'tle:linkedin:you:' not in serialized


def test_anonymous_own_and_friend_rows_hide_linkedin_names_and_keep_discord_matches(games):
    games.db.set_minigame_player_link(
        1, 'linkedin', 11, 'Private Friend LinkedIn', 'private friend linkedin',
        _QUEENS_ANONYMOUS_LINK_MARKER, 1, 10)
    games.db.set_minigame_player_link(
        1, 'linkedin', 10, 'Private Owner LinkedIn', 'private owner linkedin',
        _QUEENS_ANONYMOUS_LINK_MARKER, 1, 10)
    before = games.db.conn.total_changes
    data = preview(games, leaderboard=(
        'Private Friend LinkedIn\n0:10\nPrivate Owner LinkedIn\nYou\n0:12'))
    assert games.db.conn.total_changes == before
    assert [(row['name'], row['discord_name'], row['registered'], row['is_own'])
            for row in data['rows']] == [
        ('Anonymous', 'Alice', True, False), ('Anonymous', 'Mod', True, True)]
    serialized = json.dumps(data).casefold()
    for private_value in ('private friend linkedin', 'private owner linkedin',
                          'source_user_id', 'tle:queens:anonymous', 'raw_content'):
        assert private_value not in serialized


def test_own_label_follows_registered_match_when_named_you_matches_someone_else(games):
    games.db.delete_minigame_player_link(1, 'linkedin', 10)
    data = preview(games, leaderboard='Alice LinkedIn\nYou\n0:10')
    row, = data['rows']
    assert (row['name'], row['discord_name'], row['registered'], row['is_own']) == (
        'Alice LinkedIn', 'Alice', True, False)
    assert row['no_hints'] and row['no_mistakes']
