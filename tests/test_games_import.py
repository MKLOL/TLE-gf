from dataclasses import replace

import pytest

from tests.games_helpers import games, preview, confirm
from tle.util.complaints import ComplaintError


def test_preview_has_no_writes_and_confirm_reuses_import_and_ratings(games):
    before = games.db.conn.total_changes
    data = preview(games)
    assert games.db.conn.total_changes == before
    assert data['puzzle_number'] == 697
    assert [row['name'] for row in data['rows']] == ['Alice LinkedIn', 'Importer', 'Unknown Player']
    assert [row['registered'] for row in data['rows']] == [True, True, False]
    assert data['rows'][0]['no_mistakes'] and data['rows'][1]['no_mistakes']
    receipt = confirm(games, data)
    assert receipt['registered'] == 2 and receipt['unresolved'] == 1
    rows = games.db.get_minigame_results_for_guild(1, 'tango')
    assert sorted((row.user_id, row.time_seconds) for row in rows) == [('10', 12), ('11', 10)]
    assert len(games.db.get_minigame_ratings(1, 'tango')) == 2
    before = games.db.conn.total_changes
    assert confirm(games, data) == receipt
    assert games.db.conn.total_changes == before
    result = confirm(games, preview(games))
    assert result['registered'] == result['unresolved'] == 0
    assert result['unchanged'] == 3


def test_only_the_explicit_you_row_gets_clean_badges(games):
    data = preview(games, leaderboard='Alice LinkedIn\n🤓 No hints!\n0:10\nYou\n0:12\nUnknown Player\n0:20')
    assert [(row['no_hints'], row['no_mistakes']) for row in data['rows']] == [
        (True, False), (True, True), (False, False)]
    confirm(games, data)
    rows = games.db.get_minigame_results_for_guild(1, 'tango')
    assert {(row.user_id, row.accuracy, row.is_perfect) for row in rows} == {
        ('10', 100, 1), ('11', 0, 0)}
    data = preview(games, leaderboard='Importer\n0:12')
    assert not data['rows'][0]['no_hints'] and not data['rows'][0]['no_mistakes']


def test_rendered_badge_variants_keep_hints_and_mistakes_independent(games):
    data = preview(games, leaderboard=(
        'You\n0:29\n\nPlayer 1\n🤓💎 No hints & no mistakes!\n0:10\n\n'
        'Player 2\n🤓 No hints!\n0:27\n\nPlayer 3\n1:57\n\n'
        'Player 4\n0:38\n\nPlayer 5\n💎 No mistakes!\n0:38'))
    flags = {row['name']: (row['no_hints'], row['no_mistakes']) for row in data['rows']}
    assert flags == {'Importer': (True, True), 'Player 1': (True, True),
                     'Player 2': (True, False), 'Player 3': (False, False),
                     'Player 4': (False, False), 'Player 5': (False, True)}
    assert confirm(games, data)['unresolved'] == 5


def test_same_moderator_token_covers_all_games_and_future_catalog_entries(games):
    assert len(games.service.catalog(games.guild, games.member)['games']) == 2
    for game in ('queens', 'tango'):
        assert confirm(games, preview(games, game=game))['registered'] == 2
    game = replace(games.cog.GAMES['tango'], name='future', display_name='LinkedIn Future')
    games.cog.GAMES = {**games.cog.GAMES, 'future': game}
    assert games.service.catalog(games.guild, games.member)['games'][-1]['can_import']
    games.db.set_minigame_channel(1, 'future', 20)
    assert confirm(games, preview(games, game='future'))['registered'] == 2
    assert games.db.get_minigame_results_for_guild(1, 'queens')
    assert games.db.get_minigame_results_for_guild(1, 'tango')


@pytest.mark.parametrize('field,value', [
    ('game', 'akari'), ('game', []), ('puzzle_date', '26092026'),
    ('puzzle_date', None), ('puzzle_date', '3000-01-01'), ('puzzle_date', '1900-01-01'),
    ('puzzle_number', 1), ('puzzle_number', True), ('leaderboard', []),
    ('leaderboard', ''), ('leaderboard', 'x' * 12001),
    ('leaderboard', 'You\n0:10\nYou\n0:11'),
    ('leaderboard', 'Alice LinkedIn\n0:10\nAlice LinkedIn\n0:11'),
])
def test_invalid_preview(games, field, value):
    with pytest.raises(ComplaintError) as exc:
        preview(games, **{field: value})
    assert exc.value.status == 400
    assert games.db.get_minigame_results_for_guild(1, 'tango') == []


def test_preview_ownership_and_expiration(games):
    data = preview(games)
    _, raw = games.db.create_games_token(1, 10)
    other = games.db.authenticate_games_token(raw)
    with pytest.raises(ComplaintError, match='expired or unavailable'):
        games.service.confirm(other, games.guild, games.member, data['preview_id'])
    games.service.pending[data['preview_id']]['expires_at'] = 0
    with pytest.raises(ComplaintError, match='expired or unavailable'):
        confirm(games, data)


def test_new_preview_replaces_old_unconfirmed_preview(games):
    old = preview(games)
    new = preview(games)
    with pytest.raises(ComplaintError):
        confirm(games, old)
    assert confirm(games, new)['registered'] == 2


def test_recheck_roles_and_enabled_game_before_confirm(games):
    data = preview(games)
    games.member.roles = []
    with pytest.raises(ComplaintError, match='access required'):
        confirm(games, data)
    games.db.set_guild_config(1, 'tango_admin_user_ids', '["10"]')
    with pytest.raises(ComplaintError, match='access required'):
        preview(games, game='queens')
    games.db.set_guild_config(1, 'tango', '0')
    with pytest.raises(ComplaintError, match='not enabled'):
        confirm(games, data)


def test_registration_change_requires_new_confirmation(games):
    data = preview(games)
    games.db.set_minigame_player_link(1, 'linkedin', 11, 'Renamed', 'renamed', None, 1, 10)
    with pytest.raises(ComplaintError, match='changed') as exc:
        confirm(games, data)
    assert exc.value.status == 409
    assert games.db.get_minigame_results_for_guild(1, 'tango') == []


def test_unregistered_importer_is_rejected(games):
    games.db.conn.execute('DELETE FROM minigame_player_link WHERE user_id = ?', ('10',))
    games.db.conn.commit()
    with pytest.raises(ComplaintError, match='Register the importer'):
        preview(games)


def test_names_containing_hint_are_not_silently_lost(games):
    data = preview(games, leaderboard='Chintan Shah\n🤓 No hints!\n0:10\nShintaro Sato\n0:12\nYou\n0:13')
    assert [row['name'] for row in data['rows']] == ['Chintan Shah', 'Shintaro Sato', 'Importer']
    assert data['rows'][0]['no_hints'] and not data['rows'][1]['no_hints']


def test_per_result_rating_decision_is_shown_and_rechecked(games):
    confirm(games, preview(games))
    games.db.set_minigame_unresolved_result_rating(1, 'tango', 'alice linkedin', 697, False)
    data = preview(games)
    assert data['rows'][0]['rated'] is False
    games.db.set_minigame_unresolved_result_rating(1, 'tango', 'alice linkedin', 697, True)
    with pytest.raises(ComplaintError, match='changed'):
        confirm(games, data)


def test_bans_and_channel_changes_invalidate_preview(games):
    data = preview(games)
    games.db.ban_minigame_user(1, 'tango', 11, 1, 10)
    with pytest.raises(ComplaintError, match='changed'):
        confirm(games, data)
    data = preview(games)
    assert data['skipped'] == 1
    games.db.set_minigame_channel(1, 'tango', 21)
    with pytest.raises(ComplaintError, match='changed'):
        confirm(games, data)
