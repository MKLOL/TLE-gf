"""Unlinked moderators retain every imported row without inventing identities."""
from types import SimpleNamespace

import pytest

from tests.games_helpers import games, preview, confirm, payload
from tests.minigames_test_utils import _FakeDiscordMember
from tle.util.complaints import ComplaintError


def unlink_importer(env):
    env.db.delete_minigame_player_link(1, 'linkedin', 10)


def board(named=True, unknown_time='0:20'):
    own = 'Viewer LinkedIn\nYou' if named else 'You'
    return (f'Alice LinkedIn\n🤓 No hints!\n0:10\n{own}\n0:12\n'
            f'Unknown Player\n{unknown_time}')


def pending_key(user_id=10):
    from tle.cogs._minigame_queens_cog import _queens_unassigned_source_name
    return _queens_unassigned_source_name(user_id)


def sources(env, game):
    return {row.normalized_name: row for row in
            env.db.get_minigame_unresolved_results_for_guild(1, game)}


def register_owner(env, name='Viewer LinkedIn'):
    return env.cog._save_queens_registration_link(
        1, env.cog.GAMES['queens'], 10, name, name.casefold(), None, 10)


@pytest.mark.parametrize('game', ['queens', 'tango'])
@pytest.mark.parametrize('named', [False, True])
def test_unregistered_importer_saves_every_row_and_updates_only_changed_row(games, game, named):
    unlink_importer(games)
    before = games.db.conn.total_changes
    data = preview(games, game=game, leaderboard=board(named))
    assert games.db.conn.total_changes == before
    assert (data['registered'], data['unresolved'], data['skipped']) == (1, 2, 0)
    assert len(data['rows']) == 3
    assert [row['registered'] for row in data['rows']] == [True, False, False]
    assert [(row['no_hints'], row['no_mistakes']) for row in data['rows']] == [
        (True, False), (True, True), (False, False)]
    receipt = confirm(games, data)
    assert (receipt['registered'], receipt['unresolved'], receipt['unchanged']) == (1, 2, 0)
    own_key = 'viewer linkedin' if named else pending_key()
    saved = sources(games, game)
    assert set(saved) == {'alice linkedin', own_key, 'unknown player'}
    assert saved[own_key].external_name == ('Viewer LinkedIn' if named else 'You')
    assert (saved[own_key].accuracy, saved[own_key].is_perfect) == (100, 1)
    assert (saved['alice linkedin'].accuracy, saved['alice linkedin'].is_perfect) == (0, 0)
    assert [(row.user_id, row.time_seconds) for row in
            games.db.get_minigame_results_for_guild(1, game)] == [('11', 10)]
    assert games.db.get_minigame_player_link(1, 'linkedin', 10) is None

    before = games.db.conn.total_changes
    assert confirm(games, data) == receipt
    assert games.db.conn.total_changes == before
    duplicate = confirm(games, preview(games, game=game, leaderboard=board(named)))
    assert (duplicate['registered'], duplicate['unresolved'], duplicate['unchanged']) == (0, 0, 3)
    updated = confirm(games, preview(games, game=game, leaderboard=board(named, '0:21')))
    assert (updated['registered'], updated['unresolved'], updated['unchanged']) == (0, 1, 2)
    current = sources(games, game)
    assert current['alice linkedin'] == saved['alice linkedin']
    assert current[own_key] == saved[own_key]
    assert current['unknown player'].time_seconds == 21


def test_unregistered_importer_without_you_row_can_import_normal_names(games):
    unlink_importer(games)
    data = preview(games, leaderboard='Alice LinkedIn\n0:10\nUnknown Player\n0:20')
    assert (data['registered'], data['unresolved'], data['skipped']) == (1, 1, 0)
    assert all(not row['no_hints'] and not row['no_mistakes'] for row in data['rows'])
    confirm(games, data)
    assert set(sources(games, 'tango')) == {'alice linkedin', 'unknown player'}


def test_named_you_matches_existing_name_without_importer_link(games):
    unlink_importer(games)
    data = preview(games, leaderboard='Alice LinkedIn\nYou\n0:10\nUnknown Player\n0:20')
    assert (data['registered'], data['unresolved']) == (1, 1)
    assert data['rows'][0]['registered'] and data['rows'][0]['no_mistakes']
    confirm(games, data)
    row = games.db.get_minigame_result_for_user_puzzle(1, 'tango', 11, 697)
    assert row is not None and row.time_seconds == 10 and row.is_perfect
    assert games.db.get_minigame_result_for_user_puzzle(1, 'tango', 10, 697) is None


@pytest.mark.parametrize('game', ['queens', 'tango'])
def test_bare_you_is_scoped_to_owner_across_tokens(games, game):
    unlink_importer(games)
    confirm(games, preview(games, game=game, leaderboard='You\n0:12'))
    other_member = _FakeDiscordMember(12, 'other-mod', 'Other Mod',
                                      roles=[SimpleNamespace(name='Moderator')])
    games.guild.members.append(other_member)
    _, raw = games.db.create_games_token(1, 12)
    other_token = games.db.authenticate_games_token(raw)
    data = games.service.preview(other_token, games.guild, other_member,
                                 payload(game, leaderboard='You\n0:14'))
    games.service.confirm(other_token, games.guild, other_member, data['preview_id'])
    saved = sources(games, game)
    assert set(saved) == {pending_key(10), pending_key(12)}
    assert (saved[pending_key(10)].time_seconds, saved[pending_key(12)].time_seconds) == (12, 14)
    assert all(row.external_name == 'You' for row in saved.values())
    assert games.db.get_minigame_results_for_guild(1, game) == []
    _, raw = games.db.create_games_token(1, 10)
    renewed = games.db.authenticate_games_token(raw)
    data = games.service.preview(renewed, games.guild, games.member,
                                 payload(game, leaderboard='You\n0:12'))
    result = games.service.confirm(renewed, games.guild, games.member, data['preview_id'])
    assert (result['registered'], result['unresolved'], result['unchanged']) == (0, 0, 1)
    assert sources(games, game) == saved


@pytest.mark.parametrize('named', [False, True])
def test_registration_claims_both_games_and_preserves_source_decisions(games, named):
    unlink_importer(games)
    own_key = 'viewer linkedin' if named else pending_key()
    for game in ('queens', 'tango'):
        confirm(games, preview(games, game=game, leaderboard=board(named)))
    tango = sources(games, 'tango')[own_key]
    games.db.set_minigame_unresolved_result_rating(1, 'tango', own_key, tango.puzzle_number, False)
    before = {game: sources(games, game)[own_key] for game in ('queens', 'tango')}
    assert register_owner(games) == 2
    for game in ('queens', 'tango'):
        saved = sources(games, game)
        assert set(saved) == {'alice linkedin', 'viewer linkedin', 'unknown player'}
        own = saved['viewer linkedin']
        assert own.external_name == 'Viewer LinkedIn'
        for field in ('time_seconds', 'accuracy', 'is_perfect', 'is_rated', 'stored_at',
                      'raw_content', 'source_message_id', 'rating_override'):
            assert getattr(own, field) == getattr(before[game], field)
        materialized = games.db.get_minigame_result_for_user_puzzle(1, game, 10, own.puzzle_number)
        if game == 'queens':
            assert materialized is not None and materialized.time_seconds == 12
        else:
            assert materialized is None


@pytest.mark.parametrize('game', ['queens', 'tango'])
def test_unlinked_owners_optout_applies_to_bare_you_before_and_after_registration(games, game):
    unlink_importer(games)
    games.db.optout_minigame_user(1, game, 10, 1)
    data = preview(games, game=game, leaderboard='You\n0:12')
    assert data['unresolved'] == 1 and not data['rows'][0]['rated']
    confirm(games, data)
    assert not sources(games, game)[pending_key()].is_rated
    register_owner(games)
    source = sources(games, game)['viewer linkedin']
    assert not source.is_rated
    assert games.db.get_minigame_result_for_user_puzzle(1, game, 10, source.puzzle_number) is None


@pytest.mark.parametrize('change', ['registration', 'named_registration', 'ban', 'own_optout', 'friend_optout'])
def test_unregistered_preview_revalidates_identity_bans_and_optouts(games, change):
    unlink_importer(games)
    data = preview(games, leaderboard=board(named=change == 'named_registration'))
    if change == 'registration':
        register_owner(games)
    elif change == 'named_registration':
        games.db.set_minigame_player_link(1, 'linkedin', 12, 'Viewer LinkedIn',
                                         'viewer linkedin', None, 1, 10)
    elif change == 'ban':
        games.db.ban_minigame_user(1, 'tango', 11, 1, 10)
    elif change == 'own_optout':
        games.db.optout_minigame_user(1, 'tango', 10, 1)
    else:
        games.db.optout_minigame_user(1, 'tango', 11, 1, 'alice linkedin')
    with pytest.raises(ComplaintError, match='changed') as exc:
        confirm(games, data)
    assert exc.value.status == 409
    assert sources(games, 'tango') == {}
    assert games.db.get_minigame_results_for_guild(1, 'tango') == []


def test_client_cannot_write_or_claim_another_owners_pending_identity(games):
    from tle.cogs._minigame_helpers import MinigameCogError
    unlink_importer(games)
    confirm(games, preview(games, leaderboard='You\n0:12'))
    before = sources(games, 'tango')
    with pytest.raises(ComplaintError, match='NUL'):
        preview(games, leaderboard=f'{pending_key()}\n0:01')
    ctx = SimpleNamespace(guild=games.guild, author=games.member)
    with pytest.raises(MinigameCogError, match='NUL'):
        games.cog._cmd_queens_register_link(
            ctx, games.cog.GAMES['queens'], games.member, pending_key())
    with pytest.raises(ValueError):
        games.cog._parse_queens_backfill_entry(games.cog.GAMES['tango'], {
            'linkedin_name': pending_key(), 'puzzle_number': 697,
            'time_seconds': 1,
        })
    raw_result = SimpleNamespace(
        raw_content=f'Tango #697 | 0:01\n{pending_key()}\n0:01',
        accuracy=0, is_perfect=0, time_seconds=1)
    assert games.cog._legacy_queens_raw_source_identity(raw_result) is None
    assert sources(games, 'tango') == before
    assert games.db.get_minigame_player_link(1, 'linkedin', 10) is None


def test_registration_keeps_existing_discord_score_when_claiming_pending_you(games):
    unlink_importer(games)
    games.db.save_minigame_result(
        55, 1, 'tango', 20, 10, 697, '2026-09-04', 100, 25, True,
        'Tango #697 | 0:25')
    confirm(games, preview(games, leaderboard='You\n0:12'))
    register_owner(games)
    source = sources(games, 'tango')['viewer linkedin']
    assert source.time_seconds == 25 and source.source_message_id == '55'
    assert pending_key() not in sources(games, 'tango')
    row = games.db.get_minigame_result_for_user_puzzle(1, 'tango', 10, 697)
    assert row.time_seconds == 25 and row.message_id == '55'


def test_unlinked_named_you_honors_existing_name_optout(games):
    unlink_importer(games)
    games.db.optout_minigame_user(1, 'tango', 12, 1, 'viewer linkedin')
    data = preview(games, leaderboard='Viewer LinkedIn\nYou\n0:12')
    assert not data['rows'][0]['rated']
    confirm(games, data)
    assert not sources(games, 'tango')['viewer linkedin'].is_rated
