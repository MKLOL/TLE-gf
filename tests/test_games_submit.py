import asyncio
import datetime as dt
from types import SimpleNamespace
from unittest.mock import AsyncMock

import discord
import pytest

from tests.games_helpers import games
from tle.util.complaints import ComplaintError
from tle.util.games_submit import GamesSubmissionService


@pytest.fixture
def personal(games, monkeypatch):
    monkeypatch.setattr(discord, 'Thread', type('Thread', (), {}), raising=False)
    monkeypatch.setattr(discord, 'AllowedMentions', SimpleNamespace(none=lambda: None), raising=False)
    monkeypatch.setattr(discord, 'utils', SimpleNamespace(escape_markdown=lambda text: text, escape_mentions=lambda text: text), raising=False)
    games.member.roles = []
    games.guild.me = SimpleNamespace(id=99)
    permissions = SimpleNamespace(view_channel=True, send_messages=True)
    channel = SimpleNamespace(id=20, permissions_for=lambda who: permissions,
                              send=AsyncMock(return_value=SimpleNamespace(id=100)))
    games.guild.get_channel_or_thread = lambda cid: channel if cid == 20 else None
    games.db.set_guild_config(1, 'akari', '1')
    games.db.set_minigame_channel(1, 'akari', 20)
    games.submissions = GamesSubmissionService(games.service)
    games.channel, games.permissions = channel, permissions
    games.reauth = AsyncMock()
    return games


def body(game='akari', **kw):
    return {'game': game, 'puzzle_date': '2026-09-26',
            'puzzle_number': 629 if game == 'akari' else 719,
            'time_seconds': 39, 'accuracy': 100, 'is_perfect': True, **kw}


def submit(env, **kw):
    return asyncio.run(env.submissions.submit(env.guild, env.member, body(**kw), reauthenticate=env.reauth))


def result(env, game='akari'):
    return env.db.get_minigame_result_for_user_puzzle(1, game, 10, 629 if game == 'akari' else 719)


@pytest.mark.parametrize(('accuracy', 'perfect'), [(100, True), (87, False), (100, False), (0, False)])
def test_akari_preserves_every_ranking_field(personal, accuracy, perfect):
    posted = submit(personal, accuracy=accuracy, is_perfect=perfect)
    assert posted['posted'] and posted['registered'] and not posted['duplicate']
    row = result(personal)
    assert (row.puzzle_number, row.puzzle_date, row.accuracy, row.is_perfect, row.time_seconds) == (
        629, '2026-09-26', accuracy, perfect, 39)
    from tle.cogs._minigame_akari import parse_akari_message
    parsed = parse_akari_message(personal.channel.send.call_args.args[0])[0]
    assert (parsed.accuracy, parsed.is_perfect, parsed.time_seconds) == (accuracy, perfect, 39)
    assert personal.channel.send.call_args.kwargs['nonce']
    assert len(personal.db.get_minigame_results_for_guild(1, 'akari')) == 1


def test_same_rankings_as_manual_shares(personal):
    from tle.cogs._minigame_akari import parse_akari_message
    from tle.cogs._minigame_common import default_score_matchup
    from tle.util.akari_beta_rating import _akari_beta_pair_score
    from tle.util.akari_weekly import result_performance
    submit(personal, accuracy=96, is_perfect=False)
    row = result(personal)
    manual = parse_akari_message('Daily Akari 😊 629\n2026-09-26\n🎯 96%  🕓 0:39')[0]
    opponent = parse_akari_message('Daily Akari 😊 629\n2026-09-26\n🌟 Perfect!  🕓 1:30')[0]
    assert default_score_matchup(row, opponent) == default_score_matchup(manual, opponent)
    assert _akari_beta_pair_score(row, opponent) == _akari_beta_pair_score(manual, opponent)
    assert result_performance(row) == result_performance(manual)


@pytest.mark.parametrize(('game', 'number'), [('tango', 719), ('queens', 879)])
def test_regular_member_self_linkedin_clean_and_no_import(personal, game, number):
    submit(personal, game=game, puzzle_number=number)
    row = personal.db.get_minigame_result_for_user_puzzle(1, game, 10, number)
    assert row.user_id == '10' and row.accuracy == 100 and row.is_perfect
    with pytest.raises(ComplaintError, match='role required'):
        personal.service.context(personal.guild, personal.member, 'tango')
    assert not any(game['can_import'] for game in personal.service.catalog(personal.guild, personal.member)['games'])


def test_duplicate_retries_and_service_restart_post_once(personal):
    first = submit(personal)
    personal.submissions = GamesSubmissionService(personal.service)
    second = submit(personal)
    assert second['duplicate'] and first['message_url'] == second['message_url']
    assert personal.channel.send.await_count == 1
    with pytest.raises(ComplaintError, match='different personal'):
        submit(personal, time_seconds=40)
    assert result(personal).time_seconds == 39


@pytest.mark.parametrize('changes', [
    {'accuracy': 99, 'is_perfect': True}, {'accuracy': None}, {'accuracy': True},
    {'time_seconds': -1}, {'time_seconds': True}, {'time_seconds': 0.5},
    {'puzzle_number': 630}, {'puzzle_date': '2099-01-01'}, {'puzzle_date': 1},
    {'is_perfect': 1}, {'game': ['akari']}, {'game': 'pinpoint'},
])
def test_invalid_self_payloads_do_not_post(personal, changes):
    with pytest.raises(ComplaintError):
        submit(personal, **changes)
    personal.channel.send.assert_not_awaited()


@pytest.mark.parametrize('game', ['akari', 'tango'])
def test_game_bans_are_enforced(personal, game):
    if game == 'akari':
        personal.db.conn.execute("INSERT INTO akari_ban (guild_id,user_id,banned_at,banned_by) VALUES ('1','10',1,'99')")
    else:
        personal.db.conn.execute("INSERT INTO minigame_ban (guild_id,game,user_id,banned_at,banned_by) VALUES ('1','tango','10',1,'99')")
    with pytest.raises(ComplaintError, match='banned'):
        submit(personal, game=game)
    personal.channel.send.assert_not_awaited()


def test_channel_permissions_and_token_revalidation(personal):
    personal.permissions.send_messages = False
    with pytest.raises(ComplaintError, match='view and post'):
        submit(personal)
    personal.permissions.send_messages = True
    personal.reauth.side_effect = ComplaintError(401, 'revoked')
    with pytest.raises(ComplaintError, match='revoked'):
        submit(personal)
    personal.channel.send.assert_not_awaited()


def test_ambiguous_send_recovers_nonce_without_second_post(personal):
    async def history(**kw):
        row = personal.db.get_games_submission(1, 10, 'akari', 629)
        yield SimpleNamespace(id=100, nonce=row.nonce, author=personal.guild.me)
    personal.channel.history = history
    personal.channel.send.side_effect = asyncio.TimeoutError()
    with pytest.raises(ComplaintError, match='uncertain'):
        submit(personal)
    assert result(personal) is None
    posted = submit(personal)
    assert posted['registered'] and personal.channel.send.await_count == 1


def test_crash_after_post_recovers_score(personal, monkeypatch):
    ingest = personal.cog._ingest_message
    monkeypatch.setattr(personal.cog, '_ingest_message', AsyncMock(side_effect=RuntimeError('restart')))
    with pytest.raises(RuntimeError):
        submit(personal)
    monkeypatch.setattr(personal.cog, '_ingest_message', ingest)
    submit(personal)
    assert result(personal) and personal.channel.send.await_count == 1


@pytest.mark.parametrize('game', ['akari', 'tango'])
def test_deleted_post_never_claims_success_or_reposts(personal, game):
    submit(personal, game=game)
    asyncio.run(personal.cog.on_raw_message_delete(SimpleNamespace(guild_id=1, message_id=100)))
    with pytest.raises(ComplaintError, match='removed'):
        submit(personal, game=game)
    assert personal.channel.send.await_count == 1


def test_manual_result_arriving_during_send_is_reported_conflict(personal):
    async def arriving(*args, **kw):
        personal.db.save_minigame_result(222, 1, 'akari', 20, 10, 629, '2026-09-26', 87, 70, False, '')
        return SimpleNamespace(id=100)
    personal.channel.send.side_effect = arriving
    with pytest.raises(ComplaintError, match='different result'):
        submit(personal)
    with pytest.raises(ComplaintError, match='different result'):
        submit(personal)
    assert result(personal).time_seconds == 70
    assert personal.channel.send.await_count == 1
    assert not personal.db.get_games_submission(1, 10, 'akari', 629).completed


@pytest.mark.parametrize('same_time', [False, True])
def test_unrated_linkedin_source_remains_authoritative(personal, same_time):
    from tle.cogs._minigame_queens_cog import _QueensResolvedEntry
    game = personal.cog.GAMES['tango']
    entry = _QueensResolvedEntry(10, 'Importer', 39 if same_time else 50, True, True)
    personal.cog._save_queens_external_result(1, game, 20, entry, dt.date(2026, 9, 26), '', is_rated=False)
    personal.cog._recompute_game_ratings(1, game)
    assert result(personal, 'tango') is None
    if same_time:
        assert submit(personal, game='tango')['registered']
    else:
        with pytest.raises(ComplaintError, match='different result'):
            submit(personal, game='tango')
        personal.channel.send.assert_not_awaited()
    rows = personal.db.get_minigame_unresolved_results_for_puzzle(1, 'tango', 719)
    assert len(rows) == 1 and not rows[0].is_rated and rows[0].time_seconds == entry.time_seconds


def test_private_thread_requires_actual_membership(personal):
    thread = discord.Thread()
    thread.id, thread.archived, thread.locked = 20, False, False
    thread.is_private = lambda: True
    thread.get_member = lambda uid: None
    thread.permissions_for = lambda who: SimpleNamespace(
        view_channel=True, send_messages_in_threads=True, manage_threads=False)
    thread.fetch_member = AsyncMock(side_effect=OSError('no membership access'))
    thread.send = personal.channel.send
    personal.guild.get_channel_or_thread = lambda cid: thread
    with pytest.raises(ComplaintError, match='members of the private'):
        submit(personal)
    thread.send.assert_not_awaited()
    thread.fetch_member.side_effect = None
    thread.fetch_member.return_value = SimpleNamespace(id=10)
    assert submit(personal)['posted']
