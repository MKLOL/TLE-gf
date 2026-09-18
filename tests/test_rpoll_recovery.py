"""Recovery commands preserve votes and enforce the source channel boundary."""
from unittest.mock import AsyncMock

import discord
import pytest

from tle.cogs._rpoll_helpers import RpollError
from tle.cogs._rpoll_recovery import resolve_poll
from tests.rpoll_test_utils import CHANNEL, GUILD
from tests.rpoll_recovery_utils import (
    ORIGINAL, RESULT, VOTER, recovery, reference, run, snapshot,
)


def results(env, target=None):
    return run(env.cog.poll_results.callback(env.cog, env.ctx, target=target))


def fix(env, target=None):
    return run(env.cog.poll_fix.callback(env.cog, env.ctx, target=target))


@pytest.mark.parametrize('target', [None, '1', '#1', str(ORIGINAL),
    f'https://discord.com/channels/{GUILD}/{CHANNEL}/{ORIGINAL}',
    f'<https://ptb.discord.com/channels/{GUILD}/{CHANNEL}/{ORIGINAL}>'])
def test_results_accepts_reply_id_and_link(recovery, target):
    env = recovery
    env.db.close_rpoll(env.pid)
    before = snapshot(env.db)
    results(env, target)
    call = env.ctx.send.call_args
    assert call.args == ('Final results for poll #1',)
    assert 'Winner: **BFS**' in call.kwargs['embed'].description
    assert '**BFS** 67%' in call.kwargs['embed'].description
    assert '**DFS** 33%' in call.kwargs['embed'].description
    assert call.kwargs['reference'].message_id == ORIGINAL
    assert call.kwargs['reference'].fail_if_not_exists is False
    assert call.kwargs['allowed_mentions'].replied_user is False
    assert snapshot(env.db) == before


def test_active_results_are_labelled_current(recovery):
    results(recovery)
    call = recovery.ctx.send.call_args
    assert call.args == ('Current results for poll #1',)
    assert 'Leader: **BFS**' in call.kwargs['embed'].description
    assert 'Winner:' not in call.kwargs['embed'].description


@pytest.mark.parametrize('content', ['Poll done!', 'Current results for poll #1',
                                    'Final results for poll #1'])
@pytest.mark.parametrize('use_link', [False, True])
def test_results_resolves_bot_result_replies(recovery, content, use_link):
    env = recovery
    env.result.content = content
    env.ctx.message.reference = reference(RESULT)
    target = f'https://discord.com/channels/{GUILD}/{CHANNEL}/{RESULT}' if use_link else None
    results(env, target)
    assert env.ctx.send.call_args.kwargs['reference'].message_id == ORIGINAL


@pytest.mark.parametrize('use_result', [False, True])
def test_results_survive_deleted_original(recovery, use_result):
    env = recovery
    del env.messages[ORIGINAL]
    env.ctx.message.reference = reference(RESULT if use_result else ORIGINAL)
    results(env)
    assert env.ctx.send.call_count == 1


@pytest.mark.parametrize('command', [fix, results])
def test_recovered_result_without_reference_remains_usable(recovery, command):
    env = recovery
    del env.messages[ORIGINAL]
    env.result.content = 'Final results for poll #1'
    env.result.reference = None
    env.ctx.message.reference = reference(RESULT)
    env.db.close_rpoll(env.pid)
    command(env)
    call = env.result.edit.call_args if command is fix else env.ctx.send.call_args
    assert 'Winner: **BFS**' in call.kwargs['embed'].description


@pytest.mark.parametrize('command', [fix, results])
def test_unreferenced_result_still_enforces_source_channel(recovery, command):
    env = recovery
    env.result.content = 'Final results for poll #1'
    env.result.reference = None
    env.ctx.message.reference = reference(RESULT)
    env.db.conn.execute('UPDATE rpoll SET channel_id = ? WHERE poll_id = ?',
                        (str(CHANNEL + 1), env.pid))
    with pytest.raises(RpollError, match='original channel'):
        command(env)
    env.result.edit.assert_not_called()
    env.ctx.send.assert_not_called()


@pytest.mark.parametrize('closed,expired', [(False, False), (True, False), (False, True)])
def test_fix_preserves_votes_and_correct_button_state(recovery, closed, expired):
    env = recovery
    if closed:
        env.db.close_rpoll(env.pid)
    if expired:
        env.db.conn.execute('UPDATE rpoll SET expires_at = 1 WHERE poll_id = ?', (env.pid,))
        env.db.conn.commit()
    before = snapshot(env.db)
    fix(env)
    call = env.original.edit.call_args.kwargs
    assert call['suppress'] is False
    assert '**1600** (67%)' in call['embed'].description
    assert '**800** (33%)' in call['embed'].description
    assert ('Poll has ended.' in call['embed'].description) == (closed or expired)
    assert all(getattr(button, 'disabled', False) == (closed or expired)
               for button in call['view'].children)
    assert len(call['view'].children) == 2
    assert snapshot(env.db) == before


def test_fix_completion_reply_repairs_only_that_message(recovery):
    env = recovery
    env.db.close_rpoll(env.pid)
    env.ctx.message.reference = reference(RESULT)
    before = snapshot(env.db)
    fix(env)
    call = env.result.edit.call_args.kwargs
    assert call['content'] == 'Final results for poll #1'
    assert call['suppress'] is False
    assert 'Winner: **BFS**' in call['embed'].description
    assert 'view' not in call
    env.original.edit.assert_not_called()
    assert snapshot(env.db) == before


@pytest.mark.parametrize('command', [fix, results])
def test_anonymous_recovery_never_exposes_voters(recovery, command):
    env = recovery
    env.db.conn.execute('UPDATE rpoll SET anonymous = 1 WHERE poll_id = ?', (env.pid,))
    env.db.conn.commit()
    command(env)
    call = env.original.edit.call_args if command is fix else env.ctx.send.call_args
    assert '<@' not in call.kwargs['embed'].description
    assert str(VOTER) not in call.kwargs['embed'].description


def test_recovery_never_refreshes_saved_ratings(recovery, monkeypatch):
    env = recovery
    env.db.close_rpoll(env.pid)
    monkeypatch.setattr(env.db, 'get_rpoll_user_rating', lambda *args: pytest.fail('rating refresh'))
    results(env)
    fix(env)
    assert '**BFS** 67%' in env.ctx.send.call_args_list[0].kwargs['embed'].description


@pytest.mark.parametrize('command', [fix, results])
@pytest.mark.parametrize('different_guild', [False, True])
def test_poll_ids_cannot_expose_other_channels_or_guilds(recovery, command, different_guild):
    env = recovery
    if different_guild:
        env.ctx.guild.id += 1
    else:
        env.ctx.channel.id += 1
    with pytest.raises(RpollError, match='original channel'):
        command(env, '1')
    env.ctx.send.assert_not_called()
    env.channel.fetch_message.assert_not_called()


@pytest.mark.parametrize('target', [
    f'https://discord.com/channels/{GUILD}/{CHANNEL + 1}/{ORIGINAL}',
    f'https://discord.com/channels/{GUILD + 1}/{CHANNEL}/{ORIGINAL}',
])
def test_cross_channel_links_rejected_before_fetch(recovery, target):
    with pytest.raises(RpollError, match='original channel'):
        results(recovery, target)
    recovery.channel.fetch_message.assert_not_called()


@pytest.mark.parametrize('change', ['author', 'content', 'reference', 'cross_channel_reference'])
def test_unrelated_messages_cannot_be_treated_as_results(recovery, change):
    env = recovery
    env.ctx.message.reference = reference(RESULT)
    if change == 'author':
        env.result.author.id = VOTER
    elif change == 'content':
        env.result.content = 'Something else'
    elif change == 'reference':
        env.result.reference = None
    else:
        env.result.reference.channel_id += 1
    with pytest.raises(RpollError):
        fix(env)
    env.result.edit.assert_not_called()


def test_fix_deleted_message_explains_results_recovery(recovery):
    del recovery.messages[ORIGINAL]
    with pytest.raises(RpollError, match=';rpoll results'):
        fix(recovery)
    recovery.ctx.send.assert_not_called()


@pytest.mark.parametrize('error', [discord.Forbidden, discord.HTTPException, discord.NotFound])
def test_fix_reports_edit_failure_without_success_message(recovery, error):
    recovery.original.edit.side_effect = error(None, 'failure')
    with pytest.raises(RpollError):
        fix(recovery)
    recovery.ctx.send.assert_not_called()


def test_fix_observes_poll_closing_while_fetching(recovery):
    env = recovery

    async def fetch(_):
        env.db.close_rpoll(env.pid)
        return env.original

    env.channel.fetch_message = AsyncMock(side_effect=fetch)
    fix(env)
    assert all(button.disabled for button in env.original.edit.call_args.kwargs['view'].children)


@pytest.mark.parametrize('target', ['garbage', '#999', '9999999999999999999',
    f'https://example.com/channels/{GUILD}/{CHANNEL}/{ORIGINAL}'])
def test_bad_targets_have_friendly_errors(recovery, target):
    with pytest.raises(RpollError):
        results(recovery, target)


def test_missing_reply_and_dm_are_rejected(recovery):
    env = recovery
    env.ctx.message.reference = None
    with pytest.raises(RpollError, match='Reply to a poll'):
        results(env)
    env.ctx.guild = None
    with pytest.raises(RpollError, match='server channel'):
        results(env, '1')


def test_missing_database_is_reported(recovery, monkeypatch):
    from tle.util import codeforces_common as cf_common
    monkeypatch.setattr(cf_common, 'user_db', None)
    with pytest.raises(RpollError, match='starting up'):
        run(resolve_poll(recovery.ctx, '1'))
