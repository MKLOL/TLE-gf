"""Slow Discord writes cannot leave a repaired poll open after expiry."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tle.cogs._rpoll_locks import poll_lock
from tle.cogs._rpoll_views import RpollButton
from tests.rpoll_recovery_utils import VOTER, recovery, run


def interaction(env):
    return SimpleNamespace(
        user=SimpleNamespace(id=VOTER), guild_id=env.ctx.guild.id, message=env.original,
        response=SimpleNamespace(edit_message=AsyncMock(), send_message=AsyncMock(), defer=AsyncMock()),
        followup=SimpleNamespace(send=AsyncMock()),
    )


@pytest.mark.parametrize('first', ['fix', 'close', 'vote'])
def test_expiry_serializes_with_repairs_and_votes(recovery, first):
    env = recovery

    async def scenario():
        started, release = asyncio.Event(), asyncio.Event()
        edits = []

        async def slow_edit(**kwargs):
            if not edits:
                started.set()
                await release.wait()
            edits.append(kwargs)

        env.original.edit.side_effect = slow_edit
        vote = interaction(env)
        vote.response.edit_message.side_effect = slow_edit

        async def repair():
            await env.cog.poll_fix.callback(env.cog, env.ctx)

        async def close():
            await env.cog._close_poll(env.db.get_rpoll(env.pid))

        operations = {'fix': repair, 'close': close,
                      'vote': lambda: RpollButton.callback(RpollButton(env.pid, 1), vote)}
        task1 = asyncio.create_task(operations[first]())
        await asyncio.wait_for(started.wait(), 1)
        task2 = asyncio.create_task(repair() if first == 'close' else close())
        await asyncio.sleep(0)
        assert len(edits) == 0
        release.set()
        await asyncio.wait_for(asyncio.gather(task1, task2), 1)
        assert env.db.get_rpoll(env.pid).closed
        assert 'Poll has ended.' in edits[-1]['embed'].description
        assert all(button.disabled for button in edits[-1]['view'].children)
        assert env.channel.send.call_count == 1

    run(scenario())


@pytest.mark.parametrize('closed', [False, True])
def test_queued_vote_defers_and_checks_latest_poll_state(recovery, closed):
    env = recovery
    vote = interaction(env)
    before = env.db.get_rpoll_vote_ratings(env.pid)

    async def scenario():
        async with poll_lock(env.pid):
            task = asyncio.create_task(RpollButton.callback(RpollButton(env.pid, 1), vote))
            await asyncio.sleep(0)
            vote.response.defer.assert_awaited_once()
            if closed:
                env.db.close_rpoll(env.pid)
        await asyncio.wait_for(task, 1)

    run(scenario())
    vote.response.edit_message.assert_not_called()
    if closed:
        assert env.db.get_rpoll_vote_ratings(env.pid) == before
        vote.followup.send.assert_awaited_once_with('This poll has ended.', ephemeral=True)
        env.original.edit.assert_not_called()
    else:
        assert len(env.db.get_rpoll_vote_ratings(env.pid)) == len(before) + 1
        assert env.original.edit.call_args.kwargs['suppress'] is False


def test_concurrent_expiry_posts_results_once(recovery):
    env = recovery

    async def scenario():
        poll = env.db.get_rpoll(env.pid)
        await asyncio.gather(env.cog._close_poll(poll), env.cog._close_poll(poll))

    run(scenario())
    assert env.channel.send.call_count == 1
