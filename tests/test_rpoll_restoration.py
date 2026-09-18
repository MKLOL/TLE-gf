"""Vote and expiry paths recover suppressed poll displays."""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tle.cogs._rpoll_views import RpollButton
from tests.rpoll_recovery_utils import VOTER, recovery, run


@pytest.mark.parametrize('suppressed,has_embed', [(True, False), (True, True),
                                                (False, False), (False, True)])
def test_vote_restores_display_and_still_records_vote(recovery, suppressed, has_embed):
    env = recovery
    env.original.flags.suppress_embeds = suppressed
    if has_embed:
        env.original.embeds = [SimpleNamespace(color=123)]
    env.db._seed_cf_user(VOTER, env.ctx.guild.id, 'alice', 1600)
    env.db._seed_cf_user(VOTER + 1, env.ctx.guild.id, 'bob', 1200)
    interaction = SimpleNamespace(
        user=SimpleNamespace(id=VOTER), guild_id=env.ctx.guild.id, message=env.original,
        response=SimpleNamespace(edit_message=AsyncMock(), send_message=AsyncMock()),
    )
    # Add a second option for the same voter: the repair must not eat the vote.
    button = RpollButton(env.pid, 1)
    run(RpollButton.callback(button, interaction))
    rows = env.db.get_rpoll_vote_ratings(env.pid)
    assert len(rows) == 3
    call = interaction.response.edit_message.call_args.kwargs
    assert '**2400** (60%)' in call['embed'].description
    if suppressed:
        assert call['suppress_embeds'] is False
    else:
        assert 'suppress_embeds' not in call
    if has_embed:
        assert call['embed'].color == 123


def test_unvote_also_restores_suppressed_embed(recovery):
    env = recovery
    interaction = SimpleNamespace(
        user=SimpleNamespace(id=VOTER), guild_id=env.ctx.guild.id, message=env.original,
        response=SimpleNamespace(edit_message=AsyncMock(), send_message=AsyncMock()),
    )
    run(RpollButton.callback(RpollButton(env.pid, 0), interaction))
    assert env.db.get_rpoll_vote_count(env.pid) == 1
    assert interaction.response.edit_message.call_args.kwargs['suppress_embeds'] is False


def test_expiry_restores_suppressed_embed_and_disables_buttons(recovery):
    env = recovery
    run(env.cog._close_poll(env.db.get_rpoll(env.pid)))
    assert env.db.get_rpoll(env.pid).closed
    call = env.original.edit.call_args.kwargs
    assert call['suppress'] is False
    assert 'Poll has ended.' in call['embed'].description
    assert all(button.disabled for button in call['view'].children)
    assert '**1600** (67%)' in call['embed'].description
    assert env.channel.send.call_args.args == ('Poll done!',)
    assert env.channel.send.call_args.kwargs['allowed_mentions'].replied_user is False


def test_expiry_still_posts_results_after_original_deleted(recovery):
    env = recovery
    env.messages.clear()
    run(env.cog._close_poll(env.db.get_rpoll(env.pid)))
    assert 'Winner: **BFS**' in env.channel.send.call_args.kwargs['embed'].description
    assert env.channel.send.call_args.kwargs['reference'].fail_if_not_exists is False
