"""The problem-delta graph must never plot exact-score payout sentinels."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tle.cogs import _graph_render2 as render
from tle.cogs._codeforces_helpers import (
    _calculateGitgudScoreForDelta,
    _gitgudEncodeExactScoreAsDelta,
    _gitgudTagPenaltyDelta,
)


@pytest.fixture
def graph(monkeypatch):
    members = [SimpleNamespace(id=1, display_name='Alice'),
               SimpleNamespace(id=2, display_name='Bob')]
    ctx = SimpleNamespace(author=members[0], send=AsyncMock())
    rows = {}
    monkeypatch.setattr(render.cf_common, 'user_db', SimpleNamespace(
        howgud=lambda user_id: [(value,) for value in rows.get(user_id, [])]))
    plot = SimpleNamespace(**{name: Mock() for name in (
        'clf', 'margins', 'hist', 'xlabel', 'ylabel', 'legend')})
    monkeypatch.setattr(render, 'plt', plot)
    monkeypatch.setattr(render.gc, 'StrWrap', str, raising=False)
    monkeypatch.setattr(render.gc, 'fontprop', None, raising=False)
    monkeypatch.setattr(render.discord_common, 'cf_color_embed',
                        lambda **kwargs: SimpleNamespace(**kwargs))
    return SimpleNamespace(ctx=ctx, members=members, rows=rows, plot=plot)


def test_tagged_payouts_are_omitted_without_changing_scores(graph):
    # This mix previously made approximately ten million histogram bars.
    tagged = [_gitgudEncodeExactScoreAsDelta(score) for score in range(1, 24)]
    graph.rows[1] = [-100, 0, 100, *tagged]
    graph.rows[2] = [200, _gitgudTagPenaltyDelta(200, 1)]

    asyncio.run(render.howgud(None, graph.ctx, *graph.members))

    args, kwargs = graph.plot.hist.call_args
    assert args == ([[-100, 0, 100], [200]],)
    assert kwargs['bins'] == [-150, -50, 50, 150, 250]
    graph.plot.legend.assert_called_once_with(['Alice: 3', 'Bob: 1'], prop=None)
    embed = graph.ctx.send.call_args.kwargs['embed']
    assert '24 tagged challenge(s)' in embed.description
    assert graph.rows[1] == [-100, 0, 100, *tagged]
    assert [_calculateGitgudScoreForDelta(delta) for delta in tagged] == list(range(1, 24))
    assert _calculateGitgudScoreForDelta(graph.rows[2][1]) == 9


def test_regular_problem_deltas_keep_existing_bins(graph):
    graph.rows[1] = [-400, -100, 0, 300]

    asyncio.run(render.howgud(None, graph.ctx))

    args, kwargs = graph.plot.hist.call_args
    assert args == ([graph.rows[1]],)
    assert kwargs['bins'] == list(range(-450, 351, 100))
    assert not hasattr(graph.ctx.send.call_args.kwargs['embed'], 'description')


def test_all_tagged_challenges_report_unavailable_deltas_before_rendering(graph):
    graph.rows[1] = [_gitgudTagPenaltyDelta(300, 1)]

    with pytest.raises(render.GraphCogError, match='Tagged challenges'):
        asyncio.run(render.howgud(None, graph.ctx))

    graph.plot.clf.assert_not_called()
    graph.plot.hist.assert_not_called()
    graph.ctx.send.assert_not_called()


@pytest.mark.parametrize('outlier', [-10**18, 10**18, -1000000024])
def test_unexpected_extreme_delta_is_rejected_before_any_rendering(graph, outlier):
    graph.rows[1] = [0, outlier]

    with pytest.raises(render.GraphCogError, match='too large to plot safely'):
        asyncio.run(render.howgud(None, graph.ctx))

    graph.plot.clf.assert_not_called()
    graph.plot.hist.assert_not_called()
    graph.ctx.send.assert_not_called()


def test_empty_history_is_rejected_before_rendering(graph):
    with pytest.raises(render.GraphCogError, match='No completed challenges'):
        asyncio.run(render.howgud(None, graph.ctx))

    graph.plot.hist.assert_not_called()
