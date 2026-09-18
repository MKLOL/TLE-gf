"""Tie handling for the `;<game> top` minigame winners leaderboard — users
tied on win count must share a rank (standard '1224' ranking)."""
import asyncio
from types import SimpleNamespace

from tle.cogs import _mgimpl_sharedcmd as shared
from tle.cogs.minigames import Minigames
from tle.util import codeforces_common as cf_common
from tle.util import paginator


def _ranks(description):
    return [line.split(' ', 1)[0] for line in description.splitlines()
            if line.startswith('**#')]


def _run_top(monkeypatch, winners, *args):
    """Drive ``_cmd_top`` with an injected breakdown, returning the page embeds."""
    cog = Minigames(bot=None)
    # Neutralize the heavy read path; winners are injected via the breakdown.
    monkeypatch.setattr(cog, '_require_enabled', lambda *a, **k: None)
    monkeypatch.setattr(cog, '_sync_minigame_results_for_read',
                        lambda *a, **k: None)
    monkeypatch.setattr(cog, '_filter_minigame_banned_rows',
                        lambda gid, game, rows: rows)
    monkeypatch.setattr(cog, '_minigame_public_user_name',
                        lambda guild, game, uid: f'u{uid}')

    scoring = SimpleNamespace(is_eligible_winner=None, best_result_sort_key=None,
                              winner_result_sort_key=None, result_group_key=None)
    game = SimpleNamespace(name='akari', display_name='Akari',
                           linkedin_identity=False)
    monkeypatch.setattr(shared, 'resolve_scoring',
                        lambda g, args: (list(args), None, scoring))
    monkeypatch.setattr(shared, 'compute_top_breakdown',
                        lambda rows, **kw: list(winners))

    monkeypatch.setattr(cf_common, 'user_db', SimpleNamespace(
        get_minigame_results_for_guild=lambda *a, **k: []))
    captured = {}
    monkeypatch.setattr(paginator, 'paginate',
                        lambda bot, channel, pages, **kw: captured.update(pages=pages))

    ctx = SimpleNamespace(guild=SimpleNamespace(id=111), channel=object(),
                          author=SimpleNamespace(id=1))
    asyncio.run(cog._cmd_top(ctx, game, *args))
    return captured['pages']


def test_minigame_winners_ties_share_rank(monkeypatch):
    # 3 users tied at 5 solo wins, 1 at 2 -> ranks 1, 1, 1, 4
    winners = [('1', 5, 0), ('2', 5, 1), ('3', 5, 0), ('4', 2, 3)]
    desc = _run_top(monkeypatch, winners)[0][1].description
    assert _ranks(desc) == ['**#1**', '**#1**', '**#1**', '**#4**']


def test_minigame_top_defaults_to_solo_wins_only(monkeypatch):
    winners = [('1', 2, 4), ('2', 3, 0), ('3', 0, 9)]
    embed = _run_top(monkeypatch, winners)[0][1]
    # Ranked by solo wins alone, and the tie-only winner drops off entirely.
    assert _ranks(embed.description) == ['**#1**', '**#2**']
    assert '**3** wins' in embed.description
    assert 'u3' not in embed.description
    assert 'solo' not in embed.description
    assert 'With Ties' not in embed.title


def test_minigame_top_ties_flag_reports_both_counts(monkeypatch):
    winners = [('3', 0, 9), ('1', 2, 4), ('2', 3, 0)]
    embed = _run_top(monkeypatch, winners, '+ties')
    embed = embed[0][1]
    assert 'With Ties' in embed.title
    lines = embed.description.splitlines()
    assert lines[0] == '**#1** `u3` — **9** wins (0 solo, 9 tied)'
    assert lines[1] == '**#2** `u1` — **6** wins (2 solo, 4 tied)'
    assert lines[2] == '**#3** `u2` — **3** wins (3 solo, 0 tied)'
