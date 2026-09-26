"""`;teamrate` multipliers must follow their handle whatever the resolution order."""
import asyncio
from types import SimpleNamespace

import pytest

from tle.cogs._codeforces_problems import CodeforcesProblemsMixin
from tle.cogs._codeforces_helpers import CodeforcesCogError
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.cogs import _codeforces_problems as problems


class _Cog(CodeforcesProblemsMixin):
    converter = None
    bot = None


def _run(args, monkeypatch, canonical, renames=None):
    async def resolve(ctx, converter, handles, **kw):
        # Reversed on purpose: any code that pairs results with its input
        # positionally must fail here.
        return [canonical.get(h.lower(), h) for h in handles][::-1]
    monkeypatch.setattr(cf_common, 'resolve_handles', resolve)
    monkeypatch.setattr(cf_common, 'filter_flags',
                        lambda a, params: ([False, False], list(a)))

    async def info(*, handles):
        return [SimpleNamespace(handle=(renames or {}).get(h.lower(), h),
                                rating=2000 if h.lower() == 'tourist' else 1000,
                                maxRating=0) for h in handles]
    monkeypatch.setattr(cf, 'user', SimpleNamespace(info=info), raising=False)
    monkeypatch.setattr(cf, 'rating2rank',
                        lambda r: SimpleNamespace(color_embed=0), raising=False)
    captured = {}
    monkeypatch.setattr(problems.paginator, 'paginate',
                        lambda bot, channel, pages, **kw:
                        captured.setdefault('pages', pages))
    monkeypatch.setattr(problems, 'composeRatings',
                        lambda left, right, ratings: captured.setdefault('ratings', ratings) and 0)
    ctx = SimpleNamespace(author=SimpleNamespace(id=1), guild=SimpleNamespace(id=1),
                          channel=SimpleNamespace(id=2),
                          send=lambda **kw: asyncio.sleep(0, result=captured.setdefault('embed', kw['embed'])))
    asyncio.run(_Cog()._teamrate_impl(ctx, args))
    return captured


def test_multiplier_follows_its_handle(monkeypatch):
    got = _run(('tourist*2', 'petr'), monkeypatch, {'tourist': 'tourist', 'petr': 'Petr'})
    assert sorted(got['ratings']) == [(1000, 1), (2000, 2)]
    assert got['embed'].title == 'tourist*2, Petr'


def test_two_spellings_of_one_account_add_up_instead_of_crashing(monkeypatch):
    got = _run(('tourist*2', 'TOURIST'), monkeypatch, {'tourist': 'tourist'})
    assert got['ratings'] == [(2000, 3)]
    assert got['embed'].title == 'tourist*3'


def test_a_discord_user_and_their_typed_handle_merge_instead_of_crashing(monkeypatch):
    """`;teamrate tourist*2 !someone` where !someone is registered as Tourist."""
    got = _run(('tourist*2', '!someone'), monkeypatch,
               {'tourist': 'tourist', '!someone': 'Tourist'})
    assert got['ratings'] == [(2000, 3)]
    assert got['embed'].title == 'tourist*3'


def test_cf_rename_preserves_multiplier_and_uses_new_display_name(monkeypatch):
    got = _run(('old_name*3', 'tourist*2'), monkeypatch, {},
               renames={'old_name': 'NewName'})
    assert got['ratings'] == [(1000, 3), (2000, 2)]
    assert got['embed'].title == 'NewName*3, tourist*2'


def test_cf_rename_merges_old_and_new_account_names(monkeypatch):
    got = _run(('old_name*3', 'NewName*2'), monkeypatch, {},
               renames={'old_name': 'NewName', 'newname': 'NewName'})
    assert got['ratings'] == [(1000, 5)]
    assert got['embed'].title == 'NewName*5'


@pytest.mark.parametrize('args', [
    ('tourist*3', 'tourist*-1'), ('tourist*3', 'tourist*0'),
])
def test_each_multiplier_must_be_positive_before_aggregation(monkeypatch, args):
    with pytest.raises(CodeforcesCogError, match='nonpositive'):
        _run(args, monkeypatch, {})


@pytest.mark.parametrize('count', [12, 160])
def test_long_expanded_team_lists_use_bounded_pages(monkeypatch, count):
    handles = {f'u{i}': f'player_{i:03}_abcdefghijklm' for i in range(count)}
    got = _run(tuple(handles), monkeypatch, handles)
    pages = got['pages']
    assert len(pages) == (1 if count == 12 else 2)
    displayed = []
    for content, embed in pages:
        assert content is None
        assert embed.title == 'Team rating: 0'
        assert len(embed.title) <= 256
        assert len(embed.description) <= 4096
        assert len(embed.title) + len(embed.description) < 6000
        displayed.extend(embed.description.split(', '))
    assert displayed == list(handles.values())
