"""`;teamrate` multipliers must follow their handle whatever the resolution order."""
import asyncio
from types import SimpleNamespace

from tle.cogs._codeforces_problems import CodeforcesProblemsMixin
from tle.util import codeforces_api as cf
from tle.util import codeforces_common as cf_common
from tle.cogs import _codeforces_problems as problems


class _Cog(CodeforcesProblemsMixin):
    converter = None
    bot = None


def _run(args, monkeypatch, canonical):
    async def resolve(ctx, converter, handles, **kw):
        return [canonical.get(h.lower(), h) for h in handles]
    monkeypatch.setattr(cf_common, 'resolve_handles', resolve)
    monkeypatch.setattr(cf_common, 'filter_flags',
                        lambda a, params: ([False, False], list(a)))

    async def info(*, handles):
        return [SimpleNamespace(handle=h, rating=2000 if h.lower() == 'tourist' else 1000,
                                maxRating=0) for h in handles]
    monkeypatch.setattr(cf, 'user', SimpleNamespace(info=info), raising=False)
    monkeypatch.setattr(cf, 'rating2rank',
                        lambda r: SimpleNamespace(color_embed=0), raising=False)
    captured = {}
    monkeypatch.setattr(problems, 'composeRatings',
                        lambda left, right, ratings: captured.setdefault('ratings', ratings) and 0)
    ctx = SimpleNamespace(author=SimpleNamespace(id=1), guild=SimpleNamespace(id=1),
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
