"""Tests for the gitgud -> betting-coin reward.

Completing a gitgud challenge (`;gotgud`) also credits the player's betting
wallet with ``_GITGUD_COIN_MULTIPLIER`` (5) coins per *base* gitgud point. The
rate is a flat 5x of the base score and is deliberately immune to the
end-of-month "more points" doubling that the monthly ranklist points get.

Coins are always banked, but only announced to users who have already placed a
bet -- the same bar as appearing on ``;bet leaderboard``. A coin-only wallet is
banked silently and stays off the leaderboard.
"""
import datetime
from types import SimpleNamespace

import pytest  # noqa: F401

from tests.betting_test_utils import (  # noqa: F401
    GUILD, USER_A, USER_B, db, _make_market,
)

from tle import constants
from tle.cogs._codeforces_helpers import _GITGUD_COIN_MULTIPLIER


def test_multiplier_is_five():
    # The coin rate is contractually 5x; pin it so a refactor can't drift it.
    assert _GITGUD_COIN_MULTIPLIER == 5


class TestBetHasWagered:
    def test_false_for_fresh_user(self, db):
        assert db.bet_has_wagered(GUILD, USER_A) is False

    def test_true_after_placing_a_bet(self, db):
        mid = _make_market(db, commence=1e12)
        db.bet_place(GUILD, mid, USER_A, 'home', 10, 1.0, 1000)
        assert db.bet_has_wagered(GUILD, USER_A) is True

    def test_coin_only_wallet_is_not_a_bettor(self, db):
        # Crediting gitgud coins makes a wallet but never a wager.
        db.bet_adjust_balance(GUILD, USER_A, 40, 1000, action='gitgud')
        assert db.bet_has_wagered(GUILD, USER_A) is False

    def test_scoped_to_guild(self, db):
        mid = _make_market(db, commence=1e12)
        db.bet_place(GUILD, mid, USER_A, 'home', 10, 1.0, 1000)
        assert db.bet_has_wagered('999999', USER_A) is False


class TestLeaderboardExcludesCoinOnlyWallet:
    def test_coin_only_wallet_absent_from_balance_leaderboard(self, db):
        # USER_A only ever earned gitgud coins; USER_B actually bet.
        db.bet_adjust_balance(GUILD, USER_A, 5000, 1000, action='gitgud')
        mid = _make_market(db, commence=1e12)
        db.bet_place(GUILD, mid, USER_B, 'home', 10, 1.0, 1000)

        ids = [row.user_id for row in db.bet_balance_leaderboard(GUILD)]

        assert USER_A not in ids  # excluded despite the fat balance
        assert USER_B in ids


class TestGotgudCoinReward:
    """Drive the real ;gotgud body against an in-memory DB, mocking only the
    Codeforces API / handle resolution at the edges."""

    @pytest.fixture
    def cog(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs._codeforces_gitgud import CodeforcesGitgudMixin
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)

        class _Cog(CodeforcesGitgudMixin):
            pass

        cog = _Cog()
        cog.converter = None  # resolve_handles is mocked, so it goes unused
        return cog

    def _patch_cf(self, monkeypatch, solved_name):
        from tle.util import codeforces_common as cf_common
        from tle.util import codeforces_api as cf

        async def fake_resolve(ctx, converter, handles, **kw):
            return ['handleA']

        async def fake_status(*, handle):
            return [SimpleNamespace(
                verdict='OK', problem=SimpleNamespace(name=solved_name))]

        monkeypatch.setattr(cf_common, 'resolve_handles', fake_resolve)
        # The test stub of codeforces_api has no `user` API class, so create it.
        monkeypatch.setattr(
            cf, 'user', SimpleNamespace(status=fake_status), raising=False)

    def _ctx(self, uid, guild_id=GUILD):
        guild = None if guild_id is None else SimpleNamespace(id=guild_id)

        class _Ctx:
            def __init__(self):
                self.author = SimpleNamespace(id=uid)
                self.message = SimpleNamespace(author=SimpleNamespace(id=uid))
                self.guild = guild
                self.sent = []

            async def send(self, msg, *a, **k):
                self.sent.append(msg)

        return _Ctx()

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def _issue_challenge(self, db, uid, name, delta=0):
        prob = SimpleNamespace(name=name, contestId=1234, index='A')
        issue_time = int(datetime.datetime.now().timestamp()) - 3600
        assert db.new_challenge(uid, issue_time, prob, delta) == 1
        return name

    def test_announces_coins_to_bettor_and_never_doubles_them(
            self, db, cog, monkeypatch):
        # Force the end-of-month "more points" window ON so the monthly points
        # double -- the coins must stay flat 5x of the *base* score.
        monkeypatch.setattr(cog, '_check_more_points_active', lambda *a, **k: True)
        name = self._issue_challenge(db, USER_A, 'Coin Problem A', delta=0)

        # USER_A is an active bettor (stakes 100 of the 1000 start balance).
        mid = _make_market(db, commence=1e12)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)
        assert db.bet_get_balance(GUILD, USER_A) == 900

        self._patch_cf(monkeypatch, solved_name=name)
        ctx = self._ctx(USER_A)
        self._run(cog._gotgud_impl(ctx))

        msg = ctx.sent[0]
        # delta 0 -> base score 8; the monthly points double to 16, the coins
        # are 5 x 8 = 40 and must NOT be 5 x 16 = 80.
        assert '8 alltime' in msg
        assert '16 monthly' in msg
        assert '40 \U0001fa99' in msg       # 5 x base
        assert '80 \U0001fa99' not in msg   # not 5 x doubled monthly
        assert db.bet_get_balance(GUILD, USER_A) == 900 + 40

    def test_banks_coins_silently_for_non_bettor(self, db, cog, monkeypatch):
        name = self._issue_challenge(db, USER_B, 'Coin Problem B', delta=0)

        self._patch_cf(monkeypatch, solved_name=name)
        ctx = self._ctx(USER_B)
        self._run(cog._gotgud_impl(ctx))

        msg = ctx.sent[0]
        assert '8 alltime' in msg
        assert '\U0001fa99' not in msg  # never told about the coins
        # ...but the coins were still banked on top of the start balance.
        assert db.bet_get_balance(GUILD, USER_B) == constants.BET_START_BALANCE + 40
        assert db.bet_has_wagered(GUILD, USER_B) is False
        # and the silent wallet stays off the leaderboard.
        ids = [row.user_id for row in db.bet_balance_leaderboard(GUILD)]
        assert USER_B not in ids

    def test_double_claim_does_not_re_award_coins(self, db, cog, monkeypatch):
        from tle.cogs._codeforces_helpers import CodeforcesCogError
        name = self._issue_challenge(db, USER_A, 'Coin Problem C', delta=0)
        self._patch_cf(monkeypatch, solved_name=name)

        self._run(cog._gotgud_impl(self._ctx(USER_A)))
        balance_after_first = db.bet_get_balance(GUILD, USER_A)

        # No active challenge remains -> the second claim raises before it can
        # credit anything, so coins are awarded exactly once.
        with pytest.raises(CodeforcesCogError):
            self._run(cog._gotgud_impl(self._ctx(USER_A)))

        assert db.bet_get_balance(GUILD, USER_A) == balance_after_first

    def test_award_coins_is_a_noop_without_a_guild(self, db, cog):
        # In a DM there's no guild and thus no per-guild wallet to credit.
        coins = cog._award_gitgud_coins(self._ctx(USER_A, guild_id=None), USER_A, 8)
        assert coins is None
        assert db.bet_get_balance(GUILD, USER_A) is None  # no wallet created
