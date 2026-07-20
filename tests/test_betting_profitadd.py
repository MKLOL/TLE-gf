"""Tests for `;bet profitadd` and the config-driven betting archive
(`;meta config enable bet_archived`)."""
import asyncio

import pytest  # noqa: F401

from tests.betting_test_utils import (  # noqa: F401
    GUILD, CH, THREAD, USER_A, USER_B, db, _make_market,
    _FakeBot, _FakeChannel, _FakeGuild, _FakeBetMessage,
)


def _run(coro):
    return asyncio.run(coro)


def _ctx():
    admin = type('Member', (), {'id': '999', 'display_name': 'Admin'})()

    class _Ctx:
        def __init__(self):
            self.guild = type('G', (), {'id': int(GUILD)})()
            self.author = admin
            self.sent = []

        async def send(self, embed=None, **kw):
            self.sent.append(embed)

    return _Ctx()


def _setup(db, monkeypatch):
    from tle.util import codeforces_common as cf_common
    from tle import constants
    monkeypatch.setattr(cf_common, 'user_db', db)
    monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)


def _member(user_id, name='Late Bettor'):
    return type('Member', (), {'id': user_id, 'display_name': name})()


class TestProfitAdd:
    def test_credits_balance_and_profit(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        _setup(db, monkeypatch)
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        cog = Betting(bot=None)

        _run(Betting.profitadd.__wrapped__(cog, _ctx(), _member(USER_A), 250))

        assert db.bet_get_balance(GUILD, USER_A) == 1250
        rows = db.bet_profit_leaderboard(GUILD)
        row = next(r for r in rows if r.user_id == USER_A)
        assert row.profit == 250
        assert row.bets == 0 and row.wins == 0

    def test_negative_amount_reverts(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        _setup(db, monkeypatch)
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        cog = Betting(bot=None)

        _run(Betting.profitadd.__wrapped__(cog, _ctx(), _member(USER_A), 250))
        _run(Betting.profitadd.__wrapped__(cog, _ctx(), _member(USER_A), -250))

        assert db.bet_get_balance(GUILD, USER_A) == 1000
        rows = db.bet_profit_leaderboard(GUILD)
        row = next((r for r in rows if r.user_id == USER_A), None)
        assert row is None or row.profit == 0

    def test_zero_amount_rejected(self, db, monkeypatch):
        from tle.cogs.betting import Betting, BettingCogError
        _setup(db, monkeypatch)
        cog = Betting(bot=None)
        with pytest.raises(BettingCogError):
            _run(Betting.profitadd.__wrapped__(cog, _ctx(), _member(USER_A), 0))

    def test_adds_on_top_of_settled_wager_profit(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        _setup(db, monkeypatch)
        # USER_A wins a settled market: stake 100 at home odds 2.0 → +100.
        market_id = _make_market(db)
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        db.bet_place(GUILD, market_id, USER_A, 'home', 100, 5_000.0, 1000)
        db.bet_settle(GUILD, market_id, 'home', 2, 0, 20_000.0)
        cog = Betting(bot=None)

        _run(Betting.profitadd.__wrapped__(cog, _ctx(), _member(USER_A), 300))

        row = next(r for r in db.bet_profit_leaderboard(GUILD)
                   if r.user_id == USER_A)
        assert row.profit == 100 + 300
        assert row.bets == 1 and row.wins == 1

    def test_floored_revert_reports_what_actually_moved(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        from tle.util import discord_common
        _setup(db, monkeypatch)
        monkeypatch.setattr(discord_common, 'embed_success', lambda d: d)
        monkeypatch.setattr(discord_common, 'embed_alert', lambda d: d)
        db.bet_ensure_wallet(GUILD, USER_A, 100)  # only 100 left to claw back
        cog = Betting(bot=None)

        ctx = _ctx()
        _run(Betting.profitadd.__wrapped__(cog, ctx, _member(USER_A), -250))
        assert db.bet_get_balance(GUILD, USER_A) == 0
        assert 'Removed **100**' in ctx.sent[0]
        assert 'Requested -250' in ctx.sent[0]
        row = next(r for r in db.bet_profit_leaderboard(GUILD)
                   if r.user_id == USER_A)
        assert row.profit == -100  # profit matches the coins actually moved

        ctx2 = _ctx()  # second attempt: balance already 0 — nothing moves
        _run(Betting.profitadd.__wrapped__(cog, ctx2, _member(USER_A), -50))
        assert 'already 0' in ctx2.sent[0]
        row = next(r for r in db.bet_profit_leaderboard(GUILD)
                   if r.user_id == USER_A)
        assert row.profit == -100

    def test_ledger_records_actor_and_action(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        _setup(db, monkeypatch)
        cog = Betting(bot=None)

        _run(Betting.profitadd.__wrapped__(cog, _ctx(), _member(USER_A), 250))

        entries = db.bet_wallet_history(GUILD, USER_A)
        entry = next(e for e in entries if e.action == 'profitadd')
        assert entry.amount == 250
        assert entry.actor_id == '999'


class TestArchivedFlag:
    def test_is_archived_reads_guild_config(self, db, monkeypatch):
        from tle.cogs._betting_helpers import _is_archived
        _setup(db, monkeypatch)
        assert not _is_archived(int(GUILD))
        db.set_guild_config(GUILD, 'bet_archived', '1')
        assert _is_archived(int(GUILD))
        db.delete_guild_config(GUILD, 'bet_archived')
        assert not _is_archived(int(GUILD))

    def _check_ctx(self, command_name, qualified=None):
        ctx = _ctx()
        ctx.command = type('Cmd', (), {
            'name': command_name,
            'qualified_name': qualified or f'bet {command_name}'})()
        return ctx

    def test_cog_check_blocks_dead_commands_when_archived(self, db, monkeypatch):
        from tle.cogs.betting import Betting, BettingCogError
        _setup(db, monkeypatch)
        db.set_guild_config(GUILD, 'bet_archived', '1')
        cog = Betting(bot=None)
        for name in ('matches', 'home', 'daily', 'grant', 'settle'):
            with pytest.raises(BettingCogError):
                _run(cog.cog_check(self._check_ctx(name)))

    def test_cog_check_allows_survivors_and_bare_group(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        _setup(db, monkeypatch)
        db.set_guild_config(GUILD, 'bet_archived', '1')
        cog = Betting(bot=None)
        for name in ('leaderboard', 'me', 'profitadd'):
            assert _run(cog.cog_check(self._check_ctx(name)))
        assert _run(cog.cog_check(self._check_ctx('bet', qualified='bet')))

    def test_cog_check_allows_everything_when_live(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        _setup(db, monkeypatch)
        cog = Betting(bot=None)
        for name in ('matches', 'home', 'daily', 'grant', 'leaderboard'):
            assert _run(cog.cog_check(self._check_ctx(name)))

    def test_bare_bet_sends_archived_notice(self, db, monkeypatch):
        from tle.cogs.betting import Betting, _ARCHIVED_NOTICE
        from tle.util import discord_common
        _setup(db, monkeypatch)
        monkeypatch.setattr(discord_common, 'embed_neutral', lambda desc: desc)
        db.set_guild_config(GUILD, 'bet_archived', '1')
        cog = Betting(bot=None)
        ctx = _ctx()
        ctx.message = type('Msg', (), {'content': ';bet'})()
        ctx.prefix, ctx.invoked_with = ';', 'bet'

        _run(Betting.bet.__wrapped__(cog, ctx))

        assert ctx.sent == [_ARCHIVED_NOTICE]
        assert 'World cup has ended' in _ARCHIVED_NOTICE

    def test_settle_football_data_never_fetches_when_archived(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        from tle.util import football_data
        from tle import constants
        _setup(db, monkeypatch)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', 'fd', raising=False)
        _make_market(db, commence=10.0)  # long past kickoff, pending settlement
        db.set_guild_config(GUILD, 'bet_archived', '1')
        calls = []

        async def _fetch(token):
            calls.append(token)
            return []
        monkeypatch.setattr(football_data, 'fetch_wc_matches', _fetch)

        _run(Betting(bot=None)._settle_via_football_data())

        assert calls == []

    def test_settle_odds_api_never_fetches_when_archived(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        from tle.util import odds_api
        from tle import constants
        _setup(db, monkeypatch)
        monkeypatch.setattr(constants, 'ODDS_API_KEY', 'key', raising=False)
        monkeypatch.setattr(constants, 'BET_SETTLE_BUFFER_SECONDS', 0,
                            raising=False)
        _make_market(db, commence=10.0)
        db.set_guild_config(GUILD, 'bet_archived', '1')
        calls = []

        async def _scores(api_key, sport_key, event_ids=None):
            calls.append(sport_key)
            return []
        monkeypatch.setattr(odds_api, 'fetch_scores', _scores)

        _run(Betting(bot=None)._settle_via_odds_api())

        assert calls == []

    def test_configured_guilds_skips_archived(self, db, monkeypatch):
        from tle.cogs.betting import Betting
        _setup(db, monkeypatch)
        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        cog = Betting(bot)
        db.set_guild_config(GUILD, 'bet_channel', '222')
        assert cog._configured_guilds() == {int(GUILD): '222'}
        db.set_guild_config(GUILD, 'bet_archived', '1')
        assert cog._configured_guilds() == {}

    def test_on_message_thread_bet_dead_when_archived(self, db, monkeypatch):
        from tle.util import discord_common
        from tle.cogs.betting import Betting
        from tle import constants
        _setup(db, monkeypatch)
        monkeypatch.setattr(discord_common, '_BOT_PREFIX', ';', raising=False)
        monkeypatch.setattr(constants, 'BET_MIN_STAKE', 1, raising=False)
        import time as _t
        market_id = _make_market(db, commence=_t.time() + 3600)  # still open
        db.bet_market_set_thread(market_id, 333)
        db.bet_ensure_wallet(GUILD, '1', 1000)
        cog = Betting(bot=None)

        db.set_guild_config(GUILD, 'bet_archived', '1')
        msg = _FakeBetMessage('home 100')
        _run(cog.on_message(msg))
        assert msg.reactions == []
        assert db.bet_get_wagers_for_user(market_id, '1') == []

        # Same message with the flag off places the bet — proves the archived
        # branch (not some other filter) is what blocked it above.
        db.delete_guild_config(GUILD, 'bet_archived')
        live = _FakeBetMessage('home 100')
        _run(cog.on_message(live))
        assert live.reactions == ['✅']
        assert [w.stake for w in
                db.bet_get_wagers_for_user(market_id, '1')] == [100]
