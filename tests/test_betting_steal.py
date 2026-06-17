"""Tests for the gated ``;bet steal`` command and its wallet DB methods.

Split into its own module (per the 500-line limit); shares fixtures/constants
with the other betting test modules via ``tests/betting_test_utils``.
"""
import pytest

from tests.betting_test_utils import GUILD, USER_A, USER_B, db  # noqa: F401


class TestSteal:
    def test_success_steals_half_of_target_wallet_capped_by_thief_wallet(self, db):
        db.bet_set_balance(GUILD, USER_A, 10, 1000)
        db.bet_set_balance(GUILD, USER_B, 501, 1000)

        result = db.bet_attempt_steal(
            GUILD, USER_A, USER_B, '2026-06-15', True, 1000,
            attempted_at=7.0)

        assert result == (True, 'ok', 15, 496, 5)
        assert db.bet_get_balance(GUILD, USER_A) == 15
        assert db.bet_get_balance(GUILD, USER_B) == 496
        thief_hist = db.bet_wallet_history(GUILD, USER_A)
        victim_hist = db.bet_wallet_history(GUILD, USER_B)
        thief_row = next(r for r in thief_hist if r.action == 'steal_success')
        victim_row = next(r for r in victim_hist if r.action == 'steal_victim')
        assert thief_row.amount == 5
        assert thief_row.note == USER_B
        assert victim_row.amount == -5
        assert victim_row.actor_id == USER_A

    def test_failure_zeroes_thief_wallet(self, db):
        db.bet_set_balance(GUILD, USER_A, 400, 1000)
        db.bet_set_balance(GUILD, USER_B, 900, 1000)

        result = db.bet_attempt_steal(
            GUILD, USER_A, USER_B, '2026-06-15', False, 1000,
            attempted_at=7.0)

        assert result == (True, 'caught', 0, 900, 0)
        assert db.bet_get_balance(GUILD, USER_A) == 0
        assert db.bet_get_balance(GUILD, USER_B) == 900
        hist = db.bet_wallet_history(GUILD, USER_A)
        row = next(r for r in hist if r.action == 'steal_caught')
        assert row.amount == -400
        assert row.balance_after == 0

    def test_only_once_per_day(self, db):
        db.bet_set_balance(GUILD, USER_A, 100, 1000)
        db.bet_set_balance(GUILD, USER_B, 1000, 1000)
        db.bet_attempt_steal(GUILD, USER_A, USER_B, '2026-06-15', True, 1000)

        result = db.bet_attempt_steal(
            GUILD, USER_A, USER_B, '2026-06-15', False, 1000)

        assert result == (False, 'already', 150, 950, 0)
        assert db.bet_get_balance(GUILD, USER_A) == 150
        assert db.bet_get_balance(GUILD, USER_B) == 950
        assert [r.action for r in db.bet_wallet_history(GUILD, USER_A)].count(
            'steal_success') == 1

    def test_zero_value_steal_does_not_consume_attempt(self, db):
        db.bet_set_balance(GUILD, USER_A, 1, 1000)
        db.bet_set_balance(GUILD, USER_B, 1000, 1000)

        result = db.bet_attempt_steal(
            GUILD, USER_A, USER_B, '2026-06-15', True, 1000)

        assert result == (False, 'empty', 1, 1000, 0)
        db.bet_set_balance(GUILD, USER_A, 100, 1000)
        retry = db.bet_attempt_steal(
            GUILD, USER_A, USER_B, '2026-06-15', True, 1000)
        assert retry == (True, 'ok', 150, 950, 50)

    def test_missing_target_wallet_does_not_consume_attempt(self, db):
        db.bet_set_balance(GUILD, USER_A, 100, 1000)

        result = db.bet_attempt_steal(
            GUILD, USER_A, USER_B, '2026-06-15', True, 1000)

        assert result == (False, 'missing', 100, None, 0)
        assert db.bet_get_balance(GUILD, USER_B) is None
        retry = db.bet_attempt_steal(
            GUILD, USER_A, USER_A + '1', '2026-06-15', True, 1000)
        assert retry[1] == 'missing'


class TestStealCommand:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def _member(self, uid, name, *, bot=False):
        return type('Member', (), {
            'id': uid,
            'display_name': name,
            'bot': bot,
        })()

    def _ctx(self, author):
        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': int(GUILD)})()
                self.author = author
                self.sent = []

            async def send(self, *args, **kw):
                self.sent.append((args, kw))

        return _Ctx()

    def _sent_embed(self, ctx, index=0):
        args, kw = ctx.sent[index]
        embed = kw.get('embed')
        if embed is not None:
            return embed
        return args[0] if args else None

    def _patch_embeds(self, monkeypatch):
        from tle.util import discord_common

        def _embed(desc):
            return type('Embed', (), {'description': str(desc)})()

        monkeypatch.setattr(discord_common, 'embed_success', _embed)
        monkeypatch.setattr(discord_common, 'embed_alert', _embed)

    def _seed_cf_country(self, db, user_id, country):
        handle = f'handle{user_id}'
        db.set_handle(user_id, GUILD, handle)
        db.cache_cf_user((
            handle, '', '', country, '', '', 0, 1500, 1500, 0, 0, 0, '',
        ))

    def test_steal_command_success(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs import betting
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(betting.random, 'random', lambda: 0.1)
        self._patch_embeds(monkeypatch)

        thief = self._member(USER_A, 'Alice')
        victim = self._member(USER_B, 'Bob')
        self._seed_cf_country(db, USER_A, 'Romania')
        db.bet_set_balance(GUILD, USER_A, 100, 1000)
        db.bet_set_balance(GUILD, USER_B, 800, 1000)
        ctx = self._ctx(thief)
        cog = Betting(bot=None)

        self._run(Betting.steal.__wrapped__(cog, ctx, victim))

        assert db.bet_get_balance(GUILD, USER_A) == 100
        assert db.bet_get_balance(GUILD, USER_B) == 800
        assert 'bet steal confirm' in self._sent_embed(ctx).description

        self._run(Betting.steal_confirm.__wrapped__(cog, ctx))
        assert db.bet_get_balance(GUILD, USER_A) == 150
        assert db.bet_get_balance(GUILD, USER_B) == 750
        assert 'stole **50**' in self._sent_embed(ctx, index=1).description

    def test_steal_command_caught(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs import betting
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(betting.random, 'random', lambda: 0.9)
        self._patch_embeds(monkeypatch)

        thief = self._member(USER_A, 'Alice')
        victim = self._member(USER_B, 'Bob')
        self._seed_cf_country(db, USER_A, 'Romania')
        db.bet_set_balance(GUILD, USER_A, 300, 1000)
        db.bet_set_balance(GUILD, USER_B, 800, 1000)
        ctx = self._ctx(thief)
        cog = Betting(bot=None)

        self._run(Betting.steal.__wrapped__(cog, ctx, victim))
        self._run(Betting.steal_confirm.__wrapped__(cog, ctx))

        assert db.bet_get_balance(GUILD, USER_A) == 0
        assert db.bet_get_balance(GUILD, USER_B) == 800
        assert 'got caught by police' in self._sent_embed(ctx, index=1).description

    def test_steal_command_rejects_self(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting, BettingCogError
        monkeypatch.setattr(cf_common, 'user_db', db)

        thief = self._member(USER_A, 'Alice')
        self._seed_cf_country(db, USER_A, 'Romania')
        ctx = self._ctx(thief)

        with pytest.raises(BettingCogError, match='yourself'):
            self._run(Betting.steal.__wrapped__(Betting(bot=None), ctx, thief))

    def test_steal_command_requires_romania_cf_country(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting, BettingCogError
        monkeypatch.setattr(cf_common, 'user_db', db)

        thief = self._member(USER_A, 'Alice')
        victim = self._member(USER_B, 'Bob')
        self._seed_cf_country(db, USER_A, 'Moldova')
        ctx = self._ctx(thief)

        with pytest.raises(BettingCogError, match='Romania'):
            self._run(Betting.steal.__wrapped__(Betting(bot=None), ctx, victim))
