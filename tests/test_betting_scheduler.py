"""Betting auto-open + scheduler tests, plus startup guards and configured-guild\ndiscovery."""
import pytest  # noqa: F401

from tle.util.db.user_db_conn import bet_fixture_key
from tests.betting_test_utils import (  # noqa: F401
    GUILD, CH, THREAD, USER_A, USER_B, db, _make_market,
    _FakeThread, _FakeMsg, _FakeChannel, _FakeGuild, _FakeBot,
    _wc_event, _fixture_key, _FakeBetMessage,
)


class _AllowedMentions:
    """Stub for discord.AllowedMentions so notify-role pings are inspectable."""
    def __init__(self, **kw):
        self.kw = kw


class TestAutoOpen:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    @pytest.fixture
    def setup(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'BET_MIN_STAKE', 1, raising=False)
        monkeypatch.setattr(constants, 'BET_OPEN_LEAD_SECONDS', 7200, raising=False)
        monkeypatch.setattr(constants, 'ODDS_API_KEY', 'testkey', raising=False)
        channel = _FakeChannel(222)
        guild = _FakeGuild(int(GUILD), channel)
        bot = _FakeBot([guild], {222: channel})
        cog = Betting(bot)
        db.set_guild_config(GUILD, 'bet_channel', '222')
        return cog, db, channel

    def _arm_events(self, cog, events, monkeypatch):
        import time as _t
        cog._wc_events = events
        cog._wc_fetched_at = _t.time()

        async def _fake_ensure(max_age):
            return events
        monkeypatch.setattr(cog, '_ensure_wc_events', _fake_ensure)

    def _patch_allowed_mentions(self, monkeypatch):
        import discord
        monkeypatch.setattr(discord, 'AllowedMentions', _AllowedMentions,
                            raising=False)

    def test_opens_market_and_thread_for_due_game(self, setup, monkeypatch):
        import time as _t
        cog, db, channel = setup
        ev = _wc_event(commence=_t.time() + 3600)  # kickoff in 1h → due
        self._arm_events(cog, [ev], monkeypatch)

        async def scenario():
            await cog._refresh_schedule()

            market = db.bet_market_get_active(GUILD, '222')
            assert market is not None
            assert market.home_team == 'Spain'
            assert market.odds_home == 1.25  # frozen from the event
            assert market.message_id is not None
            assert market.thread_id is not None
            assert len(channel.sent) == 1                 # one announcement
            assert channel.sent[0].thread is not None     # thread created
            assert len(channel.sent[0].thread.sent) == 1  # intro embed posted
            intro = channel.sent[0].thread.sent[0]
            market = db.bet_market_get(market.market_id)
            assert market.thread_intro_id == str(intro.id)
            assert market.market_id in cog._close_timers  # closes exactly at kickoff
            cog._close_timers[market.market_id].cancel()

        self._run(scenario())

    def test_open_announcement_pings_configured_notify_role(self, setup,
                                                            monkeypatch):
        import time as _t
        cog, db, channel = setup
        db.set_guild_config(GUILD, 'bet_notify_role', '444')
        ev = _wc_event(commence=_t.time() + 3600)
        self._arm_events(cog, [ev], monkeypatch)
        self._patch_allowed_mentions(monkeypatch)

        async def scenario():
            await cog._refresh_schedule()
            assert len(channel.sent) == 1
            assert channel.sent[0].content == '<@&444>'
            allowed = channel.sent[0].kw['allowed_mentions']
            assert allowed.kw['roles'] is True
            assert allowed.kw['users'] is False
            assert allowed.kw['everyone'] is False
            market = db.bet_market_get_active(GUILD, '222')
            cog._close_timers[market.market_id].cancel()

        self._run(scenario())

    def test_simultaneous_kickoffs_ping_role_only_once(self, setup, monkeypatch):
        """Two games kicking off at the same time both get a market + thread,
        but the notify role is tagged in only one of the announcements."""
        import time as _t
        cog, db, channel = setup
        db.set_guild_config(GUILD, 'bet_notify_role', '444')
        kickoff = _t.time() + 3600  # both due (within the 2h window)
        ev1 = _wc_event(event_id='wc1', home='Spain', away='Cape Verde',
                        commence=kickoff)
        ev2 = _wc_event(event_id='wc2', home='France', away='Argentina',
                        commence=kickoff)
        self._arm_events(cog, [ev1, ev2], monkeypatch)
        self._patch_allowed_mentions(monkeypatch)

        async def scenario():
            await cog._refresh_schedule()
            assert len(channel.sent) == 2                       # both announced
            assert all(m.thread is not None for m in channel.sent)  # both threaded
            pinged = [m for m in channel.sent if m.content == '<@&444>']
            silent = [m for m in channel.sent if m.content is None]
            assert len(pinged) == 1   # role tagged exactly once
            assert len(silent) == 1   # the simultaneous game stays quiet
            for market in db.bet_markets_open(GUILD):
                cog._close_timers[market.market_id].cancel()

        self._run(scenario())

    def test_staggered_kickoffs_each_ping(self, setup, monkeypatch):
        """Games at different kickoff times are independent — each pings."""
        import time as _t
        cog, db, channel = setup
        db.set_guild_config(GUILD, 'bet_notify_role', '444')
        ev1 = _wc_event(event_id='wc1', home='Spain', away='Cape Verde',
                        commence=_t.time() + 3600)
        ev2 = _wc_event(event_id='wc2', home='France', away='Argentina',
                        commence=_t.time() + 3600 + 1800)  # 30 min later
        self._arm_events(cog, [ev1, ev2], monkeypatch)
        self._patch_allowed_mentions(monkeypatch)

        async def scenario():
            await cog._refresh_schedule()
            pinged = [m for m in channel.sent if m.content == '<@&444>']
            assert len(pinged) == 2  # different kickoffs → both ping
            for market in db.bet_markets_open(GUILD):
                cog._close_timers[market.market_id].cancel()

        self._run(scenario())

    def test_channel_lifecycle_has_open_and_final_result_messages(self, setup,
                                                                   monkeypatch):
        import time as _t
        cog, db, channel = setup
        ev = _wc_event(commence=_t.time() + 3600)
        self._arm_events(cog, [ev], monkeypatch)

        async def scenario():
            await cog._refresh_schedule()
            market = db.bet_market_get_active(GUILD, '222')
            thread = channel.sent[0].thread
            cog.bot._channels[int(market.thread_id)] = thread
            db.bet_place(GUILD, market.market_id, USER_A, 'home', 100, 1.0, 1000)

            await cog._do_settle(market, 'home', 2, 1, source='auto')

            assert len(channel.sent) == 2
            assert len(thread.sent) == 1
            assert thread.archived is True
            cog._close_timers[market.market_id].cancel()

        self._run(scenario())

    def test_pool_refresh_edits_first_thread_message(self, setup, monkeypatch):
        import time as _t
        cog, db, channel = setup
        ev = _wc_event(commence=_t.time() + 3600)
        self._arm_events(cog, [ev], monkeypatch)

        async def scenario():
            await cog._refresh_schedule()
            market = db.bet_market_get_active(GUILD, '222')
            thread = channel.sent[0].thread
            cog.bot._channels[int(market.thread_id)] = thread
            intro = thread.sent[0]

            db.bet_place(GUILD, market.market_id, USER_A, 'home', 100, 1.0, 1000)
            await cog._refresh_pool_message(market.market_id)

            assert len(channel.sent) == 1
            assert intro.edited_embed is not None
            fields = {field['name']: field['value']
                      for field in intro.edited_embed.fields}
            assert 'Action so far' in fields
            assert 'Spain: 1 (100' in fields['Action so far']
            cog._close_timers[market.market_id].cancel()

        self._run(scenario())

    def test_fire_close_marks_market_and_edits_message_without_posting(self, setup):
        import time as _t
        cog, db, channel = setup
        msg = self._run(channel.send(embed=None))
        thread = _FakeThread(333)
        cog.bot._channels[333] = thread
        mid = db.bet_market_create(
            GUILD, '222', 'evtClose', 'soccer_fifa_world_cup', 'Spain',
            'Cape Verde', _t.time() - 1, 1.25, 5.5, 12.0, USER_A, 0.0)
        db.bet_market_set_message(mid, msg.id)
        db.bet_market_set_thread(mid, 333)

        self._run(cog._fire_close(mid))

        market = db.bet_market_get(mid)
        assert market.bets_closed == 1
        assert msg.edited_embed is not None
        assert 'Betting ended' in msg.edited_embed.description
        assert len(thread.sent) == 0

        self._run(cog._fire_close(mid))
        assert len(thread.sent) == 0

    def test_does_not_open_game_outside_window(self, setup, monkeypatch):
        import time as _t
        cog, db, channel = setup
        ev = _wc_event(commence=_t.time() + 10 * 3600)  # 10h away → not due
        self._arm_events(cog, [ev], monkeypatch)
        self._run(cog._refresh_schedule())
        assert db.bet_markets_open(GUILD) == []

    def test_does_not_open_already_started_game(self, setup, monkeypatch):
        """A game that has already kicked off gets NO market and NO thread —
        you can't bet on it (e.g. Spain vs Cape Verde already in progress)."""
        import time as _t
        cog, db, channel = setup
        ev = _wc_event(commence=_t.time() - 600)  # kicked off 10 min ago
        self._arm_events(cog, [ev], monkeypatch)
        self._run(cog._refresh_schedule())
        assert db.bet_markets_open(GUILD) == []
        assert len(channel.sent) == 0  # nothing posted, no thread

    def test_no_thread_for_started_market_missing_one(self, setup, monkeypatch):
        """A market that exists without a thread but has already kicked off
        must NOT get a thread attached retroactively."""
        import time as _t
        cog, db, channel = setup
        now = _t.time()
        # Open market in the DB, started, with no thread/message.
        db.bet_market_create(GUILD, '222', 'evtWC', 'soccer_fifa_world_cup',
                             'Spain', 'Cape Verde', now - 600,
                             1.25, 5.5, 12.0, USER_A, 0.0)
        self._run(cog._fire_open(bet_fixture_key(
            'soccer_fifa_world_cup', 'Spain', 'Cape Verde', now - 600)))
        market = db.bet_market_get_open_for_event(GUILD, 'evtWC')
        assert market.thread_id is None  # no thread attached post-kickoff
        assert len(channel.sent) == 0

    def test_idempotent_no_double_open(self, setup, monkeypatch):
        import time as _t
        cog, db, channel = setup
        ev = _wc_event(commence=_t.time() + 3600)
        self._arm_events(cog, [ev], monkeypatch)
        self._run(cog._refresh_schedule())
        self._run(cog._refresh_schedule())  # second pass
        assert len(db.bet_markets_open(GUILD)) == 1
        assert len(channel.sent) == 1

    def test_safety_net_event_id_drift_does_not_double_open(self, setup, monkeypatch):
        import time as _t
        cog, db, channel = setup
        old = _wc_event(event_id='old-id', commence=_t.time() + 3600)
        new = _wc_event(event_id='new-id', home='Cape Verde', away='Spain',
                        commence=old['commence_time'] + 15 * 60)
        self._arm_events(cog, [old], monkeypatch)
        self._run(cog._refresh_schedule())
        self._arm_events(cog, [new], monkeypatch)
        self._run(cog._refresh_schedule())
        assert len(db.bet_markets_open(GUILD)) == 1
        assert len(channel.sent) == 1

    def test_here_opens_due_market_immediately(self, setup, monkeypatch):
        """`;prediction here` runs a one-time watch pass, so a game already
        inside the 2h window opens at once (no 5-min wait)."""
        import time as _t
        from tle import constants
        from tle.cogs.betting import Betting
        cog, db, channel = setup
        monkeypatch.setattr(constants, 'ODDS_API_KEY', 'k', raising=False)
        ev = _wc_event(commence=_t.time() + 3600)  # due (within 2h)
        self._arm_events(cog, [ev], monkeypatch)

        class _Ctx:
            def __init__(self_):
                self_.guild = type('G', (), {'id': int(GUILD)})()
                self_.channel = channel
                self_.author = type('A', (), {'roles': []})()
                self_.sent = []

            async def send(self_, embed=None, **kw):
                self_.sent.append(embed)
                return None
        ctx = _Ctx()
        # Call the command body (conftest wraps commands in a stub holding the fn).
        self._run(Betting.here.__wrapped__(cog, ctx))

        assert db.get_guild_config(GUILD, 'bet_channel') == str(channel.id)
        market = db.bet_market_get_active(GUILD, str(channel.id))
        assert market is not None and market.home_team == 'Spain'

    def test_failed_send_does_not_leave_open_market(self, setup, monkeypatch):
        """If the announcement send fails, the reserved market is voided so the
        next tick can re-open cleanly."""
        import time as _t
        import discord
        cog, db, channel = setup
        ev = _wc_event(commence=_t.time() + 3600)
        self._arm_events(cog, [ev], monkeypatch)

        async def _boom(*a, **kw):
            raise discord.HTTPException()
        monkeypatch.setattr(channel, 'send', _boom)

        self._run(cog._refresh_schedule())
        assert db.bet_markets_open(GUILD) == []  # no open orphan

    def test_duplicate_fixture_posts_no_extra_announcement(self, setup, monkeypatch):
        import time as _t
        cog, db, channel = setup
        midday_utc = int(_t.time() // 86400) * 86400 + 12 * 3600
        old = _wc_event(event_id='old-id', commence=midday_utc)
        new = _wc_event(event_id='new-id', commence=old['commence_time'] + 15 * 60)
        db.bet_market_create(
            GUILD, '222', old['event_id'], old['sport_key'], old['home_team'],
            old['away_team'], old['commence_time'], 1.25, 5.5, 12.0, USER_A, 0.0)

        self._run(cog._open_market_auto(GUILD, '222', new))

        assert len(db.bet_markets_open(GUILD)) == 1
        assert len(channel.sent) == 0

    def test_skips_when_no_channel_configured(self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_OPEN_LEAD_SECONDS', 7200, raising=False)
        monkeypatch.setattr(constants, 'ODDS_API_KEY', 'testkey', raising=False)
        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        cog = Betting(bot)
        # no ;prediction here → bet_channel unset
        ev = _wc_event(commence=_t.time() + 3600)
        self._arm_events(cog, [ev], monkeypatch)
        self._run(cog._refresh_schedule())
        assert db.bet_markets_open(GUILD) == []


class TestScheduler:
    """Precise per-fixture open timers (the rpoll pattern): a future game is
    scheduled (not opened early), an in-window game opens now, and the armed
    timer fires on time and opens the market."""

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    @pytest.fixture
    def setup(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'BET_OPEN_LEAD_SECONDS', 7200, raising=False)
        monkeypatch.setattr(constants, 'ODDS_API_KEY', 'k', raising=False)
        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        cog = Betting(bot)
        db.set_guild_config(GUILD, 'bet_channel', '222')
        return cog, db, channel

    def _arm(self, cog, events, monkeypatch):
        import time as _t
        cog._wc_events = events
        cog._wc_fetched_at = _t.time()

        async def _fake_ensure(max_age):
            return events
        monkeypatch.setattr(cog, '_ensure_wc_events', _fake_ensure)

    def test_future_game_is_scheduled_not_opened(self, setup, monkeypatch):
        import time as _t

        async def scenario():
            cog, db, channel = setup
            ev = _wc_event(commence=_t.time() + 5 * 3600)  # 5h away
            self._arm(cog, [ev], monkeypatch)
            await cog._refresh_schedule()
            # No market opened early; a precise timer is armed instead.
            assert db.bet_markets_open(GUILD) == []
            key = _fixture_key(ev)
            assert key in cog._open_timers
            cog._open_timers[key].cancel()
        self._run(scenario())

    def test_duplicate_provider_events_arm_one_fixture_timer(self, setup, monkeypatch):
        import time as _t

        async def scenario():
            cog, db, channel = setup
            base = _t.time() + 5 * 3600
            ev1 = _wc_event(event_id='old-id', commence=base)
            ev2 = _wc_event(event_id='new-id', home='Cape Verde', away='Spain',
                            commence=base + 15 * 60)
            self._arm(cog, [ev1, ev2], monkeypatch)
            await cog._refresh_schedule()
            assert len(cog._open_timers) == 1
            task = next(iter(cog._open_timers.values()))
            task.cancel()
        self._run(scenario())

    def test_in_window_game_opens_now(self, setup, monkeypatch):
        import time as _t
        cog, db, channel = setup
        ev = _wc_event(commence=_t.time() + 3600)  # inside 2h window
        self._arm(cog, [ev], monkeypatch)
        self._run(cog._refresh_schedule())
        assert db.bet_market_get_active(GUILD, '222') is not None
        assert _fixture_key(ev) not in cog._open_timers  # opened directly, no timer

    def test_armed_timer_fires_on_time(self, setup, monkeypatch):
        """Arm a timer that should fire almost immediately and confirm it opens
        the market via the precise-timer path (not the safety net)."""
        import time as _t

        async def scenario():
            cog, db, channel = setup
            # open moment ≈ now + 0.05s → timer delay ~0.05s
            ev = _wc_event(commence=_t.time() + 7200 + 0.05)
            self._arm(cog, [ev], monkeypatch)
            await cog._refresh_schedule()
            assert db.bet_markets_open(GUILD) == []   # not yet (still future)
            assert _fixture_key(ev) in cog._open_timers
            await __import__('asyncio').sleep(0.2)     # let the timer fire
            assert db.bet_market_get_active(GUILD, '222') is not None
        self._run(scenario())


class TestStartupGuards:
    """The engine's first tick fires immediately and can race the bot's
    on_ready that sets cf_common.user_db — neither the engine nor the message
    listener may crash on a None user_db."""

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_engine_skips_when_db_uninitialized(self, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', None)
        cog = Betting(bot=None)
        called = {'refresh': False}

        async def _r():
            called['refresh'] = True
        monkeypatch.setattr(cog, '_refresh_schedule', _r)
        # Invoke the task body (conftest wraps it in a fake task-spec).
        self._run(cog._safety_net_task._func(cog, None))
        assert called['refresh'] is False  # short-circuited before any DB work

    def test_on_message_ignored_when_db_uninitialized(self, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.util import discord_common
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', None)
        monkeypatch.setattr(discord_common, '_BOT_PREFIX', ';', raising=False)
        cog = Betting(bot=None)
        # A valid-looking bet message must not raise despite user_db=None.
        self._run(cog.on_message(_FakeBetMessage('home 100')))


class TestConfiguredGuilds:
    def test_reads_channel_config(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        cog = Betting(bot)
        assert cog._configured_guilds() == {}
        db.set_guild_config(GUILD, 'bet_channel', '222')
        assert cog._configured_guilds() == {int(GUILD): '222'}
