"""Guild-configurable regular-user Grok allowance tests."""
import pytest

from tle import constants
from tle.cogs import _llm_limits as llm_limits
from tle.cogs import llm as llm_cog
from tle.util import codeforces_common as cf_common
from tle.util import discord_common
from tests.llm_test_utils import FakeLlmDb
from tests.test_llm_cog import FakeCtx
from tests.test_llm_grok import _add_xai_key, _invoke, _xai_answers


@pytest.fixture(autouse=True)
def db(monkeypatch):
    database = FakeLlmDb()
    values = {}
    database.get_guild_config = (
        lambda guild_id, key: values.get((str(guild_id), key)))
    database.set_guild_config = (
        lambda guild_id, key, value:
        values.__setitem__((str(guild_id), key), value))
    database.delete_guild_config = (
        lambda guild_id, key: values.pop((str(guild_id), key), None))
    database.config_values = values
    monkeypatch.setattr(cf_common, 'user_db', database, raising=False)
    monkeypatch.setattr(constants, 'XAI_API_KEYS', '')
    monkeypatch.setattr(discord_common, 'embed_alert',
                        lambda desc: f'ALERT: {desc}', raising=False)
    monkeypatch.setattr(discord_common, 'embed_success',
                        lambda desc: f'SUCCESS: {desc}', raising=False)
    monkeypatch.setattr(discord_common, 'embed_neutral',
                        lambda desc, **kw: f'NEUTRAL: {desc}', raising=False)
    return database


@pytest.fixture
def cog():
    return llm_cog.Llm(bot=None)


class TestRateLimitCommand:
    @pytest.mark.parametrize('role', (
        constants.TLE_ADMIN, constants.TLE_MODERATOR,
    ))
    def test_admin_and_moderator_can_set_and_inspect(
            self, role, cog, db):
        ctx = FakeCtx(roles=(role,))
        _invoke(llm_cog.Llm.ratelimit, cog, ctx, '7', '30m')

        assert db.config_values[('100', llm_limits.CONFIG_KEY)] == '7:1800'
        assert '7 requests per 30 minutes' in ctx.text

        inspect = FakeCtx(roles=(role,))
        _invoke(llm_cog.Llm.ratelimit, cog, inspect)
        assert '7 requests per 30 minutes' in inspect.text
        assert 'shared safeguards still apply' in inspect.text.casefold()

    def test_regular_user_cannot_change_the_limit(self, cog, db):
        ctx = FakeCtx()
        _invoke(llm_cog.Llm.ratelimit, cog, ctx, '2', '1h')
        assert db.config_values == {}
        assert 'admins or moderators' in ctx.text

    def test_off_and_default_are_explicit_persistent_states(self, cog, db):
        ctx = FakeCtx(roles=(constants.TLE_MODERATOR,))
        _invoke(llm_cog.Llm.ratelimit, cog, ctx, 'off')
        assert db.config_values[('100', llm_limits.CONFIG_KEY)] == 'off'
        assert llm_limits.resolve(db, 100).enabled is False

        _invoke(llm_cog.Llm.ratelimit, cog, ctx, 'default')
        assert ('100', llm_limits.CONFIG_KEY) not in db.config_values
        assert llm_limits.resolve(db, 100).requests == \
            constants.XAI_USER_RATE_LIMIT

    @pytest.mark.parametrize('arguments', (
        ('-1',), ('1001',), ('nope',), ('5', '10s'),
        ('5', '25h'), ('5', '1h', 'extra'),
    ))
    def test_invalid_values_do_not_mutate(self, arguments, cog, db):
        ctx = FakeCtx(roles=(constants.TLE_MODERATOR,))
        _invoke(llm_cog.Llm.ratelimit, cog, ctx, *arguments)
        assert db.config_values == {}
        assert ctx.text.startswith('ALERT:')

    def test_corrupt_storage_falls_back_to_conservative_default(self, db):
        db.config_values[('100', llm_limits.CONFIG_KEY)] = 'garbage'
        setting = llm_limits.resolve(db, 100)
        assert setting.requests == constants.XAI_USER_RATE_LIMIT
        assert setting.window_seconds == constants.XAI_USER_RATE_WINDOW_SECONDS
        assert setting.enabled is True

    def test_out_of_range_environment_window_falls_back_safely(
            self, db, monkeypatch):
        monkeypatch.setattr(
            constants, 'XAI_USER_RATE_WINDOW_SECONDS', 40 * 86400)
        setting = llm_limits.resolve(db, 100)
        assert setting.window_seconds == 3600
        assert setting.enabled is True

    def test_out_of_range_environment_count_falls_back_safely(
            self, db, monkeypatch):
        monkeypatch.setattr(constants, 'XAI_USER_RATE_LIMIT', 1_000_000)
        setting = llm_limits.resolve(db, 100)
        assert setting.requests == 15
        assert setting.enabled is True


class TestConfiguredAdmission:
    def test_override_is_enforced_and_isolated_by_guild(
            self, cog, db, monkeypatch):
        db.config_values[('100', llm_limits.CONFIG_KEY)] = '2:1800'
        _add_xai_key(db)
        seen = _xai_answers(monkeypatch)

        for _ in range(2):
            ctx = FakeCtx(guild_id=100)
            _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')
            assert 'Grok answer' in ctx.text
        blocked = FakeCtx(guild_id=100)
        _invoke(llm_cog.Llm.llm, cog, blocked, question='+grok hello')
        assert 'all 2 Grok requests' in blocked.text
        assert 'last 30 minutes' in blocked.text

        other = FakeCtx(guild_id=200)
        _invoke(llm_cog.Llm.llm, cog, other, question='+grok hello')
        assert 'Grok answer' in other.text
        assert len(seen) == 6

    @pytest.mark.parametrize('role', (
        constants.TLE_ADMIN, constants.TLE_MODERATOR,
    ))
    def test_privileged_roles_still_bypass_only_the_personal_guard(
            self, role, cog, db, monkeypatch):
        db.config_values[('100', llm_limits.CONFIG_KEY)] = '1:3600'
        _add_xai_key(db)
        _xai_answers(monkeypatch)

        for _ in range(2):
            ctx = FakeCtx(roles=(role,))
            _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')
            assert 'Grok answer' in ctx.text

    def test_off_skips_only_the_personal_guard(
            self, cog, db, monkeypatch):
        db.config_values[('100', llm_limits.CONFIG_KEY)] = 'off'
        _add_xai_key(db)
        _xai_answers(monkeypatch)
        monkeypatch.setattr(constants, 'XAI_DAILY_REQUEST_LIMIT', 2)

        for _ in range(2):
            ctx = FakeCtx()
            _invoke(llm_cog.Llm.llm, cog, ctx, question='+grok hello')
            assert 'Grok answer' in ctx.text
        blocked = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, blocked, question='+grok hello')
        assert 'shared daily allowance' in blocked.text
        assert '2 requests' not in blocked.text

    def test_lowering_and_raising_apply_without_clearing_history(
            self, cog, db, monkeypatch):
        db.config_values[('100', llm_limits.CONFIG_KEY)] = '3:3600'
        _add_xai_key(db)
        _xai_answers(monkeypatch)
        for _ in range(3):
            _invoke(llm_cog.Llm.llm, cog, FakeCtx(), question='+grok hello')

        db.config_values[('100', llm_limits.CONFIG_KEY)] = '2:3600'
        blocked = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, blocked, question='+grok hello')
        assert 'all 2 Grok requests' in blocked.text

        db.config_values[('100', llm_limits.CONFIG_KEY)] = '4:3600'
        allowed = FakeCtx()
        _invoke(llm_cog.Llm.llm, cog, allowed, question='+grok hello')
        assert 'Grok answer' in allowed.text
