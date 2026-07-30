"""Guard-rail tests for the ``;llm`` cog.

Covers the paths that only matter when something goes wrong: a non-moderator
pasting a key, billing a failed request, and process-lifecycle edges. Split
from ``test_llm_cog.py`` to keep both files under the 500-line limit.
"""
import asyncio

import pytest
from discord.ext import commands

from tle import constants
from tle.cogs import llm as llm_cog
from tle.util import codeforces_common as cf_common
from tle.util import discord_common, gemini_api
from tle.cogs.llm import _today
from tests.llm_test_utils import FakeLlmDb, FakeMessage, run
from tests.test_llm_cog import FakeCtx


@pytest.fixture(autouse=True)
def db(monkeypatch):
    database = FakeLlmDb()
    monkeypatch.setattr(cf_common, 'user_db', database, raising=False)
    monkeypatch.setattr(discord_common, 'embed_alert',
                        lambda desc: f'ALERT: {desc}', raising=False)
    monkeypatch.setattr(discord_common, 'embed_success',
                        lambda desc: f'SUCCESS: {desc}', raising=False)
    return database


@pytest.fixture
def cog():
    return llm_cog.Llm(bot=None)


class TestLooksLikeApiKey:
    @pytest.mark.parametrize('token', [
        'AIzaSyRealLookingKeyAAAAAAAA',
        '`AIzaSyRealLookingKeyAAAAAAAA`',
        '<AIzaSyRealLookingKeyAAAAAAAA>',
        'abcdefghij0123456789_-abcdef',
    ])
    def test_credentials_are_recognized(self, token):
        assert llm_cog.looks_like_api_key(token) is True

    @pytest.mark.parametrize('token', [
        'are', 'overrated', 'keys', '', None, 'short',
        'a sentence with spaces in it and plenty of length',
        'punctuation!!!!!!!!!!!!!!!!!!!!!!',
    ])
    def test_ordinary_words_are_not(self, token):
        assert llm_cog.looks_like_api_key(token) is False


class TestNonModeratorKeyPaste:
    """A failed role check happens *before* the command body runs.

    Without cog_command_error the message is never deleted and MissingAnyRole
    is only logged, so someone who does not realise they lack the role pastes a
    live key and sees nothing at all happen.
    """

    def _run_handler(self, cog, ctx):
        return run(cog.cog_command_error(ctx, commands.MissingAnyRole(['Admin'])))

    def test_a_pasted_key_is_deleted_and_flagged(self, cog):
        message = FakeMessage(content=';llm keys AIzaSyRealLookingKeyAAAAAAAA')
        ctx = FakeCtx(message=message)
        self._run_handler(cog, ctx)
        assert message.deleted is True
        assert 'moderators' in ctx.text
        assert 'revoke' in ctx.text

    def test_the_warning_does_not_echo_the_key(self, cog):
        key = 'AIzaSyRealLookingKeyAAAAAAAA'
        ctx = FakeCtx(message=FakeMessage(content=f';llm keys {key}'))
        self._run_handler(cog, ctx)
        assert key not in ctx.text

    def test_an_undeletable_key_message_escalates_the_warning(self, cog):
        message = FakeMessage(content=';llm keys AIzaSyRealLookingKeyAAAAAAAA')
        message.delete_error = RuntimeError('missing permissions')
        ctx = FakeCtx(message=message)
        self._run_handler(cog, ctx)
        assert 'could not delete' in ctx.text
        assert 'revoke that key now' in ctx.text

    def test_a_plain_sentence_is_explained_not_deleted(self, cog):
        # "keys" is an ordinary English word, so `;llm keys are overrated`
        # dispatches the moderator subcommand rather than asking a question.
        message = FakeMessage(content=';llm keys are overrated')
        ctx = FakeCtx(message=message)
        self._run_handler(cog, ctx)
        assert message.deleted is False
        assert 'Rephrase' in ctx.text

    def test_the_error_is_marked_handled(self, cog):
        error = commands.MissingAnyRole(['Admin'])
        ctx = FakeCtx(message=FakeMessage(content=';llm keylist'))
        run(cog.cog_command_error(ctx, error))
        # Otherwise the global handler logs it as an unexpected exception.
        assert error.handled is True

    def test_unrelated_errors_are_left_alone(self, cog):
        ctx = FakeCtx(message=FakeMessage(content=';llm hello'))
        run(cog.cog_command_error(ctx, commands.BadArgument('nope')))
        assert ctx.sent == []


class TestFailedCallAccounting:
    @pytest.fixture(autouse=True)
    def _key(self, db):
        db.llm_add_key('AIzaSyExampleKeyValue1234567')

    def _fail_with(self, monkeypatch, error, attempts):
        async def fake_complete(pool, prompt, **kwargs):
            stats = kwargs.get('stats')
            if stats is not None:
                stats['attempts'] = attempts
            raise error

        monkeypatch.setattr(gemini_api, 'complete', fake_complete)

    def test_a_failure_that_reached_google_is_billed(self, cog, monkeypatch, db):
        # One invocation can walk several buckets; not counting those lets a
        # user drain the shared allowance on calls that happen to fail.
        self._fail_with(monkeypatch, gemini_api.GeminiError('boom'), attempts=3)
        _invoke(cog, FakeCtx(), 'hi?')
        assert db.llm_get_usage(100, 1, _today()) == 1

    def test_a_failure_that_never_left_the_bot_is_free(self, cog, monkeypatch, db):
        self._fail_with(monkeypatch,
                        gemini_api.NoCapacityError('spent', retry_after=60),
                        attempts=0)
        _invoke(cog, FakeCtx(), 'hi?')
        assert db.llm_get_usage(100, 1, _today()) == 0


class TestFailureMessages:
    def test_drained_pool_quotes_a_wait(self):
        err = gemini_api.NoCapacityError('spent', retry_after=7200)
        assert '2h' in llm_cog.Llm._describe_failure(err)

    def test_attempt_ceiling_does_not_say_unknown(self):
        # retry_after is None here; format_duration(None) renders "unknown",
        # which would surface as "Try again in unknown."
        err = gemini_api.NoCapacityError('gave up', attempts_exhausted=True)
        text = llm_cog.Llm._describe_failure(err)
        assert 'unknown' not in text
        assert 'moment' in text

    def test_drained_pool_without_a_hint_avoids_unknown(self):
        err = gemini_api.NoCapacityError('spent', retry_after=None)
        assert 'unknown' not in llm_cog.Llm._describe_failure(err)

    def test_raw_upstream_text_is_truncated(self):
        err = gemini_api.GeminiError('<html>' + 'padding ' * 5000 + '</html>')
        text = llm_cog.Llm._describe_failure(err)
        assert len(text) < 1000

    def test_model_misconfiguration_points_at_the_setting(self):
        err = gemini_api.ModelUnavailableError('models/x not found')
        assert 'LLM_MODELS' in llm_cog.Llm._describe_failure(err)


class TestNotReadyDuringStartup:
    """``cf_common.initialize`` assigns ``user_db`` only after fetching
    Codeforces data, and the bot accepts commands throughout — so for the
    first seconds after a restart every command in this cog hit
    ``AttributeError: 'NoneType' object has no attribute 'llm_get_keys'``.
    """

    def test_db_helper_raises_a_named_error(self, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', None, raising=False)
        with pytest.raises(llm_cog.LlmNotReadyError):
            llm_cog._db()

    def test_the_error_tells_the_user_to_wait(self, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', None, raising=False)
        try:
            llm_cog._db()
        except llm_cog.LlmNotReadyError as err:
            assert 'starting up' in str(err)

    def test_asking_before_ready_gets_a_message_not_a_traceback(self, cog,
                                                                monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', None, raising=False)
        ctx = FakeCtx()
        error = None
        try:
            run(llm_cog.Llm.llm.__wrapped__(cog, ctx, question='hi?'))
        except llm_cog.LlmNotReadyError as err:
            error = err
        assert error is not None
        run(cog.cog_command_error(ctx, error))
        assert 'starting up' in ctx.text
        assert error.handled is True

    def test_a_none_database_is_never_cached_into_the_pool(self, cog, db,
                                                           monkeypatch):
        # The original bug snapshotted user_db at first use, so a None grabbed
        # during startup would persist for the life of the process.
        monkeypatch.setattr(cf_common, 'user_db', None, raising=False)
        with pytest.raises(llm_cog.LlmNotReadyError):
            cog._get_pool()
        monkeypatch.setattr(cf_common, 'user_db', db, raising=False)
        assert cog._get_pool().db is db

    def test_the_pool_is_rebuilt_when_the_connection_changes(self, cog, db,
                                                             monkeypatch):
        first = cog._get_pool()
        replacement = FakeLlmDb()
        monkeypatch.setattr(cf_common, 'user_db', replacement, raising=False)
        assert cog._get_pool() is not first
        assert cog._get_pool().db is replacement


class TestCogUnload:
    def test_unloading_without_a_running_loop_does_not_raise(self, cog):
        cog._session = _FakeSession()
        cog.cog_unload()  # no event loop running here

    def test_unloading_inside_the_loop_closes_the_session(self, cog):
        session = _FakeSession()
        cog._session = session

        async def main():
            cog.cog_unload()
            await asyncio.sleep(0)

        asyncio.run(main())
        assert session.closed is True

    def test_no_session_is_a_no_op(self, cog):
        cog.cog_unload()


class _FakeSession:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


def _invoke(cog, ctx, question):
    return run(llm_cog.Llm.llm.__wrapped__(cog, ctx, question=question))
