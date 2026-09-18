"""Tests for integer environment settings (``tle/constants.py``).

``conftest`` replaces ``tle.constants`` with a stub, so these load the real
file under a private name. It imports only ``os`` and has no side effects
beyond computing paths, so loading it repeatedly is safe.
"""
import importlib.util
import os

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CONSTANTS_PATH = os.path.join(_ROOT, 'tle', 'constants.py')

_INT_SETTINGS = [
    'BET_START_BALANCE', 'BET_DAILY_AMOUNT',
    'LLM_MAX_PROMPT_CHARS', 'LLM_MAX_OUTPUT_TOKENS', 'LLM_MAX_IMAGES',
    'LLM_MAX_IMAGE_BYTES', 'LLM_MAX_TOTAL_IMAGE_BYTES',
    'LLM_CONTEXT_MESSAGES', 'LLM_CONTEXT_WINDOW_SECONDS',
    'LLM_CONTEXT_GAP_SECONDS', 'LLM_CONTEXT_RECENT_MAX_AGE_SECONDS',
    'LLM_REPLY_BEFORE', 'LLM_REPLY_AFTER',
    'XAI_MAX_OUTPUT_TOKENS', 'XAI_USER_RATE_LIMIT',
    'XAI_USER_RATE_WINDOW_SECONDS', 'XAI_DAILY_REQUEST_LIMIT',
]


def _load_constants(name='_real_tle_constants'):
    spec = importlib.util.spec_from_file_location(name, _CONSTANTS_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def constants():
    return _load_constants()


class TestIntEnv:
    def test_unset_uses_the_default(self, constants, monkeypatch):
        monkeypatch.delenv('SOME_SETTING', raising=False)
        assert constants._int_env('SOME_SETTING', 7) == 7

    def test_a_real_value_is_parsed(self, constants, monkeypatch):
        monkeypatch.setenv('SOME_SETTING', '42')
        assert constants._int_env('SOME_SETTING', 7) == 42

    def test_surrounding_whitespace_is_tolerated(self, constants, monkeypatch):
        monkeypatch.setenv('SOME_SETTING', '  42\n')
        assert constants._int_env('SOME_SETTING', 7) == 42

    @pytest.mark.parametrize('value', ['', '   ', '\n'])
    def test_exported_but_empty_falls_back(self, constants, monkeypatch, value):
        # environment.template teaches `export FOO=""` for settings you have
        # not filled in; a bare int('') would raise at import time.
        monkeypatch.setenv('SOME_SETTING', value)
        assert constants._int_env('SOME_SETTING', 7) == 7

    @pytest.mark.parametrize('value', ['abc', '1.5', '10k'])
    def test_unparseable_falls_back_instead_of_raising(self, constants,
                                                       monkeypatch, value):
        monkeypatch.setenv('SOME_SETTING', value)
        assert constants._int_env('SOME_SETTING', 7) == 7

    def test_negative_values_are_allowed_through(self, constants, monkeypatch):
        monkeypatch.setenv('SOME_SETTING', '-3')
        assert constants._int_env('SOME_SETTING', 7) == -3


class TestImportSurvivesEmptyEnvironment:
    """The template's `export FOO=""` idiom must not kill startup.

    constants.py is imported before logging or any error handling exists, so a
    ValueError here takes the bot down with a bare traceback.
    """

    def test_every_int_setting_exported_empty_still_imports(self, monkeypatch):
        for name in _INT_SETTINGS:
            monkeypatch.setenv(name, '')
        module = _load_constants('_constants_empty_env')
        for name in _INT_SETTINGS:
            assert isinstance(getattr(module, name), int)

    def test_every_int_setting_exported_as_junk_still_imports(self, monkeypatch):
        for name in _INT_SETTINGS:
            monkeypatch.setenv(name, 'not-a-number')
        module = _load_constants('_constants_junk_env')
        for name in _INT_SETTINGS:
            assert isinstance(getattr(module, name), int)

    def test_llm_models_survives_an_empty_export(self, monkeypatch):
        monkeypatch.setenv('LLM_MODELS', '')
        module = _load_constants('_constants_empty_models')
        # Empty means "no models configured", not a tuple holding ''.
        assert module.LLM_MODELS == ()

    def test_llm_models_ignores_stray_commas_and_spaces(self, monkeypatch):
        monkeypatch.setenv('LLM_MODELS', ' model-a , , model-b ,')
        module = _load_constants('_constants_messy_models')
        assert module.LLM_MODELS == ('model-a', 'model-b')

    def test_defaults_are_current_model_ids(self, monkeypatch):
        # gemini-2.0-flash and gemini-2.0-flash-lite are shut down; a default
        # pointing at either would break every install that does not override.
        monkeypatch.delenv('LLM_MODELS', raising=False)
        module = _load_constants('_constants_default_models')
        assert module.LLM_MODELS
        assert not any(name.startswith('gemini-2.0') for name in module.LLM_MODELS)


class TestXaiEnvironment:
    def test_default_model_is_current_grok(self, monkeypatch):
        monkeypatch.delenv('XAI_MODEL', raising=False)
        module = _load_constants('_constants_xai_default')
        assert module.XAI_MODEL == 'grok-4.5'

    def test_empty_model_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv('XAI_MODEL', '   ')
        module = _load_constants('_constants_xai_empty_model')
        assert module.XAI_MODEL == 'grok-4.5'

    def test_singular_and_plural_key_settings_are_merged(self, monkeypatch):
        monkeypatch.setenv('XAI_API_KEY', 'xai-single')
        monkeypatch.setenv('XAI_API_KEYS', 'xai-one,xai-two')
        module = _load_constants('_constants_xai_keys')
        assert module.XAI_API_KEYS == 'xai-one,xai-two,xai-single'

    def test_credit_guard_defaults_are_conservative(self, monkeypatch):
        for name in ('XAI_MAX_OUTPUT_TOKENS', 'XAI_USER_RATE_LIMIT',
                     'XAI_USER_RATE_WINDOW_SECONDS', 'XAI_DAILY_REQUEST_LIMIT',
                     'XAI_INPUT_USD_PER_MILLION',
                     'XAI_OUTPUT_USD_PER_MILLION', 'XAI_DAILY_BUDGET_USD'):
            monkeypatch.delenv(name, raising=False)
        module = _load_constants('_constants_xai_limits')
        assert module.XAI_MAX_OUTPUT_TOKENS == 1536
        assert module.XAI_USER_RATE_LIMIT == 15
        assert module.XAI_USER_RATE_WINDOW_SECONDS == 60 * 60
        assert module.XAI_DAILY_REQUEST_LIMIT == 200
        assert module.XAI_INPUT_USD_PER_MILLION == 2.00
        assert module.XAI_OUTPUT_USD_PER_MILLION == 6.00
        assert module.XAI_DAILY_BUDGET_USD == 0.50
