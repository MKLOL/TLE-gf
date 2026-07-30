"""Tests for model/reasoning-tier selection (``tle/util/llm_models.py``)."""
import pytest

from tle.util import llm_models


class TestFind:
    @pytest.mark.parametrize('name,expected', [
        ('3.5f', 'gemini-3.5-flash'),
        ('3.5l', 'gemini-3.5-flash-lite'),
        ('3.1l', 'gemini-3.1-flash-lite'),
        ('2.5f', 'gemini-2.5-flash'),
        ('2.5l', 'gemini-2.5-flash-lite'),
        ('3.6f', 'gemini-3.6-flash'),
        ('pro', 'gemini-2.5-pro'),
    ])
    def test_short_aliases_resolve(self, name, expected):
        assert llm_models.find(name).model_id == expected

    @pytest.mark.parametrize('name,expected', [
        ('gemini-3.5-flash', 'gemini-3.5-flash'),
        ('3.5-flash', 'gemini-3.5-flash'),
        ('3.5', 'gemini-3.5-flash'),
        ('  3.5F  ', 'gemini-3.5-flash'),
        ('2.5-flash-lite', 'gemini-2.5-flash-lite'),
        ('lite', 'gemini-3.1-flash-lite'),
    ])
    def test_long_spellings_still_work(self, name, expected):
        # Kept as synonyms so anything already typed keeps working.
        assert llm_models.find(name).model_id == expected

    def test_the_displayed_alias_is_the_short_one(self):
        for spec in llm_models.CATALOG:
            assert len(spec.aliases[0]) <= 4

    @pytest.mark.parametrize('name', ['', None, 'gpt-4', 'what', 'gemini-9'])
    def test_unknown_names_are_none(self, name):
        assert llm_models.find(name) is None

    def test_no_shut_down_models_are_offered(self):
        # gemini-2.0-flash and -flash-lite are retired.
        assert not any(spec.model_id.startswith('gemini-2.0')
                       for spec in llm_models.CATALOG)


class TestParseSelector:
    def test_bare_model(self):
        spec, tier = llm_models.parse_selector('3.5f')
        assert spec.model_id == 'gemini-3.5-flash'
        assert tier is None

    @pytest.mark.parametrize('token,expected', [
        ('3.5f-h', 'high'),
        ('3.5f-m', 'medium'),
        ('3.5f-l', 'low'),
        ('3.5f-min', 'minimal'),
        ('3.5f-med', 'medium'),
        ('3.5f-high', 'high'),
        ('3.5f-minimal', 'minimal'),
    ])
    def test_tier_shorthand_and_long_forms(self, token, expected):
        assert llm_models.parse_selector(token)[1] == expected

    @pytest.mark.parametrize('token', ['2.5f-off', '2.5f-no', '2.5f-0'])
    def test_off_shorthand(self, token):
        assert llm_models.parse_selector(token)[1] == 'off'

    def test_a_single_l_after_a_lite_model_is_the_tier(self):
        # `3.5l-l` is flash-lite at low reasoning, not two model names.
        spec, tier = llm_models.parse_selector('3.5l-l')
        assert spec.model_id == 'gemini-3.5-flash-lite'
        assert tier == 'low'

    def test_full_id_with_tier(self):
        spec, tier = llm_models.parse_selector('gemini-2.5-flash-off')
        assert spec.model_id == 'gemini-2.5-flash'
        assert tier == 'off'

    def test_a_lite_model_is_not_confused_with_a_tier(self):
        # "-lite" is part of the model name, not a reasoning tier.
        spec, tier = llm_models.parse_selector('2.5-flash-lite')
        assert spec.model_id == 'gemini-2.5-flash-lite'
        assert tier is None

    def test_lite_model_with_a_tier(self):
        spec, tier = llm_models.parse_selector('2.5-flash-lite-high')
        assert spec.model_id == 'gemini-2.5-flash-lite'
        assert tier == 'high'

    @pytest.mark.parametrize('token', ['what', 'why', '', None, 'hello-high',
                                       'a-h', '3.9f'])
    def test_non_models_return_none(self, token):
        assert llm_models.parse_selector(token) is None

    def test_a_tier_the_model_rejects_raises(self):
        # 2.5-pro cannot turn thinking off.
        with pytest.raises(ValueError) as excinfo:
            llm_models.parse_selector('pro-off')
        assert 'does not support' in str(excinfo.value)

    def test_the_rejection_names_the_short_alias(self):
        with pytest.raises(ValueError) as excinfo:
            llm_models.parse_selector('2.5f-min')
        assert '2.5f' in str(excinfo.value)

    def test_minimal_is_a_3x_only_tier(self):
        assert llm_models.parse_selector('3.5f-min')[1] == 'minimal'
        with pytest.raises(ValueError):
            llm_models.parse_selector('2.5f-min')


class TestSplitSelector:
    def test_leading_model_is_split_off(self):
        spec, tier, rest = llm_models.split_selector('3.5f why is this TLE?')
        assert spec.model_id == 'gemini-3.5-flash'
        assert tier is None
        assert rest == 'why is this TLE?'

    def test_leading_model_and_tier(self):
        spec, tier, rest = llm_models.split_selector('3.5f-h explain')
        assert tier == 'high'
        assert rest == 'explain'

    def test_a_plain_question_keeps_its_first_word(self):
        spec, tier, rest = llm_models.split_selector('why is this TLE?')
        assert spec is None and tier is None
        assert rest == 'why is this TLE?'

    def test_model_with_no_question(self):
        spec, tier, rest = llm_models.split_selector('3.5f')
        assert spec.model_id == 'gemini-3.5-flash'
        assert rest == ''

    @pytest.mark.parametrize('text', ['', None, '   '])
    def test_empty_input(self, text):
        assert llm_models.split_selector(text) == (None, None, '' if not text
                                                   else text.strip())

    def test_a_bad_tier_propagates(self):
        with pytest.raises(ValueError):
            llm_models.split_selector('pro-off hello')


class TestThinkingConfig:
    def test_3x_models_use_thinking_level(self):
        assert llm_models.thinking_config('gemini-3.5-flash', 'high') == \
            {'thinkingLevel': 'high'}

    def test_25_models_use_thinking_level_for_real_tiers(self):
        assert llm_models.thinking_config('gemini-2.5-flash', 'low') == \
            {'thinkingLevel': 'low'}

    def test_off_becomes_a_zero_budget(self):
        # The 2.5 family has no "off" level; a zero budget disables thinking.
        assert llm_models.thinking_config('gemini-2.5-flash', 'off') == \
            {'thinkingBudget': 0}

    def test_no_tier_sends_nothing(self):
        assert llm_models.thinking_config('gemini-3.5-flash', None) is None

    def test_unknown_model_sends_nothing(self):
        # Better to let Google apply its default than guess an encoding.
        assert llm_models.thinking_config('model-a', 'high') is None

    def test_a_tier_the_model_rejects_sends_nothing(self):
        assert llm_models.thinking_config('gemini-2.5-pro', 'off') is None


class TestDescribeCatalog:
    def test_lists_every_model_by_short_alias(self):
        text = llm_models.describe_catalog()
        for spec in llm_models.CATALOG:
            assert spec.model_id in text
            assert f'`{spec.aliases[0]}`' in text

    def test_tiers_are_described_separately_and_briefly(self):
        # Repeating "minimal/low/medium/high" on all seven rows is the noise
        # that made the list unreadable in the first place.
        assert 'minimal/low/medium/high' not in llm_models.describe_catalog()
        tiers = llm_models.describe_tiers()
        for shorthand in ('-min', '-l', '-m', '-h', '-off'):
            assert shorthand in tiers
