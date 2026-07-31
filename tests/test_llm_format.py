"""Tests for ``;llm`` presentation helpers (``tle/cogs/_llm_format.py``).

The redaction tests matter most: nothing in this cog may put key material on
screen, and these are the assertions that keep it that way.
"""
import pytest

from tle.cogs import _llm_format as llm_format
from tests.llm_test_utils import FakeLlmDb


class TestRedactKey:
    def test_keeps_only_a_recognizable_prefix_and_suffix(self):
        redacted = llm_format.redact_key('AIzaSyABCDEFGHIJKLMNOPQRSTUV1234')
        assert redacted == 'AIzaSy…1234'

    def test_the_secret_middle_is_gone(self):
        key = 'AIzaSyABCDEFGHIJKLMNOPQRSTUV1234'
        assert 'GHIJKLMNOP' not in llm_format.redact_key(key)

    def test_short_strings_are_fully_masked(self):
        # Too short to redact safely — showing 10 of 12 characters would be
        # worse than showing none.
        assert llm_format.redact_key('shortkey') == '*' * 8

    def test_empty_key(self):
        assert llm_format.redact_key('') == '(empty)'
        assert llm_format.redact_key(None) == '(empty)'


class TestFormatDuration:
    @pytest.mark.parametrize('seconds,expected', [
        (0, '0s'), (45, '45s'), (59, '59s'), (60, '1min'), (600, '10min'),
        (3600, '1h'), (12000, '3h 20min'), (None, 'unknown'), (-5, '0s'),
    ])
    def test_formats(self, seconds, expected):
        assert llm_format.format_duration(seconds) == expected


class TestSplitForEmbed:
    def test_short_text_is_one_chunk(self):
        assert llm_format.split_for_embed('hello') == ['hello']

    def test_empty_text_gets_a_placeholder(self):
        assert llm_format.split_for_embed('   ') == ['*(empty answer)*']

    def test_long_text_is_split_within_the_limit(self):
        chunks = llm_format.split_for_embed('x' * 5000, limit=1000)
        assert all(len(chunk) <= 1000 for chunk in chunks)

    def test_split_prefers_paragraph_boundaries(self):
        text = ('a' * 600) + '\n\n' + ('b' * 600)
        chunks = llm_format.split_for_embed(text, limit=1000)
        assert chunks[0] == 'a' * 600
        assert chunks[1] == 'b' * 600

    def test_no_content_is_lost_when_splitting(self):
        text = '\n\n'.join('para %d %s' % (i, 'y' * 300) for i in range(10))
        chunks = llm_format.split_for_embed(text, limit=1000, max_pages=99)
        assert ''.join(chunks).replace('\n', '') == text.replace('\n', '')

    def test_runaway_answers_are_truncated_with_a_marker(self):
        chunks = llm_format.split_for_embed('z' * 100000, limit=1000, max_pages=2)
        assert len(chunks) == 2
        assert 'truncated' in chunks[-1]


class TestBuildAnswerEmbeds:
    def test_single_embed_carries_the_answer_and_model_footer(self):
        embeds = llm_format.build_answer_embeds('the answer', 'model-a')
        assert len(embeds) == 1
        assert embeds[0].description == 'the answer'
        assert embeds[0].footer['text'] == 'model-a'

    def test_only_the_last_embed_gets_a_footer(self):
        embeds = llm_format.build_answer_embeds('q' * 9000, 'model-a')
        assert len(embeds) > 1
        assert embeds[0].footer is None
        assert embeds[-1].footer['text'] == 'model-a'

    def test_footer_extra_is_appended(self):
        embeds = llm_format.build_answer_embeds('hi', 'model-a',
                                                footer_extra='3/20 today')
        assert embeds[-1].footer['text'] == 'model-a • 3/20 today'

    def test_author_is_attributed_on_the_first_embed(self):
        author = type('A', (), {'display_name': 'nife', 'display_avatar': None})()
        embeds = llm_format.build_answer_embeds('hi', 'model-a', author=author)
        assert embeds[0].author_data['name'] == 'Asked by nife'

    def test_provider_credentials_in_model_output_are_redacted(self):
        secret = 'xai-abcdefghijklmnopqrstuv-secret'
        embeds = llm_format.build_answer_embeds(
            f'Never echo {secret}', 'grok-test')
        assert secret not in embeds[0].description
        assert '[REDACTED]' in embeds[0].description

    def test_provider_credentials_in_footer_metadata_are_redacted(self):
        secret = 'xai-abcdefghijklmnopqrstuv-secret'
        embeds = llm_format.build_answer_embeds(
            'answer', secret, footer_extra=f'fallback from {secret}')
        footer = embeds[-1].footer['text']
        assert secret not in footer
        assert 'REDACTED' in footer


class TestFormatKeyRows:
    def test_no_keys(self):
        assert 'No API keys' in llm_format.format_key_rows([])

    def test_keys_are_listed_redacted_with_their_ids(self):
        db = FakeLlmDb()
        db.llm_add_key('AIzaSyABCDEFGHIJKLMNOPQRSTUV1234', label='proj-a',
                       added_by=42)
        rendered = llm_format.format_key_rows(db.llm_get_keys())
        assert '#1' in rendered
        assert 'proj-a' in rendered
        assert 'sha256:' in rendered
        assert 'AIzaSy' not in rendered
        assert '<@42>' in rendered

    def test_raw_key_never_appears(self):
        db = FakeLlmDb()
        db.llm_add_key('AIzaSyABCDEFGHIJKLMNOPQRSTUV1234')
        rendered = llm_format.format_key_rows(db.llm_get_keys())
        assert 'AIzaSyABCDEFGHIJKLMNOPQRSTUV1234' not in rendered

    def test_secret_valued_label_is_redacted(self):
        secret = 'xai-abcdefghijklmnopqrstuv-secret'
        db = FakeLlmDb()
        db.llm_add_key(secret, label=secret, provider='xai')
        rendered = llm_format.format_key_rows(
            db.llm_get_keys(provider='xai'))
        assert secret not in rendered
        assert 'REDACTED' in rendered


class TestFormatPoolStatus:
    def test_empty_pool_tells_you_how_to_fix_it(self):
        assert ';llm keys' in llm_format.format_pool_status([])

    def test_each_state_gets_its_own_marker(self):
        rows = [
            {'key_id': 1, 'label': 'proj-a', 'model': 'model-a',
             'state': 'ready', 'wait': None},
            {'key_id': 1, 'label': 'proj-a', 'model': 'model-b',
             'state': 'daily quota spent', 'wait': 7200},
            {'key_id': 2, 'label': None, 'model': 'model-a',
             'state': 'cooling down', 'wait': 30},
        ]
        rendered = llm_format.format_pool_status(rows)
        assert '\N{LARGE GREEN CIRCLE}' in rendered
        assert '\N{LARGE RED CIRCLE}' in rendered
        assert '\N{LARGE YELLOW CIRCLE}' in rendered
        assert '2h' in rendered
        assert '30s' in rendered

    def test_buckets_are_grouped_under_their_key(self):
        rows = [
            {'key_id': 7, 'label': 'proj-x', 'model': 'model-a',
             'state': 'ready', 'wait': None},
            {'key_id': 7, 'label': 'proj-x', 'model': 'model-b',
             'state': 'ready', 'wait': None},
        ]
        rendered = llm_format.format_pool_status(rows)
        assert rendered.count('Key #7') == 1

    def test_secret_metadata_never_reaches_provider_health(self):
        secret = 'xai-abcdefghijklmnopqrstuv-secret'
        rows = [{
            'key_id': 7,
            'label': secret,
            'model': secret,
            'state': f'provider rejected {secret}',
            'wait': None,
        }]
        rendered = llm_format.format_pool_status(rows)
        assert secret not in rendered
        assert 'REDACTED' in rendered
        assert 'label' not in rendered
