"""Focused tests for the pure counting-message parser."""

import pytest

from tle.cogs._counting_parser import (
    CORRECT,
    IGNORED,
    INVALID_FORMAT,
    WRONG_NUMBER,
    classify_count_attempt,
    could_be_count_attempt,
)


class TestCouldBeCountAttempt:
    @pytest.mark.parametrize('content', [
        '1', '73.7', '+9', '-9', '(11)', '0b102', '0xzz', '1G',
        'A', 'face', 'ABC', '17 (test)', '9 then 10',
        'go 9', 'answer: 17', 'version2',
    ])
    def test_keeps_numeric_shapes_and_possible_letter_hex(self, content):
        assert could_be_count_attempt(content) is True

    @pytest.mark.parametrize('content', [
        None, '', '   ', 'hello', 'nine', 'ordinary prose', 'A word',
    ])
    def test_filters_prose_without_a_possible_count(self, content):
        assert could_be_count_attempt(content) is False


class TestCorrectCounts:
    @pytest.mark.parametrize(('content', 'expected', 'radix'), [
        ('9', 9, 10),
        ('1001', 9, 2),
        ('0b1001', 9, 2),
        ('0B1001', 9, 2),
        ('a', 10, 16),
        ('A', 10, 16),
        ('0xa', 10, 16),
        ('0XA', 10, 16),
        ('16', 16, 10),
        ('10000', 16, 2),
        ('10', 16, 16),
        ('dead', 0xDEAD, 16),
        ('1', 1, 10),
        ('  9  ', 9, 10),
        ('17 (test)', 17, 10),
        ('10001 binary note', 17, 2),
        ('11 hex note', 17, 16),
        ('test 17 okay', 17, 10),
        ('prefix10001suffix', 17, 2),
        ('wrong 1087, then 108!', 108, 10),
        ('prefix108suffix', 108, 10),
        ('the next count is 108.', 108, 10),
        ('score+108', 108, 10),
        ('108/7', 108, 10),
        ('x108x', 108, 10),
        ('version2', 2, 10),
        ('prefix 0xa suffix', 10, 16),
    ])
    def test_accepts_canonical_decimal_binary_and_hex(
            self, content, expected, radix):
        result = classify_count_attempt(content, expected)
        assert result.status == CORRECT
        assert result.is_correct is True
        assert result.is_bad_attempt is False
        assert result.reason is None
        assert result.radix == radix
        assert result.value == expected

    @pytest.mark.parametrize(('content', 'expected', 'radix'), [
        ('10', 2, 2),
        ('10', 10, 10),
        ('10', 16, 16),
        ('11', 3, 2),
        ('11', 11, 10),
        ('11', 17, 16),
    ])
    def test_expected_value_disambiguates_bare_digits(
            self, content, expected, radix):
        assert classify_count_attempt(content, expected).radix == radix

    @pytest.mark.parametrize('content', ['cat', 'faceplant', 'preface', 'a test'])
    def test_letter_only_hex_requires_token_boundaries(self, content):
        assert classify_count_attempt(content, expected=10).status == IGNORED

    def test_multi_letter_hex_does_not_match_inside_a_word(self):
        assert classify_count_attempt(
            'preface this', expected=0xFACE).status == IGNORED

    def test_bounded_letter_only_hex_can_match_inside_a_message(self):
        result = classify_count_attempt('value: (face)!', expected=0xFACE)
        assert (result.status, result.radix, result.value) == (
            CORRECT, 16, 0xFACE)


class TestBadAttempts:
    @pytest.mark.parametrize('content', [
        '11', '73', '100000', 'babe1', 'abc123',
    ])
    def test_canonical_other_numbers_are_wrong_number(self, content):
        result = classify_count_attempt(content, expected=9)
        assert result.status == WRONG_NUMBER
        assert result.is_bad_attempt is True
        assert result.reason == WRONG_NUMBER

    @pytest.mark.parametrize(('content', 'radix', 'value'), [
        ('0b11', 2, 3),
        ('0x11', 16, 17),
    ])
    def test_prefixed_wrong_number_retains_unambiguous_radix(
            self, content, radix, value):
        result = classify_count_attempt(content, expected=9)
        assert result.status == WRONG_NUMBER
        assert (result.radix, result.value) == (radix, value)

    @pytest.mark.parametrize('content', [
        '1087', '2108', '91089',
        '108a', 'c108', '1101100a', '0x108',
        '0b11011000', '0x6c7', 'a6c', '6c1',
    ])
    def test_does_not_match_inside_a_longer_integer(self, content):
        result = classify_count_attempt(content, expected=108)
        assert result.status == WRONG_NUMBER
        assert result.is_bad_attempt is True

    @pytest.mark.parametrize('content', ['0x0b1101100', '0x0x6c'])
    def test_does_not_match_nested_base_literals(self, content):
        assert classify_count_attempt(content, expected=108).status != CORRECT

    @pytest.mark.parametrize('content', [
        '111', '0x110', '0b100011',
        '0x17', '0b11', '0x10001',
    ])
    def test_does_not_match_inside_another_base_literal(self, content):
        assert classify_count_attempt(content, expected=17).status != CORRECT

    @pytest.mark.parametrize('content', [
        '73.7', '.8', '8.', '.108', '108.', '108.7',
        '08', '001000', '0b01000', '0x08',
        '0108', '0b01101100', '0x06c',
        '0b102', '0b', '0xgg', '0x',
    ])
    def test_malformed_or_noncanonical_tokens_are_invalid_format(self, content):
        result = classify_count_attempt(content, expected=9)
        assert result.status == INVALID_FORMAT
        assert result.is_bad_attempt is True
        assert result.reason == INVALID_FORMAT

    @pytest.mark.parametrize('content', [
        '09', '001001', '0b01001', '0x09',
    ])
    def test_leading_extensions_do_not_match_expected_nine(self, content):
        assert classify_count_attempt(content, expected=9).status == \
            INVALID_FORMAT

    @pytest.mark.parametrize('content', [
        '.108', '108.7', '7.108',
    ])
    def test_decimal_extensions_do_not_match_expected_108(self, content):
        assert classify_count_attempt(content, expected=108).status == \
            INVALID_FORMAT


class TestBoundedOccurrences:
    @pytest.mark.parametrize('content', [
        '(108)', '#108!', '108, okay', '1087 then (108)',
    ])
    def test_accepts_a_separately_bounded_occurrence(self, content):
        assert classify_count_attempt(content, 108).status == CORRECT

    @pytest.mark.parametrize('content', [
        '7108 and 1089', 'text 1087', '1,108', "1'108",
        '108٧', '١108٧', '108²', '108_7',
    ])
    def test_rejects_or_ignores_only_extended_occurrences(self, content):
        assert classify_count_attempt(content, 108).status != CORRECT

    def test_unicode_letters_bound_bare_hex_words(self):
        assert classify_count_attempt(
            'éfaceé', expected=0xFACE).status == IGNORED


class TestIgnoredMessages:
    @pytest.mark.parametrize('content', [
        None, '', 'hello', 'the answer is 9', 'wrong 73 here',
        'wrong 73.7 here',
        'face', 'b', 'abc', 'version2', 'hello2', 'user123',
        '2026-08-17', '123.png', '2nd', '42%', '1.2.3', '1/2',
        '9!', '(9)', '+9', '-9', '1g',
    ])
    def test_ordinary_prose_and_wrong_letter_only_hex_are_ignored(self, content):
        result = classify_count_attempt(content, expected=10)
        assert result.status == IGNORED
        assert result.is_correct is False
        assert result.is_bad_attempt is False
        assert result.reason is None

    def test_letter_only_hex_is_accepted_when_it_is_expected(self):
        result = classify_count_attempt('b', expected=11)
        assert (result.status, result.radix, result.value) == (CORRECT, 16, 11)

    @pytest.mark.parametrize('content', ['hello there', 'version2 text'])
    def test_ordinary_prose_stays_ignored(self, content):
        assert classify_count_attempt(content, expected=17).status == IGNORED


class TestExpectedValidation:
    @pytest.mark.parametrize('expected', [None, '9', 9.0, True])
    def test_expected_must_be_an_integer(self, expected):
        with pytest.raises(TypeError):
            classify_count_attempt('9', expected)

    def test_expected_must_be_non_negative(self):
        with pytest.raises(ValueError):
            classify_count_attempt('9', -1)
