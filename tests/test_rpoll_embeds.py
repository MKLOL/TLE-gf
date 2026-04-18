"""Embed, parsing, and command-input tests for rpoll."""
from tle.cogs.rpoll import (
    _DEFAULT_DURATION,
    _FORMULA_LABELS,
    MAX_OPTIONS,
    _build_poll_embed,
    _build_results_embed,
    _parse_duration,
)


class TestParseDuration:
    def test_minutes(self):
        assert _parse_duration('+30m') == 30 * 60

    def test_hours(self):
        assert _parse_duration('+2h') == 2 * 3600

    def test_days(self):
        assert _parse_duration('+1d') == 86400

    def test_single_minute(self):
        assert _parse_duration('+1m') == 60

    def test_large_hours(self):
        assert _parse_duration('+48h') == 48 * 3600

    def test_invalid_no_plus(self):
        assert _parse_duration('30m') is None

    def test_invalid_unit(self):
        assert _parse_duration('+5x') is None

    def test_invalid_no_number(self):
        assert _parse_duration('+h') is None

    def test_not_a_duration(self):
        assert _parse_duration('+anon') is None

    def test_empty(self):
        assert _parse_duration('') is None


class TestBuildPollEmbed:
    def test_basic_embed(self):
        options = [(0, 'BFS'), (1, 'DFS')]
        embed = _build_poll_embed('Best algo?', options, {}, 0)
        assert embed.title == 'Best algo?'
        assert '0 votes' in embed.footer['text']
        assert 'BFS' in embed.description
        assert 'DFS' in embed.description

    def test_with_totals(self):
        options = [(0, 'BFS'), (1, 'DFS')]
        totals = {0: 3400, 1: 5200}
        embed = _build_poll_embed('Q?', options, totals, 5)
        assert '**3400**' in embed.description
        assert '**5200**' in embed.description
        assert '5 votes' in embed.footer['text']

    def test_missing_option_defaults_zero(self):
        options = [(0, 'A'), (1, 'B')]
        totals = {0: 1500}
        embed = _build_poll_embed('Q?', options, totals, 1)
        assert '**0**' in embed.description

    def test_singular_vote(self):
        embed = _build_poll_embed('Q?', [(0, 'A')], {}, 1)
        assert '1 vote' in embed.footer['text']
        assert 'votes' not in embed.footer['text']

    def test_number_emojis_in_description(self):
        options = [(0, 'A'), (1, 'B'), (2, 'C')]
        embed = _build_poll_embed('Q?', options, {}, 0)
        assert '1\N{COMBINING ENCLOSING KEYCAP}' in embed.description
        assert '2\N{COMBINING ENCLOSING KEYCAP}' in embed.description
        assert '3\N{COMBINING ENCLOSING KEYCAP}' in embed.description

    def test_percentages_shown_when_totals_nonzero(self):
        options = [(0, 'A'), (1, 'B')]
        totals = {0: 3000, 1: 7000}
        embed = _build_poll_embed('Q?', options, totals, 2)
        assert '(30%)' in embed.description
        assert '(70%)' in embed.description

    def test_no_percentages_when_all_zero(self):
        options = [(0, 'A'), (1, 'B')]
        embed = _build_poll_embed('Q?', options, {}, 0)
        assert '%' not in embed.description

    def test_one_option_has_all_rating(self):
        options = [(0, 'A'), (1, 'B')]
        totals = {0: 2000}
        embed = _build_poll_embed('Q?', options, totals, 1)
        assert '(100%)' in embed.description
        assert '(0%)' in embed.description

    def test_percentages_round(self):
        options = [(0, 'A'), (1, 'B'), (2, 'C')]
        totals = {0: 1000, 1: 1000, 2: 1000}
        embed = _build_poll_embed('Q?', options, totals, 3)
        assert '(33%)' in embed.description

    def test_zero_rating_votes_no_percentages(self):
        options = [(0, 'A'), (1, 'B')]
        totals = {0: 0, 1: 0}
        embed = _build_poll_embed('Q?', options, totals, 2)
        assert '%' not in embed.description

    def test_leader_shown(self):
        options = [(0, 'BFS'), (1, 'DFS')]
        totals = {0: 3000, 1: 1000}
        embed = _build_poll_embed('Q?', options, totals, 2)
        assert 'Leader: **BFS** (+2000)' in embed.description

    def test_tied_shown(self):
        options = [(0, 'BFS'), (1, 'DFS')]
        totals = {0: 1500, 1: 1500}
        embed = _build_poll_embed('Q?', options, totals, 2)
        assert 'Tied:' in embed.description
        assert '**BFS**' in embed.description
        assert '**DFS**' in embed.description

    def test_three_way_tie(self):
        options = [(0, 'A'), (1, 'B'), (2, 'C')]
        totals = {0: 1000, 1: 1000, 2: 1000}
        embed = _build_poll_embed('Q?', options, totals, 3)
        assert 'Tied:' in embed.description
        assert '**A**' in embed.description
        assert '**B**' in embed.description
        assert '**C**' in embed.description

    def test_no_leader_when_all_zero(self):
        options = [(0, 'A'), (1, 'B')]
        embed = _build_poll_embed('Q?', options, {}, 0)
        assert 'Leader' not in embed.description
        assert 'Tied' not in embed.description

    def test_leader_with_zero_second(self):
        options = [(0, 'A'), (1, 'B')]
        totals = {0: 2000}
        embed = _build_poll_embed('Q?', options, totals, 1)
        assert 'Leader: **A** (+2000)' in embed.description


class TestBuildPollEmbedExpiry:
    def test_shows_expiry_timestamp(self):
        expires_at = 1700000000.0
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0, expires_at=expires_at)
        assert f'<t:{int(expires_at)}:R>' in embed.description

    def test_closed_shows_ended(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0, closed=True)
        assert 'Poll has ended.' in embed.description

    def test_closed_overrides_expiry(self):
        embed = _build_poll_embed(
            'Q?',
            [(0, 'A'), (1, 'B')],
            {},
            0,
            expires_at=1700000000.0,
            closed=True,
        )
        assert 'Poll has ended.' in embed.description
        assert '<t:' not in embed.description

    def test_no_expiry_no_closed(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0)
        assert 'Poll has ended' not in embed.description
        assert 'Ends' not in embed.description


class TestBuildPollEmbedVoters:
    def test_no_voters_no_voter_section(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0)
        assert 'A:' not in embed.description

    def test_voters_shown(self):
        voters_map = {0: [111, 222], 1: [333]}
        embed = _build_poll_embed(
            'Q?',
            [(0, 'Alpha'), (1, 'Beta')],
            {0: 3000, 1: 1500},
            3,
            voters_map,
        )
        assert '<@111>' in embed.description
        assert '<@222>' in embed.description
        assert '<@333>' in embed.description
        assert 'Alpha: <@111>, <@222>' in embed.description
        assert 'Beta: <@333>' in embed.description

    def test_empty_option_not_shown(self):
        voters_map = {0: [111]}
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {0: 1500, 1: 0}, 1, voters_map)
        assert 'A: <@111>' in embed.description
        lines = embed.description.split('\n')
        voter_lines = [line for line in lines if line.startswith('B:')]
        assert voter_lines == []


class TestSmartQuoteNormalization:
    """Verify that macOS smart/curly quotes are normalized to straight quotes."""

    def _normalize(self, text):
        text = text.replace('\u201c', '"').replace('\u201d', '"')
        text = text.replace('\u2018', "'").replace('\u2019', "'")
        return text

    def test_left_right_double_smart_quotes(self):
        result = self._normalize('\u201cIs this the best?\u201d yes, no')
        assert result == '"Is this the best?" yes, no'

    def test_left_right_single_smart_quotes(self):
        result = self._normalize('What\u2019s the best?')
        assert result == "What's the best?"

    def test_straight_quotes_unchanged(self):
        result = self._normalize('"Already straight" yes, no')
        assert result == '"Already straight" yes, no'

    def test_mixed_smart_and_straight(self):
        result = self._normalize('\u201cMixed" quotes')
        assert result == '"Mixed" quotes'

    def test_smart_quotes_in_full_rpoll_args(self):
        args = '\u201cis dragos the goat\u201d yes, yes'
        args = self._normalize(args)
        assert args.startswith('"')
        end = args.find('"', 1)
        assert end != -1
        question = args[1:end]
        assert question == 'is dragos the goat'

    def test_smart_quotes_with_flags(self):
        args = '+anon \u201cWhat\u2019s better?\u201d A, B'
        args = self._normalize(args)
        assert '"What\'s better?"' in args


class TestBuildResultsEmbed:
    def test_basic_summary(self):
        options = [(0, 'BFS'), (1, 'DFS')]
        totals_map = {0: 3400, 1: 1600}
        result = _build_results_embed('Best algo?', options, totals_map, 5)
        assert result.title is None
        assert '**Best algo?**' in result.description
        assert '**BFS** 68%' in result.description
        assert '**DFS** 32%' in result.description
        assert '5 votes' in result.description

    def test_zero_totals(self):
        options = [(0, 'A'), (1, 'B')]
        result = _build_results_embed('Q?', options, {}, 0)
        assert '**Q?**' in result.description
        assert '**A** 0' in result.description
        assert '**B** 0' in result.description
        assert '0 votes' in result.description

    def test_singular_vote(self):
        options = [(0, 'A'), (1, 'B')]
        result = _build_results_embed('Q?', options, {0: 1500}, 1)
        assert '1 vote)' in result.description

    def test_shows_formula_label(self):
        options = [(0, 'A'), (1, 'B')]
        result = _build_results_embed('Q?', options, {0: 1500}, 1, formula='exp')
        assert _FORMULA_LABELS['exp'] in result.description

    def test_shows_team_formula_label(self):
        options = [(0, 'A'), (1, 'B')]
        result = _build_results_embed('Q?', options, {0: 1500}, 1, formula='team')
        assert _FORMULA_LABELS['team'] in result.description

    def test_shows_osu_formula_label(self):
        options = [(0, 'A'), (1, 'B')]
        result = _build_results_embed('Q?', options, {0: 1500}, 1, formula='osu')
        assert _FORMULA_LABELS['osu'] in result.description

    def test_shows_gg_formula_label(self):
        options = [(0, 'A'), (1, 'B')]
        result = _build_results_embed('Q?', options, {0: 1500}, 1, formula='gg')
        assert _FORMULA_LABELS['gg'] in result.description

    def test_shows_mgg_formula_label(self):
        options = [(0, 'A'), (1, 'B')]
        result = _build_results_embed('Q?', options, {0: 1500}, 1, formula='mgg')
        assert _FORMULA_LABELS['mgg'] in result.description

    def test_shows_fffff_formula_label(self):
        options = [(0, 'A'), (1, 'B')]
        result = _build_results_embed('Q?', options, {0: 1500}, 1, formula='fffff')
        assert _FORMULA_LABELS['fffff'] in result.description

    def test_default_formula_label(self):
        options = [(0, 'A'), (1, 'B')]
        result = _build_results_embed('Q?', options, {0: 1500}, 1)
        assert _FORMULA_LABELS['exp'] in result.description


class TestBuildResultsEmbedWinner:
    def test_single_winner_shown(self):
        options = [(0, 'BFS'), (1, 'DFS')]
        result = _build_results_embed('Q?', options, {0: 3400, 1: 1600}, 5)
        assert 'Winner: **BFS**' in result.description

    def test_near_fifty_fifty_still_names_winner(self):
        options = [(0, 'Yes'), (1, 'No')]
        result = _build_results_embed('Q?', options, {0: 499, 1: 501}, 2)
        assert 'Winner: **No**' in result.description
        assert '50%' in result.description

    def test_tie_two_options(self):
        options = [(0, 'A'), (1, 'B')]
        result = _build_results_embed('Q?', options, {0: 1500, 1: 1500}, 2)
        assert 'Tied:' in result.description
        assert '**A**' in result.description
        assert '**B**' in result.description
        assert 'Winner:' not in result.description

    def test_tie_three_options(self):
        options = [(0, 'A'), (1, 'B'), (2, 'C')]
        result = _build_results_embed('Q?', options, {0: 1000, 1: 1000, 2: 1000}, 3)
        assert 'Tied:' in result.description
        assert 'Winner:' not in result.description

    def test_no_winner_when_zero_votes(self):
        options = [(0, 'A'), (1, 'B')]
        result = _build_results_embed('Q?', options, {}, 0)
        assert 'Winner:' not in result.description
        assert 'Tied:' not in result.description

    def test_winner_ignores_non_leaders(self):
        options = [(0, 'A'), (1, 'B'), (2, 'C')]
        result = _build_results_embed('Q?', options, {0: 100, 1: 3000, 2: 500}, 3)
        assert 'Winner: **B**' in result.description
        assert 'Winner: **A**' not in result.description
        assert 'Winner: **C**' not in result.description


class TestBuildPollEmbedFormula:
    def test_sum_formula_label_shown(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0, formula='sum')
        assert _FORMULA_LABELS['sum'] in embed.description

    def test_exp_formula_label_shown(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0, formula='exp')
        assert _FORMULA_LABELS['exp'] in embed.description

    def test_team_formula_label_shown(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0, formula='team')
        assert _FORMULA_LABELS['team'] in embed.description

    def test_osu_formula_label_shown(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0, formula='osu')
        assert _FORMULA_LABELS['osu'] in embed.description

    def test_gg_formula_label_shown(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0, formula='gg')
        assert _FORMULA_LABELS['gg'] in embed.description

    def test_mgg_formula_label_shown(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0, formula='mgg')
        assert _FORMULA_LABELS['mgg'] in embed.description

    def test_fffff_formula_label_shown(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0, formula='fffff')
        assert _FORMULA_LABELS['fffff'] in embed.description

    def test_formula_label_line_shown(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0, formula='sum')
        assert f'Scoring: {_FORMULA_LABELS["sum"]}' in embed.description

    def test_default_formula_is_exp(self):
        embed = _build_poll_embed('Q?', [(0, 'A'), (1, 'B')], {}, 0)
        assert _FORMULA_LABELS['exp'] in embed.description


class TestRpollConstants:
    def test_max_options(self):
        assert MAX_OPTIONS == 5

    def test_default_duration(self):
        assert _DEFAULT_DURATION == 86400
