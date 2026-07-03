"""Betting unit tests: pure helpers, odds-API parsers, football-data parsing,
and the small scheduling math (is_due / seconds_until_open)."""
from datetime import datetime, timezone

import pytest

from tle.util import odds_api
from tle.util import football_data
from tle.cogs.betting import (
    outcome_from_score, payout_amount, normalize_pick, parse_amount,
    extract_bet_tokens, resolve_pick, resolve_bet_pick, parse_settle_arg,
    rank_line, is_due, normalized_market_odds, normalize_event,
)
from tests.betting_test_utils import _raw_event, _FakeResp, _FakeSession


class TestOutcome:
    def test_home_win(self):
        assert outcome_from_score(2, 1) == 'home'

    def test_away_win(self):
        assert outcome_from_score(0, 3) == 'away'

    def test_draw(self):
        assert outcome_from_score(1, 1) == 'draw'
        assert outcome_from_score(0, 0) == 'draw'


class TestPayout:
    def test_basic(self):
        assert payout_amount(100, 2.5) == 250

    def test_rounding(self):
        assert payout_amount(100, 1.555) == 156  # round-half handled by round()

    def test_decimal_odds_one_returns_stake(self):
        assert payout_amount(50, 1.0) == 50


class TestNormalizePick:
    @pytest.mark.parametrize('text,expected', [
        ('home', 'home'), ('H', 'home'), ('1', 'home'),
        ('draw', 'draw'), ('x', 'draw'), ('tie', 'draw'),
        ('away', 'away'), ('A', 'away'), ('2', 'away'),
        ('  Home  ', 'home'),
    ])
    def test_aliases(self, text, expected):
        assert normalize_pick(text) == expected

    def test_unknown(self):
        assert normalize_pick('banana') is None
        assert normalize_pick(None) is None


class TestParseAmount:
    def test_plain_number(self):
        assert parse_amount('100', 500) == 100

    def test_all(self):
        assert parse_amount('all', 500) == 500
        assert parse_amount('max', 350) == 350

    def test_all_with_zero_balance_is_none(self):
        assert parse_amount('all', 0) is None

    def test_percentage(self):
        assert parse_amount('50%', 500) == 250
        assert parse_amount('10%', 95) == 9  # int() floors

    def test_percentage_out_of_range(self):
        assert parse_amount('0%', 500) is None
        assert parse_amount('150%', 500) is None

    def test_below_min(self):
        assert parse_amount('0', 500) is None
        assert parse_amount('-5', 500) is None

    def test_does_not_clamp_to_balance(self):
        # Over-balance plain numbers parse fine; the caller enforces the cap.
        assert parse_amount('999', 100) == 999

    def test_garbage(self):
        assert parse_amount('abc', 500) is None
        assert parse_amount('', 500) is None
        assert parse_amount(None, 500) is None


class TestExtractBetTokens:
    """Cheap, market-agnostic split — pick text is resolved later."""

    def test_pick_then_amount(self):
        assert extract_bet_tokens('home 100') == ('home', '100')

    def test_amount_then_pick(self):
        assert extract_bet_tokens('100 away') == ('away', '100')

    def test_percent_and_all(self):
        assert extract_bet_tokens('x 50%') == ('x', '50%')
        assert extract_bet_tokens('away all') == ('away', 'all')
        assert extract_bet_tokens('1 250') == ('1', '250')

    def test_multiword_team_name(self):
        # Country names can be multiple words — must still parse.
        assert extract_bet_tokens('Cape Verde 100') == ('Cape Verde', '100')
        assert extract_bet_tokens('250 Saudi Arabia') == ('Saudi Arabia', '250')
        assert extract_bet_tokens('not Saudi Arabia 250') == ('not Saudi Arabia', '250')

    def test_ignores_ordinary_chat(self):
        assert extract_bet_tokens('lets go spain') is None  # no amount token
        assert extract_bet_tokens('home') is None            # 1 token
        assert extract_bet_tokens('a really long sentence 5') is None  # >4 tokens
        assert extract_bet_tokens('') is None
        assert extract_bet_tokens(None) is None


class TestResolvePick:
    """Market-aware: outcome aliases OR the team name."""

    def test_outcome_aliases(self):
        assert resolve_pick('home', 'Spain', 'Cape Verde') == 'home'
        assert resolve_pick('x', 'Spain', 'Cape Verde') == 'draw'
        assert resolve_pick('tie', 'Spain', 'Cape Verde') == 'draw'
        assert resolve_pick('2', 'Spain', 'Cape Verde') == 'away'

    def test_team_name_exact(self):
        assert resolve_pick('Spain', 'Spain', 'Cape Verde') == 'home'
        assert resolve_pick('spain', 'Spain', 'Cape Verde') == 'home'
        assert resolve_pick('Cape Verde', 'Spain', 'Cape Verde') == 'away'
        assert resolve_pick('cape verde', 'Spain', 'Cape Verde') == 'away'

    def test_team_name_accents_and_spacing(self):
        assert resolve_pick('cote divoire', "Côte d'Ivoire", 'Brazil') == 'home'

    def test_unambiguous_prefix(self):
        assert resolve_pick('cape', 'Spain', 'Cape Verde') == 'away'

    def test_ambiguous_prefix_is_none(self):
        # Iran vs Iraq share 'ira' → refuse to guess.
        assert resolve_pick('ira', 'Iran', 'Iraq') is None

    def test_unknown_is_none(self):
        assert resolve_pick('bananas', 'Spain', 'Cape Verde') is None
        assert resolve_pick(None, 'Spain', 'Cape Verde') is None


class TestResolveBetPick:
    def test_negative_team_and_outcome(self):
        assert resolve_bet_pick('not Spain', 'Spain', 'Cape Verde') == 'not_home'
        assert resolve_bet_pick('not draw', 'Spain', 'Cape Verde') == 'not_draw'
        assert resolve_bet_pick('no Cape Verde', 'Spain', 'Cape Verde') == 'not_away'

    def test_draw_rejected_when_market_has_no_draw(self):
        assert resolve_bet_pick('draw', 'Spain', 'Cape Verde', allow_draw=False) is None
        assert resolve_bet_pick('not draw', 'Spain', 'Cape Verde',
                                allow_draw=False) is None


class TestNormalizedOdds:
    def test_removes_overround(self):
        fair = normalized_market_odds({'home': 2.0, 'draw': 4.0, 'away': 4.0})
        probs = sum(1 / fair[p] for p in ('home', 'draw', 'away'))
        assert abs(probs - 1.0) < 1e-9
        assert abs(fair['home'] - 2.0) < 1e-9

    def test_knockout_folds_draw_into_advancing_teams(self):
        fair = normalized_market_odds(
            {'home': 2.0, 'draw': 4.0, 'away': 4.0}, knockout=True)
        assert fair['draw'] == 0.0
        assert abs((1 / fair['home']) + (1 / fair['away']) - 1.0) < 1e-9
        assert abs(fair['home'] - 1.5) < 1e-3

    def test_normalize_event_knockout_drops_draw(self):
        event = {'commence_time': datetime(2026, 6, 28, 19, 0,
                                           tzinfo=timezone.utc).timestamp(),
                 'odds': {'home': 2.0, 'draw': 4.0, 'away': 4.0}}
        out = normalize_event(event, knockout=True)
        assert out['market_type'] == 'advance'
        assert out['odds']['draw'] == 0.0

    def test_normalize_event_group_keeps_draw(self):
        # A late group game (any date) must stay 1X2 — the date no longer
        # decides knockout; the caller passes the stage-derived flag.
        event = {'commence_time': datetime(2026, 6, 28, 19, 0,
                                           tzinfo=timezone.utc).timestamp(),
                 'odds': {'home': 2.0, 'draw': 4.0, 'away': 4.0}}
        out = normalize_event(event)  # knockout defaults to False (fail-safe)
        assert out['market_type'] == 'result'
        assert out['odds']['draw'] > 1


class TestStageDetection:
    def test_is_knockout_stage(self):
        assert football_data.is_knockout_stage('LAST_16') is True
        assert football_data.is_knockout_stage('QUARTER_FINALS') is True
        assert football_data.is_knockout_stage('GROUP_STAGE') is False
        assert football_data.is_knockout_stage(None) is False
        assert football_data.is_knockout_stage('') is False

    def test_find_match_stage_matches_either_orientation(self):
        matches = [{'home': 'Austria', 'away': 'Algeria',
                    'commence_time': 1000.0, 'stage': 'GROUP_STAGE'},
                   {'home': 'France', 'away': 'Brazil',
                    'commence_time': 2000.0, 'stage': 'LAST_16'}]
        # Query flipped vs the feed orientation still resolves.
        assert football_data.find_match_stage(
            'Algeria', 'Austria', 1000.0, matches) == 'GROUP_STAGE'
        assert football_data.find_match_stage(
            'Brazil', 'France', 2000.0, matches) == 'LAST_16'

    def test_find_match_stage_unknown_fixture(self):
        matches = [{'home': 'Austria', 'away': 'Algeria',
                    'commence_time': 1000.0, 'stage': 'GROUP_STAGE'}]
        assert football_data.find_match_stage(
            'Spain', 'Japan', 1000.0, matches) is None

    def test_find_match_stage_out_of_window(self):
        matches = [{'home': 'Austria', 'away': 'Algeria',
                    'commence_time': 1000.0, 'stage': 'GROUP_STAGE'}]
        assert football_data.find_match_stage(
            'Algeria', 'Austria', 1000.0 + 10 * 86400, matches) is None

    def test_is_after_group_stage(self):
        matches = [{'home': 'A', 'away': 'B', 'commence_time': 1000.0,
                    'stage': 'GROUP_STAGE'},
                   {'home': 'C', 'away': 'D', 'commence_time': 2000.0,
                    'stage': 'GROUP_STAGE'},
                   {'home': None, 'away': None, 'commence_time': 5000.0,
                    'stage': 'LAST_16'}]
        assert football_data.is_after_group_stage(2500.0, matches) is True
        # The last group game itself is not "after" the group stage.
        assert football_data.is_after_group_stage(2000.0, matches) is False
        assert football_data.is_after_group_stage(1500.0, matches) is False
        # No group data / no time → fail safe (caller keeps the draw).
        assert football_data.is_after_group_stage(2500.0, []) is False
        assert football_data.is_after_group_stage(None, matches) is False

    def test_find_match_stage_cape_verde_islands_spelling(self):
        # Regression: The Odds API says 'Cape Verde', football-data says
        # 'Cape Verde Islands'. Without unifying the two spellings the stage
        # lookup missed and a knockout tie fell back to a 1X2 draw market.
        matches = [{'home': 'Argentina', 'away': 'Cape Verde Islands',
                    'commence_time': 1000.0, 'stage': 'LAST_32'}]
        assert football_data.find_match_stage(
            'Argentina', 'Cape Verde', 1000.0, matches) == 'LAST_32'


class TestParseSettleArg:
    def test_pick_words(self):
        assert parse_settle_arg('home') == ('home', None, None)
        assert parse_settle_arg('draw') == ('draw', None, None)
        assert parse_settle_arg('away') == ('away', None, None)

    def test_scoreline(self):
        assert parse_settle_arg('2-1') == ('home', 2, 1)
        assert parse_settle_arg('0:3') == ('away', 0, 3)
        assert parse_settle_arg('1-1') == ('draw', 1, 1)

    def test_invalid(self):
        assert parse_settle_arg('banana') is None
        assert parse_settle_arg('2-x') is None
        assert parse_settle_arg('-1--2') is None
        assert parse_settle_arg(None) is None


class TestRankLine:
    def _rows(self, pairs):
        from collections import namedtuple
        R = namedtuple('R', 'user_id balance')
        return [R(str(u), b) for u, b in pairs]

    def test_found(self):
        rows = self._rows([(1, 900), (2, 500)])
        line = rank_line(rows, 2, 'balance', 'wallet')
        assert '#2' in line and '500' in line

    def test_user_id_coercion(self):
        rows = self._rows([(100, 10)])
        assert '#1' in rank_line(rows, 100, 'balance', 'wallet')
        assert '#1' in rank_line(rows, '100', 'balance', 'wallet')

    def test_not_found(self):
        rows = self._rows([(1, 900)])
        assert 'not on' in rank_line(rows, 999, 'balance', 'wallet')


class TestIsoToUnix:
    def test_z_suffix(self):
        expected = datetime(2026, 6, 20, 15, 0, tzinfo=timezone.utc).timestamp()
        assert odds_api.iso_to_unix('2026-06-20T15:00:00Z') == expected

    def test_offset(self):
        expected = datetime(2026, 6, 20, 15, 0, tzinfo=timezone.utc).timestamp()
        assert odds_api.iso_to_unix('2026-06-20T16:00:00+01:00') == expected

    def test_naive_treated_as_utc(self):
        expected = datetime(2026, 6, 20, 15, 0, tzinfo=timezone.utc).timestamp()
        assert odds_api.iso_to_unix('2026-06-20T15:00:00') == expected


def _raw_event(**over):
    raw = {
        'id': 'evt1', 'sport_key': 'soccer_epl',
        'commence_time': '2026-06-20T15:00:00Z',
        'home_team': 'Spain', 'away_team': 'Cape Verde',
        'bookmakers': [
            {'key': 'b1', 'markets': [{'key': 'h2h', 'outcomes': [
                {'name': 'Spain', 'price': 1.5},
                {'name': 'Cape Verde', 'price': 6.0},
                {'name': 'Draw', 'price': 4.0}]}]},
            {'key': 'b2', 'markets': [{'key': 'h2h', 'outcomes': [
                {'name': 'Spain', 'price': 1.6},
                {'name': 'Cape Verde', 'price': 6.5},
                {'name': 'Draw', 'price': 4.2}]}]},
        ],
    }
    raw.update(over)
    return raw


class TestParseH2H:
    def test_averages_across_bookmakers(self):
        parsed = odds_api.parse_h2h_event(_raw_event())
        assert parsed['event_id'] == 'evt1'
        assert parsed['home_team'] == 'Spain'
        assert parsed['away_team'] == 'Cape Verde'
        assert parsed['odds']['home'] == 1.55
        assert parsed['odds']['away'] == 6.25
        assert parsed['odds']['draw'] == 4.1
        assert parsed['commence_time'] == \
            datetime(2026, 6, 20, 15, 0, tzinfo=timezone.utc).timestamp()

    def test_missing_market_returns_none(self):
        assert odds_api.parse_h2h_event(_raw_event(bookmakers=[])) is None

    def test_partial_market_returns_none(self):
        # A book that only priced home/away (no draw) → incomplete 1X2.
        raw = _raw_event(bookmakers=[
            {'key': 'b', 'markets': [{'key': 'h2h', 'outcomes': [
                {'name': 'Spain', 'price': 1.5},
                {'name': 'Cape Verde', 'price': 6.0}]}]}])
        assert odds_api.parse_h2h_event(raw) is None

    def test_missing_teams_returns_none(self):
        assert odds_api.parse_h2h_event(_raw_event(home_team=None)) is None

    def test_missing_event_id_returns_none(self):
        assert odds_api.parse_h2h_event(_raw_event(id=None)) is None


class TestFetchAsync:
    """Exercise the async client wiring (URL/params, loop, None-filtering)
    with an injected session — no network, no aiohttp needed."""

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_fetch_h2h_params_and_parse(self):
        session = _FakeSession([_raw_event(), _raw_event(id='evt2'),
                                _raw_event(id='evt3', bookmakers=[])])  # last → None
        events = self._run(odds_api.fetch_h2h(
            'KEY', [odds_api.WORLD_CUP_SPORT_KEY], session=session))
        assert len(events) == 2  # the no-odds event is dropped
        url, params = session.calls[0]
        assert url.endswith('/sports/soccer_fifa_world_cup/odds')
        assert params['markets'] == 'h2h'
        assert params['oddsFormat'] == 'decimal'
        assert params['apiKey'] == 'KEY'

    def test_fetch_sports_params(self):
        session = _FakeSession([
            {'key': odds_api.WORLD_CUP_SPORT_KEY, 'title': 'FIFA World Cup 2026'}])
        sports = self._run(odds_api.fetch_sports('KEY', session=session))
        assert sports[0]['key'] == odds_api.WORLD_CUP_SPORT_KEY
        url, params = session.calls[0]
        assert url.endswith('/sports')
        assert params == {'apiKey': 'KEY'}

    def test_fetch_scores_params_and_parse(self):
        raw = [{'id': 'evt1', 'completed': True, 'home_team': 'A',
                'away_team': 'B', 'scores': [{'name': 'A', 'score': '2'},
                                             {'name': 'B', 'score': '0'}]}]
        session = _FakeSession(raw)
        scores = self._run(odds_api.fetch_scores(
            'KEY', odds_api.WORLD_CUP_SPORT_KEY, event_ids=['evt1'],
            session=session))
        assert scores == [{'event_id': 'evt1', 'completed': True,
                           'home_score': 2, 'away_score': 0}]
        url, params = session.calls[0]
        assert url.endswith('/sports/soccer_fifa_world_cup/scores')
        assert params['daysFrom'] == '1'        # cheap completed-games window
        assert params['eventIds'] == 'evt1'

    def test_fetch_h2h_raises_when_all_sports_fail(self):
        class _BadSession:
            def get(self, url, params=None):
                return _FakeResp({}, status=401, text='bad key')

        with pytest.raises(odds_api.OddsApiError) as exc:
            self._run(odds_api.fetch_h2h(
                'BAD', [odds_api.WORLD_CUP_SPORT_KEY], session=_BadSession()))
        assert 'soccer_fifa_world_cup' in str(exc.value)
        assert 'HTTP 401' in str(exc.value)


class TestParseScore:
    def test_completed_with_scores(self):
        raw = {'id': 'evt1', 'completed': True,
               'home_team': 'Spain', 'away_team': 'Cape Verde',
               'scores': [{'name': 'Spain', 'score': '2'},
                          {'name': 'Cape Verde', 'score': '1'}]}
        p = odds_api.parse_score_event(raw)
        assert p == {'event_id': 'evt1', 'completed': True,
                     'home_score': 2, 'away_score': 1}

    def test_not_completed(self):
        raw = {'id': 'evt1', 'completed': False}
        p = odds_api.parse_score_event(raw)
        assert p['completed'] is False
        assert p['home_score'] is None

    def test_completed_but_missing_score(self):
        raw = {'id': 'evt1', 'completed': True,
               'home_team': 'Spain', 'away_team': 'Cape Verde',
               'scores': [{'name': 'Spain', 'score': '2'}]}
        p = odds_api.parse_score_event(raw)
        assert p['completed'] is True
        assert p['home_score'] is None


# ── football-data.org (results) ──────────────────────────────────────────────

class TestFootballDataParse:
    def test_finished_with_scores(self):
        raw = {'status': 'FINISHED', 'utcDate': '2026-06-15T16:01:00Z',
               'homeTeam': {'name': 'Spain'}, 'awayTeam': {'name': 'Cape Verde'},
               'score': {'winner': 'HOME_TEAM',
                         'fullTime': {'home': 3, 'away': 1}}}
        p = football_data.parse_match(raw)
        assert p['finished'] is True
        assert p['home'] == 'Spain' and p['away'] == 'Cape Verde'
        assert p['home_score'] == 3 and p['away_score'] == 1
        assert p['winner'] == 'home'

    def test_finished_penalties_keeps_winner(self):
        # Real football-data v4 shape for a shootout: `fullTime` holds the LEVEL
        # regular+extra-time score (1-1), the shootout tally lives in a separate
        # `penalties` node, and `winner` is the team that won on penalties.
        raw = {'status': 'FINISHED', 'utcDate': '2026-07-01T16:01:00Z',
               'homeTeam': {'name': 'Spain'}, 'awayTeam': {'name': 'Cape Verde'},
               'score': {'winner': 'AWAY_TEAM', 'duration': 'PENALTY_SHOOTOUT',
                         'fullTime': {'home': 1, 'away': 1},
                         'regularTime': {'home': 1, 'away': 1},
                         'penalties': {'home': 3, 'away': 4}}}
        p = football_data.parse_match(raw)
        assert p['finished'] is True
        # The scoreline stays level; the WINNER carries the real result.
        assert p['home_score'] == 1 and p['away_score'] == 1
        assert p['winner'] == 'away'
        assert p['duration'] == 'PENALTY_SHOOTOUT'

    def test_in_play_not_finished(self):
        raw = {'status': 'IN_PLAY', 'utcDate': '2026-06-15T16:01:00Z',
               'homeTeam': {'name': 'A'}, 'awayTeam': {'name': 'B'},
               'score': {'fullTime': {'home': None, 'away': None}}}
        assert football_data.parse_match(raw)['finished'] is False

    def test_finished_without_scores_not_final(self):
        raw = {'status': 'FINISHED', 'utcDate': '2026-06-15T16:01:00Z',
               'homeTeam': {'name': 'A'}, 'awayTeam': {'name': 'B'},
               'score': {'fullTime': {}}}
        assert football_data.parse_match(raw)['finished'] is False


class TestFootballDataMatching:
    def _m(self, home, away, score, commence=1000.0, finished=True):
        return {'home': home, 'away': away, 'home_score': score[0],
                'away_score': score[1], 'commence_time': commence,
                'finished': finished, 'winner': None}

    def test_exact_match(self):
        fd = [self._m('Spain', 'Cape Verde', (3, 1))]
        assert football_data.find_result('Spain', 'Cape Verde', 1000.0, fd) == (3, 1)

    def test_flipped_home_away_swaps_scores(self):
        # Provider lists Cape Verde as home; map scores back to our orientation.
        fd = [self._m('Cape Verde', 'Spain', (1, 3))]
        assert football_data.find_result('Spain', 'Cape Verde', 1000.0, fd) == (3, 1)

    def test_flipped_home_away_swaps_winner(self):
        fd = [self._m('Cape Verde', 'Spain', (1, 3))]
        fd[0]['winner'] = 'away'
        result = football_data.find_match_result('Spain', 'Cape Verde', 1000.0, fd)
        assert result['winner'] == 'home'

    def test_alias_match(self):
        fd = [self._m('Korea Republic', 'Brazil', (0, 2))]
        assert football_data.find_result('South Korea', 'Brazil', 1000.0, fd) == (0, 2)

    def test_rejects_far_date(self):
        fd = [self._m('Spain', 'Cape Verde', (3, 1), commence=1000.0)]
        assert football_data.find_result(
            'Spain', 'Cape Verde', 1000.0 + 5 * 86400, fd) is None

    def test_ignores_unfinished(self):
        fd = [self._m('Spain', 'Cape Verde', (None, None), finished=False)]
        assert football_data.find_result('Spain', 'Cape Verde', 1000.0, fd) is None

    def test_no_match(self):
        fd = [self._m('France', 'Brazil', (1, 1))]
        assert football_data.find_result('Spain', 'Cape Verde', 1000.0, fd) is None


class TestFootballDataFetch:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_fetch_parses_and_sends_token(self):
        class _Resp:
            status = 200
            async def __aenter__(self_): return self_
            async def __aexit__(self_, *a): return False
            async def json(self_):
                return {'matches': [
                    {'status': 'FINISHED', 'utcDate': '2026-06-15T16:01:00Z',
                     'homeTeam': {'name': 'Spain'}, 'awayTeam': {'name': 'Brazil'},
                     'score': {'fullTime': {'home': 1, 'away': 0}}}]}
            async def text(self_): return ''

        class _Sess:
            def __init__(self_): self_.headers = None
            def get(self_, url, headers=None):
                self_.headers = headers
                self_.url = url
                return _Resp()
        sess = _Sess()
        out = self._run(football_data.fetch_wc_matches('tok', session=sess))
        assert len(out) == 1 and out[0]['home'] == 'Spain'
        assert sess.headers == {'X-Auth-Token': 'tok'}
        assert sess.url.endswith('/competitions/WC/matches')

class TestIsDue:
    def test_inside_window(self):
        assert is_due(now := 3600, 0, 7200) is True  # kickoff in 1h, lead 2h

    def test_outside_window(self):
        assert is_due(8000, 0, 7200) is False  # kickoff in >2h

    def test_already_started(self):
        assert is_due(-1, 0, 7200) is False
        assert is_due(0, 0, 7200) is False  # exactly now is not "upcoming"


class TestSecondsUntilOpen:
    def test_future_game(self):
        from tle.cogs.betting import seconds_until_open
        # kickoff in 3h, lead 2h → opens in 1h (3600s), exactly at kickoff−2h.
        assert seconds_until_open(now := 3 * 3600, 2 * 3600, 0) == 3600

    def test_in_window_floors_to_zero(self):
        from tle.cogs.betting import seconds_until_open
        # kickoff in 1h, lead 2h → already past the open moment → 0.
        assert seconds_until_open(3600, 7200, 0) == 0

