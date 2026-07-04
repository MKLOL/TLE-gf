"""Betting tests for Odds API parser/client helpers."""
from datetime import datetime, timezone

import pytest

from tle.util import odds_api
from tests.betting_test_utils import _raw_event, _FakeResp, _FakeSession


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
    """Exercise async client wiring with an injected session."""

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_fetch_h2h_params_and_parse(self):
        session = _FakeSession([_raw_event(), _raw_event(id='evt2'),
                                _raw_event(id='evt3', bookmakers=[])])
        events = self._run(odds_api.fetch_h2h(
            'KEY', [odds_api.WORLD_CUP_SPORT_KEY], session=session))
        assert len(events) == 2
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
        assert params['daysFrom'] == '1'
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
