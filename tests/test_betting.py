"""Tests for the soccer betting minigame — pure helpers, the odds-API
parsers, the wallet/market/wager DB layer, and the cog's bet-execution path."""
import sqlite3
from datetime import datetime, timezone

import pytest

from tle.util.db.user_db_conn import UserDbConn, namedtuple_factory, bet_fixture_key
from tle.util.db.user_db_upgrades import (
    upgrade_1_33_0, upgrade_1_34_0, upgrade_1_35_0,
)
from tle.util import odds_api
from tle.util import football_data
from tle.cogs.betting import (
    outcome_from_score, payout_amount, normalize_pick, parse_amount,
    extract_bet_tokens, resolve_pick, resolve_bet_pick, parse_settle_arg,
    rank_line, is_due, normalized_market_odds, normalize_event,
)


# ── Pure helpers ───────────────────────────────────────────────────────────

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

    def test_normalize_event_marks_knockout(self):
        event = {'commence_time': datetime(2026, 6, 28, 19, 0,
                                           tzinfo=timezone.utc).timestamp(),
                 'odds': {'home': 2.0, 'draw': 4.0, 'away': 4.0}}
        out = normalize_event(event)
        assert out['market_type'] == 'advance'
        assert out['odds']['draw'] == 0.0


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


# ── Odds API parsers ───────────────────────────────────────────────────────

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


class _FakeResp:
    def __init__(self, data, status=200, text=''):
        self._data = data
        self.status = status
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self):
        return self._data

    async def text(self):
        return self._text


class _FakeSession:
    def __init__(self, data):
        self._data = data
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, params))
        return _FakeResp(self._data)


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
        raw = {'status': 'FINISHED', 'utcDate': '2026-07-01T16:01:00Z',
               'homeTeam': {'name': 'Spain'}, 'awayTeam': {'name': 'Cape Verde'},
               'score': {'winner': 'AWAY_TEAM', 'duration': 'PENALTY_SHOOTOUT',
                         'fullTime': {'homeTeam': 4, 'awayTeam': 5},
                         'regularTime': {'homeTeam': 1, 'awayTeam': 1}}}
        p = football_data.parse_match(raw)
        assert p['finished'] is True
        assert p['home_score'] == 4 and p['away_score'] == 5
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


# ── DB layer ───────────────────────────────────────────────────────────────

GUILD = '111'
CH = '222'
THREAD = '333'
USER_A = '100'
USER_B = '200'


@pytest.fixture
def db():
    return UserDbConn(':memory:')


def _make_market(db, commence=10_000.0, odds=(2.0, 3.0, 4.0)):
    mid = db.bet_market_create(
        GUILD, CH, 'evt1', 'soccer_epl', 'Spain', 'Cape Verde', commence,
        odds[0], odds[1], odds[2], USER_A, 0.0)
    return mid


class TestWallet:
    def test_ensure_creates_at_start(self, db):
        assert db.bet_get_balance(GUILD, USER_A) is None
        assert db.bet_ensure_wallet(GUILD, USER_A, 1000) == 1000
        assert db.bet_get_balance(GUILD, USER_A) == 1000
        rows = db.bet_wallet_history(GUILD, USER_A)
        assert [(r.action, r.amount, r.balance_after) for r in rows] == [
            ('init', 1000, 1000)]

    def test_ensure_idempotent(self, db):
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        # second call must not reset a changed balance
        db.conn.execute('UPDATE bet_wallet SET balance = 50 WHERE user_id = ?',
                        (USER_A,))
        assert db.bet_ensure_wallet(GUILD, USER_A, 1000) == 50
        assert len(db.bet_wallet_history(GUILD, USER_A)) == 1

    def test_guild_isolation(self, db):
        db.bet_ensure_wallet('1', USER_A, 1000)
        assert db.bet_get_balance('2', USER_A) is None

    def test_transfer_moves_balance_and_logs_both_wallets(self, db):
        admin = '999'
        ok, reason, sender_bal, receiver_bal = db.bet_transfer(
            GUILD, USER_A, USER_B, 300, 1000, transferred_at=7.0,
            actor_id=admin)
        assert (ok, reason, sender_bal, receiver_bal) == (True, 'ok', 700, 1300)
        assert db.bet_get_balance(GUILD, USER_A) == 700
        assert db.bet_get_balance(GUILD, USER_B) == 1300

        sender_hist = db.bet_wallet_history(GUILD, USER_A)
        receiver_hist = db.bet_wallet_history(GUILD, USER_B)
        assert sender_hist[0].action == 'transfer_out'
        assert sender_hist[0].actor_id == admin
        assert sender_hist[0].amount == -300
        assert sender_hist[0].balance_after == 700
        assert sender_hist[0].note == USER_B
        assert receiver_hist[0].action == 'transfer_in'
        assert receiver_hist[0].actor_id == admin
        assert receiver_hist[0].amount == 300
        assert receiver_hist[0].balance_after == 1300
        assert receiver_hist[0].note == USER_A

    def test_transfer_rejects_insufficient_without_recipient_wallet(self, db):
        db.bet_set_balance(GUILD, USER_A, 50, 1000)
        ok, reason, sender_bal, receiver_bal = db.bet_transfer(
            GUILD, USER_A, USER_B, 60, 1000, transferred_at=7.0)
        assert (ok, reason, sender_bal, receiver_bal) == (
            False, 'insufficient', 50, None)
        assert db.bet_get_balance(GUILD, USER_A) == 50
        assert db.bet_get_balance(GUILD, USER_B) is None
        assert [r.action for r in db.bet_wallet_history(GUILD, USER_A)] == [
            'setbalance', 'init']


class TestDaily:
    def test_grants_once(self, db):
        granted, bal, reason = db.bet_claim_daily(GUILD, USER_A, '2026-06-15', 100, 1000)
        assert granted is True and bal == 1100 and reason == 'ok'
        rows = db.bet_wallet_history(GUILD, USER_A)
        assert [r.action for r in rows[:2]] == ['daily', 'init']
        assert rows[0].amount == 100 and rows[0].actor_id == USER_A

    def test_second_claim_same_day_refused(self, db):
        db.bet_claim_daily(GUILD, USER_A, '2026-06-15', 100, 1000)
        granted, bal, reason = db.bet_claim_daily(GUILD, USER_A, '2026-06-15', 100, 1000)
        assert granted is False and bal == 1100 and reason == 'already'
        assert [r.action for r in db.bet_wallet_history(GUILD, USER_A)].count('daily') == 1

    def test_next_day_grants_again(self, db):
        db.bet_claim_daily(GUILD, USER_A, '2026-06-15', 100, 1000)
        granted, bal, _ = db.bet_claim_daily(GUILD, USER_A, '2026-06-16', 100, 1000)
        assert granted is True and bal == 1200


class TestMarket:
    def test_create_and_get(self, db):
        mid = _make_market(db)
        m = db.bet_market_get(mid)
        assert m.home_team == 'Spain'
        assert m.status == 'open'
        assert m.odds_home == 2.0

    def test_active_lookup(self, db):
        mid = _make_market(db)
        assert db.bet_market_get_active(GUILD, CH).market_id == mid
        assert db.bet_market_get_active(GUILD, 'other') is None

    def test_thread_lookup(self, db):
        mid = _make_market(db)
        db.bet_market_set_thread(mid, THREAD)
        assert db.bet_market_get_active_by_thread(GUILD, THREAD).market_id == mid
        assert db.bet_market_get_active_by_thread(GUILD, 'nope') is None

    def test_exists_open_for_event(self, db):
        _make_market(db)
        assert db.bet_market_exists_open_for_event(GUILD, 'evt1') is True
        assert db.bet_market_exists_open_for_event(GUILD, 'evtX') is False

    def test_duplicate_open_event_returns_none(self, db):
        first = _make_market(db)
        second = db.bet_market_create(
            GUILD, '999', 'evt1', 'soccer_epl', 'Spain', 'Cape Verde', 10_000.0,
            2.0, 3.0, 4.0, USER_A, 1.0)
        assert second is None
        assert len(db.bet_markets_open(GUILD)) == 1

        db.bet_settle(GUILD, first, 'home', 1, 0, 2.0)
        reopened = db.bet_market_create(
            GUILD, '999', 'evt1', 'soccer_epl', 'Spain', 'Cape Verde', 20_000.0,
            2.0, 3.0, 4.0, USER_A, 3.0)
        assert reopened is not None

    def test_duplicate_open_fixture_with_new_event_id_returns_none(self, db):
        first = _make_market(db)
        second = db.bet_market_create(
            GUILD, '999', 'evt2', 'soccer_epl', 'Cape Verde', 'Spain', 10_900.0,
            2.0, 3.0, 4.0, USER_A, 1.0)
        assert first is not None
        assert second is None
        assert len(db.bet_markets_open(GUILD)) == 1

    def test_duplicate_fixture_can_reopen_after_terminal_status(self, db):
        first = _make_market(db)
        db.bet_void(GUILD, first, 2.0)
        reopened = db.bet_market_create(
            GUILD, '999', 'evt2', 'soccer_epl', 'Cape Verde', 'Spain', 20_000.0,
            2.0, 3.0, 4.0, USER_A, 3.0)
        assert reopened is not None

    def test_same_teams_different_utc_day_allowed(self, db):
        first = _make_market(db, commence=10_000.0)
        second = db.bet_market_create(
            GUILD, '999', 'evt2', 'soccer_epl', 'Cape Verde', 'Spain',
            10_000.0 + 86400, 2.0, 3.0, 4.0, USER_A, 1.0)
        assert first is not None
        assert second is not None

    def test_pending_settlement_by_cutoff(self, db):
        mid = _make_market(db, commence=1000.0)
        assert [m.market_id for m in db.bet_markets_pending_settlement(2000.0)] == [mid]
        assert db.bet_markets_pending_settlement(500.0) == []


class TestPlaceBet:
    def test_escrow_deducts(self, db):
        mid = _make_market(db)
        ok, reason, bal = db.bet_place(GUILD, mid, USER_A, 'home', 300, 1.0, 1000)
        assert ok and reason == 'ok' and bal == 700
        assert db.bet_get_balance(GUILD, USER_A) == 700
        rows = db.bet_wallet_history(GUILD, USER_A)
        assert [(r.action, r.amount, r.balance_after, r.market_id)
                for r in rows[:2]] == [
                    ('wager_stake', -300, 700, mid),
                    ('init', 1000, 1000, None),
                ]

    def test_insufficient_balance(self, db):
        mid = _make_market(db)
        ok, reason, bal = db.bet_place(GUILD, mid, USER_A, 'home', 5000, 1.0, 1000)
        assert ok is False and reason == 'insufficient' and bal == 1000
        # no wager recorded, balance untouched
        assert db.bet_get_wager(mid, USER_A) is None
        assert db.bet_get_balance(GUILD, USER_A) == 1000

    def test_rebet_refunds_previous_then_charges(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 300, 1.0, 1000)  # bal 700
        ok, _, bal = db.bet_place(GUILD, mid, USER_A, 'away', 200, 2.0, 1000)
        assert ok and bal == 800  # 700 + 300 refund - 200
        w = db.bet_get_wager(mid, USER_A)
        assert w.pick == 'away' and w.stake == 200  # odds derived from market
        rows = db.bet_wallet_history(GUILD, USER_A)
        assert [r.action for r in rows[:2]] == ['wager_stake', 'wager_refund']
        assert rows[0].amount == -200 and rows[0].balance_after == 800
        assert rows[1].amount == 300 and rows[1].balance_after == 1000

    def test_rebet_to_larger_stake_within_refunded_budget(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 1000, 1.0, 1000)  # all-in, bal 0
        ok, _, bal = db.bet_place(GUILD, mid, USER_A, 'home', 1000, 2.0, 1000)
        assert ok and bal == 0  # refund 1000 then stake 1000 again


class TestPool:
    def test_pool_groups_by_pick(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)
        db.bet_place(GUILD, mid, USER_B, 'home', 200, 1.0, 1000)
        pool = {p.pick: (p.cnt, p.total) for p in db.bet_pool(mid)}
        assert pool['home'] == (2, 300)


class TestSettle:
    def test_winner_credited_loser_zero(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)  # bal 900
        db.bet_place(GUILD, mid, USER_B, 'away', 100, 1.0, 1000)  # bal 900
        rows = db.bet_settle(GUILD, mid, 'home', 2, 1, 5.0)
        by_user = {r[0]: r for r in rows}
        assert by_user[USER_A][4] == 200  # payout 100*2.0
        assert by_user[USER_B][4] == 0
        assert db.bet_get_balance(GUILD, USER_A) == 1100  # 900 + 200
        assert db.bet_get_balance(GUILD, USER_B) == 900   # unchanged
        assert db.bet_market_get(mid).status == 'settled'
        assert db.bet_market_get(mid).result == 'home'
        hist = db.bet_wallet_history(GUILD, USER_A)
        assert hist[0].action == 'payout' and hist[0].amount == 200
        assert hist[0].market_id == mid

    def test_settlement_embed_reports_net_win_loss(self, db):
        from tle.cogs.betting import Betting
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)
        db.bet_place(GUILD, mid, USER_B, 'away', 50, 1.0, 1000)
        rows = db.bet_settle(GUILD, mid, 'home', 2, 1, 5.0)

        embed = Betting(None)._settlement_embed(
            db.bet_market_get(mid), 'home', 2, 1, rows, 'auto')

        desc = embed.description
        assert f'<@{USER_A}> **+100**' in desc
        assert f'<@{USER_B}> **-50**' in desc
        assert 'Total staked: **150**' in desc
        assert 'paid out: **200**' in desc
        assert 'player net: **+50**' in desc

    def test_draw_outcome(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'draw', 100, 1.0, 1000)
        db.bet_settle(GUILD, mid, 'draw', 1, 1, 5.0)
        assert db.bet_get_balance(GUILD, USER_A) == 1200  # 900 + 300

    def test_negative_pick_wins_when_event_does_not_happen(self, db):
        mid = _make_market(db, odds=(2.0, 4.0, 4.0))
        db.bet_place(GUILD, mid, USER_A, 'not_home', 100, 1.0, 1000)
        rows = db.bet_settle(GUILD, mid, 'draw', 1, 1, 5.0)
        by_user = {r[0]: r for r in rows}
        assert by_user[USER_A][4] == 200  # P(not home)=0.5 => odds 2.0
        assert db.bet_get_balance(GUILD, USER_A) == 1100

    def test_negative_pick_loses_when_event_happens(self, db):
        mid = _make_market(db, odds=(2.0, 4.0, 4.0))
        db.bet_place(GUILD, mid, USER_A, 'not_home', 100, 1.0, 1000)
        db.bet_settle(GUILD, mid, 'home', 2, 1, 5.0)
        assert db.bet_get_balance(GUILD, USER_A) == 900

    def test_profit_leaderboard(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)
        db.bet_place(GUILD, mid, USER_B, 'away', 100, 1.0, 1000)
        db.bet_settle(GUILD, mid, 'home', 2, 1, 5.0)
        prof = {r.user_id: (r.profit, r.bets, r.wins)
                for r in db.bet_profit_leaderboard(GUILD)}
        assert prof[USER_A] == (100, 1, 1)   # +200 payout - 100 stake
        assert prof[USER_B] == (-100, 1, 0)

    def test_profit_leaderboard_with_negative_pick(self, db):
        mid = _make_market(db, odds=(2.0, 4.0, 4.0))
        db.bet_place(GUILD, mid, USER_A, 'not_home', 100, 1.0, 1000)
        db.bet_settle(GUILD, mid, 'away', 0, 1, 5.0)
        prof = {r.user_id: (r.profit, r.bets, r.wins)
                for r in db.bet_profit_leaderboard(GUILD)}
        assert prof[USER_A] == (100, 1, 1)

    def test_double_settle_is_noop(self, db):
        """The status='open' guard must make a second settle pay nobody."""
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)  # bal 900
        first = db.bet_settle(GUILD, mid, 'home', 2, 1, 5.0)
        assert first is not None
        assert db.bet_get_balance(GUILD, USER_A) == 1100  # 900 + 200
        second = db.bet_settle(GUILD, mid, 'home', 2, 1, 6.0)
        assert second is None  # already settled — no work
        assert db.bet_get_balance(GUILD, USER_A) == 1100  # not paid twice

    def test_settle_after_void_is_noop(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)  # bal 900
        db.bet_void(GUILD, mid, 4.0)  # refunded → 1000
        assert db.bet_settle(GUILD, mid, 'home', 2, 1, 5.0) is None
        assert db.bet_get_balance(GUILD, USER_A) == 1000  # not paid on top


class TestVoid:
    def test_refunds_and_cancels(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 300, 1.0, 1000)  # bal 700
        refunds = db.bet_void(GUILD, mid, 9.0)
        assert dict(refunds) == {USER_A: 300}
        assert db.bet_get_balance(GUILD, USER_A) == 1000  # fully restored
        assert db.bet_market_get(mid).status == 'cancelled'
        hist = db.bet_wallet_history(GUILD, USER_A)
        assert hist[0].action == 'void_refund'
        assert hist[0].amount == 300 and hist[0].balance_after == 1000

    def test_voided_excluded_from_profit(self, db):
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 300, 1.0, 1000)
        db.bet_void(GUILD, mid, 9.0)
        assert db.bet_profit_leaderboard(GUILD) == []

    def test_void_after_settle_is_noop(self, db):
        """A void must not refund on top of a payout already credited."""
        mid = _make_market(db)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)  # bal 900
        db.bet_settle(GUILD, mid, 'home', 2, 1, 5.0)  # paid 200 → 1100
        assert db.bet_void(GUILD, mid, 9.0) is None
        assert db.bet_get_balance(GUILD, USER_A) == 1100  # unchanged
        assert db.bet_market_get(mid).status == 'settled'  # not flipped


class TestMarketsOpen:
    def test_lists_only_open(self, db):
        open_mid = _make_market(db, commence=1000.0)
        settled = db.bet_market_create(
            GUILD, CH, 'evt2', 'soccer_epl', 'A', 'B', 2000.0,
            2.0, 3.0, 4.0, USER_A, 0.0)
        db.bet_settle(GUILD, settled, 'home', 1, 0, 3.0)
        rows = db.bet_markets_open(GUILD)
        assert [m.market_id for m in rows] == [open_mid]

    def test_guild_isolation(self, db):
        _make_market(db)
        assert db.bet_markets_open('999') == []


class TestModTools:
    def test_resettle_reverses_and_reapplies(self, db):
        mid = _make_market(db)  # odds home 2.0, away 4.0
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)  # bal 900
        db.bet_place(GUILD, mid, USER_B, 'away', 100, 1.0, 1000)  # bal 900
        db.bet_settle(GUILD, mid, 'home', 2, 1, 5.0)
        assert db.bet_get_balance(GUILD, USER_A) == 1100  # +200
        assert db.bet_get_balance(GUILD, USER_B) == 900
        # Correct: it was actually an away win 1-2.
        rows = db.bet_resettle(GUILD, mid, 'away', 1, 2, 6.0)
        assert rows is not None
        assert db.bet_get_balance(GUILD, USER_A) == 900   # 1100 - 200 (reversed)
        assert db.bet_get_balance(GUILD, USER_B) == 1300  # 900 + 400 (now wins)
        m = db.bet_market_get(mid)
        assert m.result == 'away' and m.result_home == 1 and m.result_away == 2

    def test_resettle_negative_pick_delta(self, db):
        mid = _make_market(db, odds=(2.0, 4.0, 4.0))
        db.bet_place(GUILD, mid, USER_A, 'not_home', 100, 1.0, 1000)
        db.bet_settle(GUILD, mid, 'home', 1, 0, 5.0)
        assert db.bet_get_balance(GUILD, USER_A) == 900
        db.bet_resettle(GUILD, mid, 'draw', 1, 1, 6.0)
        assert db.bet_get_balance(GUILD, USER_A) == 1100

    def test_resettle_only_on_settled(self, db):
        mid = _make_market(db)
        assert db.bet_resettle(GUILD, mid, 'home', 1, 0, 5.0) is None  # still open

    def test_adjust_balance_grant_and_floor(self, db):
        assert db.bet_adjust_balance(
            GUILD, USER_A, 250, 1000, actor_id=USER_B,
            action='mod_grant') == 1250
        hist = db.bet_wallet_history(GUILD, USER_A)
        assert hist[0].action == 'mod_grant'
        assert hist[0].actor_id == USER_B and hist[0].amount == 250
        assert db.bet_adjust_balance(GUILD, USER_A, -5000, 1000) == 0  # floored
        assert db.bet_wallet_history(GUILD, USER_A)[0].amount == -1250

    def test_set_balance(self, db):
        assert db.bet_set_balance(
            GUILD, USER_A, 500, 1000, actor_id=USER_B,
            action='mod_setbalance') == 500
        hist = db.bet_wallet_history(GUILD, USER_A)
        assert hist[0].action == 'mod_setbalance'
        assert hist[0].actor_id == USER_B and hist[0].amount == -500
        assert db.bet_set_balance(GUILD, USER_A, -10, 1000) == 0

    def test_close_betting(self, db):
        mid = _make_market(db)
        assert db.bet_market_close_betting(mid) is True
        assert db.bet_market_get(mid).bets_closed == 1
        assert db.bet_market_close_betting(mid) is False  # already closed

    def test_set_odds_and_count_wagers(self, db):
        mid = _make_market(db)
        assert db.bet_market_count_wagers(mid) == 0
        assert db.bet_market_set_odds(mid, 1.5, 3.8, 6.0) is True
        m = db.bet_market_get(mid)
        assert (m.odds_home, m.odds_draw, m.odds_away) == (1.5, 3.8, 6.0)
        db.bet_place(GUILD, mid, USER_A, 'home', 10, 1.0, 1000)
        assert db.bet_market_count_wagers(mid) == 1

    def test_latest_settled_lookup(self, db):
        mid = _make_market(db)
        db.bet_market_set_thread(mid, THREAD)
        db.bet_settle(GUILD, mid, 'home', 1, 0, 5.0)
        assert db.bet_market_get_latest_settled_by_thread(
            GUILD, THREAD).market_id == mid
        assert db.bet_market_get_latest_settled_by_channel(
            GUILD, CH).market_id == mid
        assert db.bet_market_get_latest_settled_by_thread(GUILD, 'nope') is None


class TestBetsClosedExecution:
    """A market with bets_closed=1 rejects bets even before kickoff."""

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_closed_flag_blocks_betting(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'BET_MIN_STAKE', 1, raising=False)
        cog = Betting(bot=None)
        mid = _make_market(db, commence=1e12)  # far future → not kickoff-closed
        db.bet_market_close_betting(mid)
        market = db.bet_market_get(mid)
        user = type('U', (), {'id': USER_A})()
        status, _ = self._run(cog._execute_bet(GUILD, market, user, 'home', '50'))
        assert status == 'closed'


class TestBalanceLeaderboard:
    def test_orders_by_balance_desc(self, db):
        db.bet_ensure_wallet(GUILD, USER_A, 1000)
        db.bet_ensure_wallet(GUILD, USER_B, 1000)
        db.conn.execute('UPDATE bet_wallet SET balance = 1500 WHERE user_id = ?',
                        (USER_B,))
        db.conn.commit()
        rows = db.bet_balance_leaderboard(GUILD)
        assert [r.user_id for r in rows] == [USER_B, USER_A]


# ── Migration ──────────────────────────────────────────────────────────────

class TestMigration:
    def test_upgrade_creates_tables(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        conn.execute(
            'INSERT INTO bet_wallet (guild_id, user_id, balance) VALUES (?, ?, ?)',
            ('1', '10', 1000))
        conn.execute(
            'INSERT INTO bet_market (guild_id, channel_id, event_id, sport_key, '
            'home_team, away_team, commence_time, odds_home, odds_draw, '
            'odds_away, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            ('1', '2', 'e', 'soccer_epl', 'A', 'B', 0.0, 2.0, 3.0, 4.0, '9', 0.0))
        # thread_id + bets_closed columns exist
        conn.execute('UPDATE bet_market SET thread_id = ?, bets_closed = 1 '
                     'WHERE event_id = ?', ('77', 'e'))
        # bet_wager has no odds/payout columns (derived from the frozen market)
        conn.execute(
            'INSERT INTO bet_wager (market_id, user_id, pick, stake, placed_at) '
            'VALUES (?, ?, ?, ?, ?)', (1, '10', 'home', 100, 0.0))
        assert conn.execute('SELECT COUNT(*) FROM bet_wager').fetchone()[0] == 1
        cols = [r[1] for r in conn.execute('PRAGMA table_info(bet_wager)')]
        assert 'odds' not in cols and 'payout' not in cols
        indexes = [r[1] for r in conn.execute('PRAGMA index_list(bet_market)')]
        assert 'idx_bet_market_open_event' in indexes
        conn.execute(
            'INSERT INTO bet_wallet_txn '
            '(guild_id, user_id, actor_id, action, amount, balance_after, '
            'market_id, note, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            ('1', '10', '99', 'mod_grant', 50, 1050, None, None, 1.0))
        assert conn.execute('SELECT COUNT(*) FROM bet_wallet_txn').fetchone()[0] == 1
        conn.close()

    def test_upgrade_134_creates_wallet_audit_for_existing_betting_db(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        conn.execute('DROP TABLE bet_wallet_txn')
        upgrade_1_34_0(conn)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'")]
        assert 'bet_wallet_txn' in tables
        indexes = [r[1] for r in conn.execute('PRAGMA index_list(bet_wallet_txn)')]
        assert 'idx_bet_wallet_txn_user' in indexes
        conn.close()

    def test_upgrade_135_backfills_fixture_key_and_index(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        conn.execute(
            'INSERT INTO bet_market (guild_id, channel_id, event_id, sport_key, '
            'home_team, away_team, commence_time, odds_home, odds_draw, '
            'odds_away, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            ('1', '2', 'e', 'soccer_epl', 'Spain', 'Cape Verde', 10_000.0,
             2.0, 3.0, 4.0, '9', 0.0))
        upgrade_1_35_0(conn)
        cols = [r[1] for r in conn.execute('PRAGMA table_info(bet_market)')]
        assert 'fixture_key' in cols
        row = conn.execute(
            'SELECT fixture_key FROM bet_market WHERE event_id = ?', ('e',)
        ).fetchone()
        assert row.fixture_key == bet_fixture_key(
            'soccer_epl', 'Spain', 'Cape Verde', 10_000.0)
        indexes = [r[1] for r in conn.execute('PRAGMA index_list(bet_market)')]
        assert 'idx_bet_market_open_fixture' in indexes
        conn.close()

    def test_upgrade_135_refuses_existing_duplicate_open_fixture(self):
        conn = sqlite3.connect(':memory:')
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        for event_id, home, away in [
                ('e1', 'Spain', 'Cape Verde'),
                ('e2', 'Cape Verde', 'Spain')]:
            conn.execute(
                'INSERT INTO bet_market (guild_id, channel_id, event_id, sport_key, '
                'home_team, away_team, commence_time, odds_home, odds_draw, '
                'odds_away, created_by, created_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                ('1', '2', event_id, 'soccer_epl', home, away, 10_000.0,
                 2.0, 3.0, 4.0, '9', 0.0))
        with pytest.raises(RuntimeError):
            upgrade_1_35_0(conn)
        conn.close()

    def test_userdb_existing_134_runs_fixture_migration(self, tmp_path):
        from tle.util.db.user_db_upgrades import registry
        path = tmp_path / 'user.db'
        conn = sqlite3.connect(path)
        conn.row_factory = namedtuple_factory
        upgrade_1_33_0(conn)
        upgrade_1_34_0(conn)
        conn.execute('CREATE TABLE db_version (version TEXT NOT NULL)')
        conn.execute('INSERT INTO db_version (version) VALUES (?)', ('1.34.0',))
        conn.execute(
            'INSERT INTO bet_market (guild_id, channel_id, event_id, sport_key, '
            'home_team, away_team, commence_time, odds_home, odds_draw, '
            'odds_away, created_by, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            ('1', '2', 'e', 'soccer_epl', 'Spain', 'Cape Verde', 10_000.0,
             2.0, 3.0, 4.0, '9', 0.0))
        conn.commit()
        conn.close()

        db = UserDbConn(str(path))
        try:
            assert registry.get_current_version(db.conn) == '1.35.0'
            row = db.conn.execute(
                'SELECT fixture_key FROM bet_market WHERE event_id = ?', ('e',)
            ).fetchone()
            assert row.fixture_key == bet_fixture_key(
                'soccer_epl', 'Spain', 'Cape Verde', 10_000.0)
            indexes = [r.name for r in db.conn.execute('PRAGMA index_list(bet_market)')]
            assert 'idx_bet_market_open_fixture' in indexes
        finally:
            db.conn.close()


# ── Cog bet-execution path ──────────────────────────────────────────────────

class TestExecuteBet:
    """_execute_bet wires parse_amount + escrow through the cog against a real
    DB, returning a status the command/listener render differently."""

    @pytest.fixture
    def cog(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'BET_MIN_STAKE', 1, raising=False)
        return Betting(bot=None)

    def _user(self, uid):
        class _U:
            id = uid
        return _U()

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_ok(self, db, cog):
        mid = _make_market(db, commence=1e12)  # far future → open
        market = db.bet_market_get(mid)
        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', '300'))
        assert status == 'ok'
        assert data['stake'] == 300 and data['odds'] == 2.0
        assert data['potential'] == 600 and data['balance'] == 700

    def test_closed_after_kickoff(self, db, cog):
        mid = _make_market(db, commence=1.0)  # already kicked off
        market = db.bet_market_get(mid)
        status, _ = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', '100'))
        assert status == 'closed'

    def test_invalid_amount(self, db, cog):
        mid = _make_market(db, commence=1e12)
        market = db.bet_market_get(mid)
        status, _ = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', 'lots'))
        assert status == 'invalid'

    def test_insufficient(self, db, cog):
        mid = _make_market(db, commence=1e12)
        market = db.bet_market_get(mid)
        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', '5000'))
        assert status == 'insufficient' and data['balance'] == 1000

    def test_all_in_uses_full_balance(self, db, cog):
        mid = _make_market(db, commence=1e12)
        market = db.bet_market_get(mid)
        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'away', 'all'))
        assert status == 'ok' and data['stake'] == 1000 and data['balance'] == 0

    def test_negative_pick_uses_derived_odds(self, db, cog):
        mid = _make_market(db, commence=1e12, odds=(2.0, 4.0, 4.0))
        market = db.bet_market_get(mid)
        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'not_home', '100'))
        assert status == 'ok'
        assert abs(data['odds'] - 2.0) < 1e-9
        assert data['potential'] == 200

    def test_draw_rejected_on_no_draw_market(self, db, cog):
        mid = _make_market(db, commence=1e12, odds=(1.5, 0.0, 3.0))
        market = db.bet_market_get(mid)
        status, _ = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'draw', '100'))
        assert status == 'invalid_pick'

    def test_rebet_can_use_escrowed_stake(self, db, cog):
        mid = _make_market(db, commence=1e12)
        market = db.bet_market_get(mid)
        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'home', 'all'))
        assert status == 'ok' and data['balance'] == 0

        status, data = self._run(
            cog._execute_bet(GUILD, market, self._user(USER_A), 'away', 'all'))
        assert status == 'ok'
        assert data['stake'] == 1000 and data['balance'] == 0
        wager = db.bet_get_wager(mid, USER_A)
        assert wager.pick == 'away' and wager.stake == 1000


class TestCheckCommand:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_reports_key_health_without_secrets(self, monkeypatch):
        from tle import constants
        from tle.cogs.betting import Betting
        from tle.util import discord_common

        monkeypatch.setattr(constants, 'ODDS_API_KEY', 'odds-secret', raising=False)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', 'fd-secret',
                            raising=False)

        async def _sports(api_key):
            assert api_key == 'odds-secret'
            return [{'key': odds_api.WORLD_CUP_SPORT_KEY,
                     'title': 'FIFA World Cup 2026'}]

        async def _matches(token):
            assert token == 'fd-secret'
            return [{'home': 'Spain'}, {'home': 'Brazil'}]

        monkeypatch.setattr(odds_api, 'fetch_sports', _sports)
        monkeypatch.setattr(football_data, 'fetch_wc_matches', _matches)
        monkeypatch.setattr(discord_common, 'embed_neutral', lambda desc: desc)

        class _Ctx:
            def __init__(self):
                self.sent = []

            async def send(self, embed=None, **kw):
                self.sent.append(embed)

        ctx = _Ctx()
        cog = Betting(bot=None)
        self._run(Betting.check.__wrapped__(cog, ctx))

        text = ctx.sent[0]
        assert '`ODDS_API_KEY` works' in text
        assert '`FOOTBALL_DATA_API_KEY` works' in text
        assert '2 World Cup match' in text
        assert 'quota-free' in text
        assert 'odds-secret' not in text
        assert 'fd-secret' not in text


class TestTransferCommand:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def _member(self, uid, name, *, bot=False):
        return type('Member', (), {
            'id': uid,
            'display_name': name,
            'bot': bot,
        })()

    def test_transfer_command_moves_money_between_users(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)

        admin = self._member('999', 'Admin')
        source = self._member(USER_A, 'Alice')
        target = self._member(USER_B, 'Bob')

        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': int(GUILD)})()
                self.author = admin
                self.sent = []

            async def send(self, embed=None, **kw):
                self.sent.append(embed)

        db.bet_set_balance(GUILD, USER_A, 400, 1000)
        ctx = _Ctx()
        cog = Betting(bot=None)

        self._run(Betting.transfer.__wrapped__(
            cog, ctx, source, target, '25%'))

        assert db.bet_get_balance(GUILD, USER_A) == 300
        assert db.bet_get_balance(GUILD, USER_B) == 1100
        hist = db.bet_wallet_history(GUILD, USER_A)
        assert hist[0].action == 'transfer_out'
        assert hist[0].actor_id == str(admin.id)
        assert len(ctx.sent) == 1

    def test_transfer_command_rejects_same_source_and_destination(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting, BettingCogError
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)

        admin = self._member('999', 'Admin')
        source = self._member(USER_A, 'Alice')
        ctx = type('Ctx', (), {
            'guild': type('G', (), {'id': int(GUILD)})(),
            'author': admin,
        })()
        cog = Betting(bot=None)

        with pytest.raises(BettingCogError):
            self._run(Betting.transfer.__wrapped__(
                cog, ctx, source, source, '10'))


class TestHistoryCommand:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_any_user_can_inspect_another_wallet_history(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.bet_transfer(GUILD, USER_A, USER_B, 100, 1000, transferred_at=7.0)

        author = type('Member', (), {
            'id': USER_A,
            'display_name': 'Alice',
            'roles': [],
        })()
        target = type('Member', (), {
            'id': USER_B,
            'display_name': 'Bob',
            'roles': [],
        })()

        class _Ctx:
            def __init__(self):
                self.guild = type('G', (), {'id': int(GUILD)})()
                self.author = author
                self.sent = []

            async def send(self, embed=None, **kw):
                self.sent.append(embed)

        ctx = _Ctx()
        cog = Betting(bot=None)

        self._run(Betting.history.__wrapped__(cog, ctx, target))

        assert len(ctx.sent) == 1
        assert ctx.sent[0].title == 'Wallet history — Bob'


class TestBettingStaffPermissions:
    def test_mutating_betting_commands_do_not_allow_moderator_role(self):
        from pathlib import Path
        source = Path('tle/cogs/betting.py').read_text()
        assert ('@commands.has_any_role(constants.TLE_ADMIN, '
                'constants.TLE_MODERATOR)') not in source

    def test_transfer_command_is_admin_only(self):
        from pathlib import Path
        source = Path('tle/cogs/betting.py').read_text()
        block = source[source.index("@bet.command(name='transfer'"):
                       source.index('    def _wallet_txn_line')]
        assert '@commands.has_role(constants.TLE_ADMIN)' in block


# ── World Cup auto-open ─────────────────────────────────────────────────────

class TestIsDue:
    def test_inside_window(self):
        assert is_due(now := 3600, 0, 7200) is True  # kickoff in 1h, lead 2h

    def test_outside_window(self):
        assert is_due(8000, 0, 7200) is False  # kickoff in >2h

    def test_already_started(self):
        assert is_due(-1, 0, 7200) is False
        assert is_due(0, 0, 7200) is False  # exactly now is not "upcoming"


# Fakes for the auto-open engine (discord objects + bot).
class _FakeThread:
    def __init__(self, tid):
        self.id = tid
        self.sent = []
        self.archived = False

    async def send(self, embed=None, **kw):
        self.sent.append(embed)

    async def edit(self, **kw):
        self.archived = kw.get('archived', self.archived)


class _FakeMsg:
    _n = 5000

    def __init__(self):
        _FakeMsg._n += 1
        self.id = _FakeMsg._n
        self.thread = None
        self.deleted = False
        self.edited_embed = None

    async def create_thread(self, name=None, auto_archive_duration=None):
        self.thread = _FakeThread(self.id + 100000)
        return self.thread

    async def edit(self, embed=None, **kw):
        self.edited_embed = embed

    async def delete(self):
        self.deleted = True


class _FakeChannel:
    def __init__(self, cid):
        self.id = cid
        self.mention = f'<#{cid}>'
        self.sent = []
        self._messages = {}

    async def send(self, embed=None, **kw):
        m = _FakeMsg()
        self.sent.append(m)
        self._messages[m.id] = m
        return m

    async def fetch_message(self, mid):
        return self._messages[mid]


class _FakeGuild:
    def __init__(self, gid, channel):
        self.id = gid
        self._channel = channel

    def get_channel(self, cid):
        return self._channel


class _FakeBot:
    def __init__(self, guilds, channels):
        self.guilds = guilds
        self._channels = channels
        self.user = type('U', (), {'id': 999})()

    def get_channel(self, cid):
        return self._channels.get(cid)


def _wc_event(event_id='evtWC', home='Spain', away='Cape Verde', commence=None):
    return {
        'event_id': event_id, 'sport_key': 'soccer_fifa_world_cup',
        'home_team': home, 'away_team': away,
        'commence_time': commence,
        'odds': {'home': 1.25, 'draw': 5.5, 'away': 12.0},
    }


def _fixture_key(event):
    return bet_fixture_key(
        event['sport_key'], event['home_team'], event['away_team'],
        event['commence_time'])


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
            assert market.market_id in cog._close_timers  # closes exactly at kickoff
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


class TestAutoSettleFootballData:
    """The settle poller reads results from football-data.org (free) and
    settles any market past kickoff whose game has finished."""

    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    def test_settles_from_football_data(self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle.util import football_data as fd
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', 'fdkey',
                            raising=False)
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Spain',
            'Cape Verde', _t.time() - 100, 1.25, 5.5, 12.0, USER_A, 0.0)
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)  # bal 900

        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        cog = Betting(bot)

        async def _fake_fetch(token, **kw):
            return [{'home': 'Spain', 'away': 'Cape Verde',
                     'commence_time': _t.time() - 100, 'finished': True,
                     'home_score': 3, 'away_score': 1}]
        monkeypatch.setattr(fd, 'fetch_wc_matches', _fake_fetch)

        self._run(cog._settle_via_football_data())
        m = db.bet_market_get(mid)
        assert m.status == 'settled' and m.result == 'home'
        assert m.result_home == 3 and m.result_away == 1
        # payout = round(100 * 1.25) = 125 → 900 + 125
        assert db.bet_get_balance(GUILD, USER_A) == 1025

    def test_settlement_posts_result_only_to_parent_channel(self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Spain',
            'Cape Verde', _t.time() - 100, 1.25, 5.5, 12.0, USER_A, 0.0)
        db.bet_market_set_thread(mid, '333')
        db.bet_place(GUILD, mid, USER_A, 'home', 100, 1.0, 1000)

        channel = _FakeChannel(222)
        thread = _FakeThread(333)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)],
                       {222: channel, 333: thread})
        cog = Betting(bot)

        self._run(cog._do_settle(
            db.bet_market_get(mid), 'home', 2, 1, source='auto'))

        assert len(channel.sent) == 1
        assert len(thread.sent) == 0
        assert thread.archived is True

    def test_knockout_settles_by_advancing_winner(self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle.util import football_data as fd
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', 'fdkey',
                            raising=False)
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Spain',
            'Cape Verde', _t.time() - 100, 1.5, 0.0, 3.0, USER_A, 0.0)
        db.bet_place(GUILD, mid, USER_A, 'away', 100, 1.0, 1000)

        channel = _FakeChannel(222)
        bot = _FakeBot([_FakeGuild(int(GUILD), channel)], {222: channel})
        cog = Betting(bot)

        async def _fake_fetch(token, **kw):
            return [{'home': 'Spain', 'away': 'Cape Verde',
                     'commence_time': _t.time() - 100, 'finished': True,
                     'home_score': 1, 'away_score': 1, 'winner': 'away'}]
        monkeypatch.setattr(fd, 'fetch_wc_matches', _fake_fetch)

        self._run(cog._settle_via_football_data())
        m = db.bet_market_get(mid)
        assert m.status == 'settled' and m.result == 'away'
        assert db.bet_get_balance(GUILD, USER_A) == 1200

    def test_no_key_no_settle(self, db, monkeypatch):
        import time as _t
        from tle.util import codeforces_common as cf_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(constants, 'FOOTBALL_DATA_API_KEY', None,
                            raising=False)
        mid = db.bet_market_create(
            GUILD, '222', 'evtWC', 'soccer_fifa_world_cup', 'Spain',
            'Cape Verde', _t.time() - 100, 1.25, 5.5, 12.0, USER_A, 0.0)
        cog = Betting(bot=None)
        self._run(cog._settle_via_football_data())  # no key → no-op
        assert db.bet_market_get(mid).status == 'open'


class TestSecondsUntilOpen:
    def test_future_game(self):
        from tle.cogs.betting import seconds_until_open
        # kickoff in 3h, lead 2h → opens in 1h (3600s), exactly at kickoff−2h.
        assert seconds_until_open(now := 3 * 3600, 2 * 3600, 0) == 3600

    def test_in_window_floors_to_zero(self):
        from tle.cogs.betting import seconds_until_open
        # kickoff in 1h, lead 2h → already past the open moment → 0.
        assert seconds_until_open(3600, 7200, 0) == 0


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


class _FakeBetMessage:
    def __init__(self, content, channel_id=333):
        self.content = content
        self.author = type('A', (), {'bot': False, 'id': 1})()
        self.guild = type('G', (), {'id': int(GUILD)})()
        self.channel = type('C', (), {'id': channel_id})()
        self.reactions = []

    async def add_reaction(self, emoji):
        self.reactions.append(emoji)


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


class TestBettingSurfaceUX:
    def _run(self, coro):
        import asyncio
        return asyncio.run(coro)

    @pytest.fixture
    def cog(self, db, monkeypatch):
        from tle.util import codeforces_common as cf_common
        from tle.util import discord_common
        from tle import constants
        from tle.cogs.betting import Betting
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(discord_common, '_BOT_PREFIX', ';', raising=False)
        monkeypatch.setattr(constants, 'BET_START_BALANCE', 1000, raising=False)
        monkeypatch.setattr(constants, 'BET_MIN_STAKE', 1, raising=False)
        return Betting(bot=None)

    def test_parent_market_embed_points_to_existing_thread(self, db, cog):
        mid = _make_market(db, commence=1e12)
        db.bet_market_set_thread(mid, THREAD)
        market = db.bet_market_get(mid)
        embed = cog._market_embed(market, current_channel_id=CH)
        assert f'<#{THREAD}>' in embed.description
        assert 'thread below' not in embed.description

    def test_parent_channel_plain_bet_remains_thread_only(self, db, cog):
        mid = _make_market(db, commence=1e12)
        msg = _FakeBetMessage('Spain 100', channel_id=int(CH))

        self._run(cog.on_message(msg))

        assert msg.reactions == []
        assert db.bet_get_wager(mid, msg.author.id) is None


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
