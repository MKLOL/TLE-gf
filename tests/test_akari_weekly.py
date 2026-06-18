"""Weekly Daily Akari score and rating preview tests."""

import asyncio
import datetime as dt
import math
from types import SimpleNamespace

from tle.util.akari_weekly import (
    compute_weekly_ratings,
    current_week_standings,
    difficulty_weight,
    result_performance,
    score_week,
)
from tle.cogs._minigame_tables import _akari_weekly_table_rows
from tests.minigames_test_utils import db
from tests.minigames_test_utils import _FakeDiscordMember, _FakeGuild


def _row(user, puzzle, day, *, perfect=True, accuracy=100, seconds=60):
    return SimpleNamespace(
        user_id=str(user),
        puzzle_number=puzzle,
        puzzle_date=day.isoformat(),
        is_perfect=perfect,
        accuracy=accuracy,
        time_seconds=seconds,
    )


class TestWeeklyPerformance:
    def test_best_perfect_is_one_and_slower_perfect_stays_above_half(self):
        best = _row('a', 1, dt.date(2026, 6, 15), seconds=60)
        slow = _row('b', 1, dt.date(2026, 6, 15), seconds=120)
        assert result_performance(best, best_perfect_time=60) == 1.0
        score = result_performance(slow, best_perfect_time=60)
        assert math.isclose(score, 0.5 + 0.5 * math.exp(-0.7))
        assert score > 0.5

    def test_time_breaks_equal_accuracy_without_crossing_accuracy_bands(self):
        fast_99 = _row('a', 1, dt.date(2026, 6, 15),
                       perfect=False, accuracy=99, seconds=60)
        slow_99 = _row('b', 1, dt.date(2026, 6, 15),
                       perfect=False, accuracy=99, seconds=600)
        fast_98 = _row('c', 1, dt.date(2026, 6, 15),
                       perfect=False, accuracy=98, seconds=10)
        a = result_performance(
            fast_99, best_time_for_accuracy=fast_99.time_seconds)
        b = result_performance(
            slow_99, best_time_for_accuracy=fast_99.time_seconds)
        c = result_performance(
            fast_98, best_time_for_accuracy=fast_98.time_seconds)
        assert a > b > c

    def test_difficulty_curve_is_centered_and_monotonic(self):
        values = [difficulty_weight(level) for level in range(1, 6)]
        assert values == sorted(values)
        assert values[2] == 1.0
        assert math.isclose(values[-1] / values[0], 2.0)


class TestWeeklyScoring:
    def test_harder_day_contributes_more_and_scores_sum(self):
        monday = dt.date(2026, 6, 15)
        rows = [
            _row('a', 526, monday, seconds=60),
            _row('b', 526, monday, seconds=120),
            _row('a', 527, monday + dt.timedelta(days=1), seconds=120),
            _row('b', 527, monday + dt.timedelta(days=1), seconds=60),
        ]
        # Tuesday is much harder, so B's Tuesday win outweighs A's Monday win.
        standings = score_week(rows, {526: 1, 527: 5})
        assert standings[0].user_id == 'b'
        assert standings[0].score > standings[1].score
        assert all(s.days_played == 2 for s in standings)

    def test_missing_day_scores_zero(self):
        monday = dt.date(2026, 6, 15)
        rows = [
            _row('a', 526, monday),
            _row('b', 526, monday),
            _row('a', 527, monday + dt.timedelta(days=1)),
        ]
        standings = {s.user_id: s for s in score_week(rows)}
        assert standings['a'].days_played == 2
        assert standings['b'].days_played == 1
        assert standings['a'].score > standings['b'].score

    def test_current_week_only(self):
        monday = dt.date(2026, 6, 15)
        rows = [
            _row('old', 519, monday - dt.timedelta(days=7)),
            _row('now', 526, monday),
        ]
        standings = current_week_standings(
            rows, as_of_date=monday + dt.timedelta(days=3))
        assert [s.user_id for s in standings] == ['now']

    def test_table_multiplies_scores_by_1000_and_rounds(self):
        monday = dt.date(2026, 6, 15)
        standings = score_week([
            _row('10', 526, monday, seconds=60),
            _row('20', 526, monday, seconds=120),
        ])
        guild = _FakeGuild(1, members=[
            _FakeDiscordMember(10, 'Alice'),
            _FakeDiscordMember(20, 'Bob'),
        ])
        rows = _akari_weekly_table_rows(guild, standings)
        assert rows[0][1] == 'Alice'
        assert rows[0][3] == round(standings[0].score * 1000)


class TestWeeklyRatings:
    def test_only_completed_weeks_are_rated(self):
        first_monday = dt.date(2026, 6, 8)
        current_monday = first_monday + dt.timedelta(days=7)
        rows = [
            _row('a', 519, first_monday, seconds=60),
            _row('b', 519, first_monday, seconds=120),
            _row('a', 526, current_monday, seconds=120),
            _row('b', 526, current_monday, seconds=60),
        ]
        states = compute_weekly_ratings(
            rows, as_of_date=current_monday + dt.timedelta(days=2))
        assert states['a'].games == states['b'].games == 1
        assert states['a'].rating > 1200 > states['b'].rating
        # The current-week reversal is provisional and has not changed rating.
        current = current_week_standings(
            rows, as_of_date=current_monday + dt.timedelta(days=2))
        assert current[0].user_id == 'b'


class TestDifficultyCacheDb:
    def test_upsert_and_read(self, db):
        assert db.get_akari_puzzle_difficulties([526]) == {}
        assert db.upsert_akari_puzzle_difficulties(
            {526: 2, 527: 5}, 123.0) == 2
        assert db.get_akari_puzzle_difficulties([527, 526]) == {
            526: 2,
            527: 5,
        }
        db.upsert_akari_puzzle_difficulties({526: 4}, 456.0)
        assert db.get_akari_puzzle_difficulties([526]) == {526: 4}


class TestWeeklyCommand:
    def test_weekly_flag_sends_rating_and_current_score_tables(
            self, db, monkeypatch):
        from tle.cogs import minigames as minigames_module
        from tle.cogs._minigame_akari import expected_puzzle_number
        from tle.util import codeforces_common as cf_common

        db.set_guild_config(1, 'akari', '1')
        monkeypatch.setattr(cf_common, 'user_db', db)
        today = dt.date.today()
        monday = today - dt.timedelta(days=today.weekday())
        previous = monday - dt.timedelta(days=7)

        def save(message, user, day, seconds):
            db.save_minigame_result(
                message, 1, 'akari', 10, user,
                expected_puzzle_number(day), day.isoformat(),
                100, seconds, True, 'raw')

        save(1, 10, previous, 60)
        save(2, 20, previous, 120)
        save(3, 10, monday, 120)
        save(4, 20, monday, 60)

        cog = minigames_module.Minigames(bot=None)

        async def no_fetch(_numbers):
            return {}

        monkeypatch.setattr(cog, '_akari_difficulty_map', no_fetch)
        sent = []
        monkeypatch.setattr(
            minigames_module, '_get_akari_rating_table_image_file',
            lambda *args, **kwargs: ('ratings', kwargs['title']))
        monkeypatch.setattr(
            minigames_module, '_get_akari_weekly_table_image_file',
            lambda _guild, standings, *, title:
                ('weekly', title, [s.user_id for s in standings]))

        async def send(**kwargs):
            sent.append(kwargs)

        ctx = SimpleNamespace(guild=_FakeGuild(1), send=send)
        asyncio.run(cog._cmd_akari_ratings(ctx, weekly=True))

        assert len(sent) == 2
        assert sent[0]['file'][0] == 'ratings'
        assert 'weekly preview' in sent[0]['file'][1]
        assert sent[1]['file'][0] == 'weekly'
        assert sent[1]['file'][2] == ['20', '10']

    def test_public_weekly_scores_hide_opted_out_players(
            self, db, monkeypatch):
        """An unregistered player must not leak into the public scores table."""
        from tle.cogs import minigames as minigames_module
        from tle.cogs._minigame_akari import expected_puzzle_number
        from tle.util import codeforces_common as cf_common

        db.set_guild_config(1, 'akari', '1')
        monkeypatch.setattr(cf_common, 'user_db', db)
        today = dt.date.today()
        monday = today - dt.timedelta(days=today.weekday())
        previous = monday - dt.timedelta(days=7)

        def save(message, user, day, seconds):
            db.save_minigame_result(
                message, 1, 'akari', 10, user,
                expected_puzzle_number(day), day.isoformat(),
                100, seconds, True, 'raw')

        save(1, 10, previous, 60)
        save(2, 20, previous, 120)
        save(3, 10, monday, 120)
        save(4, 20, monday, 60)
        db.unregister_akari_user(1, 10, 1.0)

        cog = minigames_module.Minigames(bot=None)

        async def no_fetch(_numbers):
            return {}

        monkeypatch.setattr(cog, '_akari_difficulty_map', no_fetch)
        weekly_sent = []
        monkeypatch.setattr(
            minigames_module, '_get_akari_rating_table_image_file',
            lambda *args, **kwargs: ('ratings', kwargs['title']))
        monkeypatch.setattr(
            minigames_module, '_get_akari_weekly_table_image_file',
            lambda _guild, standings, *, title:
                ('weekly', [s.user_id for s in standings]))

        async def send(**kwargs):
            weekly_sent.append(kwargs)

        ctx = SimpleNamespace(guild=_FakeGuild(1), send=send)

        asyncio.run(cog._cmd_akari_ratings(ctx, weekly=True))
        public_scores = [k['file'] for k in weekly_sent
                         if k.get('file', (None,))[0] == 'weekly']
        assert public_scores == [('weekly', ['20'])]

        # The admin debug board is an explicit "show everyone" view and must
        # still include the opted-out player.
        weekly_sent.clear()
        asyncio.run(cog._cmd_akari_ratings_debug(ctx, weekly=True))
        debug_scores = [k['file'] for k in weekly_sent
                        if k.get('file', (None,))[0] == 'weekly']
        assert debug_scores == [('weekly', ['20', '10'])]
