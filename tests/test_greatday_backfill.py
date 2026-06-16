"""Tests for the great day feature — backfill parsing and rank-line render."""
from tests.greatday_test_utils import _FakeMessage


class TestBackfillRegex:
    def _matches(self, content):
        from tle.cogs.greatday import _GREATDAY_RE, _MENTION_RE
        if not _GREATDAY_RE.match(content):
            return None
        return _MENTION_RE.findall(content)

    def test_matches_singular(self):
        assert self._matches('I hope <@111> is having a great day!') == ['111']

    def test_matches_plural(self):
        assert self._matches(
            'I hope <@111> <@222> <@333> are having a great day!'
        ) == ['111', '222', '333']

    def test_matches_nick_mention_format(self):
        assert self._matches('I hope <@!111> is having a great day!') == ['111']

    def test_rejects_unrelated_message(self):
        assert self._matches('hello world') is None
        assert self._matches('I hope you have a great day!') is None
        assert self._matches('I hope <@1> wins!') is None

    def test_tolerates_trailing_whitespace(self):
        assert self._matches('I hope <@111> is having a great day! ') == ['111']


class TestBackfillAuthorCheck:
    """The backfill helper must reject otherwise-matching messages whose
    author isn't the bot — anyone could spoof '... having a great day!'."""

    def _msg(self, content, author_id):
        class _Author:
            pass
        a = _Author()
        a.id = author_id
        m = _FakeMessage(content)
        m.author = a
        return m

    def test_accepts_bot_authored_message(self):
        from tle.cogs.greatday import _parse_greatday_message
        m = self._msg('I hope <@111> is having a great day!', author_id=42)
        assert _parse_greatday_message(m, bot_user_id=42) == ['111']

    def test_rejects_non_bot_authored_message(self):
        from tle.cogs.greatday import _parse_greatday_message
        m = self._msg('I hope <@999> is having a great day!', author_id=7)
        assert _parse_greatday_message(m, bot_user_id=42) is None

    def test_rejects_when_bot_user_id_unknown(self):
        from tle.cogs.greatday import _parse_greatday_message
        m = self._msg('I hope <@111> is having a great day!', author_id=42)
        assert _parse_greatday_message(m, bot_user_id=None) is None

    def test_rejects_bot_authored_non_template(self):
        from tle.cogs.greatday import _parse_greatday_message
        m = self._msg('hello world', author_id=42)
        assert _parse_greatday_message(m, bot_user_id=42) is None

    def test_rejects_bot_authored_template_with_no_mentions(self):
        from tle.cogs.greatday import _parse_greatday_message
        m = self._msg('I hope they are having a great day!', author_id=42)
        assert _parse_greatday_message(m, bot_user_id=42) is None

    def test_handles_message_without_author(self):
        from tle.cogs.greatday import _parse_greatday_message
        m = _FakeMessage('I hope <@111> is having a great day!')
        # _FakeMessage has no .author — must not crash
        assert _parse_greatday_message(m, bot_user_id=42) is None


class TestPersonalRankLine:
    """;greatday stats should show the invoker's own rank/count on every
    page so they don't have to scan the whole leaderboard to find
    themselves."""

    def _row(self, user_id, cnt):
        class _R:
            pass
        r = _R()
        r.user_id = str(user_id)
        r.cnt = cnt
        return r

    def test_user_at_top(self):
        from tle.cogs.greatday import _personal_rank_line
        rows = [self._row(100, 10), self._row(200, 5), self._row(300, 2)]
        line = _personal_rank_line(rows, 100)
        assert '#1' in line
        assert '10' in line

    def test_user_in_middle(self):
        from tle.cogs.greatday import _personal_rank_line
        rows = [self._row(100, 10), self._row(200, 5), self._row(300, 2)]
        line = _personal_rank_line(rows, 200)
        assert '#2' in line
        assert '**5**' in line

    def test_user_at_bottom(self):
        from tle.cogs.greatday import _personal_rank_line
        rows = [self._row(100, 10), self._row(200, 5), self._row(300, 2)]
        line = _personal_rank_line(rows, 300)
        assert '#3' in line

    def test_user_not_in_leaderboard(self):
        from tle.cogs.greatday import _personal_rank_line
        rows = [self._row(100, 10), self._row(200, 5)]
        line = _personal_rank_line(rows, 999)
        assert "haven't" in line
        assert '#' not in line

    def test_user_id_type_coercion(self):
        """user_id stored as TEXT in SQLite; ctx.author.id is int. Must
        compare correctly regardless of which side is which type."""
        from tle.cogs.greatday import _personal_rank_line
        rows = [self._row('100', 10)]
        assert '#1' in _personal_rank_line(rows, 100)
        assert '#1' in _personal_rank_line(rows, '100')

    def test_empty_rows(self):
        from tle.cogs.greatday import _personal_rank_line
        assert "haven't" in _personal_rank_line([], 100)

    def test_returns_plain_text_no_leading_whitespace(self):
        """Renders as message content (above the embed), not as part of
        the embed description — must not start with a stray newline."""
        from tle.cogs.greatday import _personal_rank_line
        rows = [self._row(100, 10)]
        assert _personal_rank_line(rows, 100)[0] != '\n'
        assert _personal_rank_line([], 100)[0] != '\n'


class TestBackfillStopHeuristic:
    """The backfill scans newest-first and stops early once we've walked
    past the most recent match by more than the gap threshold. Greatday
    runs daily, so a multi-day gap means we've recovered the full
    history and further scanning is wasted bandwidth."""

    def _stop(self):
        from tle.cogs.greatday import _should_stop_backfill
        return _should_stop_backfill

    def test_does_not_stop_before_first_match(self):
        """No anchor yet — must keep scanning even if many days have
        passed (otherwise channels with a long no-greatday prefix would
        never get backfilled)."""
        stop = self._stop()
        assert stop(None, 1000.0, 60) is False
        assert stop(None, 0.0, 60) is False

    def test_does_not_stop_within_gap_window(self):
        stop = self._stop()
        # Match at t=1000, currently scanning t=950 → gap=50 < 60
        assert stop(1000.0, 950.0, 60) is False
        # Boundary: gap == threshold → keep scanning (strict >)
        assert stop(1000.0, 940.0, 60) is False

    def test_stops_when_gap_exceeds_threshold(self):
        stop = self._stop()
        # Match at t=1000, currently scanning t=939 → gap=61 > 60
        assert stop(1000.0, 939.0, 60) is True

    def test_five_day_threshold(self):
        """Cross-check the production threshold."""
        from tle.cogs.greatday import _BACKFILL_STOP_GAP_SECONDS
        assert _BACKFILL_STOP_GAP_SECONDS == 5 * 24 * 3600
        stop = self._stop()
        last_match = 1_000_000.0
        # 4 days, 23 hrs → keep going
        assert stop(last_match, last_match - 4 * 86400 - 23 * 3600,
                    _BACKFILL_STOP_GAP_SECONDS) is False
        # 5 days, 1 sec → stop
        assert stop(last_match, last_match - 5 * 86400 - 1,
                    _BACKFILL_STOP_GAP_SECONDS) is True
