"""Tests for great day signup/signout event tracking, stats and backfill."""
import asyncio
import sqlite3

import pytest

from tle.cogs import _greatday_events as events
from tle.util import codeforces_common as cf_common

from tests.greatday_test_utils import (  # noqa: F401
    GUILD, USER_A, USER_B, DiscordAuthor as _Author,
    DiscordContext as _Ctx, DiscordGuild as _Guild,
    DiscordEmbed, DiscordMessage as _Msg, FakeGreatDayDb, db,
)


class TestEventDb:
    def test_records_and_reads_events_newest_first(self, db):
        db.greatday_record_signup_event(GUILD, USER_A, 'signup', 100.0, 1)
        db.greatday_record_signup_event(GUILD, USER_A, 'signout', 200.0, 2)
        rows = db.greatday_get_signup_events(GUILD, USER_A)
        assert [(r.action, r.at) for r in rows] == [
            ('signout', 200.0), ('signup', 100.0)]

    def test_recording_is_idempotent_per_message(self, db):
        assert db.greatday_record_signup_event(
            GUILD, USER_A, 'signup', 100.0, 1) is True
        assert db.greatday_record_signup_event(
            GUILD, USER_A, 'signup', 100.0, 1) is False
        assert len(db.greatday_get_signup_events(GUILD, USER_A)) == 1

    def test_unknown_action_rejected(self, db):
        with pytest.raises(ValueError):
            db.greatday_record_signup_event(GUILD, USER_A, 'banned', 1.0, 1)

    def test_last_signup_ignores_signouts(self, db):
        db.greatday_record_signup_event(GUILD, USER_A, 'signup', 100.0, 1)
        db.greatday_record_signup_event(GUILD, USER_A, 'signout', 300.0, 2)
        assert db.greatday_get_last_signup(GUILD, USER_A).at == 100.0

    def test_last_signup_none_without_events(self, db):
        assert db.greatday_get_last_signup(GUILD, USER_A) is None

    def test_events_are_per_guild_and_user(self, db):
        db.greatday_record_signup_event(GUILD, USER_A, 'signup', 100.0, 1)
        assert db.greatday_get_signup_events(GUILD, USER_B) == []
        assert db.greatday_get_signup_events('999', USER_A) == []

    def test_post_times_collapse_multi_user_picks(self, db):
        db.greatday_record_picks(GUILD, [USER_A, USER_B], 10, 100.0)
        db.greatday_record_picks(GUILD, [USER_A], 11, 200.0)
        assert db.greatday_get_post_times(GUILD) == [100.0, 200.0]

    @staticmethod
    def _reject_events(db):
        db.conn.execute('''
            CREATE TRIGGER reject_greatday_event
            BEFORE INSERT ON greatday_signup_event
            BEGIN
                SELECT RAISE(ABORT, 'simulated event failure');
            END
        ''')
        db.conn.commit()

    def test_signup_and_event_roll_back_together(self, db):
        self._reject_events(db)
        with pytest.raises(sqlite3.IntegrityError):
            db.greatday_signup_with_event(GUILD, USER_A, 10.0, 1)
        assert not db.greatday_is_signed_up(GUILD, USER_A)

    def test_remove_and_event_roll_back_together(self, db):
        db.greatday_signup(GUILD, USER_A)
        self._reject_events(db)
        with pytest.raises(sqlite3.IntegrityError):
            db.greatday_remove_with_event(GUILD, USER_A, 10.0, 1)
        assert db.greatday_is_signed_up(GUILD, USER_A)

    def test_ban_removal_and_event_roll_back_together(self, db):
        db.greatday_signup(GUILD, USER_A)
        self._reject_events(db)
        with pytest.raises(sqlite3.IntegrityError):
            db.greatday_ban_with_event(GUILD, USER_A, 10.0, 1)
        assert db.greatday_is_signed_up(GUILD, USER_A)
        assert not db.greatday_is_banned(GUILD, USER_A)


class TestSignedUpPostCount:
    def _events(self, *pairs):
        return [_Row(action, at) for action, at in pairs]

    def test_counts_posts_inside_membership(self):
        rows = self._events(('signup', 100.0), ('signout', 300.0))
        count, complete = events.signed_up_post_count(
            rows, [50.0, 150.0, 250.0, 400.0], currently_signed_up=False)
        assert (count, complete) == (2, True)

    def test_open_interval_counts_later_posts(self):
        rows = self._events(('signup', 100.0))
        count, complete = events.signed_up_post_count(
            rows, [50.0, 150.0, 9e9], currently_signed_up=True)
        assert (count, complete) == (2, True)

    def test_signout_without_signup_is_incomplete(self):
        rows = self._events(('signout', 300.0))
        count, complete = events.signed_up_post_count(
            rows, [100.0, 400.0], currently_signed_up=False)
        assert (count, complete) == (0, False)

    def test_member_without_events_is_incomplete(self):
        count, complete = events.signed_up_post_count(
            [], [100.0], currently_signed_up=True)
        assert (count, complete) == (0, False)

    def test_no_events_cannot_prove_the_user_never_signed_up(self):
        count, complete = events.signed_up_post_count(
            [], [100.0], currently_signed_up=False)
        assert (count, complete) == (0, False)

    def test_missing_signout_does_not_extend_interval_to_infinity(self):
        rows = self._events(('signup', 100.0))
        count, complete = events.signed_up_post_count(
            rows, [150.0, 250.0], currently_signed_up=False)
        assert (count, complete) == (0, False)

    def test_missing_later_signup_does_not_claim_complete_history(self):
        rows = self._events(('signup', 100.0), ('signout', 200.0))
        count, complete = events.signed_up_post_count(
            rows, [150.0, 250.0], currently_signed_up=True)
        assert (count, complete) == (1, False)

    def test_repeated_signup_counts_only_the_later_known_interval(self):
        rows = self._events(('signup', 100.0), ('signup', 200.0))
        count, complete = events.signed_up_post_count(
            rows, [150.0, 250.0], currently_signed_up=True)
        assert (count, complete) == (1, False)


class _Row:
    """Stand-in for a signup event row."""

    _next_id = 0

    def __init__(self, action, at):
        self.action = action
        self.at = at
        type(self)._next_id += 1
        self.message_id = str(type(self)._next_id)


class _Pick:
    def __init__(self, picked_at, message_id):
        self.picked_at = picked_at
        self.message_id = str(message_id)


class TestCollapseEvents:
    def test_drops_repeated_signups(self):
        rows = [_Row('signup', 100.0), _Row('signup', 200.0),
                _Row('signout', 300.0), _Row('signout', 400.0),
                _Row('signup', 500.0)]
        kept = events.collapse_events(rows)
        assert [(row.action, row.at) for row in kept] == [
            ('signup', 500.0), ('signout', 300.0), ('signup', 100.0)]

    def test_accepts_newest_first_input(self):
        rows = [_Row('signup', 200.0), _Row('signup', 100.0)]
        kept = events.collapse_events(reversed(rows))
        assert [(row.action, row.at) for row in kept] == [('signup', 100.0)]

    def test_empty(self):
        assert events.collapse_events([]) == []


class TestMergeHistory:
    def test_interleaves_picks_and_events_newest_first(self):
        picks = [_Pick(200.0, 20)]
        rows = [_Row('signup', 100.0), _Row('signout', 300.0)]
        assert events.merge_history(picks, rows) == [
            'Signed out — <t:300:F> (<t:300:R>)',
            '<t:200:F> (<t:200:R>)',
            'Signed up — <t:100:F> (<t:100:R>)',
        ]

    def test_empty_when_nothing_recorded(self):
        assert events.merge_history([], []) == []


def _make_cog():
    from tle.cogs.greatday import GreatDay
    return GreatDay(bot=None)


class TestCommandsRecordEvents:
    def _ctx(self, author_id, msg_id, at):
        author = _Author(author_id)
        return _Ctx(_Guild(GUILD), author,
                    _Msg(';greatday signup', author, msg_id=msg_id, at=at))

    def test_signup_records_event(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = _make_cog()
        ctx = self._ctx(USER_A, 5, 50.0)
        asyncio.run(cog.signup.callback(cog, ctx))
        rows = db.greatday_get_signup_events(GUILD, USER_A)
        assert [(r.action, r.at) for r in rows] == [('signup', 50.0)]

    def test_repeated_signup_records_nothing_new(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = _make_cog()
        asyncio.run(cog.signup.callback(cog, self._ctx(USER_A, 5, 50.0)))
        asyncio.run(cog.signup.callback(cog, self._ctx(USER_A, 6, 60.0)))
        assert len(db.greatday_get_signup_events(GUILD, USER_A)) == 1

    def test_remove_records_signout(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = _make_cog()
        asyncio.run(cog.signup.callback(cog, self._ctx(USER_A, 5, 50.0)))
        asyncio.run(cog.remove.callback(cog, self._ctx(USER_A, 6, 60.0)))
        rows = db.greatday_get_signup_events(GUILD, USER_A)
        assert [(r.action, r.at) for r in rows] == [
            ('signout', 60.0), ('signup', 50.0)]

    def test_remove_without_signup_records_nothing(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = _make_cog()
        asyncio.run(cog.remove.callback(cog, self._ctx(USER_A, 6, 60.0)))
        assert db.greatday_get_signup_events(GUILD, USER_A) == []

    def test_admin_add_and_kick_record_target_events(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = _make_cog()
        admin = _Author('999')
        member = _Author(USER_B, 'target')
        ctx = _Ctx(_Guild(GUILD), admin,
                   _Msg(';greatday add', admin, msg_id=11, at=110.0))
        asyncio.run(cog.add_user.callback(cog, ctx, member))
        ctx = _Ctx(_Guild(GUILD), admin,
                   _Msg(';greatday kick', admin, msg_id=12, at=120.0))
        asyncio.run(cog.kick_user.callback(cog, ctx, member))
        rows = db.greatday_get_signup_events(GUILD, USER_B)
        assert [(r.action, r.at) for r in rows] == [
            ('signout', 120.0), ('signup', 110.0)]

    def test_ban_records_signout_only_when_signed_up(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        cog = _make_cog()
        admin = _Author('999')
        member = _Author(USER_B, 'target')
        db.greatday_signup(GUILD, USER_B)
        ctx = _Ctx(_Guild(GUILD), admin,
                   _Msg(';greatday ban', admin, msg_id=13, at=130.0))
        asyncio.run(cog.ban_user.callback(cog, ctx, member))
        rows = db.greatday_get_signup_events(GUILD, USER_B)
        assert [(r.action, r.at) for r in rows] == [('signout', 130.0)]

        db.greatday_unban(GUILD, USER_B)
        ctx = _Ctx(_Guild(GUILD), admin,
                   _Msg(';greatday ban', admin, msg_id=14, at=140.0))
        asyncio.run(cog.ban_user.callback(cog, ctx, member))
        assert len(db.greatday_get_signup_events(GUILD, USER_B)) == 1

    def test_signup_failure_sends_no_success_and_changes_no_membership(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)

        def _raise(*a, **kw):
            raise RuntimeError('simulated DB failure')
        monkeypatch.setattr(db, 'greatday_signup_with_event', _raise)
        cog = _make_cog()
        ctx = self._ctx(USER_A, 5, 50.0)
        with pytest.raises(RuntimeError):
            asyncio.run(cog.signup.callback(cog, ctx))
        assert not db.greatday_is_signed_up(GUILD, USER_A)
        assert ctx.sent == []

    def test_membership_result_references_command_and_carries_target_id(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        from tle.cogs import _greatday_commands as command_module
        monkeypatch.setattr(
            command_module.discord_common, 'embed_success',
            lambda description: DiscordEmbed(description))
        cog = _make_cog()
        ctx = self._ctx(USER_A, 5, 50.0)
        asyncio.run(cog.signup.callback(cog, ctx))
        assert ctx.sent[0].footer == {
            'text': f'Great Day user ID: {USER_A}'}
        assert ctx.send_kwargs[0] == {
            'reference': ctx.message, 'mention_author': False}


class TestMemberStats:
    def test_reports_last_signup_and_days(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(
            GUILD, 'greatday_signup_history_audit', 'clean:123')
        db.greatday_record_signup_event(GUILD, USER_A, 'signup', 100.0, 1)
        db.greatday_signup(GUILD, USER_A)
        db.greatday_record_picks(GUILD, [USER_A], 10, 150.0)
        db.greatday_record_picks(GUILD, [USER_B], 11, 250.0)
        text = _make_cog()._member_stats(_Guild(GUILD), _Author(USER_A, 'a'))
        assert 'great-day\'d **1** time(s)' in text
        assert 'Last signup: <t:100:F>' in text
        assert 'Days signed up: **2**' in text
        assert 'at least' not in text
        assert 'inferred from audited message history' in text

    def test_marks_unrecorded_signup(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.greatday_signup(GUILD, USER_A)
        db.greatday_record_picks(GUILD, [USER_A], 10, 150.0)
        text = _make_cog()._member_stats(_Guild(GUILD), _Author(USER_A, 'a'))
        assert 'Last signup: not recorded' in text
        assert 'Days signed up: **0** (at least' in text

    def test_warned_backfill_is_labelled_inferred_not_a_lower_bound(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(
            GUILD, 'greatday_signup_history_audit', 'incomplete:123')
        db.greatday_record_signup_event(GUILD, USER_A, 'signup', 100.0, 1)
        db.greatday_signup(GUILD, USER_A)
        db.greatday_record_picks(GUILD, [USER_A], 10, 150.0)
        text = _make_cog()._member_stats(_Guild(GUILD), _Author(USER_A, 'a'))
        assert 'inferred — backfill audit found warnings' in text
        assert 'at least' not in text

    def test_marks_absent_member(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        text = _make_cog()._member_stats(_Guild(GUILD), _Author(USER_A, 'a'))
        assert 'not recorded (not signed up)' in text
        assert 'Days signed up: **0**' in text

    def test_stats_do_not_collapse_evidence_of_a_missing_signout(
            self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        db.set_guild_config(
            GUILD, 'greatday_signup_history_audit', 'clean:123')
        db.greatday_record_signup_event(GUILD, USER_A, 'signup', 100.0, 1)
        db.greatday_record_signup_event(GUILD, USER_A, 'signup', 200.0, 2)
        db.greatday_signup(GUILD, USER_A)
        db.greatday_record_picks(GUILD, [USER_A], 10, 150.0)
        db.greatday_record_picks(GUILD, [USER_A], 11, 250.0)
        text = _make_cog()._member_stats(_Guild(GUILD), _Author(USER_A, 'a'))
        assert 'Days signed up: **1** (at least' in text
