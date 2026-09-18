"""Tests for chronological Great Day membership-history recovery."""
import asyncio
from types import SimpleNamespace

import pytest

from tle.cogs import _greatday_event_backfill as backfill
from tle.cogs import _greatday_events as events
from tle.util import codeforces_common as cf_common

from tests.greatday_test_utils import (  # noqa: F401
    GUILD, USER_A, USER_B, DiscordAuthor, DiscordEmbed, DiscordGuild,
    DiscordMessage, HistoryChannel, bot_result, db,
)


BOT_ID = 7
SIGNUP_OK = 'You have been signed up for great day pings!'
REMOVE_OK = 'You have been removed from great day pings.'


def command(content, author_id, message_id, at):
    return DiscordMessage(
        content, DiscordAuthor(author_id), message_id, at)


def scan(messages, **kwargs):
    return asyncio.run(backfill.scan_signup_events_audited(
        HistoryChannel(messages), GUILD, BOT_ID, **kwargs))


class TestCommandParsing:
    @pytest.mark.parametrize('content,kind,target', [
        (';greatday signup', 'signup', USER_A),
        (';greatday  remove', 'remove', USER_A),
        ('<@7> greatday signup', 'signup', USER_A),
        ('<@!7> greatday add <@200>', 'add', USER_B),
        (';greatday kick <@!200>', 'kick', USER_B),
        (';greatday ban <@200>', 'ban', USER_B),
        (';greatday unban dontdefense', 'unban', None),
    ])
    def test_parses_real_membership_commands(self, content, kind, target):
        parsed = backfill.parse_membership_command(
            command(content, USER_A, 1, 10), BOT_ID)
        assert parsed.kind == kind
        assert parsed.target_id == target

    @pytest.mark.parametrize('content', [
        'greatday signup', '; greatday signup', ';Greatday signup',
        ';greatday SIGNUP', '<@8> greatday signup',
        '<@7>greatday signup', '<@7>  greatday signup',
        '  ;greatday signup', ';greatday stats',
    ])
    def test_rejects_text_that_the_bot_would_not_execute(self, content):
        assert backfill.parse_membership_command(
            command(content, USER_A, 1, 10), BOT_ID) is None

    def test_compatibility_parser_requires_the_exact_bot_mention(self):
        msg = command('<@7> greatday signup', USER_A, 1, 10)
        assert events.parse_signup_command(msg, BOT_ID) == ('signup', USER_A)
        assert events.parse_signup_command(msg, 8) is None


class TestResultParsing:
    @pytest.mark.parametrize('description,kind,success,outcome,name', [
        (SIGNUP_OK, 'signup', True, 'changed', None),
        ('You are already signed up.', 'signup', False,
         'already_signed', None),
        ('`Dragos` has been added to great day pings.', 'add', True,
         'changed', 'Dragos'),
        ('`Dragos` is not signed up.', 'kick', False,
         'not_signed', 'Dragos'),
        ('`Dragos` has been banned from great day.', 'ban', True,
         'changed', 'Dragos'),
        ('`Dragos` has been unbanned from great day.', 'unban', True,
         'changed', 'Dragos'),
    ])
    def test_classifies_exact_bot_results(self, description, kind, success,
                                          outcome, name):
        parsed = backfill.parse_membership_result(
            bot_result(description, 2, 11, target_id=USER_B), BOT_ID)
        assert (parsed.kind, parsed.success, parsed.outcome,
                parsed.display_name) == (kind, success, outcome, name)
        assert parsed.target_id == USER_B

    def test_rejects_success_text_from_a_human(self):
        msg = DiscordMessage(
            '', DiscordAuthor(USER_A), 2, 11, [DiscordEmbed(SIGNUP_OK)])
        assert backfill.parse_membership_result(msg, BOT_ID) is None

    def test_rejects_substring_instead_of_exact_result(self):
        msg = bot_result('FYI: ' + SIGNUP_OK, 2, 11)
        assert backfill.parse_membership_result(msg, BOT_ID) is None


class TestChronologicalMatching:
    def test_recovers_confirmed_signup_and_remove(self):
        result = scan([
            command(';greatday signup', USER_A, 1, 10),
            bot_result(SIGNUP_OK, 2, 11),
            command(';greatday remove', USER_A, 3, 20),
            bot_result(REMOVE_OK, 4, 21),
        ])
        assert result.events == [
            (GUILD, USER_A, 'signup', 10.0, '1'),
            (GUILD, USER_A, 'signout', 20.0, '3'),
        ]
        assert result.audit.matched_successes == 2

    def test_silent_failed_command_does_not_steal_later_success(self):
        result = scan([
            command(';greatday signup', USER_A, 1, 10),
            command(';greatday signup', USER_B, 2, 12),
            bot_result(SIGNUP_OK, 3, 13),
        ])
        assert result.events == [
            (GUILD, USER_B, 'signup', 12.0, '2')]
        assert result.audit.ambiguous_matches == 1
        assert result.audit.commands_without_result == 1

    def test_explicit_rejection_consumes_its_command(self):
        result = scan([
            command(';greatday signup', USER_A, 1, 10),
            bot_result('You are already signed up.', 2, 11),
        ])
        assert result.events == []
        assert result.audit.matched_failures == 1
        assert result.audit.commands_without_result == 0

    def test_result_outside_time_window_is_not_matched(self):
        result = scan([
            command(';greatday signup', USER_A, 1, 10),
            bot_result(SIGNUP_OK, 2, 26),
        ])
        assert result.events == []
        assert result.audit.commands_without_result == 1
        assert result.audit.unmatched_results == 1

    def test_exact_reply_reference_beats_nearest_command(self):
        result = scan([
            command(';greatday signup', USER_A, 1, 10),
            command(';greatday signup', USER_B, 2, 11),
            bot_result(SIGNUP_OK, 3, 12, reference_id=1),
        ])
        assert result.events == [
            (GUILD, USER_A, 'signup', 10.0, '1')]
        assert result.audit.ambiguous_matches == 0

    def test_matches_command_kind_not_shared_action_fragment(self):
        result = scan([
            command(';greatday signup', USER_A, 1, 10),
            bot_result('`Dragos` has been added to great day pings.', 2, 11,
                       target_id=USER_B),
        ])
        assert result.events == []
        assert result.audit.unmatched_results == 1

    def test_progress_reports_scanned_and_success_counts(self):
        seen = []

        async def progress(scanned, matched):
            seen.append((scanned, matched))

        messages = [command('hello', USER_A, number, number)
                    for number in range(1, 5)]
        asyncio.run(backfill.scan_signup_events_audited(
            HistoryChannel(messages), GUILD, BOT_ID, progress=progress,
            progress_interval=2))
        assert seen == [(2, 0), (4, 0)]


class TestStateReplay:
    def test_rejection_can_establish_active_state_for_later_ban(self):
        result = scan([
            command(';greatday signup', USER_A, 1, 10),
            bot_result('You are already signed up.', 2, 11),
            command(';greatday ban <@100>', USER_B, 3, 20),
            bot_result('`someone` has been banned from great day.', 4, 21),
        ])
        assert result.events == [
            (GUILD, USER_A, 'signout', 20.0, '3')]
        assert result.audit.state_conflicts == 1

    def test_active_user_ban_creates_signout(self):
        result = scan([
            command(';greatday add <@200>', USER_A, 1, 10),
            bot_result('`Dragos` has been added to great day pings.', 2, 11),
            command(';greatday ban <@200>', USER_A, 3, 20),
            bot_result('`Dragos` has been banned from great day.', 4, 21),
        ])
        assert result.events == [
            (GUILD, USER_B, 'signup', 10.0, '1'),
            (GUILD, USER_B, 'signout', 20.0, '3'),
        ]
        assert result.audit.unknown_ban_states == 0

    def test_complete_ban_and_unban_sequence_has_no_state_conflict(self):
        result = scan([
            command(';greatday add <@200>', USER_A, 1, 10),
            bot_result('`Dragos` has been added to great day pings.', 2, 11),
            command(';greatday ban <@200>', USER_A, 3, 20),
            bot_result('`Dragos` has been banned from great day.', 4, 21),
            command(';greatday unban <@200>', USER_A, 5, 30),
            bot_result('`Dragos` has been unbanned from great day.', 6, 31),
        ], current_signup_ids=set(), current_ban_ids=set())
        assert result.audit.state_conflicts == 0
        assert result.audit.trustworthy

    def test_ban_without_known_state_does_not_invent_signout(self):
        result = scan([
            command(';greatday ban <@200>', USER_A, 1, 10),
            bot_result('`Dragos` has been banned from great day.', 2, 11),
        ])
        assert result.events == []
        assert result.audit.unknown_ban_states == 1

    def test_plain_username_and_display_name_resolve_same_member(self):
        target = DiscordAuthor(
            USER_B, 'Dragos', username='dontdefense', nick='Dragos')
        result = scan([
            command(';greatday add dontdefense', USER_A, 1, 10),
            bot_result('`Dragos` has been added to great day pings.', 2, 11),
            command(';greatday ban dontdefense', USER_A, 3, 20),
            bot_result('`Dragos` has been banned from great day.', 4, 21),
        ], guild=DiscordGuild(GUILD, [target]))
        assert [event[2] for event in result.events] == ['signup', 'signout']
        assert all(event[1] == USER_B for event in result.events)
        assert result.audit.inferred_targets == 2

    def test_unresolvable_plain_name_is_skipped(self):
        result = scan([
            command(';greatday add former_user', USER_A, 1, 10),
            bot_result('`Old Nick` has been added to great day pings.', 2, 11),
        ], guild=DiscordGuild(GUILD))
        assert result.events == []
        assert result.audit.unresolved_targets == 1

    def test_final_state_is_compared_with_current_tables(self):
        result = scan([
            command(';greatday signup', USER_A, 1, 10),
            bot_result(SIGNUP_OK, 2, 11),
        ], current_signup_ids={USER_B}, current_ban_ids=set())
        assert result.audit.membership_mismatches == 2
        assert not result.audit.trustworthy

    def test_clean_final_state_is_reported_as_trustworthy(self):
        result = scan([
            command(';greatday signup', USER_A, 1, 10),
            bot_result(SIGNUP_OK, 2, 11),
        ], current_signup_ids={USER_A}, current_ban_ids=set())
        assert result.audit.membership_mismatches == 0
        assert result.audit.trustworthy

    def test_backfill_storage_is_idempotent_and_saves_audit(self, db):
        result = scan([
            command(';greatday signup', USER_A, 1, 10),
            bot_result(SIGNUP_OK, 2, 11),
        ])
        assert db.greatday_record_signup_backfill(
            result.events, GUILD, 'clean:123') == 1
        assert db.greatday_record_signup_backfill(
            result.events, GUILD, 'clean:123') == 0
        assert db.get_guild_config(
            GUILD, 'greatday_signup_history_audit') == 'clean:123'

    def test_warned_audit_cannot_be_overwritten_by_later_clean_scan(self, db):
        result = scan([
            command(';greatday signup', USER_A, 1, 10),
            bot_result(SIGNUP_OK, 2, 11),
        ])
        db.greatday_record_signup_backfill(
            result.events, GUILD, 'incomplete:123')
        db.greatday_record_signup_backfill(result.events, GUILD, 'clean:456')
        assert db.get_guild_config(
            GUILD, 'greatday_signup_history_audit') == 'incomplete:123'

    def test_backfill_storage_rejects_events_for_another_guild(self, db):
        with pytest.raises(ValueError, match='audit guild'):
            db.greatday_record_signup_backfill(
                [('999', USER_A, 'signup', 10.0, '1')],
                GUILD, 'clean:123')

    def test_backfill_command_persists_events_and_clean_audit(
            self, db, monkeypatch):
        from tle.cogs.greatday import GreatDay

        monkeypatch.setattr(cf_common, 'user_db', db)
        db.greatday_signup(GUILD, USER_A)
        channel = HistoryChannel([
            command(';greatday signup', USER_A, 1, 10),
            bot_result(SIGNUP_OK, 2, 11),
        ])
        channel.id = 123
        channel.mention = '#great-day'
        guild = DiscordGuild(GUILD, [DiscordAuthor(USER_A)])
        author = DiscordAuthor('999')
        ctx = SimpleNamespace(guild=guild, author=author, sent=[])

        async def send(content=None, embed=None, **kwargs):
            message = DiscordMessage('', msg_id=999)
            ctx.sent.append(message)
            return message

        ctx.send = send
        cog = GreatDay(SimpleNamespace(user=DiscordAuthor(BOT_ID)))
        asyncio.run(cog.backfill_signups.callback(cog, ctx, channel))
        assert len(db.greatday_get_signup_events(GUILD, USER_A)) == 1
        assert db.get_guild_config(
            GUILD, 'greatday_signup_history_audit') == 'partial:123'
