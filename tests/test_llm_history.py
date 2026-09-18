"""Tests for channel-history collection and transcript rendering."""
from datetime import timedelta

from tle.cogs import _llm_history as llm_history
from tle.cogs import _llm_transcript as llm_transcript
from tests.llm_test_utils import (
    FakeAttachment,
    FakeHistoryChannel,
    HistMessage,
    run,
)


class TestCollectRecent:
    def test_returns_messages_oldest_first(self):
        channel = FakeHistoryChannel([
            HistMessage(content='first', offset=0),
            HistMessage(content='second', offset=10),
        ])
        anchor = HistMessage(content=';llm what?', offset=20)
        got = run(llm_history.collect_recent(channel, before=anchor))
        assert [m.content for m in got] == ['first', 'second']

    def test_the_bot_is_excluded(self):
        channel = FakeHistoryChannel([
            HistMessage(content='human', offset=0),
            HistMessage(content='bot answer', offset=5, author_id=99),
        ])
        anchor = HistMessage(offset=20)
        got = run(llm_history.collect_recent(channel, before=anchor,
                                             bot_user_id=99))
        assert [m.content for m in got] == ['human']

    def test_other_bots_are_excluded(self):
        channel = FakeHistoryChannel([
            HistMessage(content='human', offset=0),
            HistMessage(content='beep', offset=5, is_bot=True),
        ])
        got = run(llm_history.collect_recent(
            channel, before=HistMessage(offset=20)))
        assert [m.content for m in got] == ['human']

    def test_empty_messages_are_skipped(self):
        channel = FakeHistoryChannel([
            HistMessage(content='', offset=0),
            HistMessage(content='real', offset=5),
        ])
        got = run(llm_history.collect_recent(
            channel, before=HistMessage(offset=20)))
        assert [m.content for m in got] == ['real']

    def test_an_image_only_message_is_kept(self):
        channel = FakeHistoryChannel([
            HistMessage(content='', offset=0, attachments=[FakeAttachment()]),
        ])
        got = run(llm_history.collect_recent(
            channel, before=HistMessage(offset=20)))
        assert len(got) == 1

    def test_the_limit_takes_the_newest_messages_not_the_oldest(self):
        # The regression: with `after` set, discord.py defaults to
        # oldest_first=True, so a limit would take the *start* of the window
        # and walk forward — dropping everything nearest the command.
        channel = FakeHistoryChannel([
            HistMessage(content=f'msg{i}', offset=i * 10,
                        author_id=i) for i in range(10)])
        anchor = HistMessage(offset=500)
        got = run(llm_history.collect_recent(channel, before=anchor, limit=3,
                                             window_seconds=6000))
        assert [m.content for m in got] == ['msg7', 'msg8', 'msg9']

    def test_the_limit_counts_speaker_turns_not_messages(self):
        channel = FakeHistoryChannel([
            HistMessage(author='alice', author_id=1,
                        content='alice one', offset=0),
            HistMessage(author='alice', author_id=1,
                        content='alice two', offset=1),
            HistMessage(author='bob', author_id=2,
                        content='bob one', offset=2),
            HistMessage(author='bob', author_id=2,
                        content='bob two', offset=3),
            HistMessage(author='carol', author_id=3,
                        content='carol one', offset=4),
            HistMessage(author='carol', author_id=3,
                        content='carol two', offset=5),
        ])
        got = run(llm_history.collect_recent(
            channel, before=HistMessage(offset=10), limit=2,
            window_seconds=600))
        assert [message.content for message in got] == [
            'bob one', 'bob two', 'carol one', 'carol two']

    def test_one_speaker_turn_can_include_many_messages(self):
        channel = FakeHistoryChannel([
            HistMessage(author='alice', author_id=1,
                        content=f'part {index}', offset=index)
            for index in range(20)
        ])
        got = run(llm_history.collect_recent(
            channel, before=HistMessage(offset=30), limit=1,
            window_seconds=600))
        assert [message.content for message in got] == [
            f'part {index}' for index in range(20)]

    def test_same_author_after_another_speaker_is_a_new_turn(self):
        channel = FakeHistoryChannel([
            HistMessage(author='alice', author_id=1,
                        content='old alice', offset=0),
            HistMessage(author='bob', author_id=2,
                        content='bob', offset=1),
            HistMessage(author='alice', author_id=1,
                        content='new alice', offset=2),
        ])
        got = run(llm_history.collect_recent(
            channel, before=HistMessage(offset=10), limit=2,
            window_seconds=600))
        assert [message.content for message in got] == [
            'bob', 'new alice']

    def test_the_transcript_is_oldest_first_as_the_prompt_claims(self):
        channel = FakeHistoryChannel([
            HistMessage(content='older', offset=0),
            HistMessage(content='newer', offset=10),
        ])
        got = run(llm_history.collect_recent(
            channel, before=HistMessage(offset=50)))
        assert [m.content for m in got] == ['older', 'newer']

    def test_a_time_window_is_requested(self):
        channel = FakeHistoryChannel([HistMessage(offset=0)])
        anchor = HistMessage(offset=100)
        run(llm_history.collect_recent(channel, before=anchor,
                                       window_seconds=600))
        assert channel.calls[0]['after'] == anchor.created_at - timedelta(seconds=600)

    def test_recent_context_stops_at_an_inactivity_gap(self):
        channel = FakeHistoryChannel([
            HistMessage(content='stale topic', offset=0),
            HistMessage(content='active one', offset=700),
            HistMessage(content='active two', offset=800),
        ])
        anchor = HistMessage(content=';llm summarize this', offset=900)
        got = run(llm_history.collect_recent(
            channel, before=anchor, window_seconds=3600,
            gap_seconds=600))
        assert [m.content for m in got] == [
            'active one', 'active two']

    def test_command_gap_does_not_discard_the_latest_session(self):
        channel = FakeHistoryChannel([
            HistMessage(content='session one', offset=0),
            HistMessage(content='session two', offset=300),
        ])
        # The command arrives 50 minutes after the latest conversation message.
        anchor = HistMessage(
            content='@grok summarize this',
            offset=3300,
        )
        got = run(llm_history.collect_recent(
            channel,
            before=anchor,
            window_seconds=3600,
            gap_seconds=600,
        ))
        assert [message.content for message in got] == [
            'session one',
            'session two',
        ]

    def test_active_session_can_span_more_than_ten_minutes(self):
        channel = FakeHistoryChannel([
            HistMessage(content='part one', offset=0),
            HistMessage(content='part two', offset=400),
            HistMessage(content='part three', offset=800),
            HistMessage(content='part four', offset=1200),
        ])
        anchor = HistMessage(content=';llm summarize this', offset=1250)
        got = run(llm_history.collect_recent(
            channel, before=anchor, window_seconds=3600,
            gap_seconds=600))
        assert [m.content for m in got] == [
            'part one', 'part two', 'part three', 'part four']

    def test_unreadable_history_returns_empty_not_an_error(self):
        channel = FakeHistoryChannel([], fail=True)
        assert run(llm_history.collect_recent(
            channel, before=HistMessage())) == []


class TestCollectReplyWindow:
    def _channel(self):
        return FakeHistoryChannel([
            HistMessage(content='before-2', offset=0),
            HistMessage(content='before-1', offset=10),
            HistMessage(content='target', offset=20),
            HistMessage(content='after-1', offset=30),
        ])

    def test_window_surrounds_the_target_in_order(self):
        channel = self._channel()
        target = channel.messages[2]
        got = run(llm_history.collect_reply_window(channel, target))
        assert [m.content for m in got] == ['before-2', 'before-1', 'target',
                                            'after-1']

    def test_the_before_half_takes_the_nearest_messages(self):
        channel = FakeHistoryChannel(
            [HistMessage(content=f'msg{i}', offset=i * 10) for i in range(10)])
        target = channel.messages[9]
        got = run(llm_history.collect_reply_window(
            channel, target, before_count=2, after_count=0,
            window_seconds=6000))
        # The two immediately preceding it, oldest-first, then the target.
        assert [m.content for m in got] == ['msg7', 'msg8', 'msg9']

    def test_no_target_yields_nothing(self):
        assert run(llm_history.collect_reply_window(self._channel(), None)) == []

    def test_unreadable_history_still_returns_the_target(self):
        channel = FakeHistoryChannel([], fail=True)
        target = HistMessage(content='target')
        got = run(llm_history.collect_reply_window(channel, target))
        assert [m.content for m in got] == ['target']


class TestFormatTranscript:
    def test_history_module_keeps_the_transcript_facade(self):
        assert llm_history.format_transcript is llm_transcript.format_transcript
        assert llm_history.redact_secrets is llm_transcript.redact_secrets

    def test_renders_author_and_text(self):
        text = llm_history.format_transcript([
            HistMessage(author='nife', content='use a BIT'),
        ])
        assert text == 'nife: use a BIT'

    def test_attachment_filenames_are_noted_not_contents(self):
        attachment = FakeAttachment()
        attachment.filename = 'wa.png'
        text = llm_history.format_transcript([
            HistMessage(author='miguel', content='look', attachments=[attachment]),
        ])
        assert 'wa.png' in text

    def test_the_focused_message_is_marked(self):
        focus = HistMessage(author='nife', content='this one')
        text = llm_history.format_transcript(
            [HistMessage(content='other'), focus], focus=focus)
        assert 'being asked about' in text
        assert text.count('being asked about') == 1

    def test_long_messages_are_clipped(self):
        text = llm_history.format_transcript([HistMessage(content='x' * 5000)])
        assert len(text) < 1000

    def test_the_whole_transcript_is_bounded(self):
        many = [HistMessage(content='y' * 500) for _ in range(200)]
        text = llm_history.format_transcript(many)
        assert len(text) <= llm_history._MAX_TRANSCRIPT_CHARS + 200
        assert 'omitted' in text

    def test_empty_input(self):
        assert llm_history.format_transcript([]) == ''
