"""Complaint context — the messages preceding a report are stored with it."""
import asyncio
import datetime
import json
from types import SimpleNamespace

from tle.cogs._complaint_context import (
    CONTEXT_MESSAGE_COUNT, capture, serialize_messages)
from tle.util.complaints import complaint_json, load_context

from tests.complaint_helpers import ComplaintDb

GUILD = '1'
USER = '100'


def _msg(mid, text='hello', *, author_id='7', name='someone', bot=False,
         at=1_700_000_000.0, embeds=(), attachments=()):
    return SimpleNamespace(
        id=mid, content=text,
        author=SimpleNamespace(id=author_id, display_name=name, bot=bot),
        created_at=datetime.datetime.fromtimestamp(
            at, datetime.timezone.utc),
        embeds=list(embeds), attachments=list(attachments))


class _Channel:
    """Discord hands back history newest-first."""

    def __init__(self, messages, error=None):
        self.messages = messages
        self.error = error
        self.calls = []

    def history(self, *, limit, before):
        self.calls.append((limit, before))
        error = self.error
        newest_first = list(reversed(self.messages))[:limit]

        class _Iter:
            def __aiter__(self):
                return self

            async def __anext__(self):
                if error is not None:
                    raise error
                if not newest_first:
                    raise StopAsyncIteration
                return newest_first.pop(0)

        return _Iter()


class TestSerialize:
    def test_oldest_first(self):
        entries = json.loads(serialize_messages(
            [_msg(1, 'first'), _msg(2, 'second')]))
        assert [e['text'] for e in entries] == ['first', 'second']

    def test_author_and_bot_flag_are_kept(self):
        entries = json.loads(serialize_messages(
            [_msg(1, 'x', author_id='42', name='dragos'),
             _msg(2, 'y', author_id='9', name='TLE', bot=True)]))
        assert entries[0]['author_id'] == '42'
        assert entries[0]['author'] == 'dragos'
        assert entries[0]['bot'] is False
        assert entries[1]['bot'] is True

    def test_bot_embeds_are_captured(self):
        """The bot answers in embeds, so content is empty for its own output.

        A transcript built from content alone would drop exactly the messages
        a complaint about the bot is usually about.
        """
        embed = SimpleNamespace(
            title='Rating', description='Your rating is 2066',
            fields=[], footer=None, author=None, url=None)
        entries = json.loads(serialize_messages(
            [_msg(1, '', bot=True, embeds=[embed])]))
        assert 'Your rating is 2066' in entries[0]['text']

    def test_attachment_names_are_noted(self):
        entries = json.loads(serialize_messages(
            [_msg(1, 'look', attachments=[SimpleNamespace(
                filename='broken-graph.png')])]))
        assert 'broken-graph.png' in entries[0]['text']

    def test_empty_messages_are_skipped(self):
        assert serialize_messages([_msg(1, '   ')]) is None

    def test_nothing_to_store_is_none_not_empty_list(self):
        assert serialize_messages([]) is None

    def test_long_message_is_truncated(self):
        entries = json.loads(serialize_messages([_msg(1, 'x' * 900)]))
        assert len(entries[0]['text']) <= 400
        assert entries[0]['text'].endswith('…')

    def test_oversized_transcript_drops_the_oldest(self):
        entries = json.loads(serialize_messages(
            [_msg(i, f'{i} ' + 'x' * 399) for i in range(40)]))
        assert len(entries) < 40
        # The messages nearest the complaint are the ones that explain it.
        assert entries[-1]['text'].startswith('39')

    def test_a_single_huge_message_is_still_kept(self):
        entries = json.loads(serialize_messages([_msg(1, 'x' * 900)]))
        assert len(entries) == 1


class TestCapture:
    def test_reverses_discord_order(self):
        channel = _Channel([_msg(1, 'first'), _msg(2, 'second')])
        raw = asyncio.run(capture(channel, before_message=_msg(3, 'complaint')))
        assert [e['text'] for e in json.loads(raw)] == ['first', 'second']

    def test_asks_for_five_messages_before_the_complaint(self):
        channel = _Channel([_msg(i) for i in range(10)])
        invoking = _msg(99)
        asyncio.run(capture(channel, before_message=invoking))
        assert channel.calls == [(CONTEXT_MESSAGE_COUNT, invoking)]

    def test_unreadable_history_does_not_break_filing(self):
        channel = _Channel([_msg(1)], error=RuntimeError('Missing Access'))
        assert asyncio.run(capture(channel, before_message=_msg(2))) is None

    def test_empty_channel_stores_nothing(self):
        assert asyncio.run(capture(_Channel([]), before_message=_msg(1))) is None


class TestLoadContext:
    def test_round_trip(self):
        raw = serialize_messages([_msg(1, 'hi')])
        assert load_context(raw)[0]['text'] == 'hi'

    def test_missing_context_is_empty(self):
        assert load_context(None) == []
        assert load_context('') == []

    def test_malformed_json_is_empty(self):
        assert load_context('{not json') == []

    def test_non_list_payload_is_empty(self):
        assert load_context('{"a": 1}') == []


class TestPersistence:
    def _db(self):
        return ComplaintDb()

    def test_context_survives_a_round_trip(self):
        db = self._db()
        raw = serialize_messages([_msg(1, 'the message before')])
        cid = db.add_complaint(GUILD, USER, 'fix graphs', None, raw)
        assert load_context(db.get_complaint(cid).context)[0]['text'] == (
            'the message before')

    def test_complaints_without_context_stay_null(self):
        db = self._db()
        cid = db.add_complaint(GUILD, USER, 'fix graphs')
        assert db.get_complaint(cid).context is None

    def test_detail_payload_carries_the_transcript(self):
        db = self._db()
        raw = serialize_messages([_msg(1, 'the message before')])
        cid = db.add_complaint(GUILD, USER, 'fix graphs', None, raw)
        payload = complaint_json(db.get_complaint(cid), include_context=True)
        assert payload['context'][0]['text'] == 'the message before'

    def test_listings_leave_the_transcript_out(self):
        """A 100-complaint listing would otherwise carry a hundred transcripts."""
        db = self._db()
        raw = serialize_messages([_msg(1, 'the message before')])
        db.add_complaint(GUILD, USER, 'fix graphs', None, raw)
        rows = db.get_complaints(GUILD, 'all')
        assert 'context' not in complaint_json(rows[0])


class TestMigration:
    def test_existing_database_gains_the_column(self):
        """upgrade_complaint_schema only runs for fresh DBs and at 1.59.0, so
        1.60.0 re-runs it; existing complaints must survive."""
        import sqlite3
        from tle.util.db.complaint_workflow_db import upgrade_complaint_schema

        conn = sqlite3.connect(':memory:')
        conn.execute('''CREATE TABLE complaint (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT NOT NULL,
            user_id TEXT NOT NULL, text TEXT NOT NULL, created_at REAL NOT NULL,
            message_link TEXT)''')
        conn.execute(
            'INSERT INTO complaint (guild_id, user_id, text, created_at) '
            'VALUES (?, ?, ?, ?)', (GUILD, USER, 'old complaint', 1.0))
        conn.commit()

        upgrade_complaint_schema(conn)

        columns = {row[1] for row in conn.execute('PRAGMA table_info(complaint)')}
        assert 'context' in columns
        row = conn.execute(
            'SELECT text, context FROM complaint').fetchone()
        assert row == ('old complaint', None)

    def test_rerunning_the_upgrade_is_harmless(self):
        import sqlite3
        from tle.util.db.complaint_workflow_db import upgrade_complaint_schema

        conn = sqlite3.connect(':memory:')
        upgrade_complaint_schema(conn)
        upgrade_complaint_schema(conn)
        columns = [row[1] for row in conn.execute('PRAGMA table_info(complaint)')]
        assert columns.count('context') == 1
