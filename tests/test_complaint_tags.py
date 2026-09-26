"""Complaint tags — any word, and a tagged complaint leaves the default list."""
import asyncio
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tle.cogs import complain as complain_cog
from tle.cogs.complain import _parse_list_args
from tle.util import codeforces_common as cf_common
from tle.util.complaints import complaint_json
from tle.util.db.complaint_db import normalize_tag

from tests.complaint_helpers import ComplaintDb

GUILD = '1'
USER = '100'


@pytest.fixture
def db():
    return ComplaintDb()


class TestNormalize:
    def test_lowercases_and_trims(self):
        assert normalize_tag('  Games ') == 'games'

    @pytest.mark.parametrize('bad', ['', ' ', 'has space', 'ünï', 'x' * 33,
                                     '-leading', 'semi;colon'])
    def test_rejects_anything_but_a_plain_word(self, bad):
        with pytest.raises(ValueError):
            normalize_tag(bad)

    @pytest.mark.parametrize('word', ['all', 'open', 'resolved', 'untagged'])
    def test_status_words_are_reserved(self, word):
        with pytest.raises(ValueError, match='reserved'):
            normalize_tag(word)


class TestDb:
    def test_add_is_idempotent(self, db):
        cid = db.add_complaint(GUILD, USER, 'x')
        assert db.add_complaint_tag(cid, GUILD, 'games') is True
        assert db.add_complaint_tag(cid, GUILD, 'games') is False
        assert db.get_complaint_tags(cid) == ['games']

    def test_remove(self, db):
        cid = db.add_complaint(GUILD, USER, 'x')
        db.add_complaint_tag(cid, GUILD, 'games')
        assert db.remove_complaint_tag(cid, GUILD, 'games') is True
        assert db.remove_complaint_tag(cid, GUILD, 'games') is False
        assert db.get_complaint_tags(cid) == []

    def test_tags_are_sorted_and_batched(self, db):
        a = db.add_complaint(GUILD, USER, 'a')
        b = db.add_complaint(GUILD, USER, 'b')
        db.add_complaint_tag(a, GUILD, 'ui')
        db.add_complaint_tag(a, GUILD, 'games')
        assert db.get_tags_for_complaints([a, b]) == {a: ['games', 'ui']}
        assert db.get_tags_for_complaints([]) == {}

    def test_counts_ignore_removed_complaints(self, db):
        a = db.add_complaint(GUILD, USER, 'a')
        b = db.add_complaint(GUILD, USER, 'b')
        db.add_complaint_tag(a, GUILD, 'games')
        db.add_complaint_tag(b, GUILD, 'games')
        db.delete_complaint(b)
        assert [(r.tag, r.count) for r in db.get_complaint_tag_counts(GUILD)] == [
            ('games', 1)]


class TestVisibility:
    def _seed(self, db):
        plain = db.add_complaint(GUILD, USER, 'plain')
        tagged = db.add_complaint(GUILD, USER, 'tagged')
        db.add_complaint_tag(tagged, GUILD, 'games')
        return plain, tagged

    def test_default_list_hides_tagged(self, db):
        plain, tagged = self._seed(db)
        assert [r.id for r in db.get_complaints(GUILD)] == [plain]

    def test_include_tagged_shows_everything(self, db):
        plain, tagged = self._seed(db)
        ids = {r.id for r in db.get_complaints(GUILD, include_tagged=True)}
        assert ids == {plain, tagged}

    def test_a_tag_selects_only_its_complaints(self, db):
        plain, tagged = self._seed(db)
        assert [r.id for r in db.get_complaints(GUILD, tag='games')] == [tagged]
        assert db.get_complaints(GUILD, tag='other') == []

    def test_tag_filter_combines_with_status(self, db):
        plain, tagged = self._seed(db)
        db.resolve_complaint(tagged, GUILD, 10, 'fixed',
                             'https://github.com/MKLOL/TLE-gf/commit/' + 'a' * 40)
        assert db.get_complaints(GUILD, 'open', tag='games') == []
        assert [r.id for r in db.get_complaints(GUILD, 'resolved', tag='games')] == [tagged]

    def test_untagging_restores_visibility(self, db):
        plain, tagged = self._seed(db)
        db.remove_complaint_tag(tagged, GUILD, 'games')
        assert {r.id for r in db.get_complaints(GUILD)} == {plain, tagged}

    def test_pagination_cursor_still_applies(self, db):
        ids = [db.add_complaint(GUILD, USER, str(i)) for i in range(5)]
        rows = db.get_complaints(GUILD, limit=2, before=ids[3])
        assert [r.id for r in rows] == [ids[2], ids[1]]


class TestListArgs:
    def test_defaults(self):
        assert _parse_list_args(()) == ('open', None, False)

    def test_all_lifts_the_tag_filter(self):
        assert _parse_list_args(('all',)) == ('all', None, True)

    def test_a_tag_alone_keeps_open_status(self):
        assert _parse_list_args(('games',)) == ('open', 'games', False)

    def test_status_and_tag_in_either_order(self):
        assert _parse_list_args(('all', 'Games')) == ('all', 'games', True)
        assert _parse_list_args(('games', 'resolved')) == ('resolved', 'games', False)

    def test_two_tags_are_an_error(self):
        with pytest.raises(Exception):
            _parse_list_args(('games', 'ui'))

    def test_bad_tag_is_an_error(self):
        with pytest.raises(Exception):
            _parse_list_args(('no way',))


def _ctx(author_id=42):
    return SimpleNamespace(
        guild=SimpleNamespace(id=int(GUILD)),
        author=SimpleNamespace(id=author_id),
        channel=SimpleNamespace(id=2),
        send=AsyncMock())


class TestCommands:
    @pytest.fixture(autouse=True)
    def _wire(self, db, monkeypatch):
        monkeypatch.setattr(cf_common, 'user_db', db)
        monkeypatch.setattr(complain_cog.discord_common, 'embed_success',
                            lambda text: ('ok', text), raising=False)
        monkeypatch.setattr(complain_cog.discord_common, 'embed_alert',
                            lambda text: ('alert', text), raising=False)
        self.pages = []
        monkeypatch.setattr(
            complain_cog.paginator, 'paginate',
            lambda bot, channel, pages, **kw: self.pages.extend(pages))
        self.cog = complain_cog.Complain(bot=None)

    def _tag(self, cid, tag):
        ctx = _ctx()
        asyncio.run(self.cog.tag.callback(self.cog, ctx, cid, tag))
        return ctx.send.call_args.kwargs['embed']

    def test_tag_command_hides_the_complaint(self, db):
        cid = db.add_complaint(GUILD, USER, 'the games thing')
        kind, text = self._tag(cid, 'Games')
        assert kind == 'ok' and '`games`' in text
        asyncio.run(self.cog.list.callback(self.cog, _ctx()))
        assert self.pages == []  # nothing visible -> "No complaints filed."

    def test_list_all_and_list_tag_show_it(self, db):
        cid = db.add_complaint(GUILD, USER, 'the games thing')
        self._tag(cid, 'games')
        asyncio.run(self.cog.list.callback(self.cog, _ctx(), 'all'))
        assert 'the games thing' in self.pages[0][1].description
        assert '`games`' in self.pages[0][1].description
        self.pages.clear()
        asyncio.run(self.cog.list.callback(self.cog, _ctx(), 'games'))
        assert 'the games thing' in self.pages[0][1].description

    def test_tagging_twice_is_reported(self, db):
        cid = db.add_complaint(GUILD, USER, 'x')
        self._tag(cid, 'games')
        kind, text = self._tag(cid, 'games')
        assert 'already' in text

    def test_bad_tag_is_refused(self, db):
        cid = db.add_complaint(GUILD, USER, 'x')
        kind, text = self._tag(cid, 'all')
        assert kind == 'alert'
        assert db.get_complaint_tags(cid) == []

    def test_unknown_or_foreign_complaint_is_refused(self, db):
        kind, text = self._tag(999, 'games')
        assert kind == 'alert'

    def test_untag(self, db):
        cid = db.add_complaint(GUILD, USER, 'x')
        self._tag(cid, 'games')
        ctx = _ctx()
        asyncio.run(self.cog.untag.callback(self.cog, ctx, cid, 'games'))
        assert db.get_complaint_tags(cid) == []
        asyncio.run(self.cog.untag.callback(self.cog, ctx, cid, 'games'))
        assert ctx.send.call_args.kwargs['embed'][0] == 'alert'

    def test_manage_follows_the_same_visibility(self, db):
        cid = db.add_complaint(GUILD, USER, 'x')
        self._tag(cid, 'games')
        ctx = _ctx()
        asyncio.run(self.cog.manage.callback(self.cog, ctx))
        # nothing visible -> neutral "No complaints filed." goes through ctx.send
        assert ctx.send.await_count == 1
        assert 'view' not in ctx.send.call_args.kwargs


class TestPayload:
    def test_json_carries_tags(self, db):
        cid = db.add_complaint(GUILD, USER, 'x')
        db.add_complaint_tag(cid, GUILD, 'games')
        payload = complaint_json(db.get_complaint(cid), tags=db.get_complaint_tags(cid))
        assert payload['tags'] == ['games']
        assert complaint_json(db.get_complaint(cid))['tags'] == []


class TestMigration:
    def test_existing_database_gains_the_table(self):
        from tle.util.db.complaint_workflow_db import upgrade_complaint_schema
        conn = sqlite3.connect(':memory:')
        conn.execute('''CREATE TABLE complaint (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT NOT NULL,
            user_id TEXT NOT NULL, text TEXT NOT NULL, created_at REAL NOT NULL,
            message_link TEXT)''')
        upgrade_complaint_schema(conn)
        upgrade_complaint_schema(conn)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'")}
        assert 'complaint_tag' in tables
