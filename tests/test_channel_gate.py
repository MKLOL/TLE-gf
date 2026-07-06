"""Tests for the per-channel command gate (``;disallow`` / ``;allow``).

Covers the DB layer (``command_gate`` table), the pure ``gate_decision`` rule,
the migration registration, and the cog's global check that blocks commands and
posts the short notice.
"""
import asyncio
from types import SimpleNamespace

import pytest

from tests.betting_test_utils import db  # noqa: F401 (UserDbConn(':memory:') fixture)

from tle.util import discord_common
from tle.cogs._channel_gate_helpers import gate_decision


# ── DB layer ────────────────────────────────────────────────────────────────
class TestCommandGateDb:
    def test_set_and_get_plain(self, db):
        db.set_command_gate('g1', 'c1')
        row = db.get_command_gate('g1', 'c1')
        assert row.channel_id == 'c1'
        assert row.thread_id is None

    def test_set_with_thread(self, db):
        db.set_command_gate('g1', 'c1', 'tA')
        assert db.get_command_gate('g1', 'c1').thread_id == 'tA'

    def test_ids_coerced_to_text(self, db):
        db.set_command_gate(111, 222, 333)
        row = db.get_command_gate(111, 222)
        assert (row.guild_id, row.channel_id, row.thread_id) == ('111', '222', '333')

    def test_reissue_updates_thread_exception(self, db):
        db.set_command_gate('g1', 'c1', 'tA')
        db.set_command_gate('g1', 'c1')  # downgrade thread-mode -> plain
        assert db.get_command_gate('g1', 'c1').thread_id is None

    def test_get_missing_returns_none(self, db):
        assert db.get_command_gate('g1', 'nope') is None

    def test_clear_returns_true_when_removed(self, db):
        db.set_command_gate('g1', 'c1')
        assert db.clear_command_gate('g1', 'c1') is True
        assert db.get_command_gate('g1', 'c1') is None

    def test_clear_returns_false_when_absent(self, db):
        assert db.clear_command_gate('g1', 'nope') is False

    def test_scoped_per_channel_and_guild(self, db):
        db.set_command_gate('g1', 'c1')
        assert db.get_command_gate('g1', 'c2') is None
        assert db.get_command_gate('g2', 'c1') is None


# ── migration ───────────────────────────────────────────────────────────────
def test_migration_registered():
    from tle.util.db.user_db_upgrades import registry
    versions = [v for v, _, _ in registry.upgrades]
    assert '1.40.0' in versions


def test_fresh_db_has_command_gate_table(db):
    # Query must not raise — the table exists on a freshly created DB.
    assert db.get_command_gate('1', '2') is None


def test_upgrade_1_40_0_creates_table_on_existing_db():
    import sqlite3
    from tle.util.db.user_db_conn import namedtuple_factory
    from tle.util.db.user_db_upgrades import upgrade_1_40_0
    conn = sqlite3.connect(':memory:')
    conn.row_factory = namedtuple_factory
    upgrade_1_40_0(conn)
    conn.execute(
        'INSERT INTO command_gate (guild_id, channel_id, thread_id) '
        'VALUES (?, ?, ?)', ('1', '2', '3'))
    row = conn.execute(
        'SELECT guild_id, channel_id, thread_id FROM command_gate').fetchone()
    assert (row.guild_id, row.channel_id, row.thread_id) == ('1', '2', '3')
    conn.close()


# ── pure decision rule ──────────────────────────────────────────────────────
def _gate(thread_id):
    return SimpleNamespace(thread_id=thread_id)


class TestGateDecision:
    def test_no_gate_allows(self):
        assert gate_decision(None, None) == (True, None)
        assert gate_decision(None, '555') == (True, None)

    def test_plain_gate_blocks_channel_no_link(self):
        assert gate_decision(_gate(None), None) == (False, None)

    def test_plain_gate_blocks_threads_too(self):
        assert gate_decision(_gate(None), '777') == (False, None)

    def test_thread_mode_blocks_main_channel_with_link(self):
        assert gate_decision(_gate('777'), None) == (False, '777')

    def test_thread_mode_allows_the_created_thread(self):
        assert gate_decision(_gate('777'), '777') == (True, '777')

    def test_thread_mode_allows_any_other_thread(self):
        # The point of ;disallow thread: commands work in *every* thread of the
        # channel, not just the bot-created one. The link still points at the
        # created thread. The current-thread id type doesn't matter.
        assert gate_decision(_gate('777'), '888') == (True, '777')
        assert gate_decision(_gate('777'), 999) == (True, '777')


# ── cog global check ────────────────────────────────────────────────────────
class TestGateCheck:
    @pytest.fixture
    def cog(self, db, monkeypatch):
        import discord
        from tle.util import codeforces_common as cf_common
        monkeypatch.setattr(cf_common, 'user_db', db)
        # The test discord stub lacks these — add just what the check touches.
        monkeypatch.setattr(discord, 'Thread', type('Thread', (), {}),
                            raising=False)
        monkeypatch.setattr(discord, 'AllowedMentions', lambda **kw: kw,
                            raising=False)
        from tle.cogs.channel_gate import ChannelGate
        fake_bot = SimpleNamespace(add_check=lambda c: None,
                                   remove_check=lambda c: None)
        return ChannelGate(fake_bot)

    def _ctx(self, *, command_name, channel, guild_id='g1'):
        sent = []

        async def send(text, **kw):
            sent.append((text, kw))

        cmd = (SimpleNamespace(name=command_name, root_parent=None)
               if command_name else None)
        return SimpleNamespace(
            guild=SimpleNamespace(id=guild_id), command=cmd, channel=channel,
            author=SimpleNamespace(mention='@u'), send=send, sent=sent)

    def _thread(self, *, parent_id, thread_id):
        import discord
        t = discord.Thread()
        t.parent_id, t.id = parent_id, thread_id
        return t

    def test_dm_is_allowed(self, cog):
        ctx = self._ctx(command_name='gitgud', channel=SimpleNamespace(id='c1'))
        ctx.guild = None
        assert asyncio.run(cog._gate_check(ctx)) is True

    def test_exempt_commands_bypass_gate(self, db, cog):
        db.set_command_gate('g1', 'c1')
        for name in ('disallow', 'allow'):
            ctx = self._ctx(command_name=name, channel=SimpleNamespace(id='c1'))
            assert asyncio.run(cog._gate_check(ctx)) is True
            assert ctx.sent == []

    def test_rpoll_bypasses_gate(self, db, cog):
        # ;rpoll is exempt: it runs even in a channel where commands are blocked.
        db.set_command_gate('g1', 'c1')
        ctx = self._ctx(command_name='rpoll', channel=SimpleNamespace(id='c1'))
        assert asyncio.run(cog._gate_check(ctx)) is True
        assert ctx.sent == []

    def test_rpoll_subcommand_bypasses_gate(self, db, cog):
        # Subcommands (e.g. ;rpoll list) share the group's exemption via
        # root_parent, so they bypass the gate too.
        db.set_command_gate('g1', 'c1')
        ctx = self._ctx(command_name='list', channel=SimpleNamespace(id='c1'))
        ctx.command.root_parent = SimpleNamespace(name='rpoll')
        assert asyncio.run(cog._gate_check(ctx)) is True
        assert ctx.sent == []

    def test_ungated_channel_allowed(self, cog):
        ctx = self._ctx(command_name='gitgud', channel=SimpleNamespace(id='c1'))
        assert asyncio.run(cog._gate_check(ctx)) is True

    def test_plain_gate_blocks_with_no_link(self, db, cog):
        db.set_command_gate('g1', 'c1')
        ctx = self._ctx(command_name='gitgud', channel=SimpleNamespace(id='c1'))
        with pytest.raises(discord_common.FeatureDisabledSilent):
            asyncio.run(cog._gate_check(ctx))
        text = ctx.sent[0][0]
        assert 'disabled in this channel' in text
        assert '<#' not in text

    def test_thread_gate_blocks_parent_with_link(self, db, cog):
        db.set_command_gate('g1', 'c1', 't9')
        ctx = self._ctx(command_name='gitgud', channel=SimpleNamespace(id='c1'))
        with pytest.raises(discord_common.FeatureDisabledSilent):
            asyncio.run(cog._gate_check(ctx))
        assert '<#t9>' in ctx.sent[0][0]
        assert ctx.sent[0][1]['delete_after'] == 15

    def test_created_thread_allowed(self, db, cog):
        db.set_command_gate('g1', 'c1', 't9')
        ctx = self._ctx(command_name='gitgud',
                        channel=self._thread(parent_id='c1', thread_id='t9'))
        assert asyncio.run(cog._gate_check(ctx)) is True
        assert ctx.sent == []

    def test_any_other_thread_also_allowed(self, db, cog):
        # ;disallow thread allows commands in every thread of the channel,
        # not only the bot-created one.
        db.set_command_gate('g1', 'c1', 't9')
        ctx = self._ctx(command_name='gitgud',
                        channel=self._thread(parent_id='c1', thread_id='tX'))
        assert asyncio.run(cog._gate_check(ctx)) is True
        assert ctx.sent == []

    def test_plain_gate_still_blocks_threads(self, db, cog):
        # Plain ;disallow (no thread) gates the channel *and* its threads.
        db.set_command_gate('g1', 'c1')
        ctx = self._ctx(command_name='gitgud',
                        channel=self._thread(parent_id='c1', thread_id='tX'))
        with pytest.raises(discord_common.FeatureDisabledSilent):
            asyncio.run(cog._gate_check(ctx))
        assert ctx.sent[0][0].endswith('disabled in this channel.')

    def test_disallow_thread_rejects_non_text_channel(self, cog):
        # A forum/voice/category channel is not a TextChannel: reject cleanly
        # instead of letting create_thread raise an uncaught TypeError.
        from tle.cogs._channel_gate_helpers import ChannelGateError
        forum = SimpleNamespace(id='f1')
        ctx = self._ctx(command_name='disallow', channel=forum)
        with pytest.raises(ChannelGateError):
            asyncio.run(cog.disallow(ctx, 'thread'))

    def test_disallow_thread_rejected_inside_a_thread(self, cog):
        from tle.cogs._channel_gate_helpers import ChannelGateError
        ctx = self._ctx(command_name='disallow',
                        channel=self._thread(parent_id='c1', thread_id='t1'))
        with pytest.raises(ChannelGateError):
            asyncio.run(cog.disallow(ctx, 'thread'))
