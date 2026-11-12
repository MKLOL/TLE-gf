"""Legacy single-family counts recover without admitting low-count extras."""
import asyncio
import sqlite3
from types import SimpleNamespace

from tle.cogs._migrate_complete import MigrateCompleteMixin
from tle.util.db.user_db_conn import UserDbConn, namedtuple_factory
from tle.util.db.user_db_upgrades import registry, upgrade_1_68_0
from tle.util.starboard_routing import STAR, PILL

GASP, ELEPHANT = '<:chocoGasp:123>', '🐘'


def test_legacy_qualification_recovery_and_restart(tmp_path):
    path = str(tmp_path / 'legacy.db')
    db = UserDbConn(path)
    for main in (STAR, PILL):
        db.add_starboard_emoji(1, main, 4, 123)
        db.set_starboard_channel(1, main, 456)
    # The reported case: a pill-only post has already fallen below four.
    cases = [(100, PILL, {PILL: 3}),
             (101, STAR, {STAR: 2, GASP: 5, ELEPHANT: 2}),
             (102, STAR, {GASP: 4}),
             (103, PILL, {})]
    for message, main, groups in cases:
        db.add_starboard_message_v1(message, message + 1000, 1, main,
                                    author_id=42, channel_id=456)
        for emoji, count in groups.items():
            for user in range(count):
                db.add_reactor(message, emoji, user)
    # Reproduce the old database that did not have qualification history.
    db.conn.execute('DROP TRIGGER starboard_qualified_delete')
    db.conn.execute('DROP TABLE starboard_qualified')
    registry.set_version(db.conn, '1.67.0')
    db.close()

    db = UserDbConn(path)
    expected = {100: {PILL}, 101: {GASP}, 102: {GASP}, 103: {PILL}}
    for message, main, _ in cases:
        assert db.get_starboard_qualified_emojis(message, main) == expected[message]
    assert registry.get_current_version(db.conn) == registry.latest_version
    # A newly posted mixed entry must not inherit the legacy main-family rule.
    db.add_starboard_message_v1(104, 1104, 1, STAR, author_id=42, channel_id=456)
    db.add_reactor(104, STAR, 1)
    db.update_starboard_star_count(104, STAR, 1)
    db.close()
    db = UserDbConn(path)
    assert db.get_starboard_qualified_emojis(104, STAR) == set()
    assert db.get_starboard_qualified_emojis(100, PILL) == {PILL}
    db.remove_starboard_message(starboard_msg_id=1100)
    assert db.get_starboard_qualified_emojis(100, PILL) == set()
    db.close()


def test_upgrade_on_partial_schema_and_repeated_calls():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = namedtuple_factory
    upgrade_1_68_0(conn)
    upgrade_1_68_0(conn)
    assert conn.execute('SELECT * FROM starboard_qualified').fetchall() == []
    conn.close()


def test_bulk_import_preserves_admitted_group_without_live_reactors():
    db = UserDbConn(':memory:')
    entry = SimpleNamespace(original_msg_id='100', new_starboard_msg_id='200',
                            emoji=PILL, star_count=0, author_id='42', source_channel_id='456')
    imported = asyncio.run(MigrateCompleteMixin()._complete_import(
        1, db, [entry], [PILL], {}, 789))
    assert imported == 1
    assert db.get_starboard_qualified_emojis(100, PILL) == {PILL}
    assert db.get_starboard_reaction_counts(1, 100, PILL) == ([(PILL, 0)], 0)
    db.close()
