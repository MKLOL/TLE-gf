"""Structural regression tests for Queens synchronization hot paths."""

from tle.cogs._minigame_queens import QUEENS_GAME
from tle.cogs._minigame_queens import normalize_queens_name
from tle.cogs.minigames import Minigames
from tle.util import codeforces_common as cf_common

from tests.minigames_test_utils import _queens_number, db  # noqa: F401


def test_first_registration_runs_one_migration_and_one_sync(
        db, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', db)
    name = 'Alice LinkedIn'
    normalized = normalize_queens_name(name)
    db.save_minigame_unresolved_result(
        100, 'queens', normalized, name, 200,
        _queens_number('2026-06-08'), '2026-06-08',
        100, 5, True, 'source')
    cog = Minigames(bot=None)
    calls = {'migrate': 0, 'sync': 0}
    original_migrate = cog._migrate_legacy_queens_results_to_external
    original_sync = cog._sync_queens_materialized_results

    def record_migrate(*args, **kwargs):
        calls['migrate'] += 1
        return original_migrate(*args, **kwargs)

    def record_sync(*args, **kwargs):
        calls['sync'] += 1
        return original_sync(*args, **kwargs)

    monkeypatch.setattr(
        cog, '_migrate_legacy_queens_results_to_external', record_migrate)
    monkeypatch.setattr(
        cog, '_sync_queens_materialized_results', record_sync)

    claimed = cog._save_queens_registration_link(
        100, QUEENS_GAME, 300, name, normalized, None, 300)

    assert claimed == 1
    # The link is shared by every LinkedIn game, so registration claims in
    # each of them — exactly one migration and one sync per game, never more.
    linkedin_games = len(cog._linkedin_games())
    assert linkedin_games == 2
    assert calls == {'migrate': linkedin_games, 'sync': linkedin_games}
    assert db.get_minigame_rating(100, 'queens', 300) is not None


def test_legacy_migration_reads_existing_sources_once(db, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', db)
    db.save_minigame_result(
        11, 100, 'queens', 200, 300,
        _queens_number('2026-06-08'), '2026-06-08',
        100, 5, True,
        'Alice LinkedIn\n🤓💎 No hints & no mistakes!\n0:05\n')
    cog = Minigames(bot=None)
    reads = 0
    original_get = db.get_minigame_unresolved_results_for_guild

    def record_get(*args, **kwargs):
        nonlocal reads
        reads += 1
        return original_get(*args, **kwargs)

    monkeypatch.setattr(
        db, 'get_minigame_unresolved_results_for_guild', record_get)

    assert cog._migrate_legacy_queens_results_to_external(100, QUEENS_GAME) == 1
    assert reads == 1


def test_sync_without_links_skips_projection_scans(db, monkeypatch):
    monkeypatch.setattr(cf_common, 'user_db', db)
    cog = Minigames(bot=None)

    def unexpected_scan(*_args, **_kwargs):
        raise AssertionError('projection tables should not be scanned')

    monkeypatch.setattr(
        db, 'get_live_minigame_results_for_guild', unexpected_scan)
    monkeypatch.setattr(
        db, 'get_minigame_unresolved_results_for_guild', unexpected_scan)
    monkeypatch.setattr(db, 'get_minigame_optouts', unexpected_scan)

    assert cog._sync_queens_materialized_results(
        100, QUEENS_GAME, migrate_legacy=False) == 0
