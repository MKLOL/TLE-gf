"""The user.db upgrade registry appends in import order; that order must be
version order, and every migration must be registered exactly once."""


def test_registry_is_in_version_order_with_no_duplicates():
    from tle.util.db.user_db_upgrades import registry
    versions = [version for version, _, _ in registry.upgrades]

    def key(version):
        return tuple(int(part) for part in version.split('.'))
    assert versions == sorted(versions, key=key)
    assert len(versions) == len(set(versions))
    for expected in ('1.60.0', '1.61.0', '1.62.0'):
        assert versions.count(expected) == 1
    assert registry.latest_version == versions[-1]
