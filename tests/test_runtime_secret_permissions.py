import os
import stat

from tle.util import file_permissions
from tle.util.db.user_db_conn import UserDbConn


def _mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def test_private_directory_and_file_modes_are_repaired(tmp_path):
    directory = tmp_path / 'runtime'
    directory.mkdir(mode=0o755)
    secret = directory / 'secret.db'
    secret.write_text('secret')
    os.chmod(secret, 0o644)

    file_permissions.ensure_private_directory(directory)
    file_permissions.ensure_private_file(secret)

    assert _mode(directory) == 0o700
    assert _mode(secret) == 0o600


def test_private_umask_protects_library_created_sidecars(tmp_path):
    sidecar = tmp_path / 'user.db-wal'
    previous = os.umask(0)
    try:
        with file_permissions.private_umask():
            sidecar.write_text('journal data')
    finally:
        os.umask(previous)

    assert _mode(sidecar) == 0o600


def test_user_database_repairs_existing_permissive_mode(tmp_path):
    path = tmp_path / 'user.db'
    path.touch(mode=0o666)
    os.chmod(path, 0o666)

    database = UserDbConn(str(path))
    try:
        assert _mode(path) == 0o600
    finally:
        database.conn.close()


def test_sqlite_sidecar_modes_are_repaired(tmp_path):
    path = tmp_path / 'user.db'
    path.touch(mode=0o600)
    for suffix in ('-journal', '-wal', '-shm'):
        sidecar = tmp_path / f'user.db{suffix}'
        sidecar.touch(mode=0o666)
        os.chmod(sidecar, 0o666)

    file_permissions.harden_sqlite_files(path)

    assert _mode(path) == 0o600
    for suffix in ('-journal', '-wal', '-shm'):
        assert _mode(tmp_path / f'user.db{suffix}') == 0o600


def test_memory_database_does_not_create_a_literal_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    database = UserDbConn(':memory:')
    try:
        assert not (tmp_path / ':memory:').exists()
    finally:
        database.conn.close()


def test_docker_context_excludes_runtime_secrets():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, '.dockerignore'), encoding='utf-8') as source:
        patterns = {
            line.strip() for line in source
            if line.strip() and not line.lstrip().startswith('#')
        }

    assert {'environment', '.env', '.env.*', 'data/', 'logs/', '*.db',
            '.git/', 'tle-backup-service/known_hosts'} <= patterns
