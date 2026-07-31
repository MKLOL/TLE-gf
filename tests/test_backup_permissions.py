import importlib.util
import os
import stat
import sys
import types

import pytest


_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def _load_backup(monkeypatch, tmp_path):
    monkeypatch.setenv('TLE_SRC_HOST', 'source.test')
    monkeypatch.setenv('TLE_SRC_USER', 'backup-user')
    monkeypatch.setenv('TLE_SRC_PASSWORD', 'super-secret-password')
    monkeypatch.setenv('TLE_BACKUP_DIR', str(tmp_path / 'backups'))
    monkeypatch.setenv('TLE_KNOWN_HOSTS', str(tmp_path / 'ssh' / 'known_hosts'))
    monkeypatch.setenv('TLE_LOCKFILE', str(tmp_path / 'lock' / 'backup.lock'))
    paramiko = types.ModuleType('paramiko')
    paramiko.AutoAddPolicy = type('AutoAddPolicy', (), {})
    monkeypatch.setitem(sys.modules, 'paramiko', paramiko)

    name = 'test_backup_user_db'
    path = os.path.join(_ROOT, 'tle-backup-service', 'backup_user_db.py')
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module, paramiko


def test_known_hosts_and_created_parent_are_private(monkeypatch, tmp_path):
    backup, paramiko = _load_backup(monkeypatch, tmp_path)

    class Client:
        def load_host_keys(self, path):
            self.loaded = path

        def set_missing_host_key_policy(self, policy):
            self.policy = policy

        def connect(self, **kwargs):
            self.kwargs = kwargs

    client = Client()
    paramiko.SSHClient = lambda: client

    assert backup.connect() is client
    assert _mode(tmp_path / 'ssh') == 0o700
    assert _mode(tmp_path / 'ssh' / 'known_hosts') == 0o600


def test_backup_directory_partial_and_final_database_are_private(
        monkeypatch, tmp_path):
    backup, _ = _load_backup(monkeypatch, tmp_path)
    backup_dir = tmp_path / 'backups'
    backup_dir.mkdir(mode=0o755)
    commands = []

    class Sftp:
        def get(self, remote, local):
            assert _mode(local) == 0o600
            with open(local, 'wb') as output:
                output.write(b'dbbytes')

        def close(self):
            pass

    class Client:
        def open_sftp(self):
            return Sftp()

        def close(self):
            pass

    def run_remote(client, command, timeout=180):
        commands.append(command)
        if 'stat -c %s' in command:
            return 0, '7', ''
        return 0, '', ''

    monkeypatch.setattr(backup, 'connect', lambda: Client())
    monkeypatch.setattr(backup, 'run_remote', run_remote)
    monkeypatch.setattr(backup, 'verify_sqlite', lambda path: None)
    monkeypatch.setattr(backup, 'prune', lambda: None)

    backup.do_backup()

    databases = list(backup_dir.glob('user_db_*.db'))
    assert len(databases) == 1
    assert _mode(backup_dir) == 0o700
    assert _mode(databases[0]) == 0o600
    assert not list(backup_dir.glob('*.part'))
    assert any('chmod 600' in command for command in commands)
    snapshot = next(command for command in commands if '.backup' in command)
    assert 'umask 077' in snapshot
    assert commands[-1].startswith('rm -f ')


def test_remote_snapshot_is_cleaned_when_creation_fails(monkeypatch, tmp_path):
    backup, _ = _load_backup(monkeypatch, tmp_path)
    commands = []

    class Client:
        closed = False

        def close(self):
            self.closed = True

    client = Client()

    def run_remote(unused_client, command, timeout=180):
        commands.append(command)
        if '.backup' in command:
            return 1, '', 'snapshot failed'
        return 0, '', ''

    monkeypatch.setattr(backup, 'connect', lambda: client)
    monkeypatch.setattr(backup, 'run_remote', run_remote)

    with pytest.raises(RuntimeError, match='snapshot failed'):
        backup.do_backup()

    assert 'umask 077' in commands[0]
    assert commands[-1].startswith('rm -f ')
    assert client.closed is True


def test_lockfile_and_created_parent_are_private(monkeypatch, tmp_path):
    backup, _ = _load_backup(monkeypatch, tmp_path)
    monkeypatch.setattr(backup, 'do_backup', lambda: None)

    backup.main()

    assert _mode(tmp_path / 'lock') == 0o700
    assert _mode(tmp_path / 'lock' / 'backup.lock') == 0o600


def test_backup_errors_redact_passwords_and_provider_keys(monkeypatch, tmp_path):
    backup, _ = _load_backup(monkeypatch, tmp_path)
    xai_key = 'xai-' + 'SecretToken' * 3
    gemini_key = 'AIza' + 'SecretToken' * 3

    safe = backup.safe_error(
        f'password=super-secret-password xai={xai_key} gemini={gemini_key}')

    assert 'super-secret-password' not in safe
    assert xai_key not in safe
    assert gemini_key not in safe
    assert safe.count('[redacted]') == 3
