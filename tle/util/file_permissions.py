"""Owner-only permissions for runtime files that may contain credentials."""
import os
from contextlib import contextmanager


PRIVATE_DIRECTORY_MODE = 0o700
PRIVATE_FILE_MODE = 0o600
PRIVATE_UMASK = 0o077
_SQLITE_SIDECARS = ('-journal', '-wal', '-shm')


def set_private_umask():
    """Keep every subsequently-created runtime artifact owner-only."""
    return os.umask(PRIVATE_UMASK)


@contextmanager
def private_umask():
    """Temporarily protect files created by a synchronous library call."""
    previous = set_private_umask()
    try:
        yield
    finally:
        os.umask(previous)


def ensure_private_directory(path):
    """Create an application-owned directory and repair an existing mode."""
    with private_umask():
        os.makedirs(path, mode=PRIVATE_DIRECTORY_MODE, exist_ok=True)
    os.chmod(path, PRIVATE_DIRECTORY_MODE)


def ensure_private_file(path, *, create=False):
    """Enforce mode 0600, optionally creating a non-truncated empty file."""
    path = os.fspath(path)
    if create:
        flags = os.O_WRONLY | os.O_CREAT
        if hasattr(os, 'O_CLOEXEC'):
            flags |= os.O_CLOEXEC
        descriptor = os.open(path, flags, PRIVATE_FILE_MODE)
        os.close(descriptor)
    if os.path.exists(path):
        os.chmod(path, PRIVATE_FILE_MODE)


def is_file_sqlite_database(path):
    """Return false for SQLite memory databases and URI-style connections."""
    value = os.fspath(path)
    return value != ':memory:' and not value.startswith('file:')


def prepare_sqlite_database(path):
    """Privately pre-create a filesystem SQLite database before connecting."""
    if is_file_sqlite_database(path):
        ensure_private_file(path, create=True)


def harden_sqlite_files(path):
    """Repair the main database and any currently-existing journal sidecars."""
    if not is_file_sqlite_database(path):
        return
    ensure_private_file(path)
    value = os.fspath(path)
    for suffix in _SQLITE_SIDECARS:
        ensure_private_file(value + suffix)
