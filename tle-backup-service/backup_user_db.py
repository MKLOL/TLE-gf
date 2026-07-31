#!/usr/bin/env python3
"""
Off-site backup of the TLE-gf bot's user.db.

Designed to be invoked as a one-shot every 30 minutes (systemd timer or cron).
Each run:
  1. SSH to the TLE-gf server and take a CONSISTENT snapshot with `sqlite3 .backup`
     (safe to copy even while the bot is mid-write; a plain copy of a live sqlite
     file can be torn/corrupt).
  2. Download the snapshot here over SFTP, atomically (.part then rename).
  3. Verify the download is a valid sqlite DB (integrity_check + a known table).
  4. Best-effort: stamp `kvs.last_backup_at` in the live user.db so the bot's
     `;backup status` command can report when the last backup succeeded.
  5. Prune local backups older than the retention window (only after a success,
     so a run of failures never erodes existing history).

Single-run lock (flock) prevents overlapping invocations (timer + cron + manual).
Host keys are pinned trust-on-first-use; a changed key aborts the run.

Configuration is read from environment variables (see backup.env.example).
Auth: set TLE_SRC_PASSWORD for password auth, or TLE_SSH_KEY for key auth.
"""
import datetime
import fcntl
import glob
import logging
import os
import re
import shlex
import sys
import tempfile
from contextlib import contextmanager

try:
    import paramiko
except ImportError:
    sys.exit("paramiko is required. Install it: pip install paramiko")

LOG = logging.getLogger("tle-backup")


def _cfg(name, default=None, required=False):
    val = os.environ.get(name, default)
    if required and not val:
        sys.exit(f"Missing required environment variable: {name}")
    return val


_HERE = os.path.dirname(os.path.abspath(__file__))
SRC_HOST = _cfg("TLE_SRC_HOST", required=True)
SRC_PORT = int(_cfg("TLE_SRC_PORT", "22"))
SRC_USER = _cfg("TLE_SRC_USER", required=True)
SRC_PASSWORD = _cfg("TLE_SRC_PASSWORD", "")
SRC_KEYFILE = _cfg("TLE_SSH_KEY", "")
SRC_DB = _cfg("TLE_SRC_DB", "/opt/tle-gf/data/db/user.db")
BACKUP_DIR = _cfg("TLE_BACKUP_DIR", os.path.expanduser("~/tle-gf-backups"))
RETENTION_DAYS = int(_cfg("TLE_RETENTION_DAYS", "7"))
REMOTE_SNAPSHOT = _cfg("TLE_REMOTE_SNAPSHOT", "/tmp/tle_user_db_snapshot.db")
# NOTE: this key is hardcoded as the SAME literal in the bot's ;backup status
# command (tle/cogs/meta.py). If you override it here, change it there too.
KVS_KEY = _cfg("TLE_BACKUP_KVS_KEY", "last_backup_at")
SSH_TIMEOUT = int(_cfg("TLE_SSH_TIMEOUT", "30"))
KNOWN_HOSTS = _cfg("TLE_KNOWN_HOSTS", os.path.join(_HERE, "known_hosts"))
# Resolve the temp default lazily (short-circuit): tempfile.gettempdir() probes
# /tmp etc., which fails under a sandbox with no writable /tmp — only fall back to
# it when TLE_LOCKFILE is unset.
LOCKFILE = os.environ.get("TLE_LOCKFILE") or os.path.join(tempfile.gettempdir(), "tle-backup.lock")
_REDACTED = "[redacted]"
_API_KEY_RE = re.compile(
    r'(?<![A-Za-z0-9_-])(?:xai-|AIza)[A-Za-z0-9_-]{20,}'
    r'(?![A-Za-z0-9_-])',
)


@contextmanager
def private_creation_mask():
    """Make files created by libraries private, restoring the caller's mask."""
    previous = os.umask(0o077)
    try:
        yield
    finally:
        os.umask(previous)


def ensure_private_directory(path):
    """Create a service-owned directory and enforce owner-only access."""
    with private_creation_mask():
        os.makedirs(path, mode=0o700, exist_ok=True)
    os.chmod(path, 0o700)


def ensure_private_parent(path):
    """Privately create a missing parent without changing shared parents."""
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.exists(parent):
        ensure_private_directory(parent)


def open_private(path, *, truncate=False):
    """Open an owner-only service artifact without a permissive creation race."""
    ensure_private_parent(path)
    flags = os.O_WRONLY | os.O_CREAT
    flags |= os.O_TRUNC if truncate else os.O_APPEND
    fd = os.open(path, flags, 0o600)
    os.chmod(path, 0o600)
    return os.fdopen(fd, "w")


def safe_error(exc):
    """Remove configured credentials and provider-shaped keys from errors."""
    text = str(exc)
    for secret in (SRC_PASSWORD,):
        if secret:
            text = text.replace(secret, _REDACTED)
    return _API_KEY_RE.sub(_REDACTED, text)


def connect():
    client = paramiko.SSHClient()
    # Trust-on-first-use: load (or create) a pinned known_hosts file. AutoAddPolicy
    # records the host key on first connect and persists it; on later runs paramiko
    # raises BadHostKeyException if the key CHANGED (MITM protection for the DB pull).
    ensure_private_parent(KNOWN_HOSTS)
    open_private(KNOWN_HOSTS).close()
    try:
        client.load_host_keys(KNOWN_HOSTS)
    except IOError:
        pass
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    kwargs = dict(hostname=SRC_HOST, port=SRC_PORT, username=SRC_USER,
                  timeout=SSH_TIMEOUT, banner_timeout=SSH_TIMEOUT, auth_timeout=SSH_TIMEOUT)
    if SRC_KEYFILE:
        kwargs["key_filename"] = SRC_KEYFILE
    if SRC_PASSWORD:
        kwargs["password"] = SRC_PASSWORD
        kwargs["look_for_keys"] = False
        kwargs["allow_agent"] = False
    client.connect(**kwargs)
    return client


def run_remote(client, cmd, timeout=180):
    _, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    rc = stdout.channel.recv_exit_status()
    out = stdout.read().decode(errors="replace").strip()
    err = stderr.read().decode(errors="replace").strip()
    return rc, out, err


def verify_sqlite(path):
    """Raise if `path` is not a healthy TLE user.db."""
    import sqlite3
    con = sqlite3.connect(path)
    try:
        row = con.execute("PRAGMA integrity_check;").fetchone()
        if not row or row[0] != "ok":
            raise RuntimeError(f"integrity_check failed: {row}")
        # Must be the real DB, not an empty/garbage file.
        con.execute("SELECT COUNT(*) FROM user_handle").fetchone()
    finally:
        con.close()


def prune():
    cutoff = datetime.datetime.now(datetime.timezone.utc).timestamp() - RETENTION_DAYS * 86400
    removed = 0
    for f in glob.glob(os.path.join(BACKUP_DIR, "user_db_*.db")):
        if os.path.getmtime(f) < cutoff:
            os.remove(f)
            removed += 1
            LOG.info("pruned old backup: %s", os.path.basename(f))
    # Sweep stray partial downloads left by an interrupted run.
    for f in glob.glob(os.path.join(BACKUP_DIR, "*.part")):
        if os.path.getmtime(f) < cutoff:
            os.remove(f)
    remaining = len(glob.glob(os.path.join(BACKUP_DIR, "user_db_*.db")))
    LOG.info("prune: removed %d backup(s) older than %dd; %d remain",
             removed, RETENTION_DAYS, remaining)


def do_backup():
    ensure_private_directory(BACKUP_DIR)
    now = datetime.datetime.now(datetime.timezone.utc)
    dest = os.path.join(BACKUP_DIR, f"user_db_{now.strftime('%Y%m%d_%H%M%S')}.db")
    part = dest + ".part"
    snap = shlex.quote(REMOTE_SNAPSHOT)
    src = shlex.quote(SRC_DB)

    LOG.info("connecting to %s@%s:%d", SRC_USER, SRC_HOST, SRC_PORT)
    client = connect()
    try:
        # 1) consistent snapshot on the source. The umask applies before
        # sqlite creates the file, avoiding a readable window before chmod.
        rc, out, err = run_remote(
            client,
            f'rm -f {snap} && umask 077 && '
            f'sqlite3 {src} ".timeout 15000" ".backup {snap}"')
        if rc != 0:
            raise RuntimeError(
                f"remote snapshot failed (rc={rc}): {safe_error(err or out)}")
        rc, out, err = run_remote(
            client, f"test -s {snap} && chmod 600 {snap} && stat -c %s {snap}")
        if rc != 0 or not out.isdigit():
            raise RuntimeError("remote snapshot missing or empty after .backup")
        remote_size = int(out)
        LOG.info("snapshot created on source (%d bytes)", remote_size)

        # 2) download atomically
        sftp = client.open_sftp()
        try:
            # Pre-create so SFTP never opens a world-readable partial file.
            open_private(part, truncate=True).close()
            sftp.get(REMOTE_SNAPSHOT, part)
        finally:
            sftp.close()
        os.chmod(part, 0o600)
        local_size = os.path.getsize(part)
        if local_size != remote_size:
            raise RuntimeError(f"size mismatch: remote={remote_size} local={local_size}")

        # 3) verify, then commit the filename
        verify_sqlite(part)
        os.replace(part, dest)
        os.chmod(dest, 0o600)
        LOG.info("backup OK: %s (%d bytes)", dest, local_size)

        # 4) best-effort: stamp last-backup time in the live DB for ;backup status.
        # Safe alongside the running bot: sqlite serializes multi-process writes via
        # file locking, and `.timeout 8000` (its own arg, applied before the SQL)
        # waits out any lock. KVS_KEY/epoch are trusted (literal/int) so the inline
        # SQL is injection-safe. Failure here never fails the backup.
        epoch = str(int(now.timestamp()))
        sql = f"INSERT OR REPLACE INTO kvs (key, value) VALUES ('{KVS_KEY}', '{epoch}');"
        rc, out, err = run_remote(client, f'sqlite3 {src} ".timeout 8000" {shlex.quote(sql)}')
        if rc == 0:
            LOG.info("stamped %s=%s in live DB", KVS_KEY, epoch)
        else:
            LOG.warning("could not stamp backup time (non-fatal, rc=%d): %s",
                        rc, safe_error(err or out))

    finally:
        # Attempt cleanup even when snapshot creation, transfer, or validation
        # fails. A stale remote copy is still a complete credential-bearing DB.
        try:
            rc, out, err = run_remote(client, f"rm -f {snap}")
            if rc != 0:
                LOG.warning("could not remove remote snapshot (rc=%d): %s",
                            rc, safe_error(err or out))
        except Exception as cleanup_error:  # noqa: BLE001 - preserve root cause
            LOG.warning("could not remove remote snapshot: %s",
                        safe_error(cleanup_error))
        finally:
            client.close()

    # 6) prune (only reached on a successful backup)
    prune()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # Single-run lock: skip (cleanly) if a previous run is still going.
    lock_fd = open_private(LOCKFILE, truncate=True)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        LOG.info("another backup run is in progress; skipping this run")
        return
    try:
        do_backup()
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 — top-level guard for cron/journal visibility
        if not logging.getLogger().handlers:
            logging.basicConfig(level=logging.INFO)
        LOG.error("BACKUP FAILED: %s", safe_error(exc))
        # leave any *.part behind for inspection unless zero-byte; next run overwrites
        for f in glob.glob(os.path.join(BACKUP_DIR, "*.part")):
            try:
                if os.path.getsize(f) == 0:
                    os.remove(f)
            except OSError:
                pass
        sys.exit(1)
