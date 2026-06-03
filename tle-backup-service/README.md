# tle-backup-service

Pulls a **consistent** copy of the TLE-gf bot's `user.db` from the TLE-gf server
every 30 minutes, keeps **7 days** of history, and records the backup time back
into the live DB so `;backup status` works in Discord.

How it stays safe:
- Takes a `sqlite3 .backup` snapshot on the source first (a plain copy of a live
  sqlite file can be torn/corrupt), then downloads that.
- Downloads to `*.part` and renames only after an `integrity_check` passes.
- Prunes old backups **only after a successful run**, so a streak of failures
  never deletes good history.
- Stamping the backup time in the live DB is best-effort — a failed stamp never
  fails the backup.

## Setup on the backup server

```bash
# 1. Code + venv
sudo mkdir -p /opt/tle-backup-service && sudo chown "$USER" /opt/tle-backup-service
cp backup_user_db.py requirements.txt /opt/tle-backup-service/
python3 -m venv /opt/tle-backup-service/.venv
/opt/tle-backup-service/.venv/bin/pip install -r /opt/tle-backup-service/requirements.txt

# 2. Config (the SSH creds for the TLE-gf source server)
sudo mkdir -p /etc/tle-backup
sudo cp backup.env.example /etc/tle-backup/backup.env
sudo $EDITOR /etc/tle-backup/backup.env      # fill in host/user/password, backup dir
sudo chmod 600 /etc/tle-backup/backup.env

# 3. Storage dir
sudo mkdir -p /var/backups/tle-gf            # must match TLE_BACKUP_DIR

# 4. Schedule it (every 30 min)
sudo cp systemd/tle-backup.service systemd/tle-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now tle-backup.timer

# Run once now to verify:
sudo systemctl start tle-backup.service
journalctl -u tle-backup.service -n 30 --no-pager
ls -l /var/backups/tle-gf
```

### Cron alternative (instead of the systemd timer)

```cron
*/30 * * * * set -a; . /etc/tle-backup/backup.env; set +a; /opt/tle-backup-service/.venv/bin/python /opt/tle-backup-service/backup_user_db.py >> /var/log/tle-backup.log 2>&1
```

## Config (environment variables — see `backup.env.example`)

| Var | Default | Meaning |
|---|---|---|
| `TLE_SRC_HOST` / `TLE_SRC_PORT` / `TLE_SRC_USER` | — / 22 / — | SSH to the TLE-gf source |
| `TLE_SRC_PASSWORD` | — | password auth (or use `TLE_SSH_KEY`) |
| `TLE_SSH_KEY` | — | path to a private key (preferred over password) |
| `TLE_SRC_DB` | `/opt/tle-gf/data/db/user.db` | live DB on the source |
| `TLE_BACKUP_DIR` | `~/tle-gf-backups` | where backups are stored here |
| `TLE_RETENTION_DAYS` | `7` | delete backups older than this |

Backups are named `user_db_YYYYMMDD_HHMMSS.db` (UTC).
