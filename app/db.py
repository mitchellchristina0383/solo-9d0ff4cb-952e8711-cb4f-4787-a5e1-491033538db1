"""SQLite connections, schema, and shared helpers.

Two independent databases are used:

* primary.db  - the source of truth. Every write is a *commit* made of one or
                more rows in ``change_log`` (an append-only redo log).
* replica.db  - the standby. It applies primary commits batch by batch and
                records every applied delta in ``apply_log``.

Neither database stores per-batch snapshots. The state of either side "as of
commit N" is always reconstructed by replaying the relevant log up to N.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone

# Sample defect: while applying commit 3 the replica silently misses WB1008.
# This is what the DBA is meant to discover through the page.
DEFAULT_DROP_RULES: dict[int, set[str]] = {3: {"WB1008"}}

PRIMARY_SCHEMA = """
CREATE TABLE IF NOT EXISTS commits (
    commit_no   INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at  TEXT NOT NULL,
    note        TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS change_log (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    commit_no   INTEGER NOT NULL REFERENCES commits(commit_no),
    waybill_no  TEXT    NOT NULL,
    op          TEXT    NOT NULL CHECK (op IN ('upsert', 'delete')),
    status      TEXT,
    consignee   TEXT
);

CREATE INDEX IF NOT EXISTS idx_change_commit  ON change_log(commit_no, seq);
CREATE INDEX IF NOT EXISTS idx_change_waybill ON change_log(waybill_no, seq);

CREATE TABLE IF NOT EXISTS waybills (
    waybill_no     TEXT PRIMARY KEY,
    status         TEXT NOT NULL,
    consignee      TEXT,
    updated_commit INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_waybills_status ON waybills(status);
"""

REPLICA_SCHEMA = """
CREATE TABLE IF NOT EXISTS apply_log (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    commit_no   INTEGER NOT NULL,
    waybill_no  TEXT    NOT NULL,
    op          TEXT    NOT NULL CHECK (op IN ('upsert', 'delete')),
    status      TEXT,
    consignee   TEXT
);

CREATE INDEX IF NOT EXISTS idx_apply_commit  ON apply_log(commit_no, seq);
CREATE INDEX IF NOT EXISTS idx_apply_waybill ON apply_log(waybill_no, seq);

CREATE TABLE IF NOT EXISTS waybills (
    waybill_no     TEXT PRIMARY KEY,
    status         TEXT NOT NULL,
    consignee      TEXT,
    updated_commit INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS replica_state (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    applied_commit INTEGER NOT NULL DEFAULT 0,
    applied_seq    INTEGER NOT NULL DEFAULT 0,
    updated_at     TEXT    NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(path: str) -> sqlite3.Connection:
    """Open a connection tuned for a small local tool.

    ``isolation_level=None`` puts SQLite in autocommit mode so the sync engine
    can control transaction boundaries explicitly (BEGIN IMMEDIATE / COMMIT).
    """
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(path, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    conn.execute("PRAGMA busy_timeout = 15000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_primary(conn: sqlite3.Connection) -> None:
    conn.executescript(PRIMARY_SCHEMA)


def init_replica(conn: sqlite3.Connection) -> None:
    conn.executescript(REPLICA_SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO replica_state (id, applied_commit, applied_seq, updated_at)"
        " VALUES (1, 0, 0, ?)",
        (utc_now(),),
    )


def parse_drop_rules(raw: str | None) -> dict[int, set[str]]:
    """Parse DROP_RULES (JSON like {"3": ["WB1008"]}).

    ``None`` (unset) selects the built-in sample defect. An empty string is an
    explicit request for *no* drops, so the replica can be fully faithful.
    """
    if raw is None:
        return {commit: set(waybills) for commit, waybills in DEFAULT_DROP_RULES.items()}
    if raw.strip() == "":
        return {}
    data = json.loads(raw)
    return {int(commit): set(waybills) for commit, waybills in data.items()}
