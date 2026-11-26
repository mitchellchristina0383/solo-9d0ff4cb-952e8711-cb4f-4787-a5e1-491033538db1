"""Core domain logic: write to primary, advance replica, reconcile at a commit.

Design notes
------------
The primary keeps an append-only ``change_log``. A commit is a set of log rows
sharing a ``commit_no``. The materialised ``waybills`` table is only a
convenience cache for the current state.

The replica applies commits strictly in order and records exactly what it
received in ``apply_log``. It never stores a copy of the primary's tables.

Reconciliation for a chosen commit C ("as of C") replays the deltas with
``commit_no <= C`` from each side and compares the resulting per-waybill state.
Because both logs are append-only, the answer for C stays stable no matter how
far the replica has advanced or how many new commits the primary has accepted.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .db import utc_now

# A fault spec maps commit_no -> waybill_no -> options:
#   {"drop": True}              simulate a lost delta (the sample defect)
#   {"status": "..."}           replicate the row with a drifted status
#   {"extra": {...}}            inject a row the primary never had
Faults = dict[int, dict[str, dict]]

_NUMERIC_WB = re.compile(r"^(?P<prefix>[A-Za-z]*)(?P<num>\d+)$")


class CoreError(ValueError):
    """A validation problem that should be shown to the operator."""


# --------------------------------------------------------------------------
# Waybill range handling
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class WaybillRange:
    lo_raw: str
    hi_raw: str
    prefix: str | None
    lo: int | None
    hi: int | None
    lexical: bool

    def contains(self, waybill_no: str) -> bool:
        if self.lexical:
            return self.lo_raw <= waybill_no <= self.hi_raw
        match = _NUMERIC_WB.match(waybill_no)
        if not match or match.group("prefix") != self.prefix:
            return False
        return self.lo <= int(match.group("num")) <= self.hi  # type: ignore[operator]


def parse_range(lo_raw: str, hi_raw: str) -> WaybillRange:
    lo_raw = (lo_raw or "").strip()
    hi_raw = (hi_raw or "").strip()
    if not lo_raw or not hi_raw:
        raise CoreError("请填写运单号区间的起止值。")
    lo_match, hi_match = _NUMERIC_WB.match(lo_raw), _NUMERIC_WB.match(hi_raw)
    if lo_match and hi_match and lo_match.group("prefix") == hi_match.group("prefix"):
        lo, hi = int(lo_match.group("num")), int(hi_match.group("num"))
        if lo > hi:
            raise CoreError("运单号区间起点不能大于终点。")
        return WaybillRange(lo_raw, hi_raw, lo_match.group("prefix"), lo, hi, False)
    if lo_raw > hi_raw:
        raise CoreError("运单号区间起点不能大于终点。")
    return WaybillRange(lo_raw, hi_raw, None, None, None, True)


# --------------------------------------------------------------------------
# Primary writes
# --------------------------------------------------------------------------

@dataclass
class Change:
    waybill_no: str
    op: str = "upsert"  # upsert | delete
    status: str | None = None
    consignee: str | None = None

    def validate(self) -> None:
        self.waybill_no = (self.waybill_no or "").strip()
        self.op = (self.op or "upsert").strip()
        self.status = (self.status or "").strip() or None
        self.consignee = (self.consignee or "").strip() or None
        if not self.waybill_no:
            raise CoreError("运单号不能为空。")
        if self.op not in ("upsert", "delete"):
            raise CoreError(f"未知操作：{self.op}")
        if self.op == "upsert" and not self.status:
            raise CoreError(f"运单 {self.waybill_no} 缺少状态。")


def create_commit(conn, changes: list[Change], note: str = "") -> int:
    """Append one commit to the primary atomically."""
    if not changes:
        raise CoreError("一笔提交至少需要一条变更。")
    for change in changes:
        change.validate()

    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "INSERT INTO commits (created_at, note) VALUES (?, ?)",
            (utc_now(), (note or "").strip()),
        )
        commit_no = int(cur.lastrowid)
        for change in changes:
            conn.execute(
                "INSERT INTO change_log (commit_no, waybill_no, op, status, consignee)"
                " VALUES (?, ?, ?, ?, ?)",
                (commit_no, change.waybill_no, change.op, change.status, change.consignee),
            )
            _apply_materialised(conn, change, commit_no)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return commit_no


def _apply_materialised(conn, change: Change, commit_no: int) -> None:
    if change.op == "delete":
        conn.execute("DELETE FROM waybills WHERE waybill_no = ?", (change.waybill_no,))
        return
    conn.execute(
        "INSERT INTO waybills (waybill_no, status, consignee, updated_commit)"
        " VALUES (?, ?, ?, ?)"
        " ON CONFLICT(waybill_no) DO UPDATE SET"
        "   status = excluded.status, consignee = excluded.consignee,"
        "   updated_commit = excluded.updated_commit",
        (change.waybill_no, change.status, change.consignee, commit_no),
    )


# --------------------------------------------------------------------------
# Reading primary metadata
# --------------------------------------------------------------------------

def primary_head(conn) -> int:
    row = conn.execute("SELECT COALESCE(MAX(commit_no), 0) AS head FROM commits").fetchone()
    return int(row["head"])


def list_commits(conn) -> list[dict]:
    rows = conn.execute(
        "SELECT c.commit_no, c.created_at, c.note,"
        "       COUNT(l.seq) AS change_count"
        "  FROM commits c"
        "  LEFT JOIN change_log l ON l.commit_no = c.commit_no"
        " GROUP BY c.commit_no"
        " ORDER BY c.commit_no"
    ).fetchall()
    head = primary_head(conn)
    return [
        {
            "commit_no": r["commit_no"],
            "created_at": r["created_at"],
            "note": r["note"],
            "change_count": r["change_count"],
            "is_head": r["commit_no"] == head,
        }
        for r in rows
    ]


# --------------------------------------------------------------------------
# Replica advancement
# --------------------------------------------------------------------------

def replica_position(conn) -> dict:
    row = conn.execute(
        "SELECT applied_commit, applied_seq, updated_at FROM replica_state WHERE id = 1"
    ).fetchone()
    return {
        "applied_commit": int(row["applied_commit"]),
        "applied_seq": int(row["applied_seq"]),
        "updated_at": row["updated_at"],
    }


@dataclass
class AdvanceResult:
    applied: bool
    commit_no: int | None
    rows_applied: int = 0
    rows_dropped: list[str] = field(default_factory=list)
    reason: str = ""


def advance_one(primary, replica, faults: Faults | None = None) -> AdvanceResult:
    """Apply the next primary commit to the replica, one batch at a time.

    A commit is applied as a single replica transaction: either the whole batch
    lands (minus any simulated lost rows) and ``applied_commit`` advances, or
    nothing changes. The replica is allowed to believe it is caught up even
    when a delta was dropped - that is exactly the silent gap under test.
    """
    faults = faults or {}
    replica.execute("BEGIN IMMEDIATE")
    try:
        position = replica_position(replica)
        next_commit = position["applied_commit"] + 1
        head = primary_head(primary)
        if next_commit > head:
            replica.execute("COMMIT")
            return AdvanceResult(False, None, reason=f"备库已追平主库（提交 {head}）。")

        rows = primary.execute(
            "SELECT seq, waybill_no, op, status, consignee FROM change_log"
            " WHERE commit_no = ? ORDER BY seq",
            (next_commit,),
        ).fetchall()

        applied = 0
        dropped: list[str] = []
        last_seq = position["applied_seq"]
        for row in rows:
            waybill_no = row["waybill_no"]
            last_seq = row["seq"]
            fault = faults.get(next_commit, {}).get(waybill_no, {})
            if fault.get("drop"):
                dropped.append(waybill_no)
                continue
            change = Change(
                waybill_no=waybill_no,
                op=row["op"],
                status=fault.get("status", row["status"]),
                consignee=fault.get("consignee", row["consignee"]),
            )
            _write_replica_row(replica, next_commit, change)
            applied += 1

        # Test/simulation hook: inject rows the primary never had.
        for waybill_no, fault in faults.get(next_commit, {}).items():
            extra = fault.get("extra")
            if extra:
                change = Change(
                    waybill_no=waybill_no,
                    op=extra.get("op", "upsert"),
                    status=extra.get("status"),
                    consignee=extra.get("consignee"),
                )
                _write_replica_row(replica, next_commit, change)
                applied += 1

        replica.execute(
            "UPDATE replica_state SET applied_commit = ?, applied_seq = ?, updated_at = ?"
            " WHERE id = 1",
            (next_commit, last_seq, utc_now()),
        )
        replica.execute("COMMIT")
        return AdvanceResult(True, next_commit, applied, dropped)
    except Exception:
        replica.execute("ROLLBACK")
        raise


def _write_replica_row(conn, commit_no: int, change: Change) -> None:
    conn.execute(
        "INSERT INTO apply_log (commit_no, waybill_no, op, status, consignee)"
        " VALUES (?, ?, ?, ?, ?)",
        (commit_no, change.waybill_no, change.op, change.status, change.consignee),
    )
    _apply_materialised(conn, change, commit_no)


# --------------------------------------------------------------------------
# Reconciliation at a chosen commit
# --------------------------------------------------------------------------

@dataclass
class ReconcileResult:
    commit_no: int
    range_lo: str
    range_hi: str
    applied_commit: int
    pending: bool
    missing: list[dict] = field(default_factory=list)
    extra: list[dict] = field(default_factory=list)
    status_mismatch: list[dict] = field(default_factory=list)

    @property
    def total_problems(self) -> int:
        return len(self.missing) + len(self.extra) + len(self.status_mismatch)


def reconcile(primary, replica, commit_no: int, wb_range: WaybillRange) -> ReconcileResult:
    """Compare both sides' records as they stood at ``commit_no``.

    Reads are done through the append-only logs, filtered to ``commit_no``.
    Nothing here writes, and no table copy is produced.
    """
    head = primary_head(primary)
    if commit_no < 1 or commit_no > head:
        raise CoreError(f"提交号必须在 1 到 {head} 之间。")

    position = replica_position(replica)
    applied = position["applied_commit"]
    result = ReconcileResult(
        commit_no=commit_no,
        range_lo=wb_range.lo_raw,
        range_hi=wb_range.hi_raw,
        applied_commit=applied,
        pending=applied < commit_no,
    )
    if result.pending:
        return result

    primary_state = _state_as_of(primary, "change_log", commit_no, wb_range)
    replica_state = _state_as_of(replica, "apply_log", commit_no, wb_range)

    for waybill_no in sorted(set(primary_state) - set(replica_state)):
        state = primary_state[waybill_no]
        result.missing.append(
            {
                "waybill_no": waybill_no,
                "primary_status": state["status"],
                "primary_consignee": state["consignee"],
                "replica_status": None,
            }
        )

    for waybill_no in sorted(set(replica_state) - set(primary_state)):
        state = replica_state[waybill_no]
        result.extra.append(
            {
                "waybill_no": waybill_no,
                "primary_status": None,
                "replica_status": state["status"],
                "replica_consignee": state["consignee"],
            }
        )

    for waybill_no in sorted(set(primary_state) & set(replica_state)):
        p_state, r_state = primary_state[waybill_no], replica_state[waybill_no]
        if p_state["status"] != r_state["status"]:
            result.status_mismatch.append(
                {
                    "waybill_no": waybill_no,
                    "primary_status": p_state["status"],
                    "replica_status": r_state["status"],
                }
            )

    return result


def _state_as_of(conn, table: str, commit_no: int, wb_range: WaybillRange) -> dict[str, dict]:
    """Replay a delta log up to ``commit_no`` and fold it into per-waybill state.

    ``table`` is always a module constant ("change_log" / "apply_log"), never
    user input. The fold is in-memory and transient: no snapshot is persisted.
    """
    assert table in ("change_log", "apply_log")
    rows = conn.execute(
        f"SELECT waybill_no, op, status, consignee FROM {table}"
        " WHERE commit_no <= ? ORDER BY seq",
        (commit_no,),
    ).fetchall()
    state: dict[str, dict] = {}
    for row in rows:
        waybill_no = row["waybill_no"]
        if not wb_range.contains(waybill_no):
            continue
        if row["op"] == "delete":
            state.pop(waybill_no, None)
        else:
            state[waybill_no] = {
                "status": row["status"],
                "consignee": row["consignee"],
            }
    return state


def live_count(conn) -> int:
    """Count rows in the materialised live table (current state, not as-of).

    This is the operational table each side actually serves from. It is
    deliberately *not* used for reconciliation, which always replays the
    append-only logs as of the chosen commit.
    """
    return int(conn.execute("SELECT COUNT(*) AS c FROM waybills").fetchone()["c"])
