"""Flask application: local waybill replication reconciler."""

from __future__ import annotations

import os

from flask import Flask, flash, redirect, render_template, request, url_for

from . import core
from .db import (
    connect,
    init_primary,
    init_replica,
    parse_drop_rules,
)

APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DATA_DIR = os.path.join(APP_ROOT, "data")


def create_app(config: dict | None = None) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config.from_mapping(
        SECRET_KEY=os.environ.get("SECRET_KEY", "dev-only-local-reconciler"),
        PRIMARY_DB=os.environ.get("PRIMARY_DB", os.path.join(DEFAULT_DATA_DIR, "primary.db")),
        REPLICA_DB=os.environ.get("REPLICA_DB", os.path.join(DEFAULT_DATA_DIR, "replica.db")),
        DROP_RULES=os.environ.get("DROP_RULES"),
        AUTO_SEED=os.environ.get("AUTO_SEED", "0") == "1",
        FAULTS=None,
    )
    if config:
        app.config.update(config)

    def faults() -> core.Faults:
        override = app.config.get("FAULTS")
        if override is not None:
            return override
        return _faults_from_rules(app.config.get("DROP_RULES"))

    # Connection-per-request keeps SQLite usage simple and safe across the two
    # independent database files.
    def primary_conn():
        conn = connect(app.config["PRIMARY_DB"])
        init_primary(conn)
        return conn

    def replica_conn():
        conn = connect(app.config["REPLICA_DB"])
        init_replica(conn)
        return conn

    @app.get("/healthz")
    def healthz():
        # Touch both databases so the check fails fast if storage is unusable.
        with _open(primary_conn()) as primary, _open(replica_conn()) as replica:
            core.primary_head(primary)
            core.replica_position(replica)
        return {"status": "ok"}

    @app.get("/")
    def index():
        with _open(primary_conn()) as primary, _open(replica_conn()) as replica:
            commits = core.list_commits(primary)
            position = core.replica_position(replica)
            head = core.primary_head(primary)
            catalog = _catalog(primary)
            live_counts = {
                "primary": core.live_count(primary),
                "replica": core.live_count(replica),
            }
        return render_template(
            "index.html",
            commits=commits,
            position=position,
            head=head,
            catalog=catalog,
            live_counts=live_counts,
            active="dashboard",
        )

    @app.post("/primary/commit")
    def add_commit():
        waybill_no = request.form.get("waybill_no", "")
        status = request.form.get("status", "")
        consignee = request.form.get("consignee", "")
        note = request.form.get("note", "")
        try:
            change = core.Change(waybill_no=waybill_no, op="upsert", status=status, consignee=consignee)
            with _open(primary_conn()) as primary:
                commit_no = core.create_commit(primary, [change], note=note)
            flash(f"主库已提交 #{commit_no}：{waybill_no} → {status}", "success")
        except core.CoreError as exc:
            flash(str(exc), "error")
        except Exception as exc:  # pragma: no cover - surfaced to operator
            flash(f"写入主库失败：{exc}", "error")
        return redirect(url_for("index"))

    @app.post("/primary/delete")
    def delete_commit():
        waybill_no = request.form.get("waybill_no", "")
        note = request.form.get("note", "")
        try:
            change = core.Change(waybill_no=waybill_no, op="delete")
            with _open(primary_conn()) as primary:
                commit_no = core.create_commit(primary, [change], note=note or f"删除 {waybill_no}")
            flash(f"主库已提交 #{commit_no}：删除 {waybill_no}", "success")
        except core.CoreError as exc:
            flash(str(exc), "error")
        except Exception as exc:  # pragma: no cover
            flash(f"写入主库失败：{exc}", "error")
        return redirect(url_for("index"))

    @app.post("/primary/bulk")
    def bulk_commit():
        """Write the standard sample dataset + rule-change batch as one commit."""
        with _open(primary_conn()) as primary:
            head_before = core.primary_head(primary)
            if head_before:
                flash("示例数据仅可在主库为空时写入。", "error")
                return redirect(url_for("index"))
            commit_no = _seed_sample(primary)
        flash(f"已写入示例数据，共 {commit_no} 笔提交。", "success")
        return redirect(url_for("index"))

    @app.post("/replica/advance")
    def advance_replica():
        try:
            steps = max(1, min(int(request.form.get("steps", "1")), 1000))
        except ValueError:
            steps = 1
        applied_commits = []
        with _open(primary_conn()) as primary, _open(replica_conn()) as replica:
            for _ in range(steps):
                result = core.advance_one(primary, replica, faults())
                if not result.applied:
                    break
                applied_commits.append(result.commit_no)
        if applied_commits:
            flash(f"备库已前进到提交 #{applied_commits[-1]}（本次 {len(applied_commits)} 批）。", "success")
        else:
            flash("备库已追平主库，无需推进。", "info")
        return redirect(_back_to(request, "index"))

    @app.post("/replica/reset")
    def reset_replica():
        _remove_db(app.config["REPLICA_DB"])
        with _open(replica_conn()):
            pass
        flash("备库已重置为空库，可从第 1 批重新同步。", "info")
        return redirect(url_for("index"))

    @app.get("/reconcile")
    def reconcile_view():
        with _open(primary_conn()) as primary, _open(replica_conn()) as replica:
            commits = core.list_commits(primary)
            head = core.primary_head(primary)
            position = core.replica_position(replica)
            default_commit = _int_arg(request.args.get("commit"), head)
            default_lo, default_hi = _range_defaults(primary)
            lo = request.args.get("lo", default_lo)
            hi = request.args.get("hi", default_hi)
            result = None
            error = None
            submitted = "commit" in request.args or "lo" in request.args or "hi" in request.args
            if submitted:
                try:
                    wb_range = core.parse_range(lo, hi)
                    commit_no = _int_arg(request.args.get("commit"), head)
                    result = core.reconcile(primary, replica, commit_no, wb_range)
                except core.CoreError as exc:
                    error = str(exc)
            view = dict(
                commits=commits,
                head=head,
                position=position,
                result=result,
                error=error,
                sel_commit=request.args.get("commit", str(default_commit)),
                sel_lo=lo,
                sel_hi=hi,
                active="reconcile",
            )
        return render_template("reconcile.html", **view)

    @app.post("/reconcile/advance-and-check")
    def advance_and_check():
        """Catch the replica up to the chosen commit, then show the report."""
        commit_no = _int_arg(request.form.get("commit"), 1)
        lo = request.form.get("lo", "")
        hi = request.form.get("hi", "")
        with _open(primary_conn()) as primary, _open(replica_conn()) as replica:
            position = core.replica_position(replica)
            while position["applied_commit"] < commit_no:
                result = core.advance_one(primary, replica, faults())
                if not result.applied:
                    break
                position = core.replica_position(replica)
        return redirect(url_for("reconcile_view", commit=commit_no, lo=lo, hi=hi))

    @app.post("/admin/reset-all")
    def reset_all():
        _remove_db(app.config["PRIMARY_DB"])
        _remove_db(app.config["REPLICA_DB"])
        with _open(primary_conn()), _open(replica_conn()):
            pass
        flash("主库与备库均已重置。", "info")
        return redirect(url_for("index"))

    if app.config.get("AUTO_SEED"):
        with _open(primary_conn()) as primary:
            if core.primary_head(primary) == 0:
                _seed_sample(primary)

    return app


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

class _open:
    """Context manager that also closes the SQLite connection."""

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        self.conn.close()
        return False


def _back_to(request, fallback_endpoint: str):
    target = request.form.get("next") or request.referrer
    if target and target.startswith("/") and not target.startswith("//"):
        return target
    return url_for(fallback_endpoint)


def _remove_db(path: str) -> None:
    """Delete a SQLite file and its WAL sidecars (no-op when absent)."""
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass


def _int_arg(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _faults_from_rules(raw) -> core.Faults:
    rules = parse_drop_rules(raw)
    return {commit: {wb: {"drop": True} for wb in waybills} for commit, waybills in rules.items()}


def _catalog(primary) -> list[str]:
    rows = primary.execute(
        "SELECT waybill_no FROM change_log WHERE op = 'upsert' ORDER BY waybill_no"
    ).fetchall()
    return sorted({r["waybill_no"] for r in rows})


def _range_defaults(primary) -> tuple[str, str]:
    rows = primary.execute(
        "SELECT waybill_no FROM change_log WHERE op = 'upsert' ORDER BY waybill_no"
    ).fetchall()
    if not rows:
        return "", ""
    return rows[0]["waybill_no"], rows[-1]["waybill_no"]


SAMPLE_ROWS = [
    ("WB1001", "已揽收", "上海-张先生"),
    ("WB1002", "已揽收", "北京-李女士"),
    ("WB1003", "运输中", "广州-王先生"),
    ("WB1004", "运输中", "深圳-赵女士"),
    ("WB1005", "派送中", "杭州-钱先生"),
    ("WB1006", "派送中", "成都-孙女士"),
    ("WB1007", "已签收", "武汉-周先生"),
    ("WB1008", "已签收", "南京-吴女士"),
    ("WB1009", "已签收", "西安-郑先生"),
    ("WB1010", "异常", "重庆-冯女士"),
]


def _seed_sample(primary) -> int:
    """Create the sample history: intake batches, a rule change, later updates.

    Commit layout:
      1  intake WB1001..1004
      2  intake WB1005..1007
      3  intake WB1008..1010   <- replica will silently lose WB1008 here
      4  rule-change batch: status corrections across the range
      5  later updates far outside the typical checking window
    """
    batches = [
        (SAMPLE_ROWS[0:4], "首批入库（换版前）"),
        (SAMPLE_ROWS[4:7], "二批入库（换版前）"),
        (SAMPLE_ROWS[7:10], "三批入库（规则换版后）"),
    ]
    last = 0
    for rows, note in batches:
        changes = [core.Change(wb, "upsert", status, cons) for wb, status, cons in rows]
        last = core.create_commit(primary, changes, note=note)

    rule_changes = [
        core.Change("WB1003", "upsert", "已签收", "广州-王先生"),
        core.Change("WB1006", "upsert", "已签收", "成都-孙女士"),
        core.Change("WB1009", "upsert", "派送中", "西安-郑先生"),
    ]
    last = core.create_commit(primary, rule_changes, note="规则换版：状态回填")

    tail = [
        core.Change("WB1011", "upsert", "已揽收", "苏州-陈先生"),
        core.Change("WB1012", "upsert", "运输中", "天津-褚女士"),
    ]
    last = core.create_commit(primary, tail, note="规则换版后新增运单")
    return last
