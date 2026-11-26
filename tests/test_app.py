import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app  # noqa: E402
from app import core  # noqa: E402
from app.db import (  # noqa: E402
    connect,
    init_primary,
    init_replica,
    parse_drop_rules,
)


@pytest.fixture()
def dbs(tmp_path):
    primary = connect(str(tmp_path / "primary.db"))
    replica = connect(str(tmp_path / "replica.db"))
    init_primary(primary)
    init_replica(replica)
    yield primary, replica
    primary.close()
    replica.close()


@pytest.fixture()
def app(tmp_path):
    application = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "PRIMARY_DB": str(tmp_path / "primary.db"),
            "REPLICA_DB": str(tmp_path / "replica.db"),
            "DROP_RULES": '{"3": ["WB1008"]}',
            "AUTO_SEED": False,
            "FAULTS": None,
        }
    )
    yield application


@pytest.fixture()
def client(app):
    return app.test_client()


def seed(primary):
    """Mirror the app's sample layout: 3 intake batches + rule change + tail."""
    core.create_commit(
        primary,
        [
            core.Change("WB1001", "upsert", "已揽收", "上海-张先生"),
            core.Change("WB1002", "upsert", "已揽收", "北京-李女士"),
            core.Change("WB1003", "upsert", "运输中", "广州-王先生"),
            core.Change("WB1004", "upsert", "运输中", "深圳-赵女士"),
        ],
        note="首批入库",
    )
    core.create_commit(
        primary,
        [
            core.Change("WB1005", "upsert", "派送中", "杭州-钱先生"),
            core.Change("WB1006", "upsert", "派送中", "成都-孙女士"),
            core.Change("WB1007", "upsert", "已签收", "武汉-周先生"),
        ],
        note="二批入库",
    )
    core.create_commit(
        primary,
        [
            core.Change("WB1008", "upsert", "已签收", "南京-吴女士"),
            core.Change("WB1009", "upsert", "已签收", "西安-郑先生"),
            core.Change("WB1010", "upsert", "异常", "重庆-冯女士"),
        ],
        note="三批入库",
    )
    core.create_commit(
        primary,
        [
            core.Change("WB1003", "upsert", "已签收", "广州-王先生"),
            core.Change("WB1006", "upsert", "已签收", "成都-孙女士"),
            core.Change("WB1009", "upsert", "派送中", "西安-郑先生"),
        ],
        note="规则换版：状态回填",
    )
    core.create_commit(
        primary,
        [
            core.Change("WB1011", "upsert", "已揽收", "苏州-陈先生"),
            core.Change("WB1012", "upsert", "运输中", "天津-褚女士"),
        ],
        note="换版后新增",
    )


FAULTS = {3: {"WB1008": {"drop": True}}}


# --------------------------------------------------------------------------
# Range parsing
# --------------------------------------------------------------------------

def test_parse_range_numeric():
    r = core.parse_range("WB1001", "WB1010")
    assert r.contains("WB1005")
    assert not r.contains("WB1011")
    assert not r.contains("XX1005")


def test_parse_range_lexical_fallback():
    r = core.parse_range("A-01", "A-09")
    assert r.contains("A-05")
    assert not r.contains("B-05")


def test_parse_range_rejects_inverted():
    with pytest.raises(core.CoreError):
        core.parse_range("WB1010", "WB1001")


# --------------------------------------------------------------------------
# Primary writes
# --------------------------------------------------------------------------

def test_create_commit_materialises_state(dbs):
    primary, _ = dbs
    n = core.create_commit(primary, [core.Change("WB1", "upsert", "已揽收")])
    assert n == 1
    row = primary.execute("SELECT status, updated_commit FROM waybills WHERE waybill_no='WB1'").fetchone()
    assert row["status"] == "已揽收"
    assert row["updated_commit"] == 1


def test_delete_removes_materialised_row(dbs):
    primary, _ = dbs
    core.create_commit(primary, [core.Change("WB1", "upsert", "已揽收")])
    core.create_commit(primary, [core.Change("WB1", "delete")])
    assert primary.execute("SELECT COUNT(*) c FROM waybills").fetchone()["c"] == 0


def test_commit_validation():
    class Dummy:
        def execute(self, *a, **k):
            raise AssertionError("should not execute")
    with pytest.raises(core.CoreError):
        core.create_commit(Dummy(), [])


# --------------------------------------------------------------------------
# Replica advancement
# --------------------------------------------------------------------------

def test_advance_is_one_commit_per_call(dbs):
    primary, replica = dbs
    seed(primary)
    r1 = core.advance_one(primary, replica, FAULTS)
    assert r1.applied and r1.commit_no == 1
    assert core.replica_position(replica)["applied_commit"] == 1
    # commit 3 drops one row but still advances (silent gap)
    core.advance_one(primary, replica, FAULTS)
    r3 = core.advance_one(primary, replica, FAULTS)
    assert r3.commit_no == 3
    assert r3.rows_dropped == ["WB1008"]
    assert core.replica_position(replica)["applied_commit"] == 3


def test_advance_stops_at_head(dbs):
    primary, replica = dbs
    seed(primary)
    for _ in range(5):
        core.advance_one(primary, replica, FAULTS)
    result = core.advance_one(primary, replica, FAULTS)
    assert not result.applied
    assert core.replica_position(replica)["applied_commit"] == 5


# --------------------------------------------------------------------------
# Reconciliation
# --------------------------------------------------------------------------

def test_pending_when_replica_behind(dbs):
    primary, replica = dbs
    seed(primary)
    core.advance_one(primary, replica, FAULTS)  # applied 1, head 5
    result = core.reconcile(primary, replica, 3, core.parse_range("WB1001", "WB1010"))
    assert result.pending
    assert result.total_problems == 0  # no verdict attempted
    assert result.applied_commit == 1


def test_detects_missing_waybill_when_caught_up(dbs):
    primary, replica = dbs
    seed(primary)
    for _ in range(5):
        core.advance_one(primary, replica, FAULTS)
    result = core.reconcile(primary, replica, 3, core.parse_range("WB1001", "WB1010"))
    assert not result.pending
    assert [m["waybill_no"] for m in result.missing] == ["WB1008"]
    assert result.extra == []
    assert result.status_mismatch == []


def test_detects_status_mismatch(dbs):
    primary, replica = dbs
    core.create_commit(primary, [core.Change("WB1", "upsert", "已揽收")])
    faults = {1: {"WB1": {"status": "已签收"}}}
    core.advance_one(primary, replica, faults)
    result = core.reconcile(primary, replica, 1, core.parse_range("WB1", "WB1"))
    assert len(result.status_mismatch) == 1
    assert result.status_mismatch[0]["primary_status"] == "已揽收"
    assert result.status_mismatch[0]["replica_status"] == "已签收"


def test_detects_extra_waybill(dbs):
    primary, replica = dbs
    core.create_commit(primary, [core.Change("WB1", "upsert", "已揽收")])
    faults = {1: {"WB9": {"extra": {"status": "已签收"}}}}
    core.advance_one(primary, replica, faults)
    result = core.reconcile(primary, replica, 1, core.parse_range("WB1", "WB9"))
    assert [e["waybill_no"] for e in result.extra] == ["WB9"]


def test_result_is_stable_as_primary_moves_on(dbs):
    primary, replica = dbs
    seed(primary)
    for _ in range(3):
        core.advance_one(primary, replica, FAULTS)
    before = core.reconcile(primary, replica, 3, core.parse_range("WB1001", "WB1010"))
    # Primary keeps accepting new commits; replica keeps advancing.
    core.create_commit(primary, [core.Change("WB1008", "upsert", "已签收", "南京-吴女士")])
    core.advance_one(primary, replica, FAULTS)
    after = core.reconcile(primary, replica, 3, core.parse_range("WB1001", "WB1010"))
    assert before.total_problems == after.total_problems == 1
    assert [m["waybill_no"] for m in after.missing] == ["WB1008"]


def test_range_excludes_out_of_scope_waybills(dbs):
    primary, replica = dbs
    seed(primary)
    for _ in range(5):
        core.advance_one(primary, replica, FAULTS)
    # WB1008 missing, but range ends at WB1007 -> no problem reported.
    result = core.reconcile(primary, replica, 3, core.parse_range("WB1001", "WB1007"))
    assert result.total_problems == 0


def test_reconcile_rejects_bad_commit(dbs):
    primary, replica = dbs
    seed(primary)
    with pytest.raises(core.CoreError):
        core.reconcile(primary, replica, 99, core.parse_range("WB1001", "WB1010"))


# --------------------------------------------------------------------------
# Drop rule parsing
# --------------------------------------------------------------------------

def test_parse_drop_rules_default():
    assert parse_drop_rules(None) == {3: {"WB1008"}}


def test_parse_drop_rules_custom():
    assert parse_drop_rules('{"2": ["A", "B"]}') == {2: {"A", "B"}}


def test_parse_drop_rules_empty_disables_defect():
    # "" means "no drops", distinct from None (use sample defect).
    assert parse_drop_rules("") == {}


def test_faithful_replica_reconciles_clean(dbs):
    primary, replica = dbs
    seed(primary)
    for _ in range(5):
        core.advance_one(primary, replica, {})  # no faults at all
    for commit in (1, 3, 5):
        result = core.reconcile(primary, replica, commit, core.parse_range("WB1001", "WB1012"))
        assert not result.pending
        assert result.total_problems == 0, (commit, result)


# --------------------------------------------------------------------------
# HTTP flow
# --------------------------------------------------------------------------

def test_web_flow_pending_then_report(client):
    client.post("/primary/bulk")
    # Advance only to commit 1.
    client.post("/replica/advance", data={"steps": "1"})
    page = client.get("/reconcile?commit=3&lo=WB1001&hi=WB1010")
    assert "未追到核对点" in page.get_data(as_text=True)

    # Advance to the chosen commit, then reconcile.
    client.post("/reconcile/advance-and-check", data={"commit": "3", "lo": "WB1001", "hi": "WB1010"})
    page = client.get("/reconcile?commit=3&lo=WB1001&hi=WB1010")
    body = page.get_data(as_text=True)
    assert "未追到核对点" not in body
    assert "已覆盖核对点" in body
    assert "WB1008" in body
    assert "少单" in body


def test_web_seed_creates_five_commits(client):
    client.post("/primary/bulk")
    page = client.get("/")
    assert "#5" in page.get_data(as_text=True)
