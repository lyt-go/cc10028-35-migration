"""交付索引 v2：旧交付迁移、补发查询、中断续迁与回滚下载。"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

import app.services.base as base_mod  # noqa: E402
from app.services.result_deliver import ResultDeliverService  # noqa: E402


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


@pytest.fixture()
def data_root(tmp_path, monkeypatch):
    monkeypatch.setattr(base_mod, "DATA_ROOT", tmp_path)
    return tmp_path


def _legacy_snapshot(root: Path, doc: str, *, delivered_at: str, receipt: str | None,
                     with_shard: bool = True, extra_entity: bool = False) -> str:
    """构造一份旧环境快照：交付记录 + 可选分片 + 可选回执。"""
    sid = f"SNAP-{doc}-v1.0"
    _write_jsonl(root / "versions" / f"{doc}_vv1.0.jsonl", [{
        "快照编号": sid, "文档编号": doc, "版本号": "v1.0",
        "分块数量": 2, "实体数量": 1, "生成时间": delivered_at,
    }])
    entity_path = root / "entities" / f"{doc}_parsed_entities.jsonl"
    entity_rows = [{"entity_id": "E1", "chunk_id": "C1", "entity_type": "ORG",
                    "entity_text": "甲公司", "confidence": 0.9}]
    if extra_entity:
        entity_rows.append({"entity_id": "E2", "chunk_id": "C1", "entity_type": "PERSON",
                            "entity_text": "张三", "confidence": 0.99})
    _write_jsonl(entity_path, entity_rows)
    _write_jsonl(root / "deliveries" / f"{sid}_delivery.jsonl", [{
        "交付编号": f"DELIVER-{sid}", "快照编号": sid, "交付格式": "jsonl",
        "目标系统": "下游知识库", "字段范围": "实体", "记录条数": 1,
        "分片大小": 500, "交付时间": delivered_at,
    }])
    if with_shard:
        _write_jsonl(root / "deliveries" / f"{sid}_shard_001.jsonl", [{
            "type": "entity", "source": str(entity_path),
            "data": {"entity_id": "E1", "chunk_id": "C1", "entity_type": "ORG",
                     "entity_text": "甲公司", "confidence": 0.9},
        }])
    if receipt:
        receipt_path = root / "deliveries" / "receipts" / f"{sid}_receipt.json"
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_path.write_text(json.dumps({
            "回执编号": f"RCPT-{doc}", "快照编号": sid, "回执类型": receipt,
            "回执时间": delivered_at, "接收方": "下游知识库", "记录条数": 1,
        }, ensure_ascii=False), encoding="utf-8")
    return sid


def test_migrate_legacy_records_counts_and_products(data_root):
    sid = _legacy_snapshot(data_root, "DOC-A", delivered_at="2026-10-01 10:00:00", receipt="接收回执")
    result = ResultDeliverService().migrate_legacy_deliveries()

    assert result["migrated"] == 1
    summary = ResultDeliverService().index_summary()
    assert summary["source"] == "index_v2"
    assert summary["by_kind"] == {"legacy": 1}

    entry = ResultDeliverService().query_resend(snapshot_id=sid)["entries"][0]
    assert entry["snapshot_counts"]["entities"] == 1
    assert entry["receipt"]["receipt_id"] == "RCPT-DOC-A"
    assert entry["resend_status"] == "可补发"
    assert [p["type"] for p in entry["recoverable_products"]] == ["entity"]


def test_resend_distinguishes_receipt_only_and_no_receipt(data_root):
    receipt_only = _legacy_snapshot(
        data_root, "DOC-B", delivered_at="2026-10-02 10:00:00",
        receipt="下载回执", with_shard=False,
    )
    no_receipt = _legacy_snapshot(
        data_root, "DOC-C", delivered_at="2026-10-03 10:00:00", receipt=None,
    )
    ResultDeliverService().migrate_legacy_deliveries()

    b = ResultDeliverService().query_resend(snapshot_id=receipt_only)["entries"][0]
    assert b["resend_status"] == "仅回执可查"
    assert b["receipt"]["type"] == "下载回执"

    c = ResultDeliverService().query_resend(snapshot_id=no_receipt)["entries"][0]
    assert c["resend_status"] == "可补发"
    assert c["receipt_available"] is False


def test_new_v2_snapshot_coexists_with_legacy(data_root):
    _legacy_snapshot(data_root, "DOC-A", delivered_at="2026-10-01 10:00:00", receipt=None)
    _write_jsonl(data_root / "versions" / "DOC-N_vv2.0.jsonl", [{
        "快照编号": "SNAP-DOC-N-v2.0", "文档编号": "DOC-N", "版本号": "v2.0",
        "分块数量": 1, "实体数量": 1, "生成时间": "2026-11-20 09:00:00",
    }])
    _write_jsonl(data_root / "entities" / "DOC-N_parsed_entities.jsonl", [
        {"entity_id": "E1", "chunk_id": "C1", "entity_type": "ORG", "entity_text": "乙公司", "confidence": 0.9},
    ])
    svc = ResultDeliverService()
    delivered = svc.deliver_from_snapshot("SNAP-DOC-N-v2.0")
    assert delivered["snapshot_kind"] == "v2"

    svc.migrate_legacy_deliveries()
    summary = svc.index_summary()
    assert summary["by_kind"] == {"legacy": 1, "v2": 1}
    # 迁移不得反向覆盖 v2 交付。
    assert svc.index_summary()["by_kind"]["v2"] == 1


def test_later_extraction_stays_pending(data_root):
    sid = _legacy_snapshot(
        data_root, "DOC-C", delivered_at="2000-01-01 00:00:00", receipt=None, extra_entity=True,
    )
    ResultDeliverService().migrate_legacy_deliveries()
    entry = ResultDeliverService().query_resend(snapshot_id=sid)["entries"][0]

    assert len(entry["pending_verification"]) == 1
    pending = entry["pending_verification"][0]
    assert pending["review_status"] == "待核"
    assert pending["attributed_to_snapshot"] is False
    assert entry["approved_supplements"] == []
    # 待核产物不改变旧版成品的可补发条数。
    assert entry["recoverable_products"][0]["records"] == 1

    ResultDeliverService().mark_pending_review(sid, pending["artifact"], accepted=True)
    after = ResultDeliverService().query_resend(snapshot_id=sid)["entries"][0]
    assert len(after["approved_supplements"]) == 1


def test_interrupted_migration_resumes_without_duplicates(data_root):
    for i, doc in enumerate(["DOC-A", "DOC-B", "DOC-C"]):
        _legacy_snapshot(data_root, doc, delivered_at=f"2026-10-0{i + 1} 10:00:00", receipt=None)
    svc = ResultDeliverService()
    svc.migrate_legacy_deliveries()

    # 模拟中断：抹掉两个快照并标记 interrupted。
    index_path = data_root / "deliveries" / "delivery_index_v2.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    index["snapshots"].pop("SNAP-DOC-A-v1.0")
    index["snapshots"].pop("SNAP-DOC-C-v1.0")
    index["migration"] = {"status": "interrupted", "processed": ["SNAP-DOC-B-v1.0"]}
    index_path.write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")
    (data_root / "checkpoints" / "delivery_index_migration.json").write_text(
        json.dumps({"status": "interrupted", "processed": ["SNAP-DOC-B-v1.0"]}),
        encoding="utf-8",
    )

    resumed = svc.migrate_legacy_deliveries(resume=True)
    assert resumed["migrated"] == 2
    summary = svc.index_summary()
    assert summary["snapshot_count"] == 3
    assert summary["by_kind"] == {"legacy": 3}


def test_rollback_without_index_still_downloads_verified_shard(data_root):
    sid = _legacy_snapshot(data_root, "DOC-A", delivered_at="2026-10-01 10:00:00", receipt=None)
    svc = ResultDeliverService()
    svc.migrate_legacy_deliveries()

    # 回滚：索引文件丢失，只剩旧交付文件。
    (data_root / "deliveries" / "delivery_index_v2.json").unlink()
    result = svc.query_resend(snapshot_id=sid)
    assert result["index_source"] == "legacy_fallback"
    assert result["entries"][0]["resend_status"] == "可补发"

    shard = svc.resolve_shard(sid, 1)
    assert shard["verified"] is True
    assert shard["records"] == 1


def test_tampered_shard_is_not_downloadable(data_root):
    sid = _legacy_snapshot(data_root, "DOC-A", delivered_at="2026-10-01 10:00:00", receipt=None)
    svc = ResultDeliverService()
    svc.migrate_legacy_deliveries()

    # 篡改分片内容，校验值不再匹配。
    _write_jsonl(data_root / "deliveries" / f"{sid}_shard_001.jsonl", [
        {"type": "entity", "source": "x", "data": {"entity_id": "TAMPERED"}},
    ])
    blocked = svc.resolve_shard(sid, 1)
    assert "error" in blocked
    assert blocked["state"]["verified"] is False


def test_legacy_snapshot_delivery_is_preserved(data_root):
    sid = _legacy_snapshot(data_root, "DOC-A", delivered_at="2026-10-01 10:00:00", receipt=None)
    svc = ResultDeliverService()
    svc.migrate_legacy_deliveries()

    before = (data_root / "deliveries" / f"{sid}_shard_001.jsonl").read_bytes()
    result = svc.deliver_from_snapshot(sid)
    after = (data_root / "deliveries" / f"{sid}_shard_001.jsonl").read_bytes()

    assert result["preserved_legacy"] is True
    assert before == after, "重跑交付不得覆盖已发旧分片"


def test_rerun_migration_keeps_manual_review(data_root):
    sid = _legacy_snapshot(
        data_root, "DOC-C", delivered_at="2000-01-01 00:00:00", receipt=None, extra_entity=True,
    )
    svc = ResultDeliverService()
    svc.migrate_legacy_deliveries()
    artifact = svc.query_resend(snapshot_id=sid)["entries"][0]["pending_verification"][0]["artifact"]
    svc.mark_pending_review(sid, artifact, accepted=False)
    svc.record_receipt(sid, receipt_type="下载回执", received_by="知识库C")

    # 强制重跑迁移后，人工结论和补登回执都还在。
    svc.migrate_legacy_deliveries(force=True)
    entry = svc.query_resend(snapshot_id=sid)["entries"][0]
    assert entry["pending_verification"][0]["review_status"] == "已驳回"
    assert any(r["type"] == "下载回执" for r in entry["receipts"])
