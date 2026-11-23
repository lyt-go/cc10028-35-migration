"""交付索引 v2 迁移/补发/中断恢复的端到端验收脚本。

在临时数据目录中构造旧交付环境，覆盖：
1. 有回执的旧快照；
2. 只有下载回执、分片已缺失的历史条目；
3. 无回执的旧快照；
4. 新交付环境产生的 v2 快照；
5. 迁移中断后续跑；
6. 索引丢失（回滚）后仍能核实并下载旧分片。

脚本只通过 ResultDeliverService 的公开方法验证，不修改仓库内 data/。
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import app.services.base as base_mod  # noqa: E402
from app.services.result_deliver import ResultDeliverService  # noqa: E402


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def build_legacy_fixture(root: Path) -> None:
    """构造三份旧快照与一份新环境 v2 快照所需的磁盘产物。"""
    deliveries = root / "deliveries"
    versions = root / "versions"
    checkpoints = root / "checkpoints"
    entities = root / "entities"
    relations = root / "relations"

    # --- 快照 A：有回执，分片齐全 ---
    _write_jsonl(versions / "DOC-A_vv1.0.jsonl", [{
        "快照编号": "SNAP-DOC-A-v1.0", "文档编号": "DOC-A", "版本号": "v1.0",
        "分块数量": 4, "实体数量": 3, "生成时间": "2026-10-01 09:00:00",
    }])
    _write_jsonl(checkpoints / "version_DOC-A_v1.0.json", [])  # 目录占位无意义，下面写文件
    (checkpoints / "version_DOC-A_v1.0.json").write_text(json.dumps(
        {"doc_id": "DOC-A", "version": "v1.0", "pages": 2, "chunks": 4, "entities": 3, "triples": 2},
        ensure_ascii=False), encoding="utf-8")
    _write_jsonl(entities / "DOC-A_parsed_entities.jsonl", [
        {"entity_id": "E1", "chunk_id": "C1", "entity_type": "ORG", "entity_text": "甲公司", "confidence": 0.9},
        {"entity_id": "E2", "chunk_id": "C1", "entity_type": "PERSON", "entity_text": "张三", "confidence": 0.9},
    ])
    _write_jsonl(relations / "DOC-A_parsed_relations.jsonl", [
        {"relation_id": "R1", "head_entity": "甲公司", "relation_type": "EMPLOY", "tail_entity": "张三", "confidence": 0.8},
    ])
    _write_jsonl(deliveries / "SNAP-DOC-A-v1.0_delivery.jsonl", [{
        "交付编号": "DELIVER-SNAP-DOC-A-v1.0", "快照编号": "SNAP-DOC-A-v1.0",
        "交付格式": "jsonl", "目标系统": "下游知识库", "字段范围": "实体+关系",
        "记录条数": 3, "分片大小": 500, "交付时间": "2026-10-01 10:00:00",
    }])
    _write_jsonl(deliveries / "SNAP-DOC-A-v1.0_shard_001.jsonl", [
        {"type": "entity", "source": str(entities / "DOC-A_parsed_entities.jsonl"),
         "data": {"entity_id": "E1", "chunk_id": "C1", "entity_type": "ORG", "entity_text": "甲公司", "confidence": 0.9}},
        {"type": "entity", "source": str(entities / "DOC-A_parsed_entities.jsonl"),
         "data": {"entity_id": "E2", "chunk_id": "C1", "entity_type": "PERSON", "entity_text": "张三", "confidence": 0.9}},
        {"type": "relation", "source": str(relations / "DOC-A_parsed_relations.jsonl"),
         "data": {"relation_id": "R1", "head_entity": "甲公司", "relation_type": "EMPLOY", "tail_entity": "张三", "confidence": 0.8}},
    ])
    receipt_dir = deliveries / "receipts"
    receipt_dir.mkdir(parents=True, exist_ok=True)
    (receipt_dir / "SNAP-DOC-A-v1.0_receipt.json").write_text(json.dumps({
        "回执编号": "RCPT-A-01", "快照编号": "SNAP-DOC-A-v1.0", "回执类型": "接收回执",
        "回执时间": "2026-10-01 11:00:00", "接收方": "知识库A",
        "记录条数": 3,
    }, ensure_ascii=False), encoding="utf-8")

    # --- 快照 B：只有下载回执，分片已丢失 ---
    _write_jsonl(versions / "DOC-B_vv1.0.jsonl", [{
        "快照编号": "SNAP-DOC-B-v1.0", "文档编号": "DOC-B", "版本号": "v1.0",
        "分块数量": 2, "实体数量": 2, "生成时间": "2026-10-02 09:00:00",
    }])
    _write_jsonl(deliveries / "SNAP-DOC-B-v1.0_delivery.jsonl", [{
        "交付编号": "DELIVER-SNAP-DOC-B-v1.0", "快照编号": "SNAP-DOC-B-v1.0",
        "交付格式": "jsonl", "目标系统": "下游知识库", "字段范围": "实体+关系",
        "记录条数": 5, "分片大小": 500, "交付时间": "2026-10-02 10:00:00",
    }])
    (receipt_dir / "SNAP-DOC-B-v1.0_receipt.json").write_text(json.dumps({
        "回执编号": "RCPT-B-01", "快照编号": "SNAP-DOC-B-v1.0", "回执类型": "下载回执",
        "下载时间": "2026-10-02 10:30:00", "下载方": "知识库B", "记录条数": 5,
        "shards": [{"分片序号": 1, "文件名": "SNAP-DOC-B-v1.0_shard_001.jsonl",
                    "校验值": "deadbeefdeadbeef", "记录条数": 5}],
    }, ensure_ascii=False), encoding="utf-8")

    # --- 快照 C：无回执，分片存在 ---
    _write_jsonl(versions / "DOC-C_vv1.0.jsonl", [{
        "快照编号": "SNAP-DOC-C-v1.0", "文档编号": "DOC-C", "版本号": "v1.0",
        "分块数量": 3, "实体数量": 1, "生成时间": "2026-10-03 09:00:00",
    }])
    _write_jsonl(entities / "DOC-C_parsed_entities.jsonl", [
        {"entity_id": "E9", "chunk_id": "C9", "entity_type": "ORG", "entity_text": "丙公司", "confidence": 0.7},
        # 交付之后更正过的实体
        {"entity_id": "E10", "chunk_id": "C9", "entity_type": "ORG", "entity_text": "丁公司", "confidence": 0.99},
    ])
    _write_jsonl(deliveries / "SNAP-DOC-C-v1.0_delivery.jsonl", [{
        "交付编号": "DELIVER-SNAP-DOC-C-v1.0", "快照编号": "SNAP-DOC-C-v1.0",
        "交付格式": "jsonl", "目标系统": "下游知识库", "字段范围": "实体",
        "记录条数": 1, "分片大小": 500, "交付时间": "2026-10-03 10:00:00",
    }])
    _write_jsonl(deliveries / "SNAP-DOC-C-v1.0_shard_001.jsonl", [
        {"type": "entity", "source": str(entities / "DOC-C_parsed_entities.jsonl"),
         "data": {"entity_id": "E9", "chunk_id": "C9", "entity_type": "ORG", "entity_text": "丙公司", "confidence": 0.7}},
    ])


def build_v2_snapshot(service: ResultDeliverService, root: Path) -> str:
    """走新交付流水线产生一份 v2 快照。"""
    _write_jsonl(root / "versions" / "DOC-D_vv2.0.jsonl", [{
        "快照编号": "SNAP-DOC-D-v2.0", "文档编号": "DOC-D", "版本号": "v2.0",
        "分块数量": 2, "实体数量": 2, "生成时间": "2026-11-20 09:00:00",
    }])
    _write_jsonl(root / "entities" / "DOC-D_parsed_entities.jsonl", [
        {"entity_id": "E1", "chunk_id": "C1", "entity_type": "ORG", "entity_text": "戊公司", "confidence": 0.9},
        {"entity_id": "E2", "chunk_id": "C1", "entity_type": "PERSON", "entity_text": "李四", "confidence": 0.9},
    ])
    result = service.deliver_from_snapshot("SNAP-DOC-D-v2.0")
    assert result.get("index_version") == 2, result
    assert result["snapshot_kind"] == "v2", result
    return "SNAP-DOC-D-v2.0"


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="delivery-index-v2-"))
    try:
        base_mod.DATA_ROOT = tmp
        build_legacy_fixture(tmp)
        svc = ResultDeliverService()
        v2_sid = build_v2_snapshot(svc, tmp)

        summary = svc.index_summary()
        assert summary["source"] == "index_v2", summary
        assert summary["by_kind"].get("v2") == 1, summary

        # 1) 迁移旧快照
        migrated = svc.migrate_legacy_deliveries()
        assert migrated["migrated"] == 3, migrated
        summary = svc.index_summary()
        assert summary["by_kind"].get("legacy") == 3, summary

        # 2) 有回执的旧快照：可补发，含原回执
        resend_a = svc.query_resend(snapshot_id="SNAP-DOC-A-v1.0")["entries"][0]
        assert resend_a["resend_status"] == "可补发", resend_a
        assert resend_a["receipt"]["receipt_id"] == "RCPT-A-01", resend_a
        assert resend_a["snapshot_counts"]["entities"] == 3, resend_a["snapshot_counts"]
        assert {p["type"] for p in resend_a["recoverable_products"]} == {"entity", "relation"}

        # 3) 仅下载回执、分片丢失：只能查回执，不能补发
        resend_b = svc.query_resend(snapshot_id="SNAP-DOC-B-v1.0")["entries"][0]
        assert resend_b["resend_status"] == "仅回执可查", resend_b
        assert resend_b["receipt"]["type"] == "下载回执", resend_b

        # 4) 无回执的旧快照：分片可补发但需确认；交付后更正的实体只登记待核
        resend_c = svc.query_resend(snapshot_id="SNAP-DOC-C-v1.0")["entries"][0]
        assert resend_c["resend_status"] == "可补发", resend_c
        assert resend_c["receipt_available"] is False
        assert len(resend_c["pending_verification"]) == 1, resend_c["pending_verification"]
        assert resend_c["pending_verification"][0]["review_status"] == "待核"
        assert all(p["records"] == 1 for p in resend_c["recoverable_products"])
        assert resend_c["approved_supplements"] == []

        # 5) 新 v2 快照可查，且与旧快照共存
        resend_all = svc.query_resend(doc_id="DOC-D")
        assert resend_all["entries"][0]["snapshot_id"] == v2_sid
        assert resend_all["entries"][0]["snapshot_kind"] == "v2"
        assert svc.query_resend(doc_id="DOC-A")["entries"][0]["snapshot_kind"] == "legacy"

        # 6) 模拟迁移中断：只处理一份后中断，重跑应续上且不重复
        index_path = tmp / "deliveries" / "delivery_index_v2.json"
        index = json.loads(index_path.read_text(encoding="utf-8"))
        index["snapshots"].pop("SNAP-DOC-A-v1.0")
        index["snapshots"].pop("SNAP-DOC-C-v1.0")
        index["migration"] = {"status": "interrupted", "processed": ["SNAP-DOC-B-v1.0"]}
        index_path.write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")
        (tmp / "checkpoints" / "delivery_index_migration.json").write_text(json.dumps({
            "status": "interrupted", "processed": ["SNAP-DOC-B-v1.0"],
        }, ensure_ascii=False), encoding="utf-8")
        resumed = svc.migrate_legacy_deliveries(resume=True)
        assert resumed["migrated"] == 2, resumed
        summary = svc.index_summary()
        assert summary["by_kind"].get("legacy") == 3, summary
        assert summary["by_kind"].get("v2") == 1, summary

        # 7) 模拟回滚：索引丢失，旧文件仍可下载核实分片
        index_path.unlink()
        rollback = svc.query_resend(snapshot_id="SNAP-DOC-A-v1.0")
        assert rollback["index_source"] == "legacy_fallback", rollback
        assert rollback["entries"][0]["resend_status"] == "可补发", rollback
        shard = svc.resolve_shard("SNAP-DOC-A-v1.0", 1)
        assert shard["verified"] is True, shard
        assert shard["records"] == 3, shard

        # 8) 索引损坏的分片不可下载（回执校验值与旧文件不符）
        bad = svc.resolve_shard("SNAP-DOC-B-v1.0", 1)
        assert "error" in bad, bad

        # 9) 旧版成品与后来抽取分离：确认待核后才进入已确认补发
        svc.migrate_legacy_deliveries()
        confirm = svc.mark_pending_review(
            "SNAP-DOC-C-v1.0",
            resend_c["pending_verification"][0]["artifact"],
            accepted=True, reviewer="数据工程组",
        )
        assert confirm["review_status"] == "已确认", confirm
        after = svc.query_resend(snapshot_id="SNAP-DOC-C-v1.0")["entries"][0]
        assert len(after["approved_supplements"]) == 1, after["approved_supplements"]

        print("OK 交付索引 v2 迁移/补发/中断恢复/回滚下载 全部通过")
        return 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
