"""结果交付服务：抽取结果按交付格式向目标系统的分片导出与交付记录登记。

从版本快照读取产物，按指定格式和分片策略导出到目标系统。

历史交付采用「一快照一份交付记录 + 若干分片」的文件形态（兼容版本 v1）。
本服务在其上维护兼容版本的「交付索引 v2」：

* 索引按键为快照编号，可查当时实际交付的抽取产物（分片内的冻结副本）、
  当时文档计数（分页/分块/实体/三元组）与原始回执；
* 旧交付记录可迁移进索引，迁移按快照逐个原子落盘并写检查点，
  升级中断后重跑可续，回滚到旧读取方时旧文件仍原样保留；
* 补发查询区分「核实过的旧分片」「只有下载回执」「无回执」三种情况；
* 交付之后新产生的抽取产物（实体/关系被更正）只登记为「待核」，
  未经确认不会计入旧版成品，也不会参与补发；
* 分片下载在索引缺失或回滚时回退到旧文件命名，并用回执/索引里的
  校验值核实后再返回。
"""
from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService

INDEX_SCHEMA = "delivery-index"
INDEX_SCHEMA_VERSION = 2
LEGACY_COMPAT_VERSION = 1
SUPPORTED_INDEX_VERSIONS = (1, 2)
# 旧交付记录/快照里可能出现的回执字段，用于把「原回执」一并迁移。
RECEIPT_KEY_HINTS = ("回执", "receipt")


class ResultDeliverService(BaseModuleService):
    MODULE = "result_deliver"
    ENTITY = "交付任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["交付编号", "快照编号", "交付格式", "目标系统", "字段范围", "记录条数", "分片大小", "交付时间"]
    REQUIRED_FIELDS = ["交付编号", "快照编号", "交付格式"]
    STATUS_ORDER = ["草稿", "待导出", "导出中", "已交付"]
    ACTION_RULES = {"启动导出": "待导出", "确认交付": "已交付", "标记失败": "已交付"}
    NEGATIVE_ACTIONS = []

    SUPPORTED_FORMATS = {
        "jsonl": {"mime": "application/x-ndjson", "description": "每行一条JSON记录的文本格式"},
        "csv": {"mime": "text/csv", "description": "逗号分隔值表格格式"},
        "parquet": {"mime": "application/octet-stream", "description": "列式存储格式"},
        "xlsx": {"mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "description": "Excel表格格式"},
    }
# 数据操作：导出交付任务数据将版本快照产物分发到目标系统

    # ------------------------------------------------------------------
    # 交付入口（兼容旧产物命名）
    # ------------------------------------------------------------------

    def deliver_from_snapshot(self, snapshot_id: str, *, force: bool = False) -> dict[str, Any]:
        """从版本快照导出抽取产物到目标系统，支持分片分批交付。

        首次交付会同时写入旧的交付记录/分片文件（兼容 v1 读取方）和
        交付索引 v2。对已迁移的旧快照默认复用历史成品，绝不覆盖已发数据。
        """
        snapshot_data = self._find_snapshot(snapshot_id)
        if not snapshot_data:
            return {"error": f"版本快照 {snapshot_id} 不存在，请先创建版本快照"}

        index = self._load_index(create=True)
        existing = index["snapshots"].get(snapshot_id)
        if existing and existing.get("snapshot_kind") == "legacy" and not force:
            # 旧快照的成品是已经发出的历史数据，只允许查询/补发，不允许重算覆盖。
            return self._delivery_summary(existing, reused=True, preserved_legacy=True)
        if existing and not force and self._entry_shards_intact(existing):
            return self._delivery_summary(existing, reused=True)

        doc_id = snapshot_data.get("文档编号", "")
        fmt = "jsonl"
        target = "下游知识库"

        # 收集所有可交付的抽取产物
        deliverables = self._collect_deliverables(doc_id)

        if not deliverables:
            return {"error": f"快照 {snapshot_id} 无可交付的抽取产物"}

        shard_size = 500
        shards = []
# 数据产物：交付分片文件写入deliveries目录持久化文件
        batch = []
        for item in deliverables:
            batch.append(item)
            if len(batch) >= shard_size:
                shards.append(batch)
                batch = []
        if batch:
            shards.append(batch)

        # 覆盖性重导时清掉同快照的旧分片，避免残留序号更高的过期分片。
        if force:
            for stale in sorted(self._data_dir("deliveries").glob(f"{snapshot_id}_shard_*.jsonl")):
                stale.unlink(missing_ok=True)

        # 写入交付产物文件（原子替换，升级中断不会留下半截分片）
        output_files = []
        shard_metas = []
        for i, shard in enumerate(shards):
            output_path = self._data_dir("deliveries") / f"{snapshot_id}_shard_{i+1:03d}.jsonl"
            self._write_jsonl_atomic(output_path, shard)
            output_files.append(str(output_path))
            shard_metas.append(self._shard_meta(output_path, i + 1, shard))

        delivery = {
            "交付编号": f"DELIVER-{snapshot_id}",
            "快照编号": snapshot_id,
            "交付格式": fmt,
            "目标系统": target,
            "字段范围": "实体+关系+三元组+向量",
            "记录条数": len(deliverables),
            "分片大小": shard_size,
            "交付时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            # 兼容版本标记：旧读取方仍然只认上面的中文键。
            "索引版本": INDEX_SCHEMA_VERSION,
            "兼容版本": LEGACY_COMPAT_VERSION,
        }

        self._write_jsonl_atomic(
            self._data_dir("deliveries") / f"{snapshot_id}_delivery.jsonl",
            [delivery]
        )

        entry = self._build_v2_entry(
            snapshot_id=snapshot_id,
            snapshot=snapshot_data,
            delivery=delivery,
            shards=shard_metas,
            deliverables=deliverables,
        )
        index["snapshots"][snapshot_id] = entry
        self._save_index(index)

        self._write_checkpoint(f"deliver_{snapshot_id}", {
            "snapshot_id": snapshot_id,
            "format": fmt,
            "target": target,
            "total_records": len(deliverables),
            "shards": len(shards),
            "index_version": INDEX_SCHEMA_VERSION,
        })

        result = self._delivery_summary(entry)
        result["output_files"] = output_files
        return result

    def _find_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        """按快照编号在 versions 目录查找快照记录。"""
        for vf in sorted(self._data_dir("versions").glob("*.jsonl")):
            for snap in self._read_jsonl(vf):
                if snap.get("快照编号") == snapshot_id:
                    return snap
        return None

    # ------------------------------------------------------------------
    # 交付索引 v2：读取、写入、快照条目
    # ------------------------------------------------------------------

    def _index_path(self) -> Path:
        return self._data_dir("deliveries") / "delivery_index_v2.json"

    def _receipt_dir(self) -> Path:
        return self._data_dir("deliveries") / "receipts"

    def _empty_index(self) -> dict[str, Any]:
        return {
            "schema": INDEX_SCHEMA,
            "schema_version": INDEX_SCHEMA_VERSION,
            "compatible_readers": list(SUPPORTED_INDEX_VERSIONS),
            "generated_at": self._now(),
            "snapshots": {},
            "migration": {"status": "not_started"},
        }

    def _load_index(self, *, create: bool = False) -> dict[str, Any]:
        """读取索引 v2；缺失或损坏时返回空索引（create=True 时不落盘）。"""
        payload = self._read_json(self._index_path())
        if not payload:
            return self._empty_index()
        version = payload.get("schema_version")
        if version not in SUPPORTED_INDEX_VERSIONS:
            return self._empty_index()
        payload.setdefault("schema", INDEX_SCHEMA)
        payload.setdefault("snapshots", {})
        payload.setdefault("migration", {"status": "unknown"})
        if create:
            payload["schema_version"] = INDEX_SCHEMA_VERSION
            payload["compatible_readers"] = list(SUPPORTED_INDEX_VERSIONS)
        return payload

    def _save_index(self, index: dict[str, Any]) -> None:
        index["schema"] = INDEX_SCHEMA
        index["schema_version"] = INDEX_SCHEMA_VERSION
        index["compatible_readers"] = list(SUPPORTED_INDEX_VERSIONS)
        index["generated_at"] = self._now()
        self._write_json_atomic(self._index_path(), index)

    def _index_view(self) -> tuple[dict[str, Any], str]:
        """读取索引视图；索引缺失（例如回滚删档）时从旧交付文件临时重建。

        返回 (index, source)，source 为 ``index_v2`` 或 ``legacy_fallback``。
        """
        if self._index_path().exists():
            index = self._load_index()
            if index["snapshots"]:
                return index, "index_v2"
        return self._rebuild_index_from_legacy(), "legacy_fallback"

    def index_summary(self) -> dict[str, Any]:
        """输出索引概况，供交付流水线与运维核对。"""
        index, source = self._index_view()
        entries = index.get("snapshots", {})
        by_kind: dict[str, int] = {}
        for entry in entries.values():
            kind = entry.get("snapshot_kind", "unknown")
            by_kind[kind] = by_kind.get(kind, 0) + 1
        return {
            "schema": INDEX_SCHEMA,
            "schema_version": INDEX_SCHEMA_VERSION,
            "compatible_versions": list(SUPPORTED_INDEX_VERSIONS),
            "source": source,
            "snapshot_count": len(entries),
            "by_kind": by_kind,
            "migration": index.get("migration", {}),
            "snapshots": [
                {
                    "snapshot_id": sid,
                    "doc_id": entry.get("doc_id"),
                    "version": entry.get("version"),
                    "snapshot_kind": entry.get("snapshot_kind"),
                    "record_count": entry.get("record_count"),
                    "receipt_count": len(entry.get("receipts", [])),
                    "shard_count": len(entry.get("shards", [])),
                    "pending_count": len(entry.get("pending_verification", [])),
                }
                for sid, entry in sorted(entries.items())
            ],
        }

    def _build_v2_entry(
        self,
        *,
        snapshot_id: str,
        snapshot: dict[str, Any],
        delivery: dict[str, Any],
        shards: list[dict[str, Any]],
        deliverables: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """由本次交付构造索引 v2 条目，并生成分片关联明细。"""
        doc_id = snapshot.get("文档编号", "")
        version = snapshot.get("版本号", "")
        counts = self._snapshot_counts(snapshot)
        shard_associations = self._shard_associations(shards)
        return {
            "snapshot_id": snapshot_id,
            "doc_id": doc_id,
            "version": version,
            "snapshot_kind": "v2",
            "index_version": INDEX_SCHEMA_VERSION,
            "compat_version": LEGACY_COMPAT_VERSION,
            "delivery_id": delivery.get("交付编号"),
            "format": delivery.get("交付格式", "jsonl"),
            "target": delivery.get("目标系统", "下游知识库"),
            "field_scope": delivery.get("字段范围", ""),
            "record_count": delivery.get("记录条数", len(deliverables)),
            "shard_size": delivery.get("分片大小", 500),
            "delivered_at": delivery.get("交付时间"),
            "snapshot_counts": counts,
            "products": self._products_from_shards(shards),
            "shards": shards,
            "shard_associations": shard_associations,
            "receipts": [],
            "receipt": None,
            "pending_verification": [],
            "recoverable": all(s.get("checksum") for s in shards),
            "migrated_at": None,
            "delivered_index_at": self._now(),
            "migration": {"status": "completed", "source": "delivery-pipeline", "compat_version": LEGACY_COMPAT_VERSION},
        }

    def _merge_review_state(
        self,
        fresh: list[dict[str, Any]],
        previous: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """重算待核清单时保留人工已下的复核结论，避免重跑迁移把确认打回待核。"""
        decided = {
            p.get("artifact"): p for p in previous
            if p.get("review_status") in ("已确认", "已驳回")
        }
        for item in fresh:
            prior = decided.get(item.get("artifact"))
            if prior:
                for key in ("review_status", "attributed_to_snapshot", "reviewer", "reviewed_at", "review_note"):
                    if key in prior:
                        item[key] = prior[key]
        return fresh

    @staticmethod
    def _merge_receipts(
        fresh: list[dict[str, Any]],
        previous: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """合并已登记的接收方回执，按回执编号去重。"""
        by_id = {str(r.get("receipt_id")): r for r in fresh}
        for receipt in previous:
            by_id.setdefault(str(receipt.get("receipt_id")), receipt)
        return sorted(by_id.values(), key=lambda r: str(r.get("received_at") or ""))

    def _snapshot_counts(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        """取快照当时的文档计数；缺失字段回落到版本检查点。"""
        counts = {
            "pages": snapshot.get("页数", snapshot.get("pages")),
            "chunks": snapshot.get("分块数量"),
            "entities": snapshot.get("实体数量"),
            "triples": snapshot.get("三元组数量"),
        }
        doc_id = snapshot.get("文档编号", "")
        version = snapshot.get("版本号", "")
        if doc_id and version and any(v is None for v in counts.values()):
            cp = self._read_checkpoint(f"version_{doc_id}_{version}") or {}
            for key in ("pages", "chunks", "entities", "triples"):
                if counts.get(key) is None and cp.get(key) is not None:
                    counts[key] = cp[key]
        return counts

    def _shard_meta(self, path: Path, shard_no: int, records: list[dict[str, Any]]) -> dict[str, Any]:
        """生成单个分片的关联元数据（路径、校验值、条数、类型分布）。"""
        type_counts: dict[str, int] = {}
        sources: set[str] = set()
        for rec in records:
            rtype = str(rec.get("type", "unknown"))
            type_counts[rtype] = type_counts.get(rtype, 0) + 1
            if rec.get("source"):
                sources.add(str(rec["source"]))
        return {
            "shard_no": shard_no,
            "filename": path.name,
            "path": str(path),
            "checksum": self._file_checksum(path),
            "records": len(records),
            "type_counts": type_counts,
            "sources": sorted(sources),
        }

    def _shard_associations(self, shards: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """交付流水线的分片关联明细：每个分片对应哪些产物类型与源文件。"""
        return [
            {
                "shard_no": s.get("shard_no"),
                "filename": s.get("filename"),
                "records": s.get("records"),
                "type_counts": s.get("type_counts", {}),
                "source_artifacts": s.get("sources", []),
                "checksum": s.get("checksum"),
                "verified": bool(s.get("checksum")),
            }
            for s in shards
        ]

    def _products_from_shards(self, shards: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按产物类型汇总可找回的成品（以分片内冻结副本为准）。"""
        products: dict[str, dict[str, Any]] = {}
        for s in shards:
            for rtype, count in (s.get("type_counts") or {}).items():
                item = products.setdefault(rtype, {
                    "type": rtype,
                    "records": 0,
                    "shard_nos": [],
                    "origin": "delivered-shard",
                    "deliverable": True,
                })
                item["records"] += int(count)
                if s.get("shard_no") not in item["shard_nos"]:
                    item["shard_nos"].append(s.get("shard_no"))
        return [products[k] for k in sorted(products)]

    # ------------------------------------------------------------------
    # 旧交付迁移
    # ------------------------------------------------------------------

    def migrate_legacy_deliveries(
        self,
        *,
        force: bool = False,
        resume: bool = True,
    ) -> dict[str, Any]:
        """把旧快照的文档计数与交付明细迁移到兼容版本的交付索引 v2。

        每个快照处理完立即原子写入索引并推进检查点，因此升级中断后
        已迁移的快照仍可查询/下载；重跑时跳过已完成的快照继续迁移。
        """
        legacy_records = self._discover_legacy_deliveries()
        if not legacy_records:
            return {
                "ok": True,
                "migrated": 0,
                "skipped": 0,
                "message": "未发现旧交付记录",
                "index_version": INDEX_SCHEMA_VERSION,
            }

        index = self._load_index(create=True)
        snapshots = index.setdefault("snapshots", {})
        previous = self._read_checkpoint("delivery_index_migration") or {}
        processed_before = list(previous.get("processed", [])) if resume else []

        index["migration"] = {
            "status": "running",
            "compat_version": LEGACY_COMPAT_VERSION,
            "source": "legacy-delivery-records",
            "started_at": previous.get("started_at") or self._now(),
            "updated_at": self._now(),
            "total": len(legacy_records),
            "processed_before_resume": len(processed_before),
        }
        self._save_index(index)

        processed: list[str] = []
        skipped: list[str] = []
        try:
            for snapshot_id, record in legacy_records:
                entry = snapshots.get(snapshot_id)
                if entry and entry.get("snapshot_kind") == "v2":
                    # 新交付环境产生的快照不参与旧数据迁移，避免反向覆盖。
                    skipped.append(snapshot_id)
                    continue
                if (
                    entry
                    and resume
                    and not force
                    and entry.get("migration", {}).get("status") == "completed"
                    and snapshot_id in processed_before
                ):
                    skipped.append(snapshot_id)
                    continue

                migrated = self._build_legacy_entry(snapshot_id, record)
                if entry:
                    # 重跑迁移时保留已有人工复核结论与手工补登的回执。
                    migrated["pending_verification"] = self._merge_review_state(
                        migrated["pending_verification"], entry.get("pending_verification", [])
                    )
                    migrated["receipts"] = self._merge_receipts(
                        migrated["receipts"], entry.get("receipts", [])
                    )
                    migrated["receipt"] = migrated["receipts"][0] if migrated["receipts"] else None
                snapshots[snapshot_id] = migrated
                # 每迁移一个快照就原子落盘一次：中断不破坏此前成果。
                self._save_index(index)
                processed.append(snapshot_id)
                self._write_checkpoint("delivery_index_migration", {
                    "status": "running",
                    "started_at": index["migration"]["started_at"],
                    "compat_version": LEGACY_COMPAT_VERSION,
                    "processed": processed_before + processed,
                    "skipped": skipped,
                    "total": len(legacy_records),
                    "last_snapshot": snapshot_id,
                })
        except Exception:
            index["migration"].update({
                "status": "interrupted",
                "updated_at": self._now(),
                "processed": processed_before + processed,
                "skipped": skipped,
            })
            # 尽量记录中断位置；保存失败时保留上一份完整索引。
            try:
                self._save_index(index)
            except Exception:
                pass
            self._write_checkpoint("delivery_index_migration", {
                "status": "interrupted",
                "compat_version": LEGACY_COMPAT_VERSION,
                "processed": processed_before + processed,
                "skipped": skipped,
                "total": len(legacy_records),
            })
            raise

        index["migration"].update({
            "status": "completed",
            "compat_version": LEGACY_COMPAT_VERSION,
            "updated_at": self._now(),
            "processed": processed_before + processed,
            "skipped": skipped,
            "migrated": len(processed),
            "resumed": bool(processed_before),
        })
        self._save_index(index)
        self._write_checkpoint("delivery_index_migration", {
            "status": "completed",
            "compat_version": LEGACY_COMPAT_VERSION,
            "processed": processed_before + processed,
            "skipped": skipped,
            "total": len(legacy_records),
        })

        return {
            "ok": True,
            "index_version": INDEX_SCHEMA_VERSION,
            "compat_version": LEGACY_COMPAT_VERSION,
            "discovered": len(legacy_records),
            "migrated": len(processed),
            "skipped": len(skipped),
            "processed": processed,
            "migration": index["migration"],
        }

    def _discover_legacy_deliveries(self) -> list[tuple[str, dict[str, Any]]]:
        """发现旧交付记录；没有记录但有分片的快照也纳入迁移。"""
        base = self._data_dir("deliveries")
        records: dict[str, dict[str, Any]] = {}

        for path in sorted(base.glob("*_delivery.jsonl")):
            for rec in self._read_jsonl(path):
                snapshot_id = str(rec.get("快照编号") or "").strip()
                if not snapshot_id:
                    continue
                # 旧文件是追加写的，同一快照可能多行，取最后一条（最新交付）。
                records[snapshot_id] = dict(rec)

        # 只有分片、没有交付记录的旧快照也要能查回。
        for path in sorted(base.glob("*_shard_*.jsonl")):
            snapshot_id = self._snapshot_from_filename(path.name)
            if snapshot_id and snapshot_id not in records:
                records[snapshot_id] = {
                    "快照编号": snapshot_id,
                    "交付编号": f"DELIVER-{snapshot_id}",
                    "交付格式": "jsonl",
                    "目标系统": "下游知识库",
                    "记录条数": None,
                    "交付时间": None,
                    "来源": "仅分片文件",
                }

        return sorted(records.items(), key=lambda item: item[0])

    def _build_legacy_entry(self, snapshot_id: str, record: dict[str, Any]) -> dict[str, Any]:
        """构造单个旧快照的索引 v2 条目：文档计数、交付明细、原回执、待核归属。"""
        snapshot = self._find_snapshot(snapshot_id) or {}
        doc_id = snapshot.get("文档编号") or self._doc_id_from_snapshot(snapshot_id)
        version = snapshot.get("版本号") or self._version_from_snapshot(snapshot_id)

        shards = self._scan_legacy_shards(snapshot_id)
        shard_records = self._read_shard_records(shards)
        receipts = self._receipts_for(snapshot_id, record)
        delivered_by_source = self._delivered_source_counts(shard_records)
        pending = self._detect_pending_attribution(
            doc_id, delivered_by_source, shard_records, record.get("交付时间")
        )

        record_count = record.get("记录条数")
        if record_count in (None, ""):
            record_count = sum(s["records"] for s in shards)
        counts = self._snapshot_counts(snapshot) if snapshot else {
            "pages": None, "chunks": None, "entities": None, "triples": None,
        }
        counts["delivered_records"] = record_count
        counts["delivered_by_type"] = self._type_counts(shard_records)

        entry = {
            "snapshot_id": snapshot_id,
            "doc_id": doc_id,
            "version": version,
            "snapshot_kind": "legacy",
            "index_version": INDEX_SCHEMA_VERSION,
            "compat_version": LEGACY_COMPAT_VERSION,
            "delivery_id": record.get("交付编号") or f"DELIVER-{snapshot_id}",
            "format": record.get("交付格式", "jsonl"),
            "target": record.get("目标系统", "下游知识库"),
            "field_scope": record.get("字段范围", ""),
            "record_count": record_count,
            "shard_size": record.get("分片大小"),
            "delivered_at": record.get("交付时间"),
            "snapshot_counts": counts,
            "products": self._products_from_shards(shards),
            "shards": shards,
            "shard_associations": self._shard_associations(shards),
            "receipts": receipts,
            "receipt": receipts[0] if receipts else None,
            "pending_verification": pending,
            "recoverable": any(s.get("sha256") for s in shards),
            "migrated_at": self._now(),
            "delivered_index_at": None,
            "migration": {
                "status": "completed",
                "source": "legacy-delivery-record",
                "compat_version": LEGACY_COMPAT_VERSION,
                "record_source": record.get("来源", "交付记录"),
            },
        }
        return entry

    def _scan_legacy_shards(self, snapshot_id: str) -> list[dict[str, Any]]:
        """扫描旧快照分片，计算校验值并统计记录数和类型分布。"""
        pattern = re.compile(rf"^{re.escape(snapshot_id)}_shard_(\d+)\.jsonl$")
        metas: list[dict[str, Any]] = []
        for path in sorted(self._data_dir("deliveries").glob(f"{snapshot_id}_shard_*.jsonl")):
            match = pattern.match(path.name)
            shard_no = int(match.group(1)) if match else len(metas) + 1
            records = self._read_jsonl(path)
            meta = self._shard_meta(path, shard_no, records)
            meta["sha256"] = meta["checksum"]
            metas.append(meta)
        return sorted(metas, key=lambda m: int(m.get("shard_no", 0)))

    def _read_shard_records(self, shards: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """读取分片内的冻结记录，作为旧版成品的唯一依据。"""
        records: list[dict[str, Any]] = []
        for meta in shards:
            path = Path(str(meta.get("path", "")))
            if path.exists():
                records.extend(self._read_jsonl(path))
        return records

    def _delivered_source_counts(self, shard_records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """按源文件（同时记录绝对路径与文件名）汇总旧交付条数。"""
        counts: dict[str, dict[str, Any]] = {}
        for rec in shard_records:
            source = str(rec.get("source") or "")
            if not source:
                continue
            for key in {source, Path(source).name}:
                item = counts.setdefault(key, {"records": 0, "type": rec.get("type")})
                item["records"] += 1
        return counts

    def _type_counts(self, shard_records: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for rec in shard_records:
            rtype = str(rec.get("type", "unknown"))
            counts[rtype] = counts.get(rtype, 0) + 1
        return counts

    def _detect_pending_attribution(
        self,
        doc_id: str,
        delivered_by_source: dict[str, dict[str, Any]],
        shard_records: list[dict[str, Any]],
        delivered_at: str | None = None,
    ) -> list[dict[str, Any]]:
        """识别交付之后新增/变更的抽取产物，只登记为「待核」，不计入旧版成品。"""
        pending: list[dict[str, Any]] = []
        if not doc_id:
            return pending

        delivered_digests = self._delivered_source_digests(shard_records)
        mappings = (
            ("entities", "entity"),
            ("relations", "relation"),
            ("vectors", "vector"),
            ("graph", "triple"),
        )
        for sub, artifact_type in mappings:
            for path in sorted(self._data_dir(sub).glob(f"{doc_id}*.jsonl")):
                live_records = self._read_jsonl(path)
                delivered = delivered_by_source.get(str(path)) or delivered_by_source.get(path.name)
                if delivered is None:
                    # 旧交付分片里没有该产物：只有交付之后生成/改写的才可能是「后来抽取」。
                    if not self._changed_after(path, delivered_at):
                        continue
                    pending.append({
                        "artifact": str(path),
                        "artifact_type": artifact_type,
                        "status": "后来新增",
                        "delivered_records": 0,
                        "current_records": len(live_records),
                        "attributed_to_snapshot": False,
                        "review_status": "待核",
                        "reason": "旧交付分片未包含该产物，未经证实不得作为旧版成品补发",
                    })
                    continue

                delivered_count = int(delivered.get("records", 0))
                digest = self._records_digest(artifact_type, live_records)
                delivered_digest = delivered_digests.get(str(path)) or delivered_digests.get(path.name)
                changed = delivered_digest is not None and digest != delivered_digest
                if delivered_count != len(live_records) or changed:
                    pending.append({
                        "artifact": str(path),
                        "artifact_type": artifact_type,
                        "status": "已变更待核",
                        "delivered_records": delivered_count,
                        "current_records": len(live_records),
                        "attributed_to_snapshot": False,
                        "review_status": "待核",
                        "reason": "交付后实体/关系被更正，变更部分未经证实的归属",
                    })
        return pending

    def _changed_after(self, path: Path, delivered_at: str | None) -> bool:
        """判断产物是否在交付时间之后被写入；缺少交付时间时保守视为待核。"""
        if not path.exists():
            return False
        if not delivered_at:
            return True
        try:
            cutoff = time.mktime(time.strptime(delivered_at, "%Y-%m-%d %H:%M:%S"))
        except (ValueError, TypeError):
            return True
        return path.stat().st_mtime > cutoff + 1

    def _delivered_source_digests(self, shard_records: list[dict[str, Any]]) -> dict[str, str]:
        """按源文件对分片内冻结副本求摘要，用来判断现场文件是否被改写。

        分片里已按交付口径投影过字段（向量只留索引相关字段），此处沿用
        同一投影，避免因字段裁剪误报为变更。
        """
        grouped: dict[str, list[dict[str, Any]]] = {}
        for rec in shard_records:
            source = str(rec.get("source") or "")
            if not source:
                continue
            artifact_type = str(rec.get("type", ""))
            data = rec.get("data", rec)
            projected = self._project_for_digest(artifact_type, data)
            for key in {source, Path(source).name}:
                grouped.setdefault(key, []).append(projected)
        return {key: self._digest_of(records) for key, records in grouped.items()}

    def _records_digest(self, artifact_type: str, records: list[dict[str, Any]]) -> str:
        """对现场记录按交付口径投影后求摘要。"""
        projected = [self._project_for_digest(artifact_type, rec) for rec in records]
        return self._digest_of(projected)

    @staticmethod
    def _digest_of(records: list[dict[str, Any]]) -> str:
        import json as _json
        import hashlib as _hashlib

        normalized = sorted(
            _json.dumps(rec, ensure_ascii=False, sort_keys=True, default=str)
            for rec in records
        )
        return _hashlib.sha256("\n".join(normalized).encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _project_for_digest(artifact_type: str, rec: dict[str, Any]) -> dict[str, Any]:
        """投影出与交付分片一致的字段，向量交付只导出索引相关字段。"""
        if artifact_type == "vector":
            return {
                "chunk_id": rec.get("chunk_id"),
                "doc_id": rec.get("doc_id"),
                "dimension": rec.get("dimension", rec.get("vector_dim")),
                "index": rec.get("index"),
            }
        return rec

    # ------------------------------------------------------------------
    # 回执
    # ------------------------------------------------------------------

    def _receipts_for(self, snapshot_id: str, delivery_record: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """收集旧快照的原始回执：独立回执文件 + 交付记录内嵌回执。"""
        receipts: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()

        for path in self._iter_receipt_files():
            payloads: list[dict[str, Any]] = []
            if path.suffix == ".jsonl":
                payloads = self._read_jsonl(path)
            else:
                data = self._read_json(path)
                if data:
                    payloads = [data]
            for payload in payloads:
                receipt = self._normalise_receipt(payload, snapshot_id, path.name)
                if receipt and self._receipt_key(receipt) not in seen:
                    seen.add(self._receipt_key(receipt))
                    receipts.append(receipt)

        # 旧交付记录本身可能内嵌了下载回执字段。
        if delivery_record:
            embedded = {k: v for k, v in delivery_record.items()
                        if any(hint in str(k) for hint in RECEIPT_KEY_HINTS)}
            if embedded:
                receipt = self._normalise_receipt(embedded, snapshot_id, "交付记录内嵌回执")
                if receipt and self._receipt_key(receipt) not in seen:
                    seen.add(self._receipt_key(receipt))
                    receipts.append(receipt)

        return receipts

    def _iter_receipt_files(self) -> list[Path]:
        base = self._data_dir("deliveries")
        files: list[Path] = []
        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            if "index" in path.parts:
                continue
            name = path.name.lower()
            if path.parent.name == "receipts" or "receipt" in name or "回执" in path.name:
                if path.suffix in (".json", ".jsonl"):
                    files.append(path)
        return files

    @staticmethod
    def _receipt_key(receipt: dict[str, Any]) -> tuple[str, str, str]:
        return (
            str(receipt.get("receipt_id", "")),
            str(receipt.get("type", "")),
            str(receipt.get("received_at", "")),
        )

    def _normalise_receipt(
        self,
        payload: dict[str, Any],
        snapshot_hint: str,
        source_name: str,
    ) -> dict[str, Any] | None:
        """把不同年代/不同字段名的回执统一成索引内的结构。"""
        snapshot_id = (
            payload.get("快照编号") or payload.get("snapshot_id") or ""
        )
        if not snapshot_id:
            # 从文件名前缀推断快照编号（文件名以快照编号开头）。
            known = self._known_snapshot_ids()
            for candidate in sorted(known, key=len, reverse=True):
                if source_name.startswith(candidate):
                    snapshot_id = candidate
                    break
        if not snapshot_id:
            # 无法判断回执归属时宁可不迁，避免把别的快照回执记到本快照名下。
            return None
        if snapshot_id != snapshot_hint:
            return None

        raw_shards = payload.get("shards") or payload.get("分片") or payload.get("shard_list") or []
        shards = []
        if isinstance(raw_shards, list):
            for idx, item in enumerate(raw_shards, start=1):
                if isinstance(item, dict):
                    shards.append({
                        "shard_no": item.get("shard_no", item.get("分片序号", idx)),
                        "filename": item.get("filename", item.get("文件名")),
                        "checksum": item.get("checksum", item.get("校验值")),
                        "records": item.get("records", item.get("记录条数")),
                    })
                elif isinstance(item, str):
                    shards.append({"shard_no": idx, "filename": item, "checksum": None, "records": None})

        receipt_text = str(payload)
        rtype = payload.get("回执类型") or payload.get("receipt_type")
        if not rtype:
            rtype = "下载回执" if ("下载" in receipt_text or "download" in receipt_text.lower()) else "接收回执"

        return {
            "receipt_id": payload.get("回执编号") or payload.get("receipt_id") or f"RCPT-{snapshot_id}",
            "snapshot_id": snapshot_id,
            "type": rtype,
            "received_at": payload.get("回执时间") or payload.get("received_at") or payload.get("下载时间"),
            "received_by": payload.get("接收方") or payload.get("received_by") or payload.get("下载方"),
            "record_count": payload.get("记录条数") or payload.get("record_count"),
            "shards": shards,
            "source": source_name,
            "raw_present": True,
        }

    def _known_snapshot_ids(self) -> set[str]:
        ids: set[str] = set()
        for path in self._data_dir("deliveries").glob("*_delivery.jsonl"):
            for rec in self._read_jsonl(path):
                if rec.get("快照编号"):
                    ids.add(str(rec["快照编号"]))
        for path in self._data_dir("deliveries").glob("*_shard_*.jsonl"):
            sid = self._snapshot_from_filename(path.name)
            if sid:
                ids.add(sid)
        for path in self._data_dir("versions").glob("*.jsonl"):
            for rec in self._read_jsonl(path):
                if rec.get("快照编号"):
                    ids.add(str(rec["快照编号"]))
        return ids

    def record_receipt(
        self,
        snapshot_id: str,
        *,
        receipt_type: str = "接收回执",
        received_by: str = "接收方",
        shards: list[dict[str, Any]] | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        """登记接收方回执（含下载回执），并写回索引 v2。"""
        index = self._load_index(create=True)
        entry = index["snapshots"].get(snapshot_id)
        if entry is None:
            entry = self._ephemeral_entry(snapshot_id)
        if entry is None:
            return {"error": f"快照 {snapshot_id} 不存在交付索引，无法登记回执"}

        if shards is None:
            shards = [
                {
                    "shard_no": s.get("shard_no"),
                    "filename": s.get("filename"),
                    "checksum": s.get("checksum"),
                    "records": s.get("records"),
                }
                for s in entry.get("shards", [])
            ]

        seq = len(entry.get("receipts", [])) + 1
        receipt = {
            "receipt_id": f"RCPT-{snapshot_id}-{seq:02d}",
            "snapshot_id": snapshot_id,
            "type": receipt_type,
            "received_at": self._now(),
            "received_by": received_by,
            "record_count": entry.get("record_count"),
            "shards": shards,
            "note": note,
            "source": "receiver",
            "raw_present": True,
        }
        receipt_path = self._receipt_dir() / f"{snapshot_id}_receipt_{seq:02d}.json"
        self._write_json_atomic(receipt_path, receipt)

        entry.setdefault("receipts", []).append(receipt)
        entry["receipt"] = entry["receipts"][0]
        index["snapshots"][snapshot_id] = entry
        self._save_index(index)
        return {"ok": True, "receipt": receipt, "output": str(receipt_path)}

    # ------------------------------------------------------------------
    # 补发查询
    # ------------------------------------------------------------------

    def query_resend(
        self,
        *,
        doc_id: str | None = None,
        snapshot_id: str | None = None,
    ) -> dict[str, Any]:
        """查询可补发的旧结果：区分核实分片、仅下载回执、无回执与待核产物。"""
        index, source = self._index_view()
        entries = index.get("snapshots", {})

        if snapshot_id:
            selected = [entries[snapshot_id]] if snapshot_id in entries else []
            if not selected:
                fallback = self._ephemeral_entry(snapshot_id)
                selected = [fallback] if fallback else []
        elif doc_id:
            selected = [e for e in entries.values() if e.get("doc_id") == doc_id]
        else:
            selected = list(entries.values())

        if snapshot_id and not selected:
            return {"error": f"快照 {snapshot_id} 没有可补发的交付记录", "index_source": source}
        if doc_id and not selected:
            # 索引缺失时按文档从旧文件兜底重建。
            selected = self._rebuild_index_from_legacy(doc_id=doc_id).get("snapshots", {})
            selected = list(selected.values())

        selected = sorted(selected, key=lambda e: (str(e.get("doc_id", "")), str(e.get("version", "")), str(e.get("snapshot_id", ""))))
        results = [self._resend_entry(entry) for entry in selected]

        summary = {"可补发": 0, "仅回执可查": 0, "不可补发": 0}
        for item in results:
            summary[item["resend_status"]] = summary.get(item["resend_status"], 0) + 1

        return {
            "ok": True,
            "index_source": source,
            "index_version": INDEX_SCHEMA_VERSION,
            "compat_version": LEGACY_COMPAT_VERSION,
            "query": {"doc_id": doc_id, "snapshot_id": snapshot_id},
            "coexisting_versions": sorted({str(e.get("version", "")) for e in selected}),
            "summary": summary,
            "entries": results,
        }

    def _resend_entry(self, entry: dict[str, Any]) -> dict[str, Any]:
        """基于当前磁盘状态刷新单个快照的分片核实结果与补发结论。"""
        shard_states = [self._shard_state(meta, entry) for meta in entry.get("shards", [])]
        verified = [s for s in shard_states if s["verified"]]
        receipts = entry.get("receipts", [])
        has_receipt = bool(receipts)

        if verified:
            status = "可补发"
            if len(verified) < len(shard_states):
                note = f"{len(shard_states) - len(verified)} 个分片缺失或校验不符，仅可补发核实过的分片"
            elif has_receipt:
                note = "旧分片校验通过，且具备原始回执"
            else:
                note = "旧分片校验通过，但缺少回执，补发前需接收方确认"
        elif receipts:
            status = "仅回执可查"
            note = "分片文件缺失，只有回执可证明历史交付，无法补发原始分片"
        else:
            status = "不可补发"
            note = "既无可用分片也无回执，无法确认旧版成品"

        pending = entry.get("pending_verification", [])
        return {
            "snapshot_id": entry.get("snapshot_id"),
            "doc_id": entry.get("doc_id"),
            "version": entry.get("version"),
            "snapshot_kind": entry.get("snapshot_kind"),
            "index_version": entry.get("index_version"),
            "delivery_id": entry.get("delivery_id"),
            "record_count": entry.get("record_count"),
            "delivered_at": entry.get("delivered_at"),
            "snapshot_counts": entry.get("snapshot_counts", {}),
            "recoverable_products": [
                {
                    "type": p.get("type"),
                    "records": p.get("records"),
                    "shard_nos": p.get("shard_nos", []),
                    "origin": p.get("origin", "delivered-shard"),
                }
                for p in entry.get("products", [])
            ],
            "shards": shard_states,
            "shard_associations": entry.get("shard_associations", []),
            "receipt": entry.get("receipt"),
            "receipts": receipts,
            "receipt_available": has_receipt,
            "pending_verification": pending,
            "approved_supplements": [
                p for p in pending
                if p.get("review_status") == "已确认" and p.get("attributed_to_snapshot")
            ],
            "resend_status": status,
            "note": note,
        }

    def _shard_state(self, meta: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any]:
        """核验分片是否存在且校验值匹配，并给出可下载路径。"""
        path = Path(str(meta.get("path", "")))
        if not path.exists():
            fallback = self._data_dir("deliveries") / str(meta.get("filename", ""))
            if fallback.exists():
                path = fallback
        expected = meta.get("checksum") or meta.get("sha256")
        if not expected:
            expected = self._receipt_checksum(entry, meta.get("shard_no"))

        state = {
            "shard_no": meta.get("shard_no"),
            "filename": meta.get("filename"),
            "path": str(path),
            "records": meta.get("records"),
            "type_counts": meta.get("type_counts", {}),
            "checksum": expected or "",
            "verified": False,
            "downloadable": False,
            "reason": "",
        }
        if not path.exists():
            state["reason"] = "分片文件缺失"
            return state
        actual = self._file_checksum(path)
        state["records"] = len(self._read_jsonl(path))
        if expected and actual == expected:
            state["verified"] = True
            state["downloadable"] = True
            state["checksum"] = actual
        else:
            state["reason"] = "校验值不匹配，需重新核对归属"
            state["checksum"] = actual
        return state

    @staticmethod
    def _receipt_checksum(entry: dict[str, Any], shard_no: Any) -> str | None:
        for receipt in entry.get("receipts", []):
            for shard in receipt.get("shards", []):
                if shard.get("shard_no") == shard_no and shard.get("checksum"):
                    return str(shard["checksum"])
        return None

    def resolve_shard(self, snapshot_id: str, shard_no: int) -> dict[str, Any]:
        """定位并核实单个分片；索引缺失/回滚时回退旧命名并校验。"""
        index, source = self._index_view()
        entry = index["snapshots"].get(snapshot_id)
        if entry is None:
            entry = self._ephemeral_entry(snapshot_id)
        if entry is None:
            return {"error": f"快照 {snapshot_id} 没有交付记录"}

        metas = entry.get("shards", [])
        meta = next((m for m in metas if int(m.get("shard_no", 0)) == shard_no), None)
        if meta is None:
            filename = f"{snapshot_id}_shard_{shard_no:03d}.jsonl"
            path = self._data_dir("deliveries") / filename
            if not path.exists():
                return {"error": f"快照 {snapshot_id} 的第 {shard_no} 个分片不存在"}
            meta = {
                "shard_no": shard_no,
                "filename": filename,
                "path": str(path),
                "checksum": self._receipt_checksum(entry, shard_no),
                "records": None,
            }

        state = self._shard_state(meta, entry)
        if not state["downloadable"]:
            return {
                "error": f"分片不可下载：{state['reason'] or '未通过校验'}",
                "snapshot_id": snapshot_id,
                "shard_no": shard_no,
                "state": state,
                "index_source": source,
            }
        return {
            "ok": True,
            "snapshot_id": snapshot_id,
            "shard_no": shard_no,
            "path": state["path"],
            "filename": state["filename"],
            "checksum": state["checksum"],
            "records": state["records"],
            "verified": True,
            "index_source": source,
        }

    def mark_pending_review(
        self,
        snapshot_id: str,
        artifact: str,
        *,
        accepted: bool,
        reviewer: str = "复核人",
        note: str | None = None,
    ) -> dict[str, Any]:
        """登记对「待核」产物的复核结论；未确认前始终不计入旧版成品。"""
        index = self._load_index(create=True)
        entry = index["snapshots"].get(snapshot_id)
        if entry is None:
            return {"error": f"快照 {snapshot_id} 不在交付索引中"}
        target = next((p for p in entry.get("pending_verification", []) if p.get("artifact") == artifact), None)
        if target is None:
            return {"error": f"产物 {artifact} 不在待核清单中"}
        target["review_status"] = "已确认" if accepted else "已驳回"
        target["attributed_to_snapshot"] = bool(accepted)
        target["reviewer"] = reviewer
        target["reviewed_at"] = self._now()
        if note:
            target["review_note"] = note
        self._save_index(index)
        return {"ok": True, "snapshot_id": snapshot_id, "artifact": artifact,
                "review_status": target["review_status"], "entry": target}

    # ------------------------------------------------------------------
    # 索引重建 / 辅助
    # ------------------------------------------------------------------

    def _rebuild_index_from_legacy(self, *, doc_id: str | None = None) -> dict[str, Any]:
        """索引丢失时按旧交付文件重建只读视图（不落盘，保证回滚可用）。"""
        index = self._empty_index()
        if doc_id:
            for snapshot in self._snapshots_for_doc(doc_id):
                sid = str(snapshot.get("快照编号"))
                index["snapshots"][sid] = self._build_legacy_entry(sid, {"快照编号": sid})
            for sid in self._legacy_snapshot_ids_for_doc(doc_id):
                if sid not in index["snapshots"]:
                    index["snapshots"][sid] = self._build_legacy_entry(sid, {"快照编号": sid})
            return index

        for sid, record in self._discover_legacy_deliveries():
            index["snapshots"][sid] = self._build_legacy_entry(sid, record)
        return index

    def _ephemeral_entry(self, snapshot_id: str) -> dict[str, Any] | None:
        """按单个快照从旧文件临时构造索引条目。"""
        for sid, record in self._discover_legacy_deliveries():
            if sid == snapshot_id:
                return self._build_legacy_entry(sid, record)
        if (self._data_dir("deliveries") / f"{snapshot_id}_shard_001.jsonl").exists():
            return self._build_legacy_entry(snapshot_id, {"快照编号": snapshot_id})
        return None

    def _snapshots_for_doc(self, doc_id: str) -> list[dict[str, Any]]:
        snapshots = []
        for vf in sorted(self._data_dir("versions").glob(f"{doc_id}_v*.jsonl")):
            snapshots.extend(self._read_jsonl(vf))
        return snapshots

    def _legacy_snapshot_ids_for_doc(self, doc_id: str) -> list[str]:
        ids = []
        for path in sorted(self._data_dir("deliveries").glob("*_delivery.jsonl")):
            for rec in self._read_jsonl(path):
                sid = str(rec.get("快照编号") or "")
                if sid and self._doc_id_from_snapshot(sid) == doc_id:
                    ids.append(sid)
        return ids

    def _entry_shards_intact(self, entry: dict[str, Any]) -> bool:
        for meta in entry.get("shards", []):
            path = Path(str(meta.get("path", "")))
            if not path.exists():
                return False
            if meta.get("checksum") and self._file_checksum(path) != meta["checksum"]:
                return False
        return bool(entry.get("shards"))

    def _delivery_summary(self, entry: dict[str, Any], *, reused: bool = False,
                          preserved_legacy: bool = False) -> dict[str, Any]:
        return {
            "snapshot_id": entry.get("snapshot_id"),
            "delivery_id": entry.get("delivery_id"),
            "format": entry.get("format"),
            "target": entry.get("target"),
            "total_records": entry.get("record_count"),
            "shards": len(entry.get("shards", [])),
            "index_version": entry.get("index_version", INDEX_SCHEMA_VERSION),
            "snapshot_kind": entry.get("snapshot_kind"),
            "reused": reused,
            "preserved_legacy": preserved_legacy,
        }

    @staticmethod
    def _snapshot_from_filename(name: str) -> str | None:
        match = re.match(r"^(.*)_shard_\d+\.jsonl$", name)
        return match.group(1) if match else None

    @staticmethod
    def _doc_id_from_snapshot(snapshot_id: str) -> str:
        body = snapshot_id[5:] if snapshot_id.startswith("SNAP-") else snapshot_id
        if "-" not in body:
            return body
        return body.rsplit("-", 1)[0]

    @staticmethod
    def _version_from_snapshot(snapshot_id: str) -> str:
        body = snapshot_id[5:] if snapshot_id.startswith("SNAP-") else snapshot_id
        if "-" not in body:
            return ""
        return body.rsplit("-", 1)[1]

    @staticmethod
    def _now() -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------------------------------------------
    # 产物收集（保持旧版字段范围不变）
    # ------------------------------------------------------------------

    def _collect_deliverables(self, doc_id: str) -> list[dict[str, Any]]:
        """收集指定文档的所有可交付抽取产物。"""
        items = []

        # 收集实体
        entity_files = sorted(self._data_dir("entities").glob(f"{doc_id}*.jsonl"))
        for ef in entity_files:
            for e in self._read_jsonl(ef):
                items.append({"type": "entity", "source": str(ef), "data": e})

        # 收集关系
        relation_files = sorted(self._data_dir("relations").glob(f"{doc_id}*.jsonl"))
        for rf in relation_files:
            for r in self._read_jsonl(rf):
                items.append({"type": "relation", "source": str(rf), "data": r})

        # 收集向量
        vector_files = sorted(self._data_dir("vectors").glob(f"{doc_id}*.jsonl"))
        for vf in vector_files:
            for v in self._read_jsonl(vf):
                items.append({"type": "vector", "source": str(vf), "data": {
                    "chunk_id": v.get("chunk_id"),
                    "doc_id": v.get("doc_id"),
                    "dimension": v.get("vector_dim"),
                    "index": v.get("index"),
                }})

        return items

    def list_formats(self) -> list[dict[str, Any]]:
        """列出所有支持的交付格式。"""
        return [
            {"format": k, "mime": v["mime"], "description": v["description"]}
            for k, v in self.SUPPORTED_FORMATS.items()
        ]
