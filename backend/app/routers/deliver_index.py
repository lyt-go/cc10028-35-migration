"""交付索引 v2 接口：迁移、补发查询、分片下载、回执登记与待核复核。

统一走 /api/delivery-index 前缀，避免与通用资源路由 /api/{module}/{entry_id}
的动态路径抢占。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.services.result_deliver import ResultDeliverService

router = APIRouter(prefix="/api/delivery-index", tags=["交付索引"])


class MigratePayload(BaseModel):
    force: bool = False
    resume: bool = True


class ReceiptPayload(BaseModel):
    receipt_type: str = Field(default="接收回执", alias="receipt_type")
    received_by: str = "接收方"
    note: str | None = None

    model_config = {"populate_by_name": True}


class ReviewPayload(BaseModel):
    artifact: str
    accepted: bool
    reviewer: str = "复核人"
    note: str | None = None


def _service() -> ResultDeliverService:
    return ResultDeliverService()


@router.get("")
def index_summary() -> dict[str, Any]:
    """查看交付索引概况与每个快照的迁移/回执/待核状态。"""
    return _service().index_summary()


@router.post("/migrate")
def migrate(payload: MigratePayload | None = None) -> dict[str, Any]:
    """把旧快照的文档计数与交付明细迁移到交付索引 v2，可中断续跑。"""
    payload = payload or MigratePayload()
    try:
        return _service().migrate_legacy_deliveries(force=payload.force, resume=payload.resume)
    except Exception as exc:  # 中断时索引停留在最后一个完整快照
        raise HTTPException(status_code=503, detail=f"迁移中断，可重跑续迁：{exc}") from exc


@router.get("/resend")
def query_resend(
    doc_id: str | None = Query(default=None),
    snapshot_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """按文档或快照查补发：区分核实分片、仅回执、无回执与待核产物。"""
    result = _service().query_resend(doc_id=doc_id, snapshot_id=snapshot_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/{snapshot_id}/shards/{shard_no}")
def download_shard(snapshot_id: str, shard_no: int) -> FileResponse:
    """下载核实过的旧分片；索引缺失/回滚时回退旧命名再校验。"""
    resolved = _service().resolve_shard(snapshot_id, shard_no)
    if "error" in resolved:
        raise HTTPException(status_code=409, detail=resolved["error"])
    return FileResponse(
        resolved["path"],
        media_type="application/x-ndjson",
        filename=resolved["filename"],
        headers={"X-Content-SHA256-16": resolved["checksum"]},
    )


@router.post("/{snapshot_id}/receipts")
def record_receipt(snapshot_id: str, payload: ReceiptPayload | None = None) -> dict[str, Any]:
    """登记接收方回执（含下载回执）。"""
    payload = payload or ReceiptPayload()
    result = _service().record_receipt(
        snapshot_id,
        receipt_type=payload.receipt_type,
        received_by=payload.received_by,
        note=payload.note,
    )
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.post("/{snapshot_id}/pending-review")
def review_pending(snapshot_id: str, payload: ReviewPayload) -> dict[str, Any]:
    """确认或驳回交付后才出现的抽取产物归属。"""
    result = _service().mark_pending_review(
        snapshot_id,
        payload.artifact,
        accepted=payload.accepted,
        reviewer=payload.reviewer,
        note=payload.note,
    )
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result
