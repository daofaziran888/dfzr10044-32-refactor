"""业务操作接口：入湖接收、质量检查、隔离交接与旧批恢复的跨模块动作。

资源路由只承担列表/登记/状态流转；本路由把需要跨模块串联的业务动作
（接收去重、质量校验、隔离交接、重处置、入湖确认）暴露为独立端点。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.lake_store import lake_store
from app.pipeline.chain_runner import receive_batch, write_clean_batch
from app.services.ingest_batch import IngestBatchService
from app.services.quality_rule import QualityRuleService
from app.services.quarantine_zone import QuarantineZoneService

router = APIRouter(prefix="/api/operations", tags=["业务动作"])

_ingest = IngestBatchService()
_quality = QualityRuleService()
_quarantine = QuarantineZoneService()


def _require_batch(batch_id: str):
    br = lake_store.get_batch(batch_id)
    if br is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch_id} 不存在")
    return br


class IngestRowsPayload(BaseModel):
    lake_table: str
    source_system: str = "OLTP_DB"
    rows: list[dict[str, Any]]
    window_start: str = "2026-11-01T00:00:00"
    window_end: str = "2026-11-01T23:59:59"
    batch_id: str | None = None


class QualityPayload(BaseModel):
    batch_id: str
    rules: list[dict[str, Any]]


class ConfirmPayload(BaseModel):
    batch_id: str
    rule_keys: list[str] | None = None
    amount_gt: float | None = None


class NewRulePayload(BaseModel):
    batch_id: str
    rule: dict[str, Any] | list[dict[str, Any]]
    disposition: str = Field(default="reuse_previous")


class BatchIdPayload(BaseModel):
    batch_id: str


@router.post("/ingest")
def ingest(payload: IngestRowsPayload) -> dict[str, Any]:
    """接收入湖批次：同值不同字段顺序的行只接收一行。"""
    return _ingest.ingest_batch(
        payload.lake_table, payload.source_system, payload.rows,
        payload.window_start, payload.window_end, batch_id=payload.batch_id)


@router.post("/ingest/append")
def append_rows(batch_id: str = Query(...),
                payload: IngestRowsPayload | None = None) -> dict[str, Any]:
    """向同一原始批次追加行，并按共享行身份重新接收。"""
    _require_batch(batch_id)
    if payload is None or not payload.rows:
        raise HTTPException(status_code=400, detail="需要追加的行")
    return _ingest.append_batch_rows(batch_id, payload.rows)


@router.post("/quality/check")
def quality_check(payload: QualityPayload) -> dict[str, Any]:
    """执行质量检查：保留本入口的原始数与重复数。"""
    _require_batch(payload.batch_id)
    return _quality.apply_rules_to_batch(payload.batch_id, payload.rules)


@router.post("/quality/replay")
def quality_replay(payload: BatchIdPayload) -> dict[str, Any]:
    """同批号重放质量检查：默认沿用初次隔离时的规则快照。"""
    _require_batch(payload.batch_id)
    return _quality.replay_batch_check(payload.batch_id)


@router.post("/quarantine/handoff")
def quarantine_handoff(batch_id: str = Query(...)) -> dict[str, Any]:
    """隔离交接视图：区分已确认与仍需补交的记录。"""
    _require_batch(batch_id)
    return _quarantine.handoff_state(batch_id)


@router.post("/quarantine/confirm")
def quarantine_confirm(payload: ConfirmPayload) -> dict[str, Any]:
    """确认隔离记录：可按行身份、或按金额阈值确认。"""
    _require_batch(payload.batch_id)
    if payload.amount_gt is not None:
        return _quarantine.confirm_by_amount(payload.batch_id, ">", payload.amount_gt)
    return _quarantine.confirm_records(payload.batch_id, payload.rule_keys)


@router.post("/quarantine/replay")
def quarantine_replay(payload: QualityPayload) -> dict[str, Any]:
    """重跑质量检查与隔离交接，逐条回查原规则结论。"""
    _require_batch(payload.batch_id)
    return _quarantine.replay_handoff(payload.batch_id, payload.rules)


@router.post("/quarantine/apply-new-rule")
def apply_new_rule(payload: NewRulePayload) -> dict[str, Any]:
    """旧批恢复时的阈值冲突处置：延续原处置或按新规则重处置。"""
    _require_batch(payload.batch_id)
    return _quarantine.apply_new_rule_same_batch(
        payload.batch_id, payload.rule, disposition=payload.disposition)


@router.post("/quarantine/confirm-ingest")
def confirm_ingest(payload: BatchIdPayload) -> dict[str, Any]:
    """入湖确认：放行行写入批次并关闭待交记录。"""
    _require_batch(payload.batch_id)
    return _quarantine.confirm_ingest(payload.batch_id)


@router.get("/quality/recompute/{batch_id}")
def recompute(batch_id: str) -> dict[str, Any]:
    """用各入口自己的输入集复算原始数与重复数。"""
    _require_batch(batch_id)
    return _quality.recompute_entry_stats(batch_id)
