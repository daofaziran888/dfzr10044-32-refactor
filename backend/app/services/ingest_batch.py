"""入湖批次领域服务：批次入库、去重、快照绑定与校验值对比。"""
from __future__ import annotations

import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.row_identity import dedupe_rows
from app.store import store
from app.lake_store import lake_store


class IngestBatchService(BaseModuleService):
    MODULE = "ingest_batch"
    ENTITY = "入湖批次"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["批次编号", "所属湖表", "来源系统", "批次行数", "采集窗口", "落湖快照", "完整校验值", "入湖时间"]
    REQUIRED_FIELDS = ["批次编号", "所属湖表", "来源系统"]
    STATUS_ORDER = ["待入湖", "入湖中", "已入湖", "入湖失败"]
    ACTION_RULES = {"启动入湖": "入湖中", "确认入湖": "已入湖", "标记失败": "入湖失败"}
    NEGATIVE_ACTIONS = []

    def ingest_batch(self, lake_table: str, source_system: str,
                     rows: list[dict[str, Any]], window_start: str,
                     window_end: str, *,
                     batch_id: str | None = None) -> dict[str, Any]:
        """接收入湖批次数据：去重、存储、并与快照绑定。

        去重按共享行身份判断：同值但字段顺序不同的两行仍是同一行，只接收一行。
        计数口径保持本入口原有语义——``row_count`` 是实际接收行数，
        ``duplicates_removed`` 是本入口输入集内被判重复的行数。
        指定 ``batch_id`` 时沿用该批号，重复提交同一批号会覆盖接收结果而不新建批次。
        """
        batch_id = batch_id or f"batch-{lake_table}-{uuid.uuid4().hex[:6]}"
        # 先去重再存储；原始行一并保留，供质量检查入口按自己的输入集复算
        dedup = dedupe_rows(rows)
        deduped: list[dict[str, Any]] = dedup["unique_rows"]
        br = lake_store.store_batch(batch_id, lake_table, source_system,
                                    list(rows), window_start, window_end,
                                    accepted_rows=deduped)
        lake_store.record_entry_stats(
            batch_id, "ingest_receive",
            {
                "input_rows": dedup["input_rows"],
                "unique_rows": len(deduped),
                "duplicates_removed": dedup["duplicates_removed"],
            },
            input_rows=list(rows))
        lake_store.update_batch_status(batch_id, "ingesting")
        return {
            "batch_id": batch_id,
            "lake_table": lake_table,
            "row_count": len(deduped),
            "duplicates_removed": dedup["duplicates_removed"],
            "checksum": br.checksum,
            "window": f"{window_start} ~ {window_end}",
        }

    def append_batch_rows(self, batch_id: str,
                          rows: list[dict[str, Any]]) -> dict[str, Any]:
        """给同一原始批次追加行，并按共享行身份重新接收。

        追加的是原始行；接收行与接收入口计数都按整个原始行集重算，
        保证质量检查入口随后看到的仍是"原始批次"，而接收入口的计数也不丢。
        """
        br = lake_store.append_batch_rows(batch_id, rows)
        if br is None:
            return {"success": False, "error": "batch not found"}
        dedup = dedupe_rows(br.rows)
        br.accepted_rows = list(dedup["unique_rows"])
        lake_store.recompute_batch_checksum(batch_id)
        lake_store.record_entry_stats(
            batch_id, "ingest_receive",
            {
                "input_rows": dedup["input_rows"],
                "unique_rows": len(dedup["unique_rows"]),
                "duplicates_removed": dedup["duplicates_removed"],
            },
            input_rows=list(br.rows))
        return {
            "batch_id": batch_id,
            "appended_rows": len(rows),
            "raw_row_count": len(br.rows),
            "row_count": len(dedup["unique_rows"]),
            "duplicates_removed": dedup["duplicates_removed"],
        }

    def verify_batch_integrity(self, batch_id: str) -> dict[str, Any]:
        """校验批次完整性：按实际接收行重新计算校验值并对比。"""
        br = lake_store.get_batch(batch_id)
        if br is None:
            return {"success": False, "error": "batch not found"}
        source = br.accepted_rows if br.accepted_rows else br.rows
        recalc = lake_store._batch_checksum(source)
        return {
            "batch_id": batch_id,
            "stored_checksum": br.checksum,
            "recalculated_checksum": recalc,
            "integrity_ok": recalc == br.checksum,
        }

    def promote_to_ingested(self, batch_id: str, snapshot_id: str) -> dict[str, Any]:
        """操作已入湖：绑定快照并标记已入湖。"""
        br = lake_store.update_batch_status(batch_id, "ingested", snapshot_id)
        if br is None:
            return {"success": False, "error": "batch not found"}
        return {
            "batch_id": batch_id,
            "status": "ingested",
            "snapshot_id": snapshot_id,
        }