"""隔离区领域服务：存放违规批次行、待处置/回炉/留存管理。

隔离交接以共享行身份为线索：同一批次、同一规则下同一行只有一条隔离记录。
重跑质量检查、重启后重放隔离交接，都能区分"上一次处理中已确认"与"仍需补交"
的记录；已确认的高额记录不会再次交付。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.quality_rule import QualityRuleService
from app.store import store
from app.lake_store import lake_store


class QuarantineZoneService(BaseModuleService):
    MODULE = "quarantine_zone"
    ENTITY = "隔离记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["隔离编号", "所属湖表", "来源批次", "违规规则", "违规行数", "隔离目录", "处置结论", "隔离时间"]
    REQUIRED_FIELDS = ["隔离编号", "所属湖表", "来源批次"]
    STATUS_ORDER = ["已隔离", "待处置", "已回炉", "已留存"]
    ACTION_RULES = {"提交处置": "待处置", "回炉重入": "已回炉", "永久留存": "已留存"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        self._quality_service = QualityRuleService()

    def isolate_violations(self, batch_id: str, rule_id: str, rule_name: str,
                           violation_rows: list[dict[str, Any]],
                           rule: dict[str, Any] | None = None) -> dict[str, Any]:
        """将违规数据行迁入隔离区。

        按共享行身份幂等：重复提交同一行不会产生第二条隔离记录，
        已确认的记录也不会被重置回待确认。
        """
        quarantined = lake_store.quarantine_rows(
            batch_id, rule_id, rule_name, violation_rows, rule=rule)
        return {
            "batch_id": batch_id,
            "rule_id": rule_id,
            "quarantine_count": len(quarantined),
            "status": "isolated",
        }

    def isolate_from_quality_result(self, batch_id: str,
                                    quality_result: dict[str, Any]) -> dict[str, Any]:
        """按质量检查结果把违规行交接到隔离区，一条行身份只交付一次。"""
        applied = quality_result.get("rules_applied") or []
        rule = applied[0] if applied else None
        rule_id = str(rule.get("规则编号", "")) if rule else "QR-AMOUNT"
        is_range = rule is not None and rule.get("校验类型") == "RANGE"
        rule_name = "金额阈值超额" if (rule is None or is_range) else str(rule.get("校验列", ""))
        return self.isolate_violations(
            batch_id, rule_id, rule_name,
            quality_result.get("violation_data", []), rule=rule)

    def confirm_records(self, batch_id: str,
                        keys: list[str] | None = None) -> dict[str, Any]:
        """确认隔离记录：确认后退出待交队列，且不得再次交付。"""
        confirmed = lake_store.confirm_quarantine(batch_id, keys)
        return {
            "batch_id": batch_id,
            "confirmed_count": len(confirmed),
            "confirmed_keys": [qr["_quarantine_key"] for qr in confirmed],
            "overview": lake_store.quarantine_overview(batch_id),
        }

    def confirm_by_amount(self, batch_id: str, comparator: str,
                          amount: float) -> dict[str, Any]:
        """按金额确认隔离记录，例如 ``comparator='>'``、``amount=250``。"""
        keys = [
            qr["_quarantine_key"]
            for qr in lake_store.pending_quarantine(batch_id)
            if self._amount_matches(qr, comparator, amount)
        ]
        return self.confirm_records(batch_id, keys)

    def handoff_state(self, batch_id: str) -> dict[str, Any]:
        """隔离交接状态：区分已确认与仍需补交的记录，按行身份给出。"""
        overview = lake_store.quarantine_overview(batch_id)
        return {
            "batch_id": batch_id,
            "pending_rows": [
                self._public_row(qr) for qr in lake_store.pending_quarantine(batch_id)
            ],
            "confirmed_rows": [
                self._public_row(qr) for qr in lake_store.confirmed_quarantine(batch_id)
            ],
            "overview": overview,
        }

    def replay_handoff(self, batch_id: str, rules: list[dict[str, Any]],
                       *, confirm_after: bool = False) -> dict[str, Any]:
        """重放质量检查与隔离交接，可分别回查每条行的隔离结论。

        - 先按本次规则重放质量检查（输入集仍是该批次原始行）。
        - 再按行身份把违规行交接到隔离区：已交付的行不会产生第二条记录。
        - 每条记录都能回查"原规则给出的隔离结论"与"本次规则给出的结论"。
        """
        quality_result = self._quality_service.apply_rules_to_batch(
            batch_id, rules, entry="quality_replay")
        if quality_result.get("success") is False:
            return quality_result
        self.isolate_from_quality_result(batch_id, quality_result)

        lead_rule = rules[0] if rules else None
        lead_id = str(lead_rule.get("规则编号", "")) if lead_rule else ""

        conclusions = []
        for qr in lake_store.list_quarantine(batch_id):
            if qr.get("_quarantine_rule") != lead_id:
                continue
            key = qr["_quarantine_key"]
            detail = next(
                (d for d in quality_result.get("violation_details", [])
                 if d.get("row_key") == key), None)
            conclusions.append({
                "row_key": key,
                "amount": qr.get("amount"),
                "status": qr.get("_quarantine_status"),
                "first_conclusion": qr.get("_quarantine_first_conclusion"),
                "first_threshold": dict(qr.get("_quarantine_first_threshold", {})),
                "current_conclusion": qr.get("_quarantine_conclusion"),
                "current_threshold": dict(qr.get("_quarantine_threshold", {})),
                "this_run_conclusion": detail["conclusion"] if detail else "放行",
                "this_run_threshold": dict(detail["threshold"]) if detail else None,
                "conclusion_history": list(qr.get("_quarantine_conclusions", [])),
            })

        result = {
            "batch_id": batch_id,
            "quality": {
                "total_rows": quality_result["total_rows"],
                "after_dedup": quality_result["after_dedup"],
                "duplicates_removed": quality_result["duplicates_removed"],
                "clean_rows": quality_result["clean_rows"],
                "violation_rows": quality_result["violation_rows"],
            },
            "conclusions": conclusions,
            "handoff": self.handoff_state(batch_id),
        }
        if confirm_after:
            result["confirmation"] = self.confirm_records(batch_id)
        return result

    def apply_new_rule_same_batch(self, batch_id: str,
                                  rule: dict[str, Any] | list[dict[str, Any]],
                                  disposition: str = "reuse_previous") -> dict[str, Any]:
        """恢复旧批时把超额线从旧值调为新值，处置两种口径。

        - ``disposition='reuse_previous'``：延续原处置，仍判违规的行继续隔离。
        - ``disposition='reapply_new_rule'``：对未确认的行按新规则重新处置，
          核回初次隔离结论；新规则判通过的放行并进入待入湖确认，
          入湖确认后关闭待交记录；已确认的记录保持原结论、不再次交付。
        """
        br = lake_store.get_batch(batch_id)
        if br is None:
            return {"success": False, "error": "batch not found"}
        if disposition not in ("reuse_previous", "reapply_new_rule"):
            return {"success": False, "error": f"unknown disposition: {disposition}"}
        rules = rule if isinstance(rule, list) else [rule]
        if not rules:
            return {"success": False, "error": "rule required"}
        new_rule = rules[0]
        rule_id = str(new_rule.get("规则编号", ""))

        quality_result = self._quality_service.apply_rules_to_batch(
            batch_id, rules, entry="quality_reapply")
        violating_keys = {d["row_key"] for d in quality_result["violation_details"]}

        if disposition == "reuse_previous":
            # 延续原处置：不按新规则放行任何未确认记录，只把阈值记录为新值
            resolve = lake_store.resolve_quarantine(
                batch_id, rule_id,
                retained_keys={
                    qr["_quarantine_key"]
                    for qr in lake_store.list_quarantine(batch_id)
                    if qr.get("_quarantine_rule") == rule_id
                    and qr.get("_quarantine_status")
                    == lake_store.QUARANTINE_STATUS_PENDING
                },
                rule=new_rule)
            return {
                "batch_id": batch_id,
                "disposition": "reuse_previous",
                "rule_id": rule_id,
                "quality": {
                    "violation_rows": quality_result["violation_rows"],
                    "clean_rows": quality_result["clean_rows"],
                },
                "still_quarantined": len(resolve["reheld"]),
                "released": 0,
                "kept_confirmed": len(resolve["kept_confirmed"]),
                "handoff": self.handoff_state(batch_id),
            }

        resolve = lake_store.resolve_quarantine(
            batch_id, rule_id, retained_keys=violating_keys, rule=new_rule)
        # 放行行进入待入湖确认；入湖确认由 confirm_ingest 触发关闭
        return {
            "batch_id": batch_id,
            "disposition": "reapply_new_rule",
            "rule_id": rule_id,
            "quality": {
                "violation_rows": quality_result["violation_rows"],
                "clean_rows": quality_result["clean_rows"],
            },
            "released": len(resolve["released"]),
            "released_rows": [self._public_row(qr) for qr in resolve["released"]],
            "still_quarantined": len(resolve["reheld"]),
            "kept_confirmed": len(resolve["kept_confirmed"]),
            "pending_ingest_confirmation": len(resolve["released"]),
            "handoff": self.handoff_state(batch_id),
        }

    def confirm_ingest(self, batch_id: str) -> dict[str, Any]:
        """入湖确认：正常通过行与本次放行行写入批次，随后关闭待交记录。

        已确认的高额隔离记录不会写入；写入按行身份幂等，重复确认不会重复写入。
        """
        closed = lake_store.close_pending_handoffs(batch_id)
        br = lake_store.get_batch(batch_id)
        written_batch_id = None
        written_rows: list[dict[str, Any]] = []
        if br is not None:
            written_batch_id, written_rows = self._write_ingest_rows(br, closed)
        return {
            "batch_id": batch_id,
            "closed_count": len(closed),
            "closed_keys": [qr["_quarantine_key"] for qr in closed],
            "written_batch_id": written_batch_id,
            "written_rows": len(written_rows),
            "handoff": self.handoff_state(batch_id),
        }

    def _write_ingest_rows(self, br, closed: list[dict[str, Any]]):
        """写入质量检查通过行 + 本次放行行，按行身份只写一次。

        仍未确认或已确认的隔离记录不会被写入：即使某次重处置的质量结果把它们
        暂时算进通过行，只要它们还在隔离待交/已确认状态，就不得绕过隔离入湖。
        """
        from app.row_identity import row_identity
        from app.pipeline.chain_runner import write_clean_batch

        blocked = {
            qr["_quarantine_key"] for qr in lake_store.list_quarantine(br.batch_id)
            if qr.get("_quarantine_status") != lake_store.QUARANTINE_STATUS_RELEASED
            and qr.get("_quarantine_status") != lake_store.QUARANTINE_STATUS_CLOSED
        }
        candidates: list[dict[str, Any]] = list(br.clean_rows or [])
        candidates.extend(
            {k: v for k, v in qr.items() if not k.startswith("_quarantine_")}
            for qr in closed
        )
        fresh: list[dict[str, Any]] = []
        for row in candidates:
            key = row_identity(row)
            if key in blocked or key in br.written_keys:
                continue
            br.written_keys.add(key)
            fresh.append(row)
        if not fresh:
            return br.written_batch_id, []
        written = write_clean_batch(br.lake_table, fresh)
        br.written_batch_id = written["batch_id"]
        br.written_row_count += written["row_count"]
        return br.written_batch_id, fresh

    def reconcile_record(self, batch_id: str,
                         row: dict[str, Any]) -> dict[str, Any] | None:
        """按行身份核回初次隔离结论，并给出本次规则下的处置对照。"""
        return lake_store.quarantine_record_for_row(batch_id, row)

    def release_for_retry(self, batch_id: str) -> dict[str, Any]:
        """回炉重入：从隔离区取出数据行备重新入湖。

        已确认的记录不再交出；同一行身份只交出一次。
        """
        rows = lake_store.release_quarantine(batch_id)
        return {
            "batch_id": batch_id,
            "released_count": len(rows),
            "released_rows": rows,
        }

    def dispose_permanently(self, batch_id: str) -> dict[str, Any]:
        """永久留存：标记处置结论为留存，不释放。"""
        saved = lake_store.list_quarantine(batch_id)
        return {
            "batch_id": batch_id,
            "disposition": "permanent_archive",
            "saved_count": len(saved),
        }

    def summary_by_lake_table(self, lake_table: str) -> dict[str, Any]:
        """汇总指定湖表的隔离区统计。"""
        all_quarantined = lake_store.list_quarantine()
        table_quarantined: dict[str, int] = {}
        for qr in all_quarantined:
            batch_id = qr.get("_quarantine_batch", "")
            br = lake_store.get_batch(batch_id)
            if br and br.lake_table == lake_table:
                rule = qr.get("_quarantine_rule", "unknown")
                table_quarantined[rule] = table_quarantined.get(rule, 0) + 1
        return {
            "lake_table": lake_table,
            "total_quarantined": sum(table_quarantined.values()),
            "by_rule": table_quarantined,
        }

    @staticmethod
    def _amount_matches(qr: dict[str, Any], comparator: str, amount: float) -> bool:
        try:
            value = float(str(qr.get("amount")))
        except (ValueError, TypeError):
            return False
        if comparator == ">":
            return value > amount
        if comparator == ">=":
            return value >= amount
        if comparator == "<":
            return value < amount
        if comparator == "<=":
            return value <= amount
        return value == amount

    @staticmethod
    def _public_row(qr: dict[str, Any]) -> dict[str, Any]:
        return {
            "row_key": qr.get("_quarantine_key"),
            "rule_id": qr.get("_quarantine_rule"),
            "status": qr.get("_quarantine_status"),
            "conclusion": qr.get("_quarantine_conclusion"),
            "first_conclusion": qr.get("_quarantine_first_conclusion"),
            "amount": qr.get("amount"),
            "row": {k: v for k, v in qr.items()
                    if not k.startswith("_quarantine_")},
        }
