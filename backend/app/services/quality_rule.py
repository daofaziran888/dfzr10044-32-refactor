"""入湖质量规则领域服务：校验列、阈值、违规处置与规则生命周期管理。

质量检查入口与接收入口共用同一份行身份判断（``app.row_identity``），但各自在
自己的输入集上去重：接收入口看到的是提交上来的行，质量检查入口看到的是原始
批次里的行。两边各自记录原始数与重复数，重放时可分别复算，互不覆盖。
"""
from __future__ import annotations

import time
from typing import Any

from app.row_identity import dedupe_rows, row_identity
from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store


class QualityRuleService(BaseModuleService):
    MODULE = "quality_rule"
    ENTITY = "质量规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "所属湖表", "校验列", "校验类型", "阈值下限", "阈值上限", "违规处置", "命中批次"]
    REQUIRED_FIELDS = ["规则编号", "所属湖表", "校验列"]
    STATUS_ORDER = ["草稿", "已启用", "已抑制", "已废弃"]
    ACTION_RULES = {"启用规则": "已启用", "抑制规则": "已抑制", "废弃规则": "已废弃"}
    NEGATIVE_ACTIONS = []

    def apply_rules_to_batch(self, batch_id: str,
                             rules: list[dict[str, Any]],
                             *,
                             entry: str = "quality_check") -> dict[str, Any]:
        """对一个批次的全部行应用质量规则，返回违规行和通过行。

        去重按共享行身份判断，输入集是本入口拿到的整批原始行；因此无论接收入口
        是否已经去重，质量检查入口都会保留自己的 ``total_rows``（原始数）与
        ``duplicates_removed``（重复数）。重复出现的超额行只保留一次违规记录。
        """
        br = lake_store.get_batch(batch_id)
        if br is None:
            return {"success": False, "error": "batch not found"}

        rows = list(br.rows)
        dedup = dedupe_rows(rows)
        unique_rows: list[dict[str, Any]] = dedup["unique_rows"]

        violations: dict[int, list[str]] = {}
        violation_details: list[dict[str, Any]] = []
        clean_data: list[dict[str, Any]] = []
        violation_data: list[dict[str, Any]] = []
        for i, row in enumerate(unique_rows):
            matched = self.evaluate_row(row, rules)
            if matched:
                rule_ids = [str(hit.get("规则编号", "")) for hit in matched]
                violations[i] = rule_ids
                violation_data.append(row)
                lead = matched[0]
                violation_details.append({
                    "row_key": row_identity(row),
                    "row": row,
                    "rule_id": str(lead.get("规则编号", "")),
                    "rule_ids": rule_ids,
                    "conclusion": "隔离",
                    "threshold": {
                        "阈值下限": lead.get("阈值下限"),
                        "阈值上限": lead.get("阈值上限"),
                    },
                    "rule": dict(lead),
                })
            else:
                clean_data.append(row)

        # 同一行可能命中多条规则；去重后按行计一次违规
        violation_row_count = len(violation_data)

        quality_result = {
            "batch_id": batch_id,
            "total_rows": len(rows),
            "after_dedup": len(unique_rows),
            "duplicates_removed": dedup["duplicates_removed"],
            "clean_rows": len(clean_data),
            "violation_rows": violation_row_count,
            "violations_by_row": {str(k): v for k, v in violations.items()},
            "violation_data": violation_data,
            "clean_data": clean_data,
            "violation_details": violation_details,
            "rules_applied": [dict(rule) for rule in rules],
            "checked_at": time.time(),
        }

        br.clean_rows = list(clean_data)
        br.violation_rows = list(violation_data)
        br.quality_result = dict(quality_result)
        lake_store.record_entry_stats(
            batch_id, entry,
            {
                "input_rows": dedup["input_rows"],
                "unique_rows": len(unique_rows),
                "duplicates_removed": dedup["duplicates_removed"],
                "clean_rows": len(clean_data),
                "violation_rows": violation_row_count,
            },
            input_rows=rows)
        if not br.rule_snapshot:
            # 初次隔离结论所依据的规则：之后调阈值也不改写它
            br.rule_snapshot = {
                str(rule.get("规则编号", "")): dict(rule) for rule in rules
            }
        br.rule_history.append({
            "at": quality_result["checked_at"],
            "rules": [dict(rule) for rule in rules],
        })
        return quality_result

    def evaluate_row(self, row: dict[str, Any],
                     rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """对单行应用规则，返回命中的规则列表（含规则定义）。"""
        matched: list[dict[str, Any]] = []
        for rule in rules:
            if self._row_hits_rule(row, rule):
                matched.append(rule)
        return matched

    def _row_hits_rule(self, row: dict[str, Any], rule: dict[str, Any]) -> bool:
        col = rule.get("校验列", "")
        rule_type = rule.get("校验类型", "")
        if col not in row:
            return False
        val = row[col]
        if rule_type == "NOT_NULL":
            return val is None or str(val).strip() == ""
        if rule_type == "UNIQUE":
            return False
        if rule_type == "RANGE" and val is not None and str(val).strip() != "":
            lo = self._parse_limit(rule.get("阈值下限"))
            hi = self._parse_limit(rule.get("阈值上限"))
            try:
                num_val = float(str(val))
            except (ValueError, TypeError):
                return True
            if (lo is not None and num_val < lo) or (hi is not None and num_val > hi):
                return True
        return False

    def replay_batch_check(self, batch_id: str,
                           rules: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        """重放质量检查：默认沿用初次隔离时的规则快照。

        同一个批号重跑只读取该批次已有的原始行，不重新接收，也不清空隔离区；
        返回的计数与首次一致，可用于复算各入口原有的原始数/重复数。
        """
        br = lake_store.get_batch(batch_id)
        if br is None:
            return {"success": False, "error": "batch not found"}
        applied = rules if rules is not None else list(br.rule_snapshot.values())
        return self.apply_rules_to_batch(batch_id, applied, entry="quality_replay")

    def recompute_entry_stats(self, batch_id: str) -> dict[str, Any]:
        """重放复算：各入口仍用自己的输入集重新算原始数与重复数。

        对每个入口，按它历次记录的输入集分别复算并与当时的计数对比，
        因此同批号重跑后仍能逐一回查各入口原有的计数。
        """
        br = lake_store.get_batch(batch_id)
        if br is None:
            return {"success": False, "error": "batch not found"}

        comparisons: dict[str, Any] = {}
        all_match = True
        for entry, history in br.entry_stats_history.items():
            runs: list[dict[str, Any]] = []
            for index, record in enumerate(history):
                input_rows = record.get("input_rows_sample")
                if input_rows is None:
                    runs.append({
                        "run": index + 1,
                        "stored": {k: v for k, v in record.items()
                                   if k not in ("at", "input_rows_sample")},
                        "recomputed": None,
                        "matches": None,
                    })
                    continue
                dedup = dedupe_rows(input_rows)
                recalc = {
                    "input_rows": dedup["input_rows"],
                    "unique_rows": len(dedup["unique_rows"]),
                    "duplicates_removed": dedup["duplicates_removed"],
                }
                matches = all(
                    record.get(field) == recalc[field]
                    for field in ("input_rows", "unique_rows", "duplicates_removed")
                )
                all_match = all_match and matches
                runs.append({
                    "run": index + 1,
                    "stored": {
                        "input_rows": record.get("input_rows"),
                        "unique_rows": record.get("unique_rows"),
                        "duplicates_removed": record.get("duplicates_removed"),
                        "clean_rows": record.get("clean_rows"),
                        "violation_rows": record.get("violation_rows"),
                    },
                    "recomputed": recalc,
                    "matches": matches,
                })
            comparisons[entry] = runs
        return {
            "batch_id": batch_id,
            "entries": comparisons,
            "all_match": all_match,
        }

    def record_conclusion_for_record(self, batch_id: str,
                                     row: dict[str, Any]) -> dict[str, Any] | None:
        """按行身份回查某行在初次隔离时依据原规则给出的结论。"""
        return lake_store.quarantine_record_for_row(batch_id, row)

    def _parse_limit(self, value: Any) -> float | None:
        if value is None or str(value).strip() == "":
            return None
        try:
            return float(str(value))
        except (ValueError, TypeError):
            return None
