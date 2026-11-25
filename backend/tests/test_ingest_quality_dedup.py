"""入湖批次接收与质量检查共享行去重、隔离交接重放与新规则重处置的回归测试。

覆盖题面场景：
- 同值但字段顺序不同的两行，接收入口只接收一行；
- 同样两行随原始批次进入质量检查，入口仍保留自己的原始数与重复数；
- 追加一对字段顺序不同但金额相同的超额记录，再追加一笔金额不同的超额记录；
- 质量检查输出隔离后暂停，确认第一条违规、第二条未确认时重启；
- 同批号重跑质量检查与隔离交接，可分别回查原规则给出的隔离结论；
- 重复金额只保留一次，正常行仍能进入批次写入，重放仍可复算各入口计数；
- 已确认的高额记录不再交付；180 元未确认行在旧批恢复与超额线 100→200 的冲突下，
  延续原处置仍隔离、按新规则重处置则核回初次结论并在入湖确认后关闭待交记录；
- 新批号提交同样输入按新规则独立计算。
"""
from __future__ import annotations

import pytest

from app.lake_store import LakeStore
from app.row_identity import dedupe_rows, row_identity, same_row
from app.services.ingest_batch import IngestBatchService
from app.services.quality_rule import QualityRuleService
from app.services.quarantine_zone import QuarantineZoneService
from app import lake_store as lake_store_module


ROW_A = {
    "event_time": "2026-11-01", "user_id": "u1", "action": "login",
    "amount": "100.00", "partition_dt": "20261101",
}
ROW_A_SHUFFLED = {
    "partition_dt": "20261101", "amount": "100.00", "action": "login",
    "user_id": "u1", "event_time": "2026-11-01",
}


def _rule(rule_id: str = "QR-AMOUNT", upper: int | float = 100) -> dict:
    return {
        "规则编号": rule_id,
        "所属湖表": "ods_evt",
        "校验列": "amount",
        "校验类型": "RANGE",
        "阈值下限": 0,
        "阈值上限": upper,
        "违规处置": "隔离",
    }


def _excess_row(amount: str, user: str = "u5") -> dict:
    return {
        "event_time": "2026-11-01", "user_id": user, "action": "purchase",
        "amount": amount, "partition_dt": "20261101",
    }


def _shuffled(row: dict) -> dict:
    """同一行换个字段顺序。"""
    return {key: row[key] for key in reversed(list(row.keys()))}


@pytest.fixture
def fresh_store(monkeypatch):
    """每个用例独立的存储引擎，避免跨用例污染。"""
    store = LakeStore()
    monkeypatch.setattr(lake_store_module, "lake_store", store)
    # 领域服务与链路模块内是 from app.lake_store import lake_store 的绑定，需一并替换。
    for module_name in (
        "app.services.ingest_batch",
        "app.services.quality_rule",
        "app.services.quarantine_zone",
        "app.pipeline.chain_runner",
    ):
        module = __import__(module_name, fromlist=["lake_store"])
        monkeypatch.setattr(module, "lake_store", store)
    return store


@pytest.fixture
def services(fresh_store):
    return IngestBatchService(), QualityRuleService(), QuarantineZoneService()


class TestSharedRowIdentity:
    def test_identity_ignores_field_order(self):
        assert row_identity(ROW_A) == row_identity(ROW_A_SHUFFLED)
        assert same_row(ROW_A, ROW_A_SHUFFLED)

    def test_identity_ignores_quarantine_meta(self):
        enriched = dict(ROW_A, _quarantine_rule="QR-AMOUNT", _quarantine_key="x")
        assert row_identity(enriched) == row_identity(ROW_A)

    def test_identity_distinguishes_values(self):
        assert row_identity(ROW_A) != row_identity(_excess_row("100.01"))

    def test_dedupe_counts_duplicates_not_uniques(self):
        result = dedupe_rows([ROW_A, ROW_A_SHUFFLED, _excess_row("5.00")])
        assert result["input_rows"] == 3
        assert len(result["unique_rows"]) == 2
        assert result["duplicates_removed"] == 1


class TestReceiveAndQualityDedupe:
    def test_receive_keeps_one_of_equivalent_rows(self, services):
        ingest, _, _ = services
        result = ingest.ingest_batch(
            "ods_evt", "OLTP", [ROW_A, ROW_A_SHUFFLED],
            "2026-11-01T00:00:00", "2026-11-01T23:59:59")
        assert result["row_count"] == 1
        assert result["duplicates_removed"] == 1

    def test_quality_entry_keeps_own_input_and_duplicate_counts(self, services):
        ingest, quality, _ = services
        received = ingest.ingest_batch(
            "ods_evt", "OLTP", [ROW_A, ROW_A_SHUFFLED],
            "2026-11-01T00:00:00", "2026-11-01T23:59:59")

        result = quality.apply_rules_to_batch(received["batch_id"], [_rule()])
        # 质量检查入口的输入集是原始批次（两行），不是接收后的那一行
        assert result["total_rows"] == 2
        assert result["duplicates_removed"] == 1
        assert result["after_dedup"] == 1
        assert result["clean_rows"] == 1

    def test_append_pair_and_single_extra(self, services):
        ingest, quality, quarantine = services
        received = ingest.ingest_batch(
            "ods_evt", "OLTP", [ROW_A, ROW_A_SHUFFLED],
            "2026-11-01T00:00:00", "2026-11-01T23:59:59")
        batch_id = received["batch_id"]

        pair = _excess_row("300.00", "u5")
        extra = _excess_row("999.00", "u6")
        appended = ingest.append_batch_rows(batch_id, [pair, _shuffled(pair), extra])
        assert appended["raw_row_count"] == 5
        assert appended["duplicates_removed"] == 2  # 100 对 + 300 对

        result = quality.apply_rules_to_batch(batch_id, [_rule(upper=100)])
        assert result["total_rows"] == 5
        assert result["duplicates_removed"] == 2
        # 重复的 300 只保留一次违规，外加 999 一次
        assert result["violation_rows"] == 2
        keys = [detail["row_key"] for detail in result["violation_details"]]
        assert len(keys) == len(set(keys))

        quarantine.isolate_from_quality_result(batch_id, result)
        assert len(lake_store_records(batch_id)) == 2


def lake_store_records(batch_id):
    """读取当前（被 monkeypatch 替换后的）存储引擎里的隔离记录。"""
    from app.services.quarantine_zone import lake_store as qs_store
    return qs_store.list_quarantine(batch_id)


class TestRestartAndReplay:
    def _prepare_paused_batch(self, services, fresh_store=None):
        """质量检查输出隔离后暂停：确认第一条违规，第二条仍待确认。"""
        ingest, quality, quarantine = services
        received = ingest.ingest_batch(
            "ods_evt", "OLTP", [ROW_A, ROW_A_SHUFFLED],
            "2026-11-01T00:00:00", "2026-11-01T23:59:59")
        batch_id = received["batch_id"]
        pair = _excess_row("300.00", "u5")
        extra = _excess_row("999.00", "u6")
        ingest.append_batch_rows(batch_id, [pair, _shuffled(pair), extra])

        result = quality.apply_rules_to_batch(batch_id, [_rule(upper=100)])
        quarantine.isolate_from_quality_result(batch_id, result)
        first_key = result["violation_details"][0]["row_key"]
        confirmed = quarantine.confirm_records(batch_id, [first_key])
        assert confirmed["confirmed_count"] == 1  # 只确认第一条
        state = quarantine.handoff_state(batch_id)["overview"]
        assert state["confirmed_count"] == 1
        assert state["pending_count"] == 1
        return batch_id

    def test_replay_preserves_entry_counts_and_records(self, services):
        ingest, quality, quarantine = services
        batch_id = self._prepare_paused_batch(services)

        replay = quarantine.replay_handoff(batch_id, [_rule(upper=100)])
        assert replay["quality"] == {
            "total_rows": 5,
            "after_dedup": 3,
            "duplicates_removed": 2,
            "clean_rows": 1,
            "violation_rows": 2,
        }
        # 重跑不产生第二条隔离记录
        assert len(lake_store_records(batch_id)) == 2

        conclusions = {item["amount"]: item for item in replay["conclusions"]}
        # 第一条被确认、第二条仍待确认，重启后原规则结论可分别回查
        assert sorted(
            item["status"] for item in conclusions.values()) == ["已确认", "待确认"]
        for item in conclusions.values():
            assert item["first_conclusion"] == "隔离"
            assert item["first_threshold"]["阈值上限"] == 100
            assert item["this_run_conclusion"] == "隔离"

        recompute = quality.recompute_entry_stats(batch_id)
        assert recompute["all_match"] is True
        # 两个入口各自复算，互不覆盖
        assert "ingest_receive" in recompute["entries"]
        assert "quality_check" in recompute["entries"]
        assert recompute["entries"]["ingest_receive"][0]["recomputed"] == {
            "input_rows": 2, "unique_rows": 1, "duplicates_removed": 1}

    def test_restart_replay_uses_only_persisted_state(self, services, fresh_store):
        """模拟重启：把状态导出到新存储引擎后重跑，结论与计数不依赖进程内对象。"""
        ingest, quality, quarantine = services
        batch_id = self._prepare_paused_batch(services)

        snapshot = fresh_store.export_state()

        # 全新引擎实例，只载入导出状态，服务和行身份都不携带进程内记忆
        restarted = LakeStore()
        restarted.load_state(snapshot)
        for module_name in (
            "app.services.ingest_batch",
            "app.services.quality_rule",
            "app.services.quarantine_zone",
            "app.pipeline.chain_runner",
        ):
            module = __import__(module_name, fromlist=["lake_store"])
            setattr(module, "lake_store", restarted)

        restarted_quarantine = QuarantineZoneService()
        restarted_quality = QualityRuleService()
        replay = restarted_quarantine.replay_handoff(batch_id, [_rule(upper=100)])
        assert replay["quality"]["total_rows"] == 5
        assert replay["quality"]["duplicates_removed"] == 2
        statuses = sorted(item["status"] for item in replay["conclusions"])
        assert statuses == ["已确认", "待确认"]
        for item in replay["conclusions"]:
            assert item["first_conclusion"] == "隔离"
            assert item["first_threshold"]["阈值上限"] == 100

        state = restarted_quarantine.handoff_state(batch_id)
        assert len(state["confirmed_rows"]) == 1
        assert len(state["pending_rows"]) == 1
        assert restarted_quality.recompute_entry_stats(batch_id)["all_match"] is True

    def test_confirmed_record_never_redelivered(self, services):
        _, _, quarantine = services
        batch_id = self._prepare_paused_batch(services)
        confirmed_amount = quarantine.handoff_state(batch_id)["confirmed_rows"][0]["amount"]

        released = quarantine.release_for_retry(batch_id)
        # 待确认的另一条可以补交，已确认的记录不交出
        assert released["released_count"] == 1
        assert released["released_rows"][0]["amount"] != confirmed_amount
        # 已确认记录仍在隔离区、仍可回查
        remaining = lake_store_records(batch_id)
        assert len(remaining) == 1
        assert remaining[0]["amount"] == confirmed_amount
        assert remaining[0]["_quarantine_status"] == "已确认"

    def test_handoff_identifies_confirmed_and_pending_after_restart(self, services):
        _, _, quarantine = services
        batch_id = self._prepare_paused_batch(services)

        state = quarantine.handoff_state(batch_id)
        assert len(state["confirmed_rows"]) == 1
        assert len(state["pending_rows"]) == 1
        confirmed_row = state["confirmed_rows"][0]
        pending_row = state["pending_rows"][0]
        assert confirmed_row["row_key"] == row_identity(confirmed_row["row"])
        assert pending_row["row_key"] == row_identity(pending_row["row"])
        assert confirmed_row["row_key"] != pending_row["row_key"]
        # 已确认与待补交可以按同一行身份在批次原始行里定位到
        batch = quality_store_get_batch(batch_id)
        raw_keys = {row_identity(row) for row in batch.rows}
        assert confirmed_row["row_key"] in raw_keys
        assert pending_row["row_key"] in raw_keys


class TestThresholdConflictOnRestore:
    def _prepare_with_180(self, services):
        """隔离 180 与 300，只确认 300，超额线原为 100。"""
        ingest, quality, quarantine = services
        received = ingest.ingest_batch(
            "ods_evt", "OLTP", [ROW_A, ROW_A_SHUFFLED],
            "2026-11-01T00:00:00", "2026-11-01T23:59:59")
        batch_id = received["batch_id"]
        near = _excess_row("180.00", "u5")
        high = _excess_row("300.00", "u6")
        ingest.append_batch_rows(batch_id, [near, high])

        result = quality.apply_rules_to_batch(batch_id, [_rule(upper=100)])
        quarantine.isolate_from_quality_result(batch_id, result)
        confirmed = quarantine.confirm_by_amount(batch_id, ">", 250)
        assert confirmed["confirmed_count"] == 1
        assert quarantine.handoff_state(batch_id)["overview"]["pending_count"] == 1
        return batch_id

    def test_reuse_previous_keeps_180_quarantined(self, services):
        _, _, quarantine = services
        batch_id = self._prepare_with_180(services)

        outcome = quarantine.apply_new_rule_same_batch(
            batch_id, _rule(upper=200), disposition="reuse_previous")
        assert outcome["released"] == 0
        assert outcome["still_quarantined"] == 1
        assert outcome["kept_confirmed"] == 1

        records = {r["amount"]: r for r in lake_store_records(batch_id)}
        assert records["180.00"]["_quarantine_status"] == "待确认"
        assert records["180.00"]["_quarantine_conclusion"] == "隔离"
        # 初次结论与阈值不被覆盖
        assert records["180.00"]["_quarantine_first_threshold"]["阈值上限"] == 100

    def test_reapply_new_rule_releases_180_and_closes_after_ingest(self, services):
        _, quality, quarantine = services
        batch_id = self._prepare_with_180(services)

        outcome = quarantine.apply_new_rule_same_batch(
            batch_id, _rule(upper=200), disposition="reapply_new_rule")
        assert outcome["released"] == 1
        assert outcome["pending_ingest_confirmation"] == 1
        assert outcome["kept_confirmed"] == 1

        released_row = outcome["released_rows"][0]
        assert released_row["amount"] == "180.00"
        # 核回初次隔离结论
        assert released_row["first_conclusion"] == "隔离"
        reconciliation = quarantine.reconcile_record(batch_id, _excess_row("180.00", "u5"))
        assert reconciliation["first_conclusion"] == "隔离"
        assert reconciliation["first_threshold"]["阈值上限"] == 100
        assert reconciliation["current_conclusion"] == "放行"
        assert reconciliation["status"] == "已解除"

        confirmed_record = {r["amount"]: r for r in lake_store_records(batch_id)}["300.00"]
        assert confirmed_record["_quarantine_status"] == "已确认"
        assert confirmed_record["_quarantine_conclusion"] == "隔离"

        closed = quarantine.confirm_ingest(batch_id)
        assert closed["closed_count"] == 1
        assert closed["written_batch_id"] is not None
        written = quality_store_get_batch(closed["written_batch_id"])
        # 正常行 100.00 与放行行 180.00 都进入批次写入；已确认的 300.00 不写入
        assert sorted(row["amount"] for row in written.rows) == ["100.00", "180.00"]
        final = closed["handoff"]["overview"]
        assert final["pending_count"] == 0
        assert final["released_count"] == 0
        assert final["confirmed_count"] == 1
        assert final["closed_count"] == 1

    def test_confirmed_record_not_redelivered_under_new_rule(self, services):
        _, _, quarantine = services
        batch_id = self._prepare_with_180(services)
        quarantine.apply_new_rule_same_batch(
            batch_id, _rule(upper=200), disposition="reapply_new_rule")
        closed = quarantine.confirm_ingest(batch_id)
        written = quality_store_get_batch(closed["written_batch_id"])
        assert all(row["amount"] != "300.00" for row in written.rows)
        # 再次交接也不得把已确认的 300 交出去
        handed = quarantine.handoff_state(batch_id)
        assert all(row["amount"] != "300.00" for row in handed["pending_rows"])
        assert [row["amount"] for row in handed["confirmed_rows"]] == ["300.00"]

    def test_clean_rows_still_written_to_batch(self, services):
        _, quality, quarantine = services
        batch_id = self._prepare_with_180(services)
        outcome = quarantine.apply_new_rule_same_batch(
            batch_id, _rule(upper=200), disposition="reapply_new_rule")
        assert outcome["released"] == 1
        closed = quarantine.confirm_ingest(batch_id)
        written = quality_store_get_batch(closed["written_batch_id"])
        # 正常通过行照常入批，放行行也一并写入
        assert written.row_count == 2
        amounts = sorted(row["amount"] for row in written.rows)
        assert amounts == ["100.00", "180.00"]
        # 重复确认不会重复写入
        again = quarantine.confirm_ingest(batch_id)
        assert again["written_rows"] == 0
        assert quality_store_get_batch(closed["written_batch_id"]).row_count == 2


class TestNewBatchIdIndependent:
    def test_new_batch_id_computes_under_new_rule(self, services):
        ingest, quality, quarantine = services
        rows = [ROW_A, ROW_A_SHUFFLED, _excess_row("180.00", "u5"),
                _excess_row("300.00", "u6")]
        received = ingest.ingest_batch(
            "ods_evt", "OLTP", rows, "2026-11-01T00:00:00", "2026-11-01T23:59:59",
            batch_id="BATCH-NEW-1")
        assert received["batch_id"] == "BATCH-NEW-1"
        assert received["row_count"] == 3
        assert received["duplicates_removed"] == 1

        result = quality.apply_rules_to_batch("BATCH-NEW-1", [_rule(upper=200)])
        # 新批号按 200 线独立计算：100 通过，180 通过，300 超额
        assert result["total_rows"] == 4
        assert result["duplicates_removed"] == 1
        assert result["clean_rows"] == 2
        assert result["violation_rows"] == 1
        assert result["violation_details"][0]["row"]["amount"] == "300.00"

        quarantine.isolate_from_quality_result("BATCH-NEW-1", result)
        state = quarantine.handoff_state("BATCH-NEW-1")["overview"]
        assert state["pending_count"] == 1
        assert state["confirmed_count"] == 0

    def test_new_batch_does_not_touch_old_batch(self, services):
        ingest, quality, quarantine = services
        old = ingest.ingest_batch(
            "ods_evt", "OLTP", [ROW_A, ROW_A_SHUFFLED],
            "2026-11-01T00:00:00", "2026-11-01T23:59:59")
        quality.apply_rules_to_batch(old["batch_id"], [_rule(upper=100)])

        ingest.ingest_batch(
            "ods_evt", "OLTP", [_excess_row("300.00")],
            "2026-11-01T00:00:00", "2026-11-01T23:59:59", batch_id="BATCH-NEW-2")
        new_result = quality.apply_rules_to_batch("BATCH-NEW-2", [_rule(upper=200)])

        old_replay = quality.replay_batch_check(old["batch_id"])
        assert old_replay["total_rows"] == 2
        assert old_replay["duplicates_removed"] == 1
        assert new_result["total_rows"] == 1


class TestNormalRowsWritten:
    def test_clean_rows_written_by_receive_and_quality(self, services):
        ingest, quality, _ = services
        received = ingest.ingest_batch(
            "ods_evt", "OLTP", [ROW_A, ROW_A_SHUFFLED],
            "2026-11-01T00:00:00", "2026-11-01T23:59:59")
        result = quality.apply_rules_to_batch(received["batch_id"], [_rule(upper=100)])
        assert result["clean_rows"] == 1
        assert [row["amount"] for row in result["clean_data"]] == ["100.00"]

        from app.pipeline.chain_runner import write_clean_batch
        written = write_clean_batch("ods_evt", result["clean_data"])
        batch = quality_store_get_batch(written["batch_id"])
        assert batch.row_count == 1
        # 重放同一批通过行不会写出重复数据
        again = write_clean_batch("ods_evt", result["clean_data"] + result["clean_data"])
        assert again["row_count"] == 1


class TestOriginalConclusionLookup:
    def test_each_amount_has_own_original_conclusion(self, services):
        ingest, quality, quarantine = services
        received = ingest.ingest_batch(
            "ods_evt", "OLTP", [ROW_A, ROW_A_SHUFFLED],
            "2026-11-01T00:00:00", "2026-11-01T23:59:59")
        batch_id = received["batch_id"]
        ingest.append_batch_rows(batch_id, [
            _excess_row("180.00", "u5"), _excess_row("300.00", "u6")])
        result = quality.apply_rules_to_batch(batch_id, [_rule(upper=100)])
        quarantine.isolate_from_quality_result(batch_id, result)

        by_amount = {
            detail["row"]["amount"]: detail for detail in result["violation_details"]}
        assert set(by_amount) == {"180.00", "300.00"}

        for amount, detail in by_amount.items():
            record = quarantine.reconcile_record(batch_id, detail["row"])
            assert record is not None
            assert record["row_key"] == detail["row_key"]
            assert record["amount"] == amount
            assert record["first_conclusion"] == "隔离"
            assert record["first_threshold"]["阈值上限"] == 100

        # 用字段顺序打乱的行也能按行身份回查同一结论
        shuffled_180 = _shuffled(_excess_row("180.00", "u5"))
        record = quarantine.reconcile_record(batch_id, shuffled_180)
        assert record["amount"] == "180.00"


class TestSameBatchIdRepost:
    def test_repost_same_batch_id_preserves_history(self, services):
        ingest, quality, _ = services
        first = ingest.ingest_batch(
            "ods_evt", "OLTP", [ROW_A, ROW_A_SHUFFLED],
            "2026-11-01T00:00:00", "2026-11-01T23:59:59", batch_id="BATCH-REPOST")
        quality.apply_rules_to_batch("BATCH-REPOST", [_rule(upper=100)])

        second = ingest.ingest_batch(
            "ods_evt", "OLTP", [ROW_A, ROW_A_SHUFFLED, _excess_row("300.00")],
            "2026-11-01T00:00:00", "2026-11-01T23:59:59", batch_id="BATCH-REPOST")
        assert second["batch_id"] == "BATCH-REPOST"
        assert second["row_count"] == 2
        assert second["duplicates_removed"] == 1

        # 同一批号重新接收后，历次入口计数仍可复算
        recompute = quality.recompute_entry_stats("BATCH-REPOST")
        runs = recompute["entries"]["ingest_receive"]
        assert len(runs) >= 2
        assert all(run["matches"] for run in runs if run["matches"] is not None)


def quality_store_get_batch(batch_id):
    """读取当前存储引擎里的批次记录。"""
    from app.services.quarantine_zone import lake_store as qs_store
    return qs_store.get_batch(batch_id)
