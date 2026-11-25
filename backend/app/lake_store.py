"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按批次隔离
- 写锁：并发提交冲突检测

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


# ----------------------------- 文件注册 -----------------------------

@dataclass
class PartitionFile:
    """分区文件：每个文件属于一个分区。"""
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。

    ``rows`` 保留原始批次行（含重复行），各入口在自己的输入集上去重并把自己的
    计数写进 ``entry_stats``，这样重放时能复算每个入口原有的原始数/重复数。
    """
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 原始批次行（未去重，保留各入口的输入集）
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float
    accepted_rows: list[dict[str, Any]] = field(default_factory=list)  # 接收入口去重后接收的行
    clean_rows: list[dict[str, Any]] = field(default_factory=list)  # 质量检查通过行
    violation_rows: list[dict[str, Any]] = field(default_factory=list)  # 质量检查违规行
    entry_stats: dict[str, dict[str, Any]] = field(default_factory=dict)  # 各入口最近一次计数
    entry_inputs: dict[str, list[dict[str, Any]]] = field(default_factory=dict)  # 各入口自己的输入集
    entry_stats_history: dict[str, list[dict[str, Any]]] = field(default_factory=dict)  # 各入口历次计数
    rule_snapshot: dict[str, dict[str, Any]] = field(default_factory=dict)  # 初次校验的规则阈值
    rule_history: list[dict[str, Any]] = field(default_factory=list)  # 每次校验用的规则
    quality_result: dict[str, Any] = field(default_factory=dict)  # 最近一次质量检查结果
    written_batch_id: str | None = None  # 通过行写入后生成的写入批次
    written_row_count: int = 0  # 已写入的通过行数
    written_keys: set[str] = field(default_factory=set)  # 已写入的行身份，保证幂等


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据
    - quarantine: 隔离区数据
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：batch_id -> BatchRecord
        self._batches: dict[str, BatchRecord] = {}

        # 隔离区：batch_id -> list[dict]（被拦截的数据行，含交接状态）
        self._quarantine: dict[str, list[dict[str, Any]]] = {}

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

    # ---- 文件操作 ----

    def register_file(
        self, lake_table: str, partition_key: str, rows: int, bytes_: int, manifest: str | None = None
    ) -> PartitionFile:
        file_id = f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(f"{lake_table}:{partition_key}:{time.time()}".encode()).hexdigest()
        pf = PartitionFile(
            file_id=file_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=time.time(),
        )
        self._files.setdefault(lake_table, {})[file_id] = pf
        return pf

    def list_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        files = list(self._files.get(lake_table, {}).values())
        if partition_key is not None:
            files = [f for f in files if f.partition_key == partition_key]
        return files

    def get_file(self, lake_table: str, file_id: str) -> PartitionFile | None:
        return self._files.get(lake_table, {}).get(file_id)

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        table_files = self._files.get(lake_table, {})
        removed = 0
        for fid in file_ids:
            if fid in table_files:
                del table_files[fid]
                removed += 1
        return removed

    def replace_file_list(
        self, lake_table: str, old_file_ids: list[str], new_files: list[PartitionFile]
    ) -> None:
        """原子替换：先注册新文件，再删除旧文件。保证归并不留半份清单。"""
        table_files = self._files.setdefault(lake_table, {})
        for nf in new_files:
            table_files[nf.file_id] = nf
        for fid in old_file_ids:
            table_files.pop(fid, None)

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
    ) -> LakeSnapshot:
        snap_id = f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = hashlib.md5(
            ",".join(sorted(file_ids)).encode()
        ).hexdigest()
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status="generating",
            created_at=time.time(),
        )
        chain = self._snapshots.setdefault(lake_table, [])
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    @staticmethod
    def _batch_checksum(rows: list[dict[str, Any]]) -> str:
        return hashlib.md5(
            str(sorted([str(r) for r in rows])).encode()
        ).hexdigest()

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
        *, accepted_rows: list[dict[str, Any]] | None = None,
    ) -> BatchRecord:
        accepted = list(accepted_rows) if accepted_rows is not None else list(rows)
        # 校验值覆盖实际接收的行，保持与"去重后落湖"的原有语义一致
        checksum = self._batch_checksum(accepted)
        previous = self._batches.get(batch_id)
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=checksum,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
            accepted_rows=accepted,
        )
        if previous is not None:
            # 同批号重新接收时保留历次计数与规则快照，重放仍能复算各入口原有计数
            br.entry_stats_history = dict(previous.entry_stats_history)
            br.entry_inputs = dict(previous.entry_inputs)
            br.rule_snapshot = dict(previous.rule_snapshot)
            br.rule_history = list(previous.rule_history)
        self._batches[batch_id] = br
        return br

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        return self._batches.get(batch_id)

    def append_batch_rows(self, batch_id: str,
                          rows: list[dict[str, Any]]) -> BatchRecord | None:
        """给已有批次的原始行追加数据。

        追加只影响原始行集合；接收行（``accepted_rows``）由调用方按共享行身份
        重新去重后写回，再调用 ``recompute_batch_checksum`` 对齐校验值。
        """
        br = self._batches.get(batch_id)
        if br is None:
            return None
        br.rows.extend(list(rows))
        br.row_count = len(br.rows)
        return br

    def recompute_batch_checksum(self, batch_id: str) -> BatchRecord | None:
        """按当前接收行（缺省为原始行）重算批次校验值。"""
        br = self._batches.get(batch_id)
        if br is None:
            return None
        source = br.accepted_rows if br.accepted_rows else br.rows
        br.checksum = self._batch_checksum(source)
        return br

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [b for b in self._batches.values() if b.lake_table == lake_table]

    def update_batch_status(self, batch_id: str, status: str, snapshot_id: str | None = None) -> BatchRecord | None:
        br = self._batches.get(batch_id)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    def record_entry_stats(self, batch_id: str, entry: str,
                           stats: dict[str, Any],
                           input_rows: list[dict[str, Any]] | None = None) -> None:
        """记录某入口本次的计数与输入集，供重放时复算该入口原有的原始数/重复数。

        每个入口保留自己的输入集；同批号重跑时新结果追加到历史而不覆盖首次
        结果，因此各入口原有的计数始终能按当时的输入集复算出来。
        """
        br = self._batches.get(batch_id)
        if br is None:
            return
        record = dict(stats)
        record["at"] = time.time()
        br.entry_stats[entry] = dict(record)
        history_record = dict(record)
        if input_rows is not None:
            br.entry_inputs[entry] = list(input_rows)
            history_record["input_rows_sample"] = list(input_rows)
        br.entry_stats_history.setdefault(entry, []).append(history_record)

    # ---- 隔离区 ----

    QUARANTINE_STATUS_PENDING = "待确认"
    QUARANTINE_STATUS_CONFIRMED = "已确认"
    QUARANTINE_STATUS_RELEASED = "已解除"
    QUARANTINE_STATUS_CLOSED = "已关闭"

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]],
        rule: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。

        去重与幂等按共享行身份判断：同一批次、同一规则下的同一行只保留一条隔离
        记录；重跑质量检查不会重复交付，已确认的记录会原样保留、不再进入待交队列。
        """
        from app.row_identity import row_identity

        existing = self._quarantine.setdefault(batch_id, [])
        by_key = {
            (qr.get("_quarantine_rule"), qr.get("_quarantine_key")): qr
            for qr in existing
        }
        touched: list[dict[str, Any]] = []
        now = time.time()
        for row in rows:
            key = row_identity(row)
            record_key = (rule_id, key)
            if record_key in by_key:
                touched.append(by_key[record_key])
                continue
            qr = dict(row)
            qr["_quarantine_batch"] = batch_id
            qr["_quarantine_rule"] = rule_id
            qr["_quarantine_rule_name"] = rule_name
            qr["_quarantine_time"] = now
            qr["_quarantine_key"] = key
            qr["_quarantine_status"] = self.QUARANTINE_STATUS_PENDING
            qr["_quarantine_conclusion"] = "隔离"
            qr["_quarantine_first_conclusion"] = "隔离"
            qr["_quarantine_threshold"] = self._rule_threshold(rule)
            qr["_quarantine_first_threshold"] = self._rule_threshold(rule)
            qr["_quarantine_rule_def"] = dict(rule or {})
            qr["_quarantine_first_rule_def"] = dict(rule or {})
            qr["_quarantine_confirmed_at"] = None
            qr["_quarantine_closed_at"] = None
            qr["_quarantine_replay_count"] = 0
            existing.append(qr)
            by_key[record_key] = qr
            touched.append(qr)
        return touched

    @staticmethod
    def _rule_threshold(rule: dict[str, Any] | None) -> dict[str, Any]:
        if not rule:
            return {"阈值下限": None, "阈值上限": None}
        return {
            "阈值下限": rule.get("阈值下限"),
            "阈值上限": rule.get("阈值上限"),
        }

    def list_quarantine(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is not None:
            return list(self._quarantine.get(batch_id, []))
        result: list[dict[str, Any]] = []
        for rows in self._quarantine.values():
            result.extend(rows)
        return result

    def pending_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """仍需补交/待处置的隔离记录：未确认、未解除、未关闭。"""
        return [
            qr for qr in self._quarantine.get(batch_id, [])
            if qr.get("_quarantine_status") == self.QUARANTINE_STATUS_PENDING
        ]

    def confirmed_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """已确认的隔离记录：已确认的高额记录不得再次交付。"""
        return [
            qr for qr in self._quarantine.get(batch_id, [])
            if qr.get("_quarantine_status") == self.QUARANTINE_STATUS_CONFIRMED
        ]

    def confirm_quarantine(self, batch_id: str, keys: list[str] | None = None) -> list[dict[str, Any]]:
        """确认隔离记录：确认后的记录不再进入待交队列。"""
        confirmed: list[dict[str, Any]] = []
        for qr in self._quarantine.get(batch_id, []):
            if qr.get("_quarantine_status") != self.QUARANTINE_STATUS_PENDING:
                continue
            if keys is not None and qr.get("_quarantine_key") not in keys:
                continue
            qr["_quarantine_status"] = self.QUARANTINE_STATUS_CONFIRMED
            qr["_quarantine_confirmed_at"] = time.time()
            confirmed.append(qr)
        return confirmed

    def resolve_quarantine(
        self, batch_id: str, rule_id: str, retained_keys: set[str],
        rule: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """按新规则重新处置同一批号的隔离记录。

        - 已确认的记录：保持结论，不重新交付（``kept_confirmed``）。
        - 未确认且新规则仍判违规：维持隔离，继续待交（``reheld``）。
        - 未确认且新规则判通过：核回初次隔离结论，改为放行并进入待入湖确认，
          入湖确认后关闭待交记录（``released``）。
        """
        released: list[dict[str, Any]] = []
        reheld: list[dict[str, Any]] = []
        kept_confirmed: list[dict[str, Any]] = []
        threshold = self._rule_threshold(rule)
        for qr in self._quarantine.get(batch_id, []):
            if qr.get("_quarantine_rule") != rule_id:
                continue
            status = qr.get("_quarantine_status")
            if status == self.QUARANTINE_STATUS_CONFIRMED:
                kept_confirmed.append(qr)
                continue
            if status in (self.QUARANTINE_STATUS_RELEASED, self.QUARANTINE_STATUS_CLOSED):
                continue
            qr["_quarantine_replay_count"] = int(qr.get("_quarantine_replay_count", 0)) + 1
            qr["_quarantine_threshold"] = threshold
            qr["_quarantine_rule_def"] = dict(rule or {})
            if qr.get("_quarantine_key") in retained_keys:
                reheld.append(qr)
                conclusion = "隔离"
            else:
                qr["_quarantine_status"] = self.QUARANTINE_STATUS_RELEASED
                qr["_quarantine_conclusion"] = "放行"
                qr["_quarantine_resolution"] = {
                    "basis": "reapply_new_rule",
                    "first_conclusion": qr.get("_quarantine_first_conclusion"),
                    "first_threshold": dict(qr.get("_quarantine_first_threshold", {})),
                    "new_threshold": dict(threshold),
                    "resolved_at": time.time(),
                }
                released.append(qr)
                conclusion = "放行"
            history = qr.setdefault("_quarantine_conclusions", [])
            history.append({
                "rule_id": rule_id,
                "threshold": dict(threshold),
                "conclusion": conclusion,
                "at": time.time(),
            })
        return {
            "released": released,
            "reheld": reheld,
            "kept_confirmed": kept_confirmed,
        }

    def close_pending_handoffs(self, batch_id: str) -> list[dict[str, Any]]:
        """入湖确认后关闭待交记录：放行行已重新入湖，隔离交接闭环。"""
        closed: list[dict[str, Any]] = []
        for qr in self._quarantine.get(batch_id, []):
            if qr.get("_quarantine_status") == self.QUARANTINE_STATUS_RELEASED:
                qr["_quarantine_status"] = self.QUARANTINE_STATUS_CLOSED
                qr["_quarantine_closed_at"] = time.time()
                closed.append(qr)
        return closed

    def quarantine_record_for_row(self, batch_id: str,
                                  row: dict[str, Any]) -> dict[str, Any] | None:
        """按行身份回查某行的隔离记录与结论（原规则结论保留在初次结论里）。"""
        from app.row_identity import row_identity

        key = row_identity(row)
        for qr in self._quarantine.get(batch_id, []):
            if qr.get("_quarantine_key") != key:
                continue
            return {
                "batch_id": batch_id,
                "row_key": key,
                "rule_id": qr.get("_quarantine_rule"),
                "amount": qr.get("amount"),
                "status": qr.get("_quarantine_status"),
                "first_conclusion": qr.get("_quarantine_first_conclusion"),
                "first_threshold": dict(qr.get("_quarantine_first_threshold", {})),
                "first_rule": dict(qr.get("_quarantine_first_rule_def", {})),
                "current_conclusion": qr.get("_quarantine_conclusion"),
                "current_threshold": dict(qr.get("_quarantine_threshold", {})),
                "resolution": dict(qr["_quarantine_resolution"])
                if qr.get("_quarantine_resolution") else None,
                "conclusion_history": list(qr.get("_quarantine_conclusions", [])),
            }
        return None

    def quarantine_overview(self, batch_id: str) -> dict[str, Any]:
        """隔离交接视图：按行身份区分已确认、待交、已放行、已关闭。"""
        records = self._quarantine.get(batch_id, [])
        buckets: dict[str, list[str]] = {
            self.QUARANTINE_STATUS_PENDING: [],
            self.QUARANTINE_STATUS_CONFIRMED: [],
            self.QUARANTINE_STATUS_RELEASED: [],
            self.QUARANTINE_STATUS_CLOSED: [],
        }
        for qr in records:
            status = qr.get("_quarantine_status")
            key = qr.get("_quarantine_key")
            if status in buckets and key not in buckets[status]:
                buckets[status].append(key)
        return {
            "batch_id": batch_id,
            "pending_keys": buckets[self.QUARANTINE_STATUS_PENDING],
            "confirmed_keys": buckets[self.QUARANTINE_STATUS_CONFIRMED],
            "released_keys": buckets[self.QUARANTINE_STATUS_RELEASED],
            "closed_keys": buckets[self.QUARANTINE_STATUS_CLOSED],
            "pending_count": len(buckets[self.QUARANTINE_STATUS_PENDING]),
            "confirmed_count": len(buckets[self.QUARANTINE_STATUS_CONFIRMED]),
            "released_count": len(buckets[self.QUARANTINE_STATUS_RELEASED]),
            "closed_count": len(buckets[self.QUARANTINE_STATUS_CLOSED]),
        }

    def release_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """回炉重入：从隔离区取出仍需补交的数据行。

        已确认的记录不交出、也不删除；已放行/已关闭的记录不再重复交出；
        其余待确认记录按行身份去重后返回，并从隔离区移出。
        """
        records = self._quarantine.get(batch_id, [])
        kept = [
            r for r in records
            if r.get("_quarantine_status") in (
                self.QUARANTINE_STATUS_CONFIRMED,
                self.QUARANTINE_STATUS_RELEASED,
                self.QUARANTINE_STATUS_CLOSED,
            )
        ]
        deliverable = [
            r for r in records
            if r.get("_quarantine_status") == self.QUARANTINE_STATUS_PENDING
        ]
        if kept:
            self._quarantine[batch_id] = kept
        else:
            self._quarantine.pop(batch_id, None)
        cleaned: list[dict[str, Any]] = []
        seen: set[str] = set()
        for r in deliverable:
            key = r.get("_quarantine_key")
            if key in seen:
                continue
            seen.add(key)
            cleaned_row = {k: v for k, v in r.items() if not k.startswith("_quarantine_")}
            cleaned.append(cleaned_row)
        return cleaned

    # ---- 归并检查点 ----

    def save_merge_checkpoint(self, merge_id: str, partial_file_ids: list[str]) -> None:
        self._merge_checkpoints[merge_id] = {
            "partial_file_ids": list(partial_file_ids),
            "saved_at": time.time(),
        }

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_checkpoints.get(merge_id)

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        self._merge_checkpoints.pop(merge_id, None)

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 存储引擎自检与复位 ----

    def export_state(self) -> dict[str, Any]:
        """导出全部运行时数据，用于模拟进程重启或跨实例恢复。"""
        return copy.deepcopy({
            "files": self._files,
            "snapshots": self._snapshots,
            "watermarks": self._watermarks,
            "batches": self._batches,
            "quarantine": self._quarantine,
            "merge_checkpoints": self._merge_checkpoints,
        })

    def load_state(self, state: dict[str, Any]) -> None:
        """载入先前导出的运行时数据；载入后各入口仍按原有计数与行身份工作。"""
        self._files = copy.deepcopy(state.get("files", {}))
        self._snapshots = copy.deepcopy(state.get("snapshots", {}))
        self._watermarks = copy.deepcopy(state.get("watermarks", {}))
        self._batches = copy.deepcopy(state.get("batches", {}))
        self._quarantine = copy.deepcopy(state.get("quarantine", {}))
        self._merge_checkpoints = copy.deepcopy(state.get("merge_checkpoints", {}))

    def reset(self) -> None:
        """清空全部运行时数据，供测试与新批号场景使用。"""
        self._files.clear()
        self._snapshots.clear()
        self._watermarks.clear()
        self._batches.clear()
        self._quarantine.clear()
        self._merge_checkpoints.clear()

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }


# 全局单例
lake_store = LakeStore()