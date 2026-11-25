"""共享行身份判断：同一行不论字段顺序如何，都得到同一个身份。

入湖批次接收与入湖链路质量检查原本各自维护一份"排序字段再哈希"的去重实现，
两处逻辑一旦漂移就会出现"接收去重了、质量检查没去重"的偏差。本模块把行身份
和去重收敛为唯一实现，但**不共享输入集**：每个入口仍对自己的 rows 调用
``dedupe_rows``，按本入口收到的数据独立计数。

放在 ``app/`` 顶层而不是 ``app/services/``，是为了让存储引擎也能直接复用，
同时避免 "services 包初始化 -> lake_store -> services" 的循环导入。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

QUARANTINE_META_PREFIX = "_quarantine_"


def canonical_row_items(
    row: dict[str, Any], *, drop_quarantine_meta: bool = True
) -> list[tuple[str, Any]]:
    """把一行转换成与字段顺序无关的规范形（按字段名排序的键值对）。"""
    items: list[tuple[str, Any]] = []
    for raw_key, value in row.items():
        key = str(raw_key)
        if drop_quarantine_meta and key.startswith(QUARANTINE_META_PREFIX):
            continue
        items.append((key, value))
    items.sort(key=lambda item: item[0])
    return items


def row_identity(row: dict[str, Any], *, drop_quarantine_meta: bool = True) -> str:
    """计算行身份：字段顺序不影响结果。

    默认忽略隔离区附加的 ``_quarantine_*`` 元数据，这样隔离记录与它在批次里的
    原始行仍然能被识别为同一行，重放或回查时不会产生第二份身份。
    """
    payload = json.dumps(
        canonical_row_items(row, drop_quarantine_meta=drop_quarantine_meta),
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# 语义化别名：调用方按自己的语境取用，实现只有一份。
row_key = row_identity
row_fingerprint = row_identity


def same_row(left: dict[str, Any], right: dict[str, Any]) -> bool:
    """判断两行是否为同一行：字段顺序、隔离元数据都不参与比较。"""
    return row_identity(left) == row_identity(right)


def dedupe_rows(
    rows: Iterable[dict[str, Any]], *, drop_quarantine_meta: bool = True
) -> dict[str, Any]:
    """按共享行身份对一组行去重，保留首次出现的行。

    返回的 ``duplicates_removed`` 是重复出现的行数（不是去重后的行数），
    方便各入口沿用自己原有的计数口径。每个入口传入自己的输入集，
    因此 "入口原始数 / 重复数" 仍然只反映该入口看到的数据。
    """
    unique_rows: list[dict[str, Any]] = []
    duplicate_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    input_rows = 0
    for row in rows:
        input_rows += 1
        key = row_identity(row, drop_quarantine_meta=drop_quarantine_meta)
        if key in seen:
            duplicate_rows.append(row)
        else:
            seen.add(key)
            unique_rows.append(row)
    return {
        "input_rows": input_rows,
        "unique_rows": unique_rows,
        "duplicate_rows": duplicate_rows,
        "duplicates_removed": len(duplicate_rows),
    }


# 同义入口名，便于不同模块沿用各自原有的命名习惯。
deduplicate_rows = dedupe_rows
