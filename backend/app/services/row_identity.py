"""共享行身份的模块内入口：真实实现在 ``app.row_identity``。

这里只做转出，保证领域服务层与存储层引用的是同一份行身份判断，
不会再出现第二份 "排序字段再哈希" 的实现。
"""
from __future__ import annotations

from app.row_identity import (  # noqa: F401  (re-export)
    QUARANTINE_META_PREFIX,
    canonical_row_items,
    dedupe_rows,
    deduplicate_rows,
    row_fingerprint,
    row_identity,
    row_key,
    same_row,
)
