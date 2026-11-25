"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

跨模块业务动作集中放在 ``operations``，其余模块仍走统一的资源路由。
"""
from __future__ import annotations

from app.routers import operations, resource

ROUTERS = [resource, operations]
