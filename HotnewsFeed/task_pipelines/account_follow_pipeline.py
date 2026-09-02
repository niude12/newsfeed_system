# -*- coding: utf-8 -*-
"""任务③ 关注某账户新闻 / 作品发布（底层功能入口）

该模块负责执行「账户追踪」流水线：给定账户标识与平台，从账户主页采集其最近
发布的新闻 / 作品（按 since 过滤「新发布」），并把结果统一包装成流水线使用的
:class:`~task_pipelines.schemas.PipelineResult` 数据结构。

供协调调度 Agent 调用，也可被前端作为功能直选直接调用。
执行链路：账户监控采集(轮询) → 新发布过滤 → 输出优化 → [A2A 简报交接]

模块依赖:
- ``mcp_servers/mcp_access.fetch_account_posts`` : 账户发布采集网关（async）。
  优先经 MCP 协议连远端采集服务器（:8004）调用同名工具，远端不可达时降级
  进程内直调 tools/collect.py，返回 ``AccountPost`` 列表（时间倒序）。
- ``task_pipelines/schemas.PipelineResult``     : 所有流水线统一的返回结构
  dataclass，内含 task_type / items / queried_at / elapsed_ms / error 字段。

典型调用链::

    协调调度 Agent / 前端
      -> account_follow_pipeline.run(account, platform, since, limit)
      -> mcp_servers.mcp_access.fetch_account_posts(...)   # MCP 网关
      ->   (优先) MCP collect 服务器 :8004 的 fetch_account_posts 工具
      ->   (降级) tools/collect.fetch_account_posts(...)   # 进程内直调
      -> PipelineResult(task_type="account_follow", items=AccountPost[...])

对外只暴露一个接口：
- run : 异步执行「账户追踪」流水线，返回 PipelineResult。
"""

import time
from datetime import datetime, timezone
from typing import Optional

from mcp_servers.mcp_access import fetch_account_posts
from .schemas import PipelineResult


async def run(
    account: str,
    platform: str,
    since: Optional[str] = None,
    limit: int = 20,
) -> PipelineResult:
    """检查指定账户的新闻 / 作品发布。

    采集网关 fetch_account_posts 会按 since 过滤「新发布」并时间倒序；
    未配置的账户会降级为【模拟】样例数据（避免整条流水线失败）。

    参数:
        account:  账户标识（如 weibo uid / 公众号名），透传给采集网关。
        platform: 平台，如 "weibo" / "wechat" / "xiaohongshu"，决定按哪个源采集。
        since:    仅返回该时间点之后的新发布（ISO 8601 字符串）；None 时返回
                  最近一次检查以来的新内容。
        limit:    最多返回条数，默认 20。

    返回:
        PipelineResult：task_type 固定为 "account_follow"，items 为 AccountPost
        列表（时间倒序），queried_at 为本次执行时间、elapsed_ms 为耗时毫秒数。

    抛出:
        通常不直接抛异常——MCP 网关内部已捕获远端失败并降级为进程内直调；
        若进程内采集也失败，底层异常会原样向上抛给协调调度 Agent 处理。

    说明:
        fetch_account_posts 是 mcp_servers/mcp_access.py 提供的异步网关函数，
        内部用 mcp_call_tool 经 streamable-http 连接远端 MCP 采集服务器，
        失败时降级直调 tools/collect.fetch_account_posts。
        PipelineResult 是 task_pipelines/schemas.py 里的 dataclass，是所有
        流水线统一的返回结构，便于前端 / 协调器统一消费。
    """
    started = time.time()  # 记录流水线起始时间，用于计算本次执行耗时（elapsed_ms）。
    # 经 MCP 网关采集账户发布内容；网关内部负责「远端优先、进程内兜底」。
    posts = await fetch_account_posts(account, platform, since=since, limit=limit)
    # 把采集结果统一包装成 PipelineResult（流水线标准返回结构），供上层/前端统一消费。
    result = PipelineResult(
        task_type="account_follow",  # 任务类型标识，供上层路由 / 前端区分三类任务。
        items=posts[:limit],         # 网关已按 limit 截断，此处再截断一次是双保险。
        # 统一用 UTC 当前时间、精确到秒的 ISO 8601 字符串作为本次执行时间戳。
        queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        # 把起始时刻的差值换算成毫秒整数，供上层统计 / 展示耗时。
        elapsed_ms=int((time.time() - started) * 1000),
    )
    return result  # 返回统一结构，调用方无需关心内部采集细节。
