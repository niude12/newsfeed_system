# -*- coding: utf-8 -*-
"""任务② 查询某模块最新新闻（底层功能入口）

该模块负责执行「最新资讯」流水线：按模块（可选关键词过滤）采集多源新闻，
内部已完成「标题指纹去重 + 按时间倒序」，最后把结果统一包装成流水线使用的
:class:`~task_pipelines.schemas.PipelineResult` 数据结构。

供协调调度 Agent 调用，也可被前端作为功能直选直接调用。
执行链路：采集(按模块，可选关键词过滤) → 去重 / 按时间倒序 → 输出优化 → [A2A 简报交接]

模块依赖:
- ``mcp_servers/mcp_access.collect_news`` : 多源资讯采集网关（async）。
  优先经 MCP 协议连远端采集服务器（:8004），远端不可达时降级进程内直调
  tools/collect.py；返回 ``NewsItem`` 列表（已去重、时间倒序）。
- ``task_pipelines/schemas.PipelineResult`` : 所有流水线统一的返回结构
  dataclass，内含 task_type / items / queried_at / elapsed_ms / error 字段。
- ``create_logger.logger``                : 全局日志器（本模块为对齐其它模块而导入，
  实际日志埋点在网关 / 采集工具内部完成）。

典型调用链::

    协调调度 Agent / 前端
      -> latest_news_pipeline.run(module, count, keywords)
      -> mcp_servers.mcp_access.collect_news(module, keywords, limit=count)  # MCP 网关
      ->   (优先) MCP collect 服务器 :8004 的 collect_news 工具
      ->   (降级) tools/collect.collect_news(...)  # 进程内直调
      -> PipelineResult(task_type="latest_news", items=NewsItem[...])

对外只暴露一个接口：
- run : 异步执行「最新资讯」流水线，返回 PipelineResult。
"""

import time
from datetime import datetime, timezone
from typing import List, Optional

from create_logger import logger
from mcp_servers.mcp_access import collect_news
from .schemas import PipelineResult


async def run(
    module: str,
    count: int = 20,
    keywords: Optional[List[str]] = None,
) -> PipelineResult:
    """查询某模块最新新闻。

    采集网关 collect_news 内部已做「标题指纹去重 + 时间倒序」，
    这里直接取前 count 条即是最新且不重复的资讯。

    参数:
        module:   新闻模块，如 "科技" / "财经" / "体育"。
        count:    返回条数，默认 20。
        keywords: 话题关键词过滤（标题/摘要含任一即保留），如 ["足球"]；无则模块全量。
                  关键词无命中时 collect_news 内部已优雅降级：重试一次 → 放宽为模块
                  RSS 源全量，避免拿到【模拟】兜底或 HN 这类模块无关噪音。

    返回:
        PipelineResult：task_type 固定为 "latest_news"，items 为 NewsItem 列表
        （时间倒序，已去重），queried_at 为本次执行时间、elapsed_ms 为耗时毫秒数。

    抛出:
        通常不直接抛异常——MCP 网关内部已捕获远端失败并降级为进程内直调；
        若进程内采集也失败，底层异常会原样向上抛给协调调度 Agent 处理。

    说明:
        collect_news 是 mcp_servers/mcp_access.py 提供的异步网关函数，内部用
        mcp_call_tool 经 streamable-http 连接远端 MCP 采集服务器（:8004），
        失败时降级直调 tools/collect.collect_news。
        NewsItem / PipelineResult 都是 task_pipelines/schemas.py 里的 dataclass，
        前者是单条资讯模型（news_id / module / title / source / published_at / url），
        后者是所有流水线统一的返回结构。
    """
    started = time.time()  # 记录流水线起始时间，用于计算本次执行耗时（elapsed_ms）。
    # 经 MCP 网关采集最新新闻；网关内部负责「远端优先、进程内兜底」，已按 count 截断。
    news = await collect_news(module, keywords=keywords, limit=count)
    # 把采集结果统一包装成 PipelineResult（流水线标准返回结构），供上层/前端统一消费。
    result = PipelineResult(
        task_type="latest_news",  # 任务类型标识，供上层路由 / 前端区分三类任务。
        items=news[:count],       # 网关已截断，此处再截断一次是双保险。
        # 统一用 UTC 当前时间、精确到秒的 ISO 8601 字符串作为本次执行时间戳。
        queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        # 把起始时刻的差值换算成毫秒整数，供上层统计 / 展示耗时。
        elapsed_ms=int((time.time() - started) * 1000),
    )
    return result  # 返回统一结构，调用方无需关心内部采集细节。
