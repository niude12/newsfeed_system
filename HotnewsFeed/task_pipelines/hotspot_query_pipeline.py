# -*- coding: utf-8 -*-
"""任务① 查询某模块当前热点新闻（底层功能入口）

该模块负责执行「热点查询」流水线：按模块（可选关键词过滤）多源采集新闻 → Embedding
聚类成事件簇 → 热度排序 → 多源交叉核验，最后把结果统一包装成流水线使用的
:class:`~task_pipelines.schemas.PipelineResult` 数据结构。

供协调调度 Agent 调用，也可被前端作为功能直选直接调用。
执行链路：采集(按模块，可选关键词过滤) → 聚类 → 热度排序 → 核验 → 输出优化 → [A2A 简报交接]

模块依赖:
- ``mcp_servers/mcp_access.collect_news``   : 多源资讯采集网关（async）。优先经 MCP 协议
  连远端采集服务器（:8004），远端不可达时降级进程内直调 tools/collect.py；返回 NewsItem 列表。
- ``mcp_servers/mcp_access.cluster_events`` : 事件聚类网关（async）。Embedding 相似度聚成
  事件簇（降级为 TF 向量），返回 EventCluster 列表。
- ``mcp_servers/mcp_access.score_heat``     : 热度评分网关（async）。按「来源数 × 时效衰减 ×
  讨论量 × 权重」给事件簇打分，返回 HotEvent 列表。
- ``mcp_servers/mcp_access.verify_events``  : 多源交叉核验网关（async）。LLM 回填
  可信 / 存疑 / 证据不足，返回带 credibility 的 HotEvent 列表。
- ``task_pipelines/schemas.PipelineResult`` : 所有流水线统一的返回结构 dataclass。
- ``create_logger.logger``                  : 全局日志器（本模块为对齐其它模块而导入，
  实际日志埋点在网关 / 采集工具内部完成）。

典型调用链::

    协调调度 Agent / 前端
      -> hotspot_query_pipeline.run(module, top_n, time_window_hours, keywords)
      -> mcp_servers.mcp_access.collect_news(module, keywords, limit=top_n*5)  # ① 采集
      -> mcp_servers.mcp_access.cluster_events(news)                            # ② 聚类
      -> mcp_servers.mcp_access.score_heat(clusters, time_window_hours)         # ③ 热度排序
      -> mcp_servers.mcp_access.verify_events(events)                           # ④ 核验
      -> PipelineResult(task_type="hotspot_query", items=HotEvent[...])

对外只暴露一个接口：
- run : 异步执行「热点查询」流水线，返回 PipelineResult。
"""

import time
from datetime import datetime, timezone
from typing import List, Optional

from create_logger import logger
from mcp_servers.mcp_access import (cluster_events, collect_news,
                                    score_heat, verify_events)
from .schemas import PipelineResult


async def run(
    module: str,
    top_n: int = 10,
    time_window_hours: int = 24,
    keywords: Optional[List[str]] = None,
) -> PipelineResult:
    """查询某模块当前热点新闻。

    全程走 mcp_servers/mcp_access 网关：优先经 MCP 协议连远端 MCP 服务器
    （采集 :8004 → 加工 :8005）调用对应工具，远端不可达时降级进程内直调 tools.*。

    参数:
        module:            新闻模块，如 "科技" / "财经" / "体育"。
        top_n:             返回热点数量，默认 10。
        time_window_hours: 统计热度的回溯窗口（小时），默认 24。
        keywords:          话题关键词过滤（标题/摘要含任一即保留），如 ["足球"]；无则模块全量。
                           关键词无命中时 collect_news 内部已优雅降级：重试一次 → 放宽为模块
                           RSS 源全量，避免拿到【模拟】兜底或 HN 这类模块无关噪音。

    返回:
        PipelineResult：task_type 固定为 "hotspot_query"，items 为 HotEvent 列表
        （已按热度排序并核验），queried_at 为本次执行时间、elapsed_ms 为耗时毫秒数。

    抛出:
        通常不直接抛异常——MCP 网关内部已捕获远端失败并降级为进程内直调；
        若进程内任一环节（采集 / 聚类 / 热度 / 核验）也失败，底层异常会原样
        向上抛给协调调度 Agent 处理。

    说明:
        - collect_news : mcp_servers/mcp_access.py 的采集网关，返回 NewsItem 列表。
        - cluster_events : 聚类网关，用 Embedding 相似度把新闻聚成事件簇
          （返回 tools/process.EventCluster），embedding 不可用时降级 TF 向量。
        - score_heat : 热度评分网关，按「来源数 × 时效衰减 × 讨论量 × 权重」打分，
          把事件簇转成 HotEvent。
        - verify_events : 核验网关，用 LLM 对事件做多源交叉核验，给 credibility
          回填「可信 / 存疑 / 证据不足」。
        - PipelineResult : task_pipelines/schemas.py 的 dataclass，所有流水线统一返回结构。
    """
    started = time.time()  # 记录流水线起始时间，用于计算本次执行耗时（elapsed_ms）。
    # ① 采集：按模块多源抓取。limit 取 max(top_n*5, 30)：放大 5 倍候选量保证聚类够用，
    #    又兜底至少 30 条，避免 top_n 过小时候选不足。
    news = await collect_news(module, keywords=keywords, limit=max(top_n * 5, 30))
    # ② 聚类：Embedding 相似度聚成事件簇（降级为 TF 向量）。
    clusters = await cluster_events(news)
    # ③ 热度排序：来源数 × 时效 × 讨论量 → 热点事件。
    events = await score_heat(clusters, time_window_hours)
    # ④ 多源交叉核验：LLM 回填 可信 / 存疑 / 证据不足。
    verified = await verify_events(events)
    # 把核验后的热点事件统一包装成 PipelineResult（流水线标准返回结构）。
    result = PipelineResult(
        task_type="hotspot_query",  # 任务类型标识，供上层路由 / 前端区分三类任务。
        items=verified[:top_n],     # 核验后的热点事件，只取前 top_n 条返回。
        # 统一用 UTC 当前时间、精确到秒的 ISO 8601 字符串作为本次执行时间戳。
        queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        # 把起始时刻的差值换算成毫秒整数，供上层统计 / 展示耗时。
        elapsed_ms=int((time.time() - started) * 1000),
    )
    return result  # 返回统一结构，调用方无需关心内部聚类/核验细节。
