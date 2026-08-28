# -*- coding: utf-8 -*-
"""任务① 查询某模块当前热点新闻（底层功能入口）

供协调调度 Agent 调用，也可被前端作为功能直选直接调用。
执行：采集(按模块，可选关键词过滤) → 聚类 → 热度排序 → 核验 → 输出优化 → [A2A 简报交接]
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

    Args:
        module: 新闻模块，如 "科技" / "财经" / "体育"。
        top_n: 返回热点数量，默认 10。
        time_window_hours: 统计热度的回溯窗口（小时），默认 24。
        keywords: 话题关键词过滤（标题/摘要含任一即保留），如 ["足球"]；无则模块全量。
                  关键词无命中时 collect_news 内部已优雅降级：重试一次 → 放宽为模块 RSS 源全量，
                  避免拿到【模拟】兜底或 HN 这类模块无关噪音。

    Returns:
        PipelineResult: task_type="hotspot_query"，items 为 HotEvent 列表。
    """
    started = time.time()
    # ① 采集：按模块多源抓取（top_n 放大 5 倍采集量，保证聚类后有足够候选）
    news = await collect_news(module, keywords=keywords, limit=max(top_n * 5, 30))
    # ② 聚类：Embedding 相似度聚成事件簇（降级为 TF 向量）
    clusters = await cluster_events(news)
    # ③ 热度排序：来源数 × 时效 × 讨论量 → 热点事件
    events = await score_heat(clusters, time_window_hours)
    # ④ 多源交叉核验：LLM 回填 可信 / 存疑 / 证据不足
    verified = await verify_events(events)
    result = PipelineResult(
        task_type="hotspot_query",
        items=verified[:top_n],
        queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        elapsed_ms=int((time.time() - started) * 1000),
    )
    return result
