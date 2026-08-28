# -*- coding: utf-8 -*-
"""任务② 查询某模块最新新闻（底层功能入口）

供协调调度 Agent 调用，也可被前端作为功能直选直接调用。
执行：采集(按模块，可选关键词过滤) → 去重 / 按时间倒序 → 输出优化 → [A2A 简报交接]
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

    采集工具 tools.collect.collect_news 内部已做「标题指纹去重 + 时间倒序」，
    这里直接取前 count 条即是最新且不重复的资讯。

    Args:
        module: 新闻模块，如 "科技" / "财经" / "体育"。
        count: 返回条数，默认 20。
        keywords: 话题关键词过滤（标题/摘要含任一即保留），如 ["足球"]；无则模块全量。
                  关键词无命中时 collect_news 内部已优雅降级：重试一次 → 放宽为模块 RSS 源全量，
                  避免拿到【模拟】兜底或 HN 这类模块无关噪音。

    Returns:
        PipelineResult: task_type="latest_news"，items 为 NewsItem 列表（时间倒序）。
    """
    started = time.time()
    news = await collect_news(module, keywords=keywords, limit=count)
    result = PipelineResult(
        task_type="latest_news",
        items=news[:count],
        queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        elapsed_ms=int((time.time() - started) * 1000),
    )
    return result
