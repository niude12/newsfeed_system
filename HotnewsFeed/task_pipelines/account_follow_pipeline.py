# -*- coding: utf-8 -*-
"""任务③ 关注某账户新闻 / 作品发布（底层功能入口）

供协调调度 Agent 调用，也可被前端作为功能直选直接调用。
执行：账户监控采集(轮询) → 新发布过滤 → 输出优化 → [A2A 简报交接]
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

    采集工具 tools.collect.fetch_account_posts 会按 since 过滤「新发布」并时间倒序，
    未配置的账户会降级为【模拟】样例数据（避免整条流水线失败）。

    Args:
        account: 账户标识（如 weibo uid / 公众号名）。
        platform: 平台，如 "weibo" / "wechat" / "xiaohongshu"。
        since: 仅返回该时间点之后的新发布（ISO 8601）；None 时返回最近一次检查以来的新内容。
        limit: 最多返回条数，默认 20。

    Returns:
        PipelineResult: task_type="account_follow"，items 为 AccountPost 列表（时间倒序）。
    """
    started = time.time()
    posts = await fetch_account_posts(account, platform, since=since, limit=limit)
    result = PipelineResult(
        task_type="account_follow",
        items=posts[:limit],
        queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        elapsed_ms=int((time.time() - started) * 1000),
    )
    return result
