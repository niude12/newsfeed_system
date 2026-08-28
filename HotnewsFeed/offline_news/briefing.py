# -*- coding: utf-8 -*-
"""离线每日简报：按板块读取今日新闻，复用 Publish MCP 生成和推送。"""

from datetime import datetime, timezone
from typing import Dict

from config import Config
from create_logger import logger
from mcp_servers.mcp_access import publish_briefing
from offline_news.stores import MySQLNewsStore
from task_pipelines.schemas import NewsItem, PipelineResult

conf = Config()


async def generate_daily_briefings() -> Dict[str, dict]:
    """为每个配置板块生成一份简报，单个板块失败不影响其他板块。"""
    cfg = conf.offline_news
    store = MySQLNewsStore()
    results: Dict[str, dict] = {}

    for module in cfg["modules"]:
        rows = store.fetch_today_by_module(module, cfg["daily_briefing_top_n"])
        if not rows:
            logger.info(f"[offline-briefing] {module} 今日无新闻，跳过")
            results[module] = {"skipped": True, "reason": "今日无新闻"}
            continue

        items = [NewsItem(
            news_id=str(row["id"]), module=row["module"], title=row["title"],
            source=row["source"], published_at=row.get("published_at") or "",
            url=row["url"], summary=row.get("summary") or row.get("content", "")[:300],
        ) for row in rows]
        task_result = PipelineResult(
            task_type=f"offline_daily_{module}", items=items,
            queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )
        try:
            published = await publish_briefing(
                task_result,
                channels=cfg["daily_briefing_channels"],
                template="default",
            )
            results[module] = {
                "briefing_id": published.briefing_id,
                "items": len(items),
                "channels": published.channels,
                "error": published.error,
            }
            logger.info(f"[offline-briefing] {module} 简报完成: {results[module]}")
        except Exception as exc:
            results[module] = {"error": str(exc), "items": len(items)}
            logger.error(f"[offline-briefing] {module} 简报失败: {exc}", exc_info=True)

    return results
