# -*- coding: utf-8 -*-
"""离线每日简报：按板块读取今日新闻，复用 Publish MCP 生成和推送。

该模块负责离线资讯库的“简报”环节：按 config [offline_news] modules 遍历每个板块，
从 MySQL 读取今日新闻（fetch_today_by_module），组装成 PipelineResult 后调用
mcp_access.publish_briefing 走 Publish MCP 生成简报并推送。

模块依赖:
- ``MySQLNewsStore.fetch_today_by_module`` : 取某板块今日抓取的新闻行。
- ``publish_briefing``                    : mcp_servers/mcp_access.py 的发布网关，
                                             优先走 MCP publish 服务器，失败降级进程内直调。
- ``NewsItem / PipelineResult``           : task_pipelines/schemas.py 的 DTO，简报入参结构。
- ``Config``                              : offline_news 属性提供 modules / daily_briefing_top_n /
                                             daily_briefing_channels / daily_briefing_enabled。

典型调用链::

    service.ingest() -> generate_daily_briefings()
                     -> MySQLNewsStore.fetch_today_by_module(module, top_n)
                     -> publish_briefing(PipelineResult, channels, template="default")

对外暴露：
- generate_daily_briefings : 为每个配置板块生成一份简报，返回 {板块: 结果} 字典。
"""

from datetime import datetime, timezone
from typing import Dict

from config import Config
from create_logger import logger
from mcp_servers.mcp_access import publish_briefing
from offline_news.stores import MySQLNewsStore
from task_pipelines.schemas import NewsItem, PipelineResult

conf = Config()


async def generate_daily_briefings() -> Dict[str, dict]:
    """为每个配置板块生成一份简报，单个板块失败不影响其他板块。

    遍历 config [offline_news] modules，对每个板块：
    - 从 MySQL 取今日新闻（fetch_today_by_module，最多 daily_briefing_top_n 条）；
    - 无新闻则记 skipped 结果并跳过；
    - 有新闻则组装 NewsItem 列表与 PipelineResult，调用 publish_briefing 生成并推送。

    返回:
        Dict[str, dict]：板块名 → 结果字典。无新闻时为 {"skipped": True, ...}；
        发布成功时为 briefing_id / items / channels / error；发布异常时 error 为异常信息。

    说明:
        - NewsItem 是简报条目 DTO：summary 缺省时用 content 前 300 字兜底。
        - PipelineResult.task_type 记成 "offline_daily_<module>"，便于前端区分简报来源。
        - publish_briefing 是 mcp_access 网关：优先走 MCP publish 服务器（:8006），
          远端不可达时降级进程内 tools.publish.publish_briefing。
        - 单板块失败只把 error 写进 results 并记日志，continue 处理下一个板块（故障隔离）。
    """
    cfg = conf.offline_news  # [offline_news] 配置段（modules / daily_briefing_top_n 等键）。
    store = MySQLNewsStore()  # MySQL 访问层，用于读取今日新闻。
    results: Dict[str, dict] = {}  # 板块名 → 简报结果的汇总字典。

    for module in cfg["modules"]:  # 遍历配置里的每个板块。
        # 取该板块今日抓取/更新的新闻，按时间倒序，数量上限 daily_briefing_top_n。
        rows = store.fetch_today_by_module(module, cfg["daily_briefing_top_n"])
        if not rows:  # 今日无新闻：不生成简报，记 skipped 结果后跳过。
            logger.info(f"[offline-briefing] {module} 今日无新闻，跳过")
            results[module] = {"skipped": True, "reason": "今日无新闻"}
            continue

        # 把 MySQL 行映射成 NewsItem DTO；summary 缺省时用 content 前 300 字兜底。
        items = [NewsItem(
            news_id=str(row["id"]), module=row["module"], title=row["title"],
            source=row["source"], published_at=row.get("published_at") or "",
            url=row["url"], summary=row.get("summary") or row.get("content", "")[:300],
        ) for row in rows]
        # PipelineResult 是简报的统一入参结构，queried_at 用 UTC ISO 时间。
        task_result = PipelineResult(
            task_type=f"offline_daily_{module}", items=items,  # 任务类型含板块名，便于区分简报来源。
            queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )
        try:
            # publish_briefing 生成简报并推送到配置的通道（web_ui / feishu / email / webhook）。
            published = await publish_briefing(
                task_result,
                channels=cfg["daily_briefing_channels"],  # 简报推送通道列表。
                template="default",  # 使用默认简报模板。
            )
            results[module] = {
                "briefing_id": published.briefing_id,  # 简报唯一 ID。
                "items": len(items),  # 本次简报包含的新闻条数。
                "channels": published.channels,  # 各通道推送成功与否的映射。
                "error": published.error,  # 推送失败时的错误信息。
            }
            logger.info(f"[offline-briefing] {module} 简报完成: {results[module]}")
        except Exception as exc:
            # 单个板块失败不阻断其它板块，只记录错误信息。
            results[module] = {"error": str(exc), "items": len(items)}
            logger.error(f"[offline-briefing] {module} 简报失败: {exc}", exc_info=True)

    return results
