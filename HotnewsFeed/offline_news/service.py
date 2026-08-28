# -*- coding: utf-8 -*-
"""离线新闻服务：采集入库和 Redis → Milvus → MySQL 查询编排。"""

import time
from typing import Dict, List

from config import Config
from create_logger import logger
from offline_news.crawler import crawl_all_modules
from offline_news.stores import MilvusNewsIndex, MySQLNewsStore, RedisQueryCache

conf = Config()


class OfflineNewsService:
    def __init__(self):
        self.mysql = MySQLNewsStore()
        self.redis = RedisQueryCache()
        self.milvus = MilvusNewsIndex()
        self._initialized = False

    def initialize(self) -> None:
        """创建 MySQL 表和 Milvus 集合。"""
        self.mysql.init_schema()
        self.milvus.connect()
        self._initialized = True
        logger.info("[offline] MySQL 表与 Milvus 集合初始化完成")

    async def ingest(self) -> Dict[str, object]:
        """采集所有板块，写 MySQL/Milvus，并清理超过3天的数据。"""
        started = time.time()
        self.initialize()
        crawled = await crawl_all_modules()
        stored = self.mysql.upsert(
            [item.to_dict() for item in crawled],
            retention_days=conf.offline_news["retention_days"],
        )
        # 入库后清理，避免来源返回的旧文章短暂留在数据库中。
        expired_ids = self.mysql.expired_ids()
        self.milvus.delete(expired_ids)
        deleted = self.mysql.delete_expired()
        # 重新索引所有有效新闻，使上次在 MySQL 写入后中断的任务可自动恢复。
        active_rows = self.mysql.active_for_index()
        self.milvus.upsert(active_rows)
        self.redis.clear_queries()
        briefings = {}
        if conf.offline_news["daily_briefing_enabled"]:
            from offline_news.briefing import generate_daily_briefings
            briefings = await generate_daily_briefings()
        result = {
            "crawled": len(crawled), "indexed": len(active_rows),
            "deleted": deleted, "elapsed_ms": int((time.time() - started) * 1000),
            "briefings": briefings,
        }
        logger.info(f"[offline] 每日入库完成: {result}")
        return result

    def query(self, query: str, limit: int = 3) -> Dict:
        """精确缓存命中直接返回；否则向量召回 ID，再从 MySQL 获取原文。"""
        limit = min(3, max(1, limit))
        cached = self.redis.get(query)
        if cached is not None:
            return {"source": "redis", "items": cached[:limit]}

        if not self._initialized:
            self.initialize()
        ids = self.milvus.search_ids(query, limit=limit)
        rows: List[Dict] = self.mysql.fetch_by_ids(ids, limit=limit)
        self.redis.set(query, rows)
        return {"source": "milvus+mysql", "items": rows}
