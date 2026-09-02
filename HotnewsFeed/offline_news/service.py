# -*- coding: utf-8 -*-
"""离线新闻服务：采集入库和 Redis → Milvus → MySQL 查询编排。

聚合 offline_news 子模块对外提供统一门面：
- initialize()：初始化 MySQL 表结构与 Milvus 集合；
- ingest()    ：采集 → 写 MySQL → 重建 Milvus → 清 Redis 缓存 → 按配置生成每日简报；
- query()     ：离线问答查询，走「Redis 精确缓存 → Milvus 向量召回 → MySQL 取原文」三级链路。

模块依赖:
- ``crawl_all_modules``        : offline_news/crawler.py，并发采集全部板块。
- ``MySQLNewsStore``           : offline_news/stores.py，MySQL 原文存储。
- ``RedisQueryCache``          : offline_news/stores.py，精确查询缓存。
- ``MilvusNewsIndex``          : offline_news/stores.py，向量索引。
- ``generate_daily_briefings`` : offline_news/briefing.py（延迟 import），每日简报。
- ``Config``                   : offline_news 属性提供 retention_days / daily_briefing_enabled。

典型调用链::

    offline_main.py / scheduler  ->  OfflineNewsService().ingest()
                                  ->  initialize()
                                  ->  crawl_all_modules()
                                  ->  mysql.upsert(...) / milvus.upsert(...) / redis.clear_queries()
    offline_main.py              ->  OfflineNewsService().query(question)
                                  ->  redis.get -> milvus.search_ids -> mysql.fetch_by_ids
"""

import time
from typing import Dict, List

from config import Config
from create_logger import logger
from offline_news.crawler import crawl_all_modules
from offline_news.stores import MilvusNewsIndex, MySQLNewsStore, RedisQueryCache

conf = Config()


class OfflineNewsService:
    """离线新闻服务门面：把爬虫、三存储与简报组装成可调用的服务对象。

    说明:
        内部持有 MySQLNewsStore / RedisQueryCache / MilvusNewsIndex 三个存储实例，
        _initialized 标记 MySQL 表与 Milvus 集合是否已初始化
        （query 首次调用会自动补初始化，避免外部必须先调用 initialize）。
    """

    def __init__(self):
        """创建服务实例：初始化三个存储访问对象与初始化标记。"""
        self.mysql = MySQLNewsStore()  # MySQL 原文存储访问层。
        self.redis = RedisQueryCache()  # Redis 精确查询缓存访问层。
        self.milvus = MilvusNewsIndex()  # Milvus 向量索引访问层。
        self._initialized = False  # MySQL 表 / Milvus 集合是否已初始化的标记。

    def initialize(self) -> None:
        """创建 MySQL 表和 Milvus 集合。

        说明:
            mysql.init_schema() 建库建表；milvus.connect() 建立客户端并确保集合存在；
            成功后置 _initialized=True，避免 query 路径重复初始化。
        """
        self.mysql.init_schema()  # 建库建表（幂等，可重复执行）。
        self.milvus.connect()  # 建立 Milvus 客户端并确保数据库/集合存在。
        self._initialized = True  # 标记已初始化，query 路径不必重复初始化。
        logger.info("[offline] MySQL 表与 Milvus 集合初始化完成")

    async def ingest(self) -> Dict[str, object]:
        """采集所有板块，写 MySQL/Milvus，并清理超过3天的数据。

        一轮完整入库的编排步骤：
        1. initialize() 确保表 / 集合就绪；
        2. crawl_all_modules() 并发采集全部板块；
        3. mysql.upsert() 批量写入原文（拿到含自增 id 的行）；
        4. 过期清理：先 milvus.delete(过期 id)，再 mysql.delete_expired()；
        5. mysql.active_for_index() 取全部未过期行，milvus.upsert() 整体重建索引，
           失败重跑时向量可自动补齐；
        6. redis.clear_queries() 清空精确缓存，避免命中昨日旧数据；
        7. 若 daily_briefing_enabled 为真，调用 generate_daily_briefings() 生成每日简报。

        返回:
            Dict[str, object]：统计结果字典，含 crawled（采集数）、indexed（入库行数）、
            deleted（删除行数）、elapsed_ms（耗时毫秒）、briefings（各板块简报结果）。

        说明:
            耗时统计用 time.time() 两次差值并转成毫秒整数；简报模块延迟 import，
            让未启用简报时不必引入其依赖。
        """
        started = time.time()  # 记录入库开始时间，用于统计耗时。
        self.initialize()  # 先确保表与集合就绪。
        crawled = await crawl_all_modules()  # 并发采集全部板块并去重。
        # 批量写入原文；返回值含自增 id 的行（本轮后续通过 active_for_index 整体重建索引）。
        stored = self.mysql.upsert(
            [item.to_dict() for item in crawled],  # CrawledNews 转 dict 批量写入。
            retention_days=conf.offline_news["retention_days"],  # 保留天数决定过期时间。
        )
        # 入库后清理，避免来源返回的旧文章短暂留在数据库中。
        expired_ids = self.mysql.expired_ids()  # 查所有已过期记录 id。
        self.milvus.delete(expired_ids)  # 先删向量，避免残留孤儿向量。
        deleted = self.mysql.delete_expired()  # 再删 MySQL 过期行。
        # 重新索引所有有效新闻，使上次在 MySQL 写入后中断的任务可自动恢复。
        active_rows = self.mysql.active_for_index()  # 取全部未过期行。
        self.milvus.upsert(active_rows)  # 整体重建向量索引。
        # 清空精确查询缓存，避免昨日入库前的旧结果被命中。
        self.redis.clear_queries()
        briefings = {}  # 默认无简报；仅启用每日简报时填充。
        if conf.offline_news["daily_briefing_enabled"]:  # [offline_news] 每日简报开关。
            # 延迟 import：仅启用每日简报时才引入 briefing 模块及其依赖。
            from offline_news.briefing import generate_daily_briefings
            briefings = await generate_daily_briefings()  # 为每个配置板块生成简报。
        result = {
            "crawled": len(crawled),  # 本轮采集到的新闻条数。
            "indexed": len(active_rows),  # 写入向量库的行数。
            "deleted": deleted,  # 清理掉的过期行数。
            "elapsed_ms": int((time.time() - started) * 1000),  # 入库耗时（毫秒）。
            "briefings": briefings,  # 各板块简报结果。
        }
        logger.info(f"[offline] 每日入库完成: {result}")
        return result

    def query(self, query: str, limit: int = 3) -> Dict:
        """精确缓存命中直接返回；否则向量召回 ID，再从 MySQL 获取原文。

        参数:
            query: 用户自然语言问题 / 关键词。
            limit: 期望返回条数（默认 3，强制收敛到 [1, 3] 区间）。

        返回:
            Dict：{"source": ..., "items": [...]}。source 为 "redis"（缓存命中）或
            "milvus+mysql"（向量召回后回查原文）；items 是记录行列表（最多 limit 条）。

        说明:
            - limit = min(3, max(1, limit))：保证 TopK 不超过配置的 query_top_k（≤3）且至少 1。
            - redis.get(query) 命中则直接截断返回，不进向量库；
            - 未命中且未初始化则先 initialize()；
            - milvus.search_ids(query, limit) 向量召回 id → mysql.fetch_by_ids(ids, limit)
              回 MySQL 取原文 → redis.set(query, rows) 写缓存供下次命中。
        """
        # 收敛 TopK：离线查询上限 3（与 [offline_news] query_top_k 一致），且至少 1。
        limit = min(3, max(1, limit))
        cached = self.redis.get(query)  # 先查精确缓存。
        if cached is not None:  # 缓存命中：直接返回，不进向量库。
            return {"source": "redis", "items": cached[:limit]}  # 缓存结果按 limit 截断。

        # 未初始化时先建表/建集合，保证后续查询可用。
        if not self._initialized:
            self.initialize()
        ids = self.milvus.search_ids(query, limit=limit)  # 向量召回相似新闻的 MySQL id。
        rows: List[Dict] = self.mysql.fetch_by_ids(ids, limit=limit)  # 回 MySQL 取原文。
        self.redis.set(query, rows)  # 写缓存，供下次相同查询命中。
        return {"source": "milvus+mysql", "items": rows}  # 标记来源链路为向量 + 原文。
