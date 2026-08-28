# -*- coding: utf-8 -*-
"""离线新闻库：定时采集入库，以及 Redis → Milvus → MySQL 查询。"""

from offline_news.service import OfflineNewsService

__all__ = ["OfflineNewsService"]
