# -*- coding: utf-8 -*-
"""离线新闻的 MySQL、Redis 和 Milvus 访问层。"""

import hashlib
import json
import re
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

from config import Config
from create_logger import logger

conf = Config()


def normalize_query(query: str) -> str:
    """Redis 精确匹配使用的标准化：小写、去首尾空白、合并空格。"""
    return re.sub(r"\s+", " ", query.strip().lower())


class MySQLNewsStore:
    """MySQL 保存新闻原文，Milvus 只保存同一个整数 ID 和向量。"""

    def _ensure_database(self) -> None:
        """首次运行自动创建配置中的数据库。"""
        import pymysql
        cfg = dict(conf.mysql)
        database = cfg.pop("database")
        if not re.fullmatch(r"[A-Za-z0-9_]+", database):
            raise ValueError(f"非法 MySQL 数据库名: {database}")
        with pymysql.connect(**cfg, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{database}` "
                    "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )

    def _connect(self):
        import pymysql
        return pymysql.connect(**conf.mysql, autocommit=False,
                               cursorclass=pymysql.cursors.DictCursor)

    def init_schema(self) -> None:
        self._ensure_database()
        sql = """
        CREATE TABLE IF NOT EXISTS offline_news (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            news_uid CHAR(64) NOT NULL UNIQUE,
            module VARCHAR(32) NOT NULL,
            title VARCHAR(500) NOT NULL,
            source VARCHAR(255) NOT NULL DEFAULT '',
            url VARCHAR(1500) NOT NULL DEFAULT '',
            summary TEXT,
            content MEDIUMTEXT,
            published_at DATETIME NULL,
            crawled_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            expires_at DATETIME NOT NULL,
            INDEX idx_module_time (module, published_at),
            INDEX idx_expires_at (expires_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()

    @staticmethod
    def _mysql_time(value: str) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return None

    def upsert(self, rows: Iterable[Dict], retention_days: int) -> List[Dict]:
        """批量写入并返回带 MySQL id 的记录，重复 URL/UID 更新而不重复插入。"""
        rows = list(rows)
        if not rows:
            return []
        sql = """
        INSERT INTO offline_news
          (news_uid,module,title,source,url,summary,content,published_at,expires_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          module=VALUES(module), title=VALUES(title), source=VALUES(source),
          url=VALUES(url), summary=VALUES(summary), content=VALUES(content),
          published_at=COALESCE(VALUES(published_at), published_at),
          crawled_at=NOW(), expires_at=VALUES(expires_at)
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                params = []
                for row in rows:
                    published = self._mysql_time(row.get("published_at", ""))
                    expires_at = (published or datetime.now()) + timedelta(days=retention_days)
                    params.append((
                        row["news_uid"], row["module"], row["title"], row["source"],
                        row["url"], row["summary"], row["content"], published, expires_at,
                    ))
                cursor.executemany(sql, params)
                placeholders = ",".join(["%s"] * len(rows))
                cursor.execute(
                    f"SELECT id,news_uid,module,title,summary,content FROM offline_news "
                    f"WHERE news_uid IN ({placeholders})",
                    [row["news_uid"] for row in rows],
                )
                stored = cursor.fetchall()
            conn.commit()
        return stored

    def expired_ids(self) -> List[int]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM offline_news WHERE expires_at < NOW()")
                return [int(row["id"]) for row in cursor.fetchall()]

    def active_for_index(self) -> List[Dict]:
        """返回三天保留期内的全部新闻，用于失败重跑时补齐 Milvus。"""
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id,news_uid,module,title,summary,content "
                    "FROM offline_news WHERE expires_at >= NOW() ORDER BY id"
                )
                return cursor.fetchall()

    def delete_expired(self) -> int:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                count = cursor.execute("DELETE FROM offline_news WHERE expires_at < NOW()")
            conn.commit()
        return count

    def fetch_by_ids(self, ids: List[int], limit: int = 3) -> List[Dict]:
        if not ids:
            return []
        ids = [int(value) for value in ids[:limit]]
        placeholders = ",".join(["%s"] * len(ids))
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT id,module,title,source,url,summary,content,published_at "
                    f"FROM offline_news WHERE expires_at >= NOW() AND id IN ({placeholders})",
                    ids,
                )
                rows = cursor.fetchall()
        order = {value: index for index, value in enumerate(ids)}
        rows.sort(key=lambda row: order.get(int(row["id"]), len(ids)))
        for row in rows:
            if row.get("published_at"):
                row["published_at"] = row["published_at"].isoformat(timespec="seconds")
        return rows

    def fetch_today_by_module(self, module: str, limit: int = 10) -> List[Dict]:
        """取今天抓取/更新的某板块新闻，供每日简报使用。"""
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id,module,title,source,url,summary,content,published_at "
                    "FROM offline_news "
                    "WHERE module=%s AND crawled_at >= CURDATE() AND expires_at >= NOW() "
                    "ORDER BY COALESCE(published_at, crawled_at) DESC LIMIT %s",
                    (module, max(1, int(limit))),
                )
                rows = cursor.fetchall()
        for row in rows:
            if row.get("published_at"):
                row["published_at"] = row["published_at"].isoformat(timespec="seconds")
        return rows


class RedisQueryCache:
    """只缓存标准化后完全相同的查询。Redis 故障时返回 miss。"""

    def __init__(self):
        self.cfg = conf.redis

    def _client(self):
        import redis
        return redis.Redis(host=self.cfg["host"], port=self.cfg["port"],
                           password=self.cfg["password"], db=self.cfg["db"],
                           decode_responses=True, socket_timeout=2)

    def _key(self, query: str) -> str:
        digest = hashlib.sha256(normalize_query(query).encode("utf-8")).hexdigest()
        return f"{self.cfg['key_prefix']}:query:{digest}"

    def get(self, query: str) -> Optional[List[Dict]]:
        try:
            value = self._client().get(self._key(query))
            return json.loads(value) if value else None
        except Exception as exc:
            logger.warning(f"[offline-redis] 查询失败，按未命中处理: {exc}")
            return None

    def set(self, query: str, rows: List[Dict]) -> None:
        try:
            self._client().setex(self._key(query), self.cfg["cache_ttl_seconds"],
                                 json.dumps(rows, ensure_ascii=False, default=str))
        except Exception as exc:
            logger.warning(f"[offline-redis] 写缓存失败: {exc}")

    def clear_queries(self) -> None:
        try:
            client = self._client()
            pattern = f"{self.cfg['key_prefix']}:query:*"
            keys = list(client.scan_iter(match=pattern, count=200))
            if keys:
                client.delete(*keys)
        except Exception as exc:
            logger.warning(f"[offline-redis] 清理缓存失败: {exc}")


class MilvusNewsIndex:
    """Milvus 保存 news.id → embedding，用 ID 回 MySQL 获取原文。"""

    def __init__(self):
        self.cfg = conf.milvus
        self.client = None

    def connect(self):
        if self.client is not None:
            return self.client
        from pymilvus import MilvusClient
        bootstrap = MilvusClient(uri=self.cfg["uri"])
        if self.cfg["database_name"] not in bootstrap.list_databases():
            bootstrap.create_database(self.cfg["database_name"])
        bootstrap.close()
        self.client = MilvusClient(uri=self.cfg["uri"], db_name=self.cfg["database_name"])
        if not self.client.has_collection(self.cfg["collection_name"]):
            self.client.create_collection(
                collection_name=self.cfg["collection_name"],
                dimension=self.cfg["dimension"],
                primary_field_name="id", vector_field_name="vector",
                id_type="int", metric_type="COSINE", auto_id=False,
            )
        return self.client

    def embed(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        from openai import OpenAI
        client = OpenAI(base_url=conf.llm["base_url"], api_key=conf.llm["api_key"])
        vectors: List[List[float]] = []
        # DashScope text-embedding-v3 单次最多处理 10 条文本。
        for start in range(0, len(texts), 10):
            response = client.embeddings.create(
                model=conf.embedding_model,
                input=texts[start:start + 10],
                dimensions=self.cfg["dimension"],
            )
            vectors.extend(
                item.embedding
                for item in sorted(response.data, key=lambda item: item.index)
            )
        return vectors

    def upsert(self, rows: List[Dict]) -> None:
        if not rows:
            return
        texts = [f"{row['module']} {row['title']} {row.get('summary') or ''} "
                 f"{(row.get('content') or '')[:1500]}" for row in rows]
        vectors = self.embed(texts)
        self.connect().upsert(self.cfg["collection_name"], [
            {"id": int(row["id"]), "vector": vector}
            for row, vector in zip(rows, vectors)
        ])

    def search_ids(self, query: str, limit: int = 3) -> List[int]:
        vector = self.embed([query])[0]
        result = self.connect().search(
            collection_name=self.cfg["collection_name"], data=[vector],
            limit=min(3, limit), search_params={"metric_type": "COSINE", "params": {}},
        )
        return [int(hit["id"]) for hit in (result[0] if result else [])]

    def delete(self, ids: List[int]) -> None:
        if ids:
            self.connect().delete(self.cfg["collection_name"], ids=[int(i) for i in ids])
