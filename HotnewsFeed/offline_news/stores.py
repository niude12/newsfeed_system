# -*- coding: utf-8 -*-
"""离线新闻的 MySQL、Redis 和 Milvus 访问层。

统一封装离线资讯库的三种存储访问：
- MySQL（MySQLNewsStore）  ：保存新闻原文与元数据，是事实数据的唯一来源；
- Milvus（MilvusNewsIndex）：只保存「MySQL 整数 id → embedding 向量」的映射，
                             查询时用向量召回 id，再回 MySQL 取原文；
- Redis（RedisQueryCache） ：精确查询结果缓存，相同查询直接命中缓存，降低向量库压力。

模块依赖:
- ``Config``     : 全局配置单例；mysql / milvus / redis 属性是 dict，llm / embedding_model 提供嵌入模型。
- ``pymysql``    : MySQL 驱动（延迟 import）。
- ``pymilvus``   : Milvus 向量库客户端（延迟 import）。
- ``redis``      : Redis 客户端（延迟 import）。
- ``openai``     : OpenAI 兼容接口，用于调 DashScope text-embedding-v3 生成向量（延迟 import）。

典型调用链::

    service.ingest()      -> MySQLNewsStore.init_schema()   # 建库建表
                          -> MySQLNewsStore.upsert(...)     # 写原文
                          -> MilvusNewsIndex.upsert(...)    # 写向量
    service.query(query)  -> RedisQueryCache.get(query)     # 1. 精确缓存
                          -> MilvusNewsIndex.search_ids()   # 2. 向量召回 id
                          -> MySQLNewsStore.fetch_by_ids()  # 3. 回 MySQL 取原文

对外暴露的类：
- MySQLNewsStore / RedisQueryCache / MilvusNewsIndex，以及标准化函数 normalize_query。
"""

import hashlib
import json
import re
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

from config import Config
from create_logger import logger

conf = Config()


def normalize_query(query: str) -> str:
    """Redis 精确匹配使用的标准化：小写、去首尾空白、合并空格。

    参数:
        query: 用户原始查询串。

    返回:
        str：标准化后的查询串（全小写、去首尾空白、连续空白压缩为单个空格）。

    说明:
        re.sub(r"\\s+", " ", query.strip().lower())：\\s+ 匹配任意连续空白，
        替换为单个空格；小写保证同一语义的大小写变体命中同一缓存键。
    """
    # strip 去首尾空白、lower 统一小写、\s+ 合并连续空白 → 语义相同的查询得到同一缓存键。
    return re.sub(r"\s+", " ", query.strip().lower())


class MySQLNewsStore:
    """MySQL 保存新闻原文，Milvus 只保存同一个整数 ID 和向量。

    职责：
    - init_schema 建库建表（offline_news 表以 news_uid 为唯一键）；
    - upsert 批量写入原文（重复 news_uid 走 ON DUPLICATE KEY UPDATE 更新）；
    - 查询接口：fetch_by_ids 按 id 回查原文、fetch_today_by_module 按板块取今日新闻；
    - 过期清理：expired_ids / delete_expired / active_for_index 配合 Milvus 增量重建。

    说明:
        与 Milvus 的协作约定：MySQL 里的整数自增 id 就是 Milvus 向量的主键；
        Milvus 侧不存原文，只存 id→vector，召回 id 后再回本类取详情。
    """

    def _ensure_database(self) -> None:
        """首次运行自动创建配置中的数据库。

        从 conf.mysql 配置字典里弹出 database 键（其余键作为连接参数），
        用一个 autocommit 的临时连接执行 CREATE DATABASE IF NOT EXISTS，
        保证后续建表前数据库一定存在。

        抛出:
            ValueError: database 名不满足「字母/数字/下划线」正则时抛出，防止 SQL 注入。

        说明:
            pymysql.connect(**cfg, autocommit=True)：cfg 是去掉 database 后的
            连接参数（host/port/user/password/charset），autocommit=True 让 CREATE 立即生效。
        """
        # 延迟 import：仅真正建库时才引入 pymysql，保持模块顶层轻量。
        import pymysql
        # 拷贝配置字典，后续 pop 出 database 键，剩余键作为连接参数。
        cfg = dict(conf.mysql)
        database = cfg.pop("database")
        # 数据库名必须是字母/数字/下划线，防止 SQL 注入（反引号包裹也防不住非法名）。
        if not re.fullmatch(r"[A-Za-z0-9_]+", database):
            raise ValueError(f"非法 MySQL 数据库名: {database}")
        # 用 autocommit 临时连接执行建库，使 CREATE DATABASE 立即生效。
        with pymysql.connect(**cfg, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    # IF NOT EXISTS 保证幂等；utf8mb4 + unicode_ci 支持中文且大小写不敏感。
                    f"CREATE DATABASE IF NOT EXISTS `{database}` "
                    "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )

    def _connect(self):
        """建立一个新的 pymysql 连接（业务方法每次调用都新建）。

        返回:
            pymysql.Connection：autocommit=False 的事务连接，游标为 DictCursor。

        说明:
            pymysql.cursors.DictCursor 让查询结果每行返回 dict（键为列名），
            方便按 row["id"] / row["title"] 访问；调用方负责 commit 与 close。
        """
        # 延迟 import pymysql；autocommit=False 让调用方显式 commit 控制事务边界。
        import pymysql
        # DictCursor 让查询行返回 dict（键为列名），方便按 row["id"] 访问。
        return pymysql.connect(**conf.mysql, autocommit=False,
                               cursorclass=pymysql.cursors.DictCursor)

    def init_schema(self) -> None:
        """初始化 MySQL 表结构（幂等，可重复执行）。

        先 _ensure_database 确保库存在，再执行 CREATE TABLE IF NOT EXISTS 建 offline_news 表；
        news_uid 是 CHAR(64) 唯一键（对应 crawler 里的 sha256 指纹），
        content 用 MEDIUMTEXT 容纳最长 20000 字的正文，expires_at 用于按保留天数过期。

        说明:
            - 建表 SQL 走事务：执行后显式 conn.commit() 提交。
            - 索引 idx_module_time(module, published_at) 加速按板块+时间查询，
              idx_expires_at(expires_at) 加速过期清理。
            - ENGINE=InnoDB + utf8mb4 保证事务与中文存储。
        """
        # 先确保数据库存在，再建表。
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
        # 建表走事务：执行 SQL 后显式 commit 提交。
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()

    @staticmethod
    def _mysql_time(value: str) -> Optional[datetime]:
        """把 ISO 8601 时间字符串转成 MySQL DATETIME 可用的 naive datetime。

        参数:
            value: ISO 8601 时间字符串（可带 "Z" 后缀，如 "2026-08-30T06:00:00Z"）。

        返回:
            无时区的 datetime 对象；value 为空或格式非法时返回 None。

        说明:
            datetime.fromisoformat 解析 ISO 时间；replace("Z", "+00:00") 把 UTC 后缀
            换成显式时区（Python 3.11 前 fromisoformat 不认 "Z"）；replace(tzinfo=None)
            去掉时区信息，转成 MySQL DATETIME 可直接存储的 naive 时间。
        """
        # 空串视为“无发布时间”，返回 None 让 MySQL 存 NULL。
        if not value:
            return None
        try:
            # 把 "Z"(UTC) 换成 "+00:00" 再解析；replace(tzinfo=None) 转成 naive 时间供 DATETIME 存储。
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            # 格式非法时不抛异常，统一按 None 处理（发布时间未知）。
            return None

    def upsert(self, rows: Iterable[Dict], retention_days: int) -> List[Dict]:
        """批量写入并返回带 MySQL id 的记录，重复 URL/UID 更新而不重复插入。

        参数:
            rows:           CrawledNews.to_dict() 产生的字典列表（含 news_uid/module/title 等）。
            retention_days: 保留天数，expires_at = published_at + retention_days。

        返回:
            List[Dict]：刚从 MySQL 查回的整批记录（含自增 id），供 Milvus 写向量时作主键。

        说明:
            - INSERT ... ON DUPLICATE KEY UPDATE：以 news_uid 唯一键判定冲突，
              冲突时更新业务字段并把 crawled_at 置为 NOW()；
              published_at 用 COALESCE(VALUES(published_at), published_at) 保留非空的旧发布时间。
            - cursor.executemany 批量执行；随后用 IN 查询把刚写入的整批记录连同自增 id 取回，
              placeholders 由 len(rows) 个 %s 拼接而成。
        """
        # 转成 list 以便多次遍历；空输入直接返回空列表。
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
                    # 解析发布时间；解析失败为 None 时用当前时间作为过期时间起点。
                    published = self._mysql_time(row.get("published_at", ""))
                    # 过期时间 = 发布时间（或当前时间）+ 保留天数。
                    expires_at = (published or datetime.now()) + timedelta(days=retention_days)
                    params.append((
                        row["news_uid"], row["module"], row["title"], row["source"],
                        row["url"], row["summary"], row["content"], published, expires_at,
                    ))
                # executemany 批量执行 INSERT...ON DUPLICATE KEY UPDATE。
                cursor.executemany(sql, params)
                # 用 IN 查询把刚写入的整批记录连同自增 id 取回（placeholders 是 %s 占位符列表）。
                placeholders = ",".join(["%s"] * len(rows))
                cursor.execute(
                    f"SELECT id,news_uid,module,title,summary,content FROM offline_news "
                    f"WHERE news_uid IN ({placeholders})",
                    [row["news_uid"] for row in rows],
                )
                stored = cursor.fetchall()
            # 提交整个事务（写入 + 回查一起生效）。
            conn.commit()
        return stored

    def expired_ids(self) -> List[int]:
        """查询所有已过期（expires_at < NOW()）记录的 id 列表。

        返回:
            List[int]：过期记录的主键 id，供 MilvusNewsIndex.delete 删除对应向量。

        说明:
            先删向量再删 MySQL 行（见 service.ingest），保证两边数据一致。
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                # 查所有已过期的 id（expires_at 早于当前时间）。
                cursor.execute("SELECT id FROM offline_news WHERE expires_at < NOW()")
                # 返回 int 列表供 MilvusNewsIndex.delete 使用。
                return [int(row["id"]) for row in cursor.fetchall()]

    def active_for_index(self) -> List[Dict]:
        """返回保留期（默认三天）内的全部新闻，用于失败重跑时补齐 Milvus。

        返回:
            List[Dict]：未过期记录的 id/news_uid/module/title/summary/content，按 id 升序。

        说明:
            该方法在 ingest 末尾调用：除了本轮写入的行，历史未过期的行也会一并喂给 Milvus
            重建索引——上次在 MySQL 写入后中断的任务由此可自动恢复，Milvus 不会缺向量。
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                # 取全部未过期记录，按 id 升序；供 ingest 末尾整体重建 Milvus 索引。
                cursor.execute(
                    "SELECT id,news_uid,module,title,summary,content "
                    "FROM offline_news WHERE expires_at >= NOW() ORDER BY id"
                )
                return cursor.fetchall()

    def delete_expired(self) -> int:
        """物理删除所有过期记录。

        返回:
            int：被删除的行数。

        说明:
            调用前应先 milvus.delete(expired_ids) 删掉对应向量，避免残留孤儿向量。
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                # 物理删除过期记录；execute 返回受影响行数。
                count = cursor.execute("DELETE FROM offline_news WHERE expires_at < NOW()")
            conn.commit()  # 提交删除事务。
        return count

    def fetch_by_ids(self, ids: List[int], limit: int = 3) -> List[Dict]:
        """按 id 列表回查 MySQL 原文，保持与输入 id 相同的顺序。

        参数:
            ids:    Milvus 向量召回返回的 id 列表（已按相似度降序）。
            limit:  最多返回前 limit 条（默认 3，与离线查询 TopK 一致）。

        返回:
            List[Dict]：按输入 id 顺序排列的记录行（published_at 已格式化为 ISO 字符串）。

        说明:
            - 先截断 ids 到 limit 条，构造 IN 占位符一次查回；
            - 用 order dict 记录 id→下标，查回后在内存里按该顺序排序（SQL 的 IN 不保证顺序）；
            - published_at 是 datetime 对象，转成 isoformat 便于 JSON 序列化。
        """
        # 空 id 列表直接返回，避免拼出非法 IN 查询。
        if not ids:
            return []
        # 先截断到 limit 条并统一转 int（Milvus 回传的 id 可能是其它数字类型）。
        ids = [int(value) for value in ids[:limit]]
        # 构造 IN 子句的 %s 占位符串，如 "%s,%s,%s"。
        placeholders = ",".join(["%s"] * len(ids))
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    # 只取未过期记录；过期数据已删/即将删，避免返回陈旧原文。
                    f"SELECT id,module,title,source,url,summary,content,published_at "
                    f"FROM offline_news WHERE expires_at >= NOW() AND id IN ({placeholders})",
                    ids,
                )
                rows = cursor.fetchall()
        # 按输入 id 顺序重排结果（IN 查询返回顺序不保证与 ids 一致）。
        order = {value: index for index, value in enumerate(ids)}
        rows.sort(key=lambda row: order.get(int(row["id"]), len(ids)))
        for row in rows:
            # datetime 转成 ISO 字符串，便于 JSON 序列化回传调用方。
            if row.get("published_at"):
                row["published_at"] = row["published_at"].isoformat(timespec="seconds")
        return rows

    def fetch_today_by_module(self, module: str, limit: int = 10) -> List[Dict]:
        """取今天抓取/更新的某板块新闻，供每日简报使用。

        参数:
            module: 板块名（如 "科技"）。
            limit:  最多返回条数（默认 10，会强转为至少 1）。

        返回:
            List[Dict]：今天抓取/更新且未过期的新闻，按时间倒序。

        说明:
            crawled_at >= CURDATE() 限定「今日」，expires_at >= NOW() 排除过期；
            ORDER BY COALESCE(published_at, crawled_at) DESC 让发布时间缺失的行
            用抓取时间参与排序，保证简报条目时间可读。
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    # 今日抓取/更新且未过期的记录；COALESCE 让无发布时间的行用抓取时间排序。
                    "SELECT id,module,title,source,url,summary,content,published_at "
                    "FROM offline_news "
                    "WHERE module=%s AND crawled_at >= CURDATE() AND expires_at >= NOW() "
                    "ORDER BY COALESCE(published_at, crawled_at) DESC LIMIT %s",
                    (module, max(1, int(limit))),  # limit 强转 int 且至少为 1。
                )
                rows = cursor.fetchall()
        for row in rows:
            # datetime 转成 ISO 字符串，供简报 DTO 直接使用。
            if row.get("published_at"):
                row["published_at"] = row["published_at"].isoformat(timespec="seconds")
        return rows


class RedisQueryCache:
    """只缓存标准化后完全相同的查询。Redis 故障时返回 miss。

    把查询串先标准化（小写、合并空格），再 sha256 成固定长度 key；
    命中直接返回缓存结果，未命中返回 None（miss），由上层继续走向量库。
    Redis 连接失败时所有方法都只记 warning 并优雅降级（get 返回 None、set 静默跳过）。

    说明:
        缓存只对标准化后完全相同的查询生效（key 由指纹决定），
        离线查询语义仍是向量召回为主，Redis 只是精确缓存加速。
    """

    def __init__(self):
        """读取 conf.redis 配置字典并保存到 self.cfg。"""
        # 读取 [redis] 配置段（host/port/password/db/key_prefix/cache_ttl_seconds）。
        self.cfg = conf.redis

    def _client(self):
        """创建 Redis 连接（每次调用新建）。

        返回:
            redis.Redis 客户端对象。

        说明:
            decode_responses=True 让返回值直接是 str 而非 bytes；
            socket_timeout=2 秒，Redis 不可用时快速失败而不是挂死调用方。
        """
        # 延迟 import redis，保持模块顶层轻量。
        import redis
        # decode_responses=True 让返回值是 str 而非 bytes；socket_timeout=2 秒快速失败。
        return redis.Redis(host=self.cfg["host"], port=self.cfg["port"],
                           password=self.cfg["password"], db=self.cfg["db"],
                           decode_responses=True, socket_timeout=2)

    def _key(self, query: str) -> str:
        """把标准化查询哈希成 Redis 缓存键。

        参数:
            query: 原始查询串（内部先 normalize_query 标准化）。

        返回:
            str：形如 "<key_prefix>:query:<sha256 hexdigest>" 的缓存键。
        """
        # 标准化查询后取 sha256 十六进制摘要，作为定长缓存键。
        digest = hashlib.sha256(normalize_query(query).encode("utf-8")).hexdigest()
        # key 前缀 + 命名空间 + 指纹，便于按前缀批量清理。
        return f"{self.cfg['key_prefix']}:query:{digest}"

    def get(self, query: str) -> Optional[List[Dict]]:
        """读缓存；未命中或 Redis 故障时返回 None。

        参数:
            query: 原始查询串。

        返回:
            List[Dict] 或 None；None 表示未命中 / 缓存过期 / Redis 不可用。

        说明:
            json.loads 还原之前 set 存下的 JSON；任何异常被捕获并降级为 miss。
        """
        try:
            # 读缓存；键不存在时 Redis 返回 None。
            value = self._client().get(self._key(query))
            # 命中则反序列化 JSON；未命中返回 None（miss）。
            return json.loads(value) if value else None
        except Exception as exc:
            # Redis 故障按未命中处理，保证主查询链路可用。
            logger.warning(f"[offline-redis] 查询失败，按未命中处理: {exc}")
            return None

    def set(self, query: str, rows: List[Dict]) -> None:
        """写缓存：把查询结果序列化为 JSON 并设置 TTL。

        参数:
            query: 原始查询串。
            rows:  要缓存的记录列表（dict）。

        说明:
            setex 同时写入值与 TTL（cache_ttl_seconds）；
            json.dumps(..., ensure_ascii=False, default=str) 保证中文可读，
            datetime 等非 JSON 类型用 str 兜底序列化。
        """
        try:
            # setex 同时写入值与 TTL；ensure_ascii=False 保留中文，default=str 兜底非 JSON 类型。
            self._client().setex(self._key(query), self.cfg["cache_ttl_seconds"],
                                 json.dumps(rows, ensure_ascii=False, default=str))
        except Exception as exc:
            # 写缓存失败只记 warning，不影响主查询链路。
            logger.warning(f"[offline-redis] 写缓存失败: {exc}")

    def clear_queries(self) -> None:
        """清理所有查询缓存（每日入库后调用，防止命中昨日旧数据）。

        说明:
            scan_iter 按模式 "<key_prefix>:query:*" 分批扫描全部缓存键后批量 delete；
            Redis 故障时只记 warning 不抛出。
        """
        try:
            client = self._client()
            # 用 key 前缀 + "query:*" 模式匹配全部查询缓存键。
            pattern = f"{self.cfg['key_prefix']}:query:*"
            # scan_iter 分批扫描（count=200），避免 keys 全量命令阻塞 Redis。
            keys = list(client.scan_iter(match=pattern, count=200))
            if keys:
                client.delete(*keys)  # 批量删除扫描到的缓存键。
        except Exception as exc:
            logger.warning(f"[offline-redis] 清理缓存失败: {exc}")


class MilvusNewsIndex:
    """Milvus 保存 news.id → embedding，用 ID 回 MySQL 获取原文。

    职责：
    - connect() 懒连接并自动建库建集合（幂等）；
    - embed() 用 OpenAI 兼容接口调 DashScope text-embedding-v3 生成向量；
    - upsert() 写向量、search_ids() 向量召回、delete() 删过期向量。

    说明:
        Milvus 侧只存 id + vector 两列，与 MySQLNewsStore 的主键 id 一一对应。
    """

    def __init__(self):
        """读取 conf.milvus 配置；self.client 在首次 connect() 时懒初始化。"""
        # 读取 [milvus] 配置段（uri/database_name/collection_name/dimension）。
        self.cfg = conf.milvus
        # client 懒初始化：首次 connect() 时才真正建立连接。
        self.client = None

    def connect(self):
        """建立并返回 Milvus 客户端（幂等：已连接则直接复用）。

        返回:
            pymilvus.MilvusClient 实例。

        说明:
            - MilvusClient(uri=...) 是轻量客户端，指向 Milvus 服务器地址；
            - 若配置的 database 不存在则先 create_database；
            - 若 collection 不存在则 create_collection：dimension 取配置值，
              主键 id 为 int、向量字段名 vector、相似度度量 COSINE、auto_id=False。
        """
        # 幂等：已建立客户端则直接复用，避免重复握手。
        if self.client is not None:
            return self.client
        # 延迟 import pymilvus，保持模块顶层轻量。
        from pymilvus import MilvusClient
        # 先用无 db 的客户端探测：目标 database 不存在则创建。
        bootstrap = MilvusClient(uri=self.cfg["uri"])
        if self.cfg["database_name"] not in bootstrap.list_databases():
            bootstrap.create_database(self.cfg["database_name"])
        bootstrap.close()  # 探测客户端用完即关，释放连接。
        # 正式客户端带上 db_name，后续读写都在该库内进行。
        self.client = MilvusClient(uri=self.cfg["uri"], db_name=self.cfg["database_name"])
        # 集合不存在则按配置建集合（id 主键 int + COSINE 度量）。
        if not self.client.has_collection(self.cfg["collection_name"]):
            self.client.create_collection(
                collection_name=self.cfg["collection_name"],
                dimension=self.cfg["dimension"],  # 向量维度与 embedding 模型一致。
                primary_field_name="id", vector_field_name="vector",  # 主键=MySQL id，向量字段=vector。
                id_type="int", metric_type="COSINE", auto_id=False,  # id 由 MySQL 提供，不自增。
            )
        return self.client

    def embed(self, texts: List[str]) -> List[List[float]]:
        """用 embedding 模型把文本列表批量转成向量。

        参数:
            texts: 文本列表（每条为「板块 标题 摘要 正文前 1500 字」拼接串）。

        返回:
            List[List[float]]：与 texts 一一对应的向量列表。

        抛出:
            底层 openai 调用异常会向上传播（由调用方 upsert / search_ids 处理）。

        说明:
            - openai.OpenAI(base_url=conf.llm["base_url"], api_key=conf.llm["api_key"])
              是 OpenAI 兼容客户端，base_url 指向 DashScope 兼容端点；
            - conf.embedding_model 是模型名（默认 text-embedding-v3），conf.llm 提供密钥；
            - DashScope 单次最多处理 10 条文本，故按 10 分片循环；
            - 返回结果按 item.index 排序再展平，保证向量顺序与输入 texts 一致。
        """
        # 空列表直接返回，避免发起无意义请求。
        if not texts:
            return []
        # 延迟 import openai 客户端；base_url 指向 DashScope 兼容端点。
        from openai import OpenAI
        # 复用 [llm] 段的 base_url/api_key，模型名取 conf.embedding_model。
        client = OpenAI(base_url=conf.llm["base_url"], api_key=conf.llm["api_key"])
        vectors: List[List[float]] = []
        # DashScope text-embedding-v3 单次最多处理 10 条文本。
        for start in range(0, len(texts), 10):
            response = client.embeddings.create(
                model=conf.embedding_model,  # 嵌入模型名（默认 text-embedding-v3）。
                input=texts[start:start + 10],  # 分片输入，每次最多 10 条。
                dimensions=self.cfg["dimension"],  # 输出向量维度与 Milvus 集合一致。
            )
            # 按 item.index 排序后展平，保证向量顺序与输入一致。
            vectors.extend(
                item.embedding
                for item in sorted(response.data, key=lambda item: item.index)
            )
        return vectors

    def upsert(self, rows: List[Dict]) -> None:
        """把 MySQL 行写入向量库：拼文本 → embed → 按 id 写向量。

        参数:
            rows: MySQLNewsStore 返回的记录行（必须含 id / module / title / summary / content）。

        说明:
            拼接规则「模块 标题 摘要 正文前1500字」让向量语义更完整；
            content 只取前 1500 字控制 token 开销（远距离截断对语义影响较小）。
        """
        # 空行直接返回，避免无意义调用。
        if not rows:
            return
        # 拼接向量化文本：板块 + 标题 + 摘要 + 正文前 1500 字，让语义更完整。
        texts = [f"{row['module']} {row['title']} {row.get('summary') or ''} "
                 f"{(row.get('content') or '')[:1500]}" for row in rows]
        vectors = self.embed(texts)  # 批量生成向量，顺序与 rows 一一对应。
        self.connect().upsert(self.cfg["collection_name"], [
            # Milvus 侧只存 id→vector，原文仍在 MySQL。
            {"id": int(row["id"]), "vector": vector}
            for row, vector in zip(rows, vectors)
        ])

    def search_ids(self, query: str, limit: int = 3) -> List[int]:
        """向量召回：把查询串向量化后在 Milvus 里做 COSINE 最近邻搜索。

        参数:
            query: 用户查询串。
            limit: 期望返回条数（内部会再与配置 TopK 对齐，最多 3）。

        返回:
            List[int]：按相似度降序的 MySQL id 列表，供 fetch_by_ids 取原文。

        说明:
            MilvusClient.search 的 data 参数要求二维向量；返回结果第一维对应单条查询，
            每条命中 hit["id"] 即向量主键（MySQL id）。
        """
        # 先把查询串向量化，得到单条查询向量。
        vector = self.embed([query])[0]
        # COSINE 最近邻搜索，返回与查询最相似的前 limit 条命中。
        result = self.connect().search(
            collection_name=self.cfg["collection_name"], data=[vector],
            limit=min(3, limit), search_params={"metric_type": "COSINE", "params": {}},
        )
        # 取第一维（单条查询）的命中 id，转成 int 返回给 MySQL 回查。
        return [int(hit["id"]) for hit in (result[0] if result else [])]

    def delete(self, ids: List[int]) -> None:
        """按 id 删除向量（配合 MySQL 过期清理）。

        参数:
            ids: 要删除的 MySQL 主键 id 列表。

        说明:
            MilvusClient.delete 的 ids 参数要求全部转成 int。
        """
        # 非空才删；ids 统一转 int（Milvus 要求 int 主键）。
        if ids:
            self.connect().delete(self.cfg["collection_name"], ids=[int(i) for i in ids])
