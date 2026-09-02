# -*- coding: utf-8 -*-
"""MySQL 监控状态和账户发布去重存储。

账户监控的持久化层（Store 模式）。用 MySQL 保存两样东西：
① 监控账户表 monitored_accounts —— 谁在被监控、最后检查时间、是否启用；
② 账户发布表 account_posts —— 已发现的发布（含转写/摘要/关键词/通知标记），
   UNIQUE(platform, post_id) 保证按平台去重。
另负责初始化表结构、删除模拟数据、写入/查询发布与检查状态。

模块依赖:
- ``pymysql``      : MySQL 驱动。connect 参数直接来自 ``conf.mysql``（config.ini [mysql]），
                     autocommit=False 手动提交、DictCursor 返回字典行。
- ``Config``       : 全局配置单例。``conf.mysql`` 属性给出 host/port/user/password/
                     database/charset 等连接参数。

对外暴露的接口（class AccountMonitorStore）：
- init_schema / delete_mock_posts / register / enabled_accounts / disable /
  has_history / existing_ids / save_posts / finish_check / status
"""

from datetime import datetime
from typing import Dict, Iterable, List

import pymysql

from config import Config

conf = Config()


class AccountMonitorStore:
    """账户监控的 MySQL 存储层。

    每个方法都独立创建短连接（self._connect），用完即关；配合 autocommit=False
    由方法内显式 conn.commit() 提交，保证一批操作要么全成要么全不生效。
    """

    def _connect(self):
        """创建 MySQL 连接（短连接）。

        说明:
            - ``pymysql.connect`` 参数用 ``**conf.mysql`` 展开（见 config.py 的 mysql 属性）。
            - autocommit=False：需要方法内手动 commit；DictCursor 让查询结果每行是 dict，
              可通过列名（如 row["account"]）访问。
        """
        # pymysql.connect 参数用 **conf.mysql 展开（host/port/user/password/database 等）。
        return pymysql.connect(
            **conf.mysql, autocommit=False,  # 手动提交事务，由方法内显式 commit。
            cursorclass=pymysql.cursors.DictCursor,  # 查询结果每行是 dict，按列名访问。
        )

    def init_schema(self) -> None:
        """建表（幂等）：monitored_accounts 与 account_posts。

        说明:
            - monitored_accounts：监控账户注册表。UNIQUE(account, platform) 保证同一平台
              账户唯一；enabled 为停用标记。
            - account_posts：账户发布表。UNIQUE(platform, post_id) 保证同一平台发布不重复
              （去重的核心约束）；idx_account_time 加速按账户+时间查询。
            - 均在 autocommit=False 连接里逐条 execute 后统一 commit。
        """
        # 两张表：monitored_accounts 记录监控账户；account_posts 记录已发现的发布。
        statements = [
            """
            CREATE TABLE IF NOT EXISTS monitored_accounts (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                account VARCHAR(255) NOT NULL,
                platform VARCHAR(32) NOT NULL,
                space_url VARCHAR(1500) NOT NULL,
                last_checked_at DATETIME NULL,
                last_error TEXT NULL,
                enabled TINYINT(1) NOT NULL DEFAULT 1,
                UNIQUE KEY uk_account_platform (account, platform)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS account_posts (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                platform VARCHAR(32) NOT NULL,
                account VARCHAR(255) NOT NULL,
                post_id VARCHAR(128) NOT NULL,
                title VARCHAR(500) NOT NULL DEFAULT '',
                content MEDIUMTEXT,
                transcript MEDIUMTEXT,
                summary TEXT,
                keywords TEXT,
                content_source VARCHAR(32) NOT NULL DEFAULT 'metadata',
                url VARCHAR(1500) NOT NULL DEFAULT '',
                published_at DATETIME NULL,
                discovered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                notified TINYINT(1) NOT NULL DEFAULT 0,
                UNIQUE KEY uk_platform_post (platform, post_id),
                INDEX idx_account_time (account, published_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ]
        with self._connect() as conn:  # 短连接，with 退出自动关闭。
            with conn.cursor() as cursor:  # 获取游标执行 SQL。
                # 逐条执行建表语句（CREATE TABLE IF NOT EXISTS，重复调用不报错）。
                for statement in statements:
                    cursor.execute(statement)
            # autocommit=False，需显式提交才真正生效。
            conn.commit()

    def delete_mock_posts(self) -> int:
        """监控系统不允许保存模拟发布。

        删除 post_id 以 mock- 开头或标题带【模拟】的发布记录（采集端降级样例数据）。

        返回:
            int：删除的行数。
        """
        with self._connect() as conn:  # 短连接。
            with conn.cursor() as cursor:  # 获取游标。
                # LIKE 'mock-%%'：%% 在 pymysql 参数化 SQL 里转义成字面 %。
                count = cursor.execute(
                    "DELETE FROM account_posts WHERE post_id LIKE 'mock-%%' OR title LIKE '【模拟】%%'"
                )
            conn.commit()  # 提交删除。
        return count  # 返回删除行数。

    def register(self, account: str, platform: str, url: str) -> None:
        """注册（或重新启用）一个监控账户。

        参数:
            account:  账户标识。
            platform: 平台。
            url:      账户主页 / 订阅地址。

        说明:
            - INSERT ... ON DUPLICATE KEY UPDATE：账户+平台已存在时更新地址并置 enabled=1，
              实现「注册 = 新增或恢复启用」的幂等语义。
        """
        with self._connect() as conn:  # 短连接。
            with conn.cursor() as cursor:  # 获取游标。
                # INSERT ... ON DUPLICATE KEY UPDATE：已存在则更新地址并重新启用。
                cursor.execute(
                    "INSERT INTO monitored_accounts(account,platform,space_url) VALUES(%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE space_url=VALUES(space_url),enabled=1",
                    (account, platform, url),
                )
            conn.commit()  # 提交注册。

    def enabled_accounts(self) -> List[Dict]:
        """返回所有已启用监控账户，作为定时任务的持久化任务清单。

        返回:
            List[Dict]：每行含 account / platform / space_url，按 id 升序。
        """
        with self._connect() as conn:  # 短连接。
            with conn.cursor() as cursor:  # 获取游标。
                cursor.execute(
                    "SELECT account,platform,space_url FROM monitored_accounts "
                    "WHERE enabled=1 ORDER BY id"  # 只取启用账户，按 id 升序。
                )
                return cursor.fetchall()  # 返回字典行列表。

    def disable(self, account: str, platform: str) -> bool:
        """停用一个监控任务，但保留历史发布和转写内容。

        参数:
            account:  账户标识。
            platform: 平台。

        返回:
            bool：True 表示确有记录被更新（账户存在且处于启用态）；否则 False。
        """
        with self._connect() as conn:  # 短连接。
            with conn.cursor() as cursor:  # 获取游标。
                # 只停用监控任务（enabled=0），不删除历史发布数据。
                changed = cursor.execute(
                    "UPDATE monitored_accounts SET enabled=0 "
                    "WHERE account=%s AND platform=%s",
                    (account, platform),
                )
            conn.commit()  # 提交停用。
        return bool(changed)  # 转 bool：是否有记录被更新。

    def has_history(self, account: str, platform: str) -> bool:
        """判断该账户在该平台是否已有历史发布记录（用于区分首次运行）。

        说明:
            - SELECT 1 ... LIMIT 1：只探测是否存在，不取数据。
            - 返回 fetchone() 是否非 None；据此判断 first_run，进而决定是否通知。
        """
        with self._connect() as conn:  # 短连接。
            with conn.cursor() as cursor:  # 获取游标。
                # SELECT 1 ... LIMIT 1：只探测是否存在，不取数据。
                cursor.execute(
                    "SELECT 1 FROM account_posts WHERE account=%s AND platform=%s LIMIT 1",
                    (account, platform),
                )
                return cursor.fetchone() is not None  # 查到行 = 有历史记录。

    def existing_ids(self, platform: str, post_ids: Iterable[str]) -> set:
        """查询该平台下已存在的发布 ID 集合，用于去重。

        参数:
            platform: 平台。
            post_ids: 待判重的新发布 ID 列表（空值会被过滤掉）。

        返回:
            set：已在库中的 post_id 集合；传空列表时返回空集合。
        """
        # 过滤空值并统一转字符串，避免 None 混入 SQL 参数。
        ids = [str(value) for value in post_ids if value]
        if not ids:
            return set()  # 空输入直接返回空集合。
        # 动态拼占位符：IN (%s, %s, ...)，参数个数与 ID 数量一致。
        placeholders = ",".join(["%s"] * len(ids))
        with self._connect() as conn:  # 短连接。
            with conn.cursor() as cursor:  # 获取游标。
                cursor.execute(
                    f"SELECT post_id FROM account_posts WHERE platform=%s "
                    f"AND post_id IN ({placeholders})",
                    [platform, *ids],  # 平台 + 展开 ID 列表作为参数。
                )
                return {str(row["post_id"]) for row in cursor.fetchall()}  # 已存在的 ID 集合。

    @staticmethod
    def _time(value: str):
        """把 ISO 时间字符串转成 MySQL DATETIME 可接受的对象。

        参数:
            value: ISO 8601 时间字符串（可能带 "Z" 后缀）。

        返回:
            datetime 对象（无时区）或 None（值为空 / 格式非法时）。

        说明:
            - 先 ``replace("Z", "+00:00")`` 让 fromisoformat 能解析 UTC 后缀
              （Python 3.11 前 fromisoformat 不识别 "Z"）。
            - 再 ``replace(tzinfo=None)`` 去掉时区，避免 MySQL 驱动写入带时区对象报错。
        """
        if not value:
            return None  # 空值 -> None（MySQL NULL）。
        try:
            # "Z" 替换成 +00:00 让 fromisoformat 能解析 UTC 后缀，再去掉时区。
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return None  # 格式非法 -> None。

    def save_posts(self, rows: List[Dict], notified: bool = False) -> None:
        """批量写入发布记录；已存在的按 post_id 更新内容与通知标记。

        参数:
            rows:    发布记录 dict 列表（含 platform/account/post_id/title/content/
                     transcript/summary/keywords/content_source/url/published_at）。
            notified: 本次写入/更新是否视为「已通知」。True 时把 notified 置 1
                      （GREATEST 保证只升不降）；False 时只入库不置通知位。

        说明:
            - ON DUPLICATE KEY UPDATE 以 (platform, post_id) 为准，重复发布时更新内容、
              转写、摘要等；published_at 用 COALESCE 保留已存在的旧值；notified 用
              GREATEST 取两边的较大值（0/1），避免「已通知」被覆盖回未通知。
            - keywords 是 list，先 ",".join 拼成逗号分隔字符串再入库。
            - ``cursor.executemany`` 一次批量执行；autocommit=False，需显式 commit。
        """
        if not rows:
            return  # 空列表直接返回，不执行 SQL。
        sql = """
        INSERT INTO account_posts
          (platform,account,post_id,title,content,transcript,summary,keywords,
           content_source,url,published_at,notified)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          title=VALUES(title),content=VALUES(content),transcript=VALUES(transcript),
          summary=VALUES(summary),keywords=VALUES(keywords),content_source=VALUES(content_source),
          url=VALUES(url),published_at=COALESCE(VALUES(published_at),published_at),
          notified=GREATEST(notified,VALUES(notified))
        """  # 批量 upsert：重复按 post_id 更新，通知位只升不降。
        # 逐行把 dict 拍平成 SQL 参数元组；keywords 列表用逗号连接成字符串。
        params = [(
            row["platform"], row["account"], row["post_id"], row.get("title", ""),
            row.get("content", ""), row.get("transcript", ""), row.get("summary", ""),
            ",".join(row.get("keywords") or []), row.get("content_source", "metadata"),
            row.get("url", ""), self._time(row.get("published_at", "")), int(notified),
        ) for row in rows]
        with self._connect() as conn:  # 短连接。
            with conn.cursor() as cursor:  # 获取游标。
                cursor.executemany(sql, params)  # 批量执行插入/更新。
            conn.commit()  # 提交批量写入。

    def finish_check(self, account: str, platform: str, error: str = "") -> None:
        """记录一次检查完成：更新 last_checked_at 与 last_error。

        参数:
            account:  账户标识。
            platform: 平台。
            error:    本次检查的错误信息；为空串则置 NULL（成功）。

        说明:
            - last_checked_at=NOW() 由 MySQL 服务端时间写入。
            - ``error or None``：空串转 None，保证成功时 last_error 字段干净。
        """
        with self._connect() as conn:  # 短连接。
            with conn.cursor() as cursor:  # 获取游标。
                cursor.execute(
                    "UPDATE monitored_accounts SET last_checked_at=NOW(),last_error=%s "
                    "WHERE account=%s AND platform=%s",
                    (error or None, account, platform),  # 空串错误转 None。
                )
            conn.commit()  # 提交检查状态。

    def status(self) -> List[Dict]:
        """查询各监控账户状态：含已发现发布数、最后检查时间与错误。

        说明:
            - LEFT JOIN account_posts 按 account+platform 关联，COUNT(p.id) 统计
              每个账户已入库的发布数；未发现发布的账户 post_count 为 0。
            - GROUP BY m.id 按监控账户分组，ORDER BY m.id 保持注册顺序。
        """
        with self._connect() as conn:  # 短连接。
            with conn.cursor() as cursor:  # 获取游标。
                cursor.execute(
                    "SELECT m.account,m.platform,m.space_url,m.last_checked_at,m.last_error,"
                    "COUNT(p.id) AS post_count FROM monitored_accounts m "
                    "LEFT JOIN account_posts p ON p.account=m.account AND p.platform=m.platform "
                    "GROUP BY m.id ORDER BY m.id"  # 按监控账户分组，统计发布数。
                )
                return cursor.fetchall()  # 返回各账户状态字典行。
