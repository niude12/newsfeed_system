# -*- coding: utf-8 -*-
"""MySQL 监控状态和账户发布去重存储。"""

from datetime import datetime
from typing import Dict, Iterable, List

import pymysql

from config import Config

conf = Config()


class AccountMonitorStore:
    def _connect(self):
        return pymysql.connect(
            **conf.mysql, autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def init_schema(self) -> None:
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
        with self._connect() as conn:
            with conn.cursor() as cursor:
                for statement in statements:
                    cursor.execute(statement)
            conn.commit()

    def delete_mock_posts(self) -> int:
        """监控系统不允许保存模拟发布。"""
        with self._connect() as conn:
            with conn.cursor() as cursor:
                count = cursor.execute(
                    "DELETE FROM account_posts WHERE post_id LIKE 'mock-%%' OR title LIKE '【模拟】%%'"
                )
            conn.commit()
        return count

    def register(self, account: str, platform: str, url: str) -> None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO monitored_accounts(account,platform,space_url) VALUES(%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE space_url=VALUES(space_url),enabled=1",
                    (account, platform, url),
                )
            conn.commit()

    def enabled_accounts(self) -> List[Dict]:
        """返回所有已启用监控账户，作为定时任务的持久化任务清单。"""
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT account,platform,space_url FROM monitored_accounts "
                    "WHERE enabled=1 ORDER BY id"
                )
                return cursor.fetchall()

    def disable(self, account: str, platform: str) -> bool:
        """停用一个监控任务，但保留历史发布和转写内容。"""
        with self._connect() as conn:
            with conn.cursor() as cursor:
                changed = cursor.execute(
                    "UPDATE monitored_accounts SET enabled=0 "
                    "WHERE account=%s AND platform=%s",
                    (account, platform),
                )
            conn.commit()
        return bool(changed)

    def has_history(self, account: str, platform: str) -> bool:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM account_posts WHERE account=%s AND platform=%s LIMIT 1",
                    (account, platform),
                )
                return cursor.fetchone() is not None

    def existing_ids(self, platform: str, post_ids: Iterable[str]) -> set:
        ids = [str(value) for value in post_ids if value]
        if not ids:
            return set()
        placeholders = ",".join(["%s"] * len(ids))
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT post_id FROM account_posts WHERE platform=%s "
                    f"AND post_id IN ({placeholders})",
                    [platform, *ids],
                )
                return {str(row["post_id"]) for row in cursor.fetchall()}

    @staticmethod
    def _time(value: str):
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return None

    def save_posts(self, rows: List[Dict], notified: bool = False) -> None:
        if not rows:
            return
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
        """
        params = [(
            row["platform"], row["account"], row["post_id"], row.get("title", ""),
            row.get("content", ""), row.get("transcript", ""), row.get("summary", ""),
            ",".join(row.get("keywords") or []), row.get("content_source", "metadata"),
            row.get("url", ""), self._time(row.get("published_at", "")), int(notified),
        ) for row in rows]
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, params)
            conn.commit()

    def finish_check(self, account: str, platform: str, error: str = "") -> None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE monitored_accounts SET last_checked_at=NOW(),last_error=%s "
                    "WHERE account=%s AND platform=%s",
                    (error or None, account, platform),
                )
            conn.commit()

    def status(self) -> List[Dict]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT m.account,m.platform,m.space_url,m.last_checked_at,m.last_error,"
                    "COUNT(p.id) AS post_count FROM monitored_accounts m "
                    "LEFT JOIN account_posts p ON p.account=m.account AND p.platform=m.platform "
                    "GROUP BY m.id ORDER BY m.id"
                )
                return cursor.fetchall()
