# -*- coding: utf-8 -*-
"""账户监控编排：发现新发布、视频内容提取、去重和通知。"""

import asyncio
import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, List

from config import Config
from create_logger import logger
from mcp_servers.mcp_access import extract_video_content, fetch_account_posts, publish_briefing
from task_pipelines.schemas import AccountPost, PipelineResult
from task_pipelines.schemas import dto_from_dict
from tools.video import VideoContent
from account_monitor.store import AccountMonitorStore

conf = Config()


class AccountMonitorService:
    def __init__(self):
        self.store = AccountMonitorStore()

    def initialize(self) -> None:
        self.store.init_schema()
        removed = self.store.delete_mock_posts()
        if removed:
            logger.warning(f"[account-monitor] 已清理 {removed} 条模拟发布，监控库只保存真实数据")
        for account, url in conf.accounts.items():
            platform = "bilibili" if "bilibili.com" in url or account.startswith("bilibili_") else "rss"
            self.store.register(account, platform, url)

    async def _extract_video(self, url: str) -> VideoContent:
        """优先 A2A 委派 VideoAgent；未启动时直接走 Video MCP 网关。"""
        try:
            from a2a.protocol import delegate
            text = await asyncio.to_thread(
                delegate, "video", "extract_video_content",
                {"video_url": url, "platform": "bilibili", "prefer_subtitle": True},
                "account-monitor",
            )
            payload = json.loads(text)
            if not payload.get("ok"):
                raise RuntimeError(payload.get("error") or "VideoAgent 处理失败")
            return dto_from_dict(VideoContent, payload.get("result") or {})
        except Exception as exc:
            logger.warning(f"[account-monitor] VideoAgent 不可用，直接走 Video MCP: {exc}")
            return await extract_video_content(url, platform="bilibili")

    def register_account(self, account: str, platform: str, url: str) -> Dict:
        """建立持久化监控任务；定时器后续会从 MySQL 读取该任务。"""
        if not account.strip():
            raise ValueError("账户名称不能为空")
        if not url.strip():
            raise ValueError("账户主页/订阅地址不能为空")
        self.store.init_schema()
        self.store.register(account.strip(), platform.strip().lower(), url.strip())
        return {"account": account.strip(), "platform": platform.strip().lower(),
                "url": url.strip(), "registered": True}

    def stop_account(self, account: str, platform: str) -> Dict:
        """停止持续监控，保留已经入库的视频和转写。"""
        self.store.init_schema()
        stopped = self.store.disable(account, platform)
        return {"account": account, "platform": platform, "stopped": stopped}

    async def check_account(self, account: str, platform: str, limit: int = None,
                            source_url: str = "") -> Dict:
        cfg = conf.account_monitor
        limit = limit or cfg["latest_limit"]
        first_run = not self.store.has_history(account, platform)
        try:
            # 动态注册的账户不一定写在 config.ini；把持久化主页地址直接交给采集工具，
            # 抓取完成后再统一恢复成用户指定的账户名。
            fetch_key = source_url or account
            posts = await fetch_account_posts(fetch_key, platform, limit=limit)
            for post in posts:
                post.account = account
            if any(
                post.post_id.startswith("mock-") or post.title.startswith("【模拟】")
                for post in posts
            ):
                raise RuntimeError(
                    "采集端返回了模拟账户数据，监控任务已拒绝入库。"
                    "请重启 Collect MCP 以加载真实平台适配器。"
                )
            known = self.store.existing_ids(platform, [post.post_id for post in posts])
            new_posts = [post for post in posts if post.post_id and post.post_id not in known]
            rows: List[Dict] = []
            enrich = cfg["process_video_content"] and (not first_run or cfg["notify_on_first_run"])
            for post in new_posts:
                row = asdict(post)
                row.update({"transcript": "", "summary": "", "keywords": [], "content_source": "metadata"})
                if enrich and platform == "bilibili" and post.url:
                    try:
                        content = await self._extract_video(post.url)
                        row.update({
                            "transcript": content.transcript,
                            "summary": content.summary,
                            "keywords": content.keywords,
                            "content_source": content.content_source,
                            "content": content.summary or post.content,
                        })
                    except Exception as exc:
                        logger.warning(f"[account-monitor] 视频 {post.post_id} 内容提取失败: {exc}")
                rows.append(row)

            should_notify = bool(rows) and (not first_run or cfg["notify_on_first_run"])
            self.store.save_posts(rows, notified=False)
            publish_result = None
            if should_notify:
                notify_posts = [AccountPost(
                    post_id=row["post_id"], account=row["account"], platform=row["platform"],
                    title=row["title"], content=row.get("summary") or row.get("content", ""),
                    published_at=row.get("published_at", ""), url=row.get("url", ""),
                ) for row in rows]
                publish_result = await publish_briefing(
                    PipelineResult(
                        task_type="account_follow", items=notify_posts,
                        queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    ),
                    channels=cfg["notify_channels"],
                )
                self.store.save_posts(rows, notified=True)
            self.store.finish_check(account, platform)
            return {
                "account": account, "platform": platform, "fetched": len(posts),
                "new": len(rows), "first_run": first_run,
                "notified": bool(publish_result),
                "publish": asdict(publish_result) if publish_result else None,
            }
        except Exception as exc:
            self.store.finish_check(account, platform, str(exc))
            logger.error(f"[account-monitor] {account}@{platform} 检查失败: {exc}")
            return {"account": account, "platform": platform, "error": str(exc)}

    async def check_all(self) -> Dict[str, Dict]:
        self.initialize()
        results = {}
        for row in self.store.enabled_accounts():
            account, platform, url = row["account"], row["platform"], row["space_url"]
            results[account] = await self.check_account(account, platform, source_url=url)
        return results

    def status(self) -> List[Dict]:
        self.initialize()
        return self.store.status()
