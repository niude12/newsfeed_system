# -*- coding: utf-8 -*-
"""账户监控编排：发现新发布、视频内容提取、去重和通知。

账户监控的核心业务层（Service 模式）。负责：注册/停用监控账户、增量检查账户新发布、
对 B 站新视频提取内容（经 mcp_access 网关，优先 Video MCP、失败进程内直调）、
按配置决定是否通知，并把结果持久化到 MySQL（account_monitor.store）。

模块依赖:
- ``Config``                   : 全局配置单例。account_monitor 属性给出 latest_limit /
                                 notify_on_first_run / process_video_content / notify_channels 等。
- ``mcp_servers.mcp_access``   : Agent → MCP 访问层。fetch_account_posts 采集账户发布；
                                 extract_video_content 提取视频内容；publish_briefing 发通知。
- ``task_pipelines.schemas``   : AccountPost / PipelineResult 数据模型。
- ``tools.video``              : VideoContent 视频内容模型（提取结果还原 DTO）。
- ``account_monitor.store``    : AccountMonitorStore，MySQL 持久化（注册/去重/状态/发布保存）。
- ``create_logger.logger``     : 全局日志器。

典型调用链::

    AccountMonitorAgent.handle_message
      -> AccountMonitorService.register_account / check_all / status / stop_account
      -> check_account(account, platform, ...)
        -> mcp_access.fetch_account_posts(...)        # 1 采集新发布
        -> store.existing_ids(...)                    # 2 去重
        -> _extract_video(url)                        # 3 视频内容（mcp_access 网关直连）
        -> store.save_posts(row, notified)            # 4 每条视频即时入库
        -> mcp_access.publish_briefing(...)           # 5 通知
        -> store.finish_check(...)                    # 6 记录检查时间/错误

对外暴露的接口（class AccountMonitorService）：
- initialize / register_account / stop_account / check_account / check_all / status
- _extract_video（内部辅助，视频内容提取）
"""

import asyncio
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, List

from config import Config
from create_logger import logger
from mcp_servers.mcp_access import extract_video_content, fetch_account_posts, publish_briefing
from task_pipelines.schemas import AccountPost, PipelineResult
from tools.video import VideoContent
from account_monitor.store import AccountMonitorStore

conf = Config()


class AccountMonitorService:
    """账户监控业务编排类。

    封装账户监控的全部业务动作，供 AccountMonitorAgent 与定时调度器
    （scheduler/account_monitor_scheduler.py）复用。构造时创建 AccountMonitorStore
    用于 MySQL 持久化。
    """

    def __init__(self):
        # 存储层：负责监控账户注册、发布去重、检查状态等 MySQL 读写。
        self.store = AccountMonitorStore()

    def initialize(self) -> None:
        """初始化监控库并注册 config.ini 中配置的账户。

        说明:
            - ``store.init_schema()`` 创建两张表（monitored_accounts / account_posts）。
            - ``store.delete_mock_posts()`` 清理历史遗留的【模拟】发布，监控库只保存真实数据。
            - 遍历 ``conf.accounts``（config.ini [accounts]），按 URL/账户名前缀判断平台，
              逐个 ``store.register`` 注册为启用状态。
        """
        self.store.init_schema()  # 建表（幂等）：monitored_accounts / account_posts。
        removed = self.store.delete_mock_posts()  # 清理历史遗留的【模拟】发布。
        if removed:
            logger.warning(f"[account-monitor] 已清理 {removed} 条模拟发布，监控库只保存真实数据")
        # 遍历 config.ini [accounts] 里配置的账户，逐个注册为启用状态。
        for account, url in conf.accounts.items():
            # 按 URL/账户名前缀判断平台：B 站地址或 bilibili_ 前缀归 bilibili，否则 rss。
            platform = "bilibili" if "bilibili.com" in url or account.startswith("bilibili_") else "rss"
            self.store.register(account, platform, url)  # 幂等注册：已存在则更新地址并启用。

    async def _extract_video(self, url: str) -> VideoContent:
        """经 mcp_access 网关提取视频内容，返回 VideoContent。

        参数:
            url: 视频页面地址（B 站）。

        返回:
            VideoContent：视频内容模型（转写、摘要、关键词、内容来源）。

        说明:
            - 早期版本先 A2A 委派 VideoAgent(:8008)，但它只是透传、且 A2A 客户端
              30s 读超时撑不住 1~2 分钟的转写，导致每条视频被重复提取两次
              （ASR 费用 ×2、时间 ×2），故改为直接走 mcp_access 网关。
            - ``mcp_servers.mcp_access.extract_video_content`` 优先走 Video MCP
              （:8007，300s 超时），失败再进程内直调 tools/video.py。
            - 整体用 ``asyncio.wait_for`` 限时 300s：进程内直调（yt-dlp/ffmpeg/ASR）
              没有总时长上限，单条视频挂住会把整轮 check 堵死，必须给硬超时。
        """
        # wait_for 兜底：MCP 路径自带 300s 传输超时，进程内直调路径没有，统一限时。
        return await asyncio.wait_for(
            extract_video_content(url, platform="bilibili"), timeout=300
        )

    def register_account(self, account: str, platform: str, url: str) -> Dict:
        """建立持久化监控任务；定时器后续会从 MySQL 读取该任务。

        参数:
            account:  账户标识（非空，会去除首尾空白）。
            platform: 平台（会转小写）。
            url:      账户主页 / 订阅地址（非空）。

        返回:
            dict：{account, platform, url, registered: True}。

        抛出:
            ValueError: account 或 url 去除空白后为空时抛出。

        说明:
            - ``store.init_schema()`` 确保表存在；``store.register`` 执行
              INSERT ... ON DUPLICATE KEY UPDATE，账户已存在则更新地址并重新启用。
        """
        if not account.strip():
            raise ValueError("账户名称不能为空")
        if not url.strip():
            raise ValueError("账户主页/订阅地址不能为空")
        self.store.init_schema()  # 确保表存在，防止写入时报 no such table。
        # 幂等注册：已存在则更新地址并重新启用。
        self.store.register(account.strip(), platform.strip().lower(), url.strip())
        return {
            "account": account.strip(),            # 清洗后的账户名。
            "platform": platform.strip().lower(),  # 平台统一小写。
            "url": url.strip(),                    # 清洗后的抓取地址。
            "registered": True,                    # 固定为注册成功标记。
        }

    def stop_account(self, account: str, platform: str) -> Dict:
        """停止持续监控，保留已经入库的视频和转写。

        参数:
            account:  账户标识。
            platform: 平台。

        返回:
            dict：{account, platform, stopped}，stopped 为 True 表示确有账户被停用。

        说明:
            - ``store.disable`` 执行 UPDATE ... SET enabled=0，只停用监控任务，
              不删除 account_posts 里的历史数据。
        """
        self.store.init_schema()  # 确保表存在。
        # disable 执行 UPDATE ... SET enabled=0，返回是否确有记录被更新。
        stopped = self.store.disable(account, platform)
        return {
            "account": account,    # 账户标识原样回传。
            "platform": platform,  # 平台原样回传。
            "stopped": stopped,    # True 表示确有账户被停用。
        }

    async def check_account(self, account: str, platform: str, limit: int = None,
                            source_url: str = "") -> Dict:
        """检查单个账户的新发布：采集 → 去重 → 视频处理 → 入库 → 通知。

        参数:
            account:    账户标识。
            platform:   平台。
            limit:      本次最多采集条数；None 时用配置 latest_limit。
            source_url: 持久化的抓取地址。动态注册的账户不一定在 config.ini，抓取用
                        该地址，抓完再统一恢复成用户指定的账户名。

        返回:
            dict：{account, platform, fetched, new, first_run, notified, publish}；
            失败时返回 {account, platform, error}。

        说明:
            - ``mcp_access.fetch_account_posts`` 走 MCP 采集网关（优先远端 collect MCP，
              失败降级 tools/collect.py）。
            - 采集端返回 mock 数据（post_id 以 mock- 开头或标题带【模拟】）时拒绝入库，
              抛 RuntimeError 提示重启 Collect MCP。
            - 视频内容处理开关：cfg["process_video_content"]，且仅在“非首次”或配置了
              notify_on_first_run 时对 B 站视频做内容提取。
            - 通知开关：``bool(rows) and (not first_run or cfg["notify_on_first_run"])``，
              即首次运行默认不通知（避免把历史旧内容全部推送一遍）。
            - ``publish_briefing`` 构造 PipelineResult（task_type="account_follow"）推送给
              配置通道（notify_channels），成功后把通知标记写回 MySQL。
            - 每条视频处理完立即写入 MySQL（``save_posts`` 是幂等 upsert），即使中途
              中断/重启也不丢已处理结果；整批结束后再统一回写通知标记。
        """
        cfg = conf.account_monitor  # [account_monitor] 配置段 dict。
        limit = limit or cfg["latest_limit"]  # 未指定时用配置的拉取上限。
        first_run = not self.store.has_history(account, platform)  # 首次运行 = 无历史记录。
        try:
            # 动态注册的账户不一定写在 config.ini；把持久化主页地址直接交给采集工具，
            # 抓取完成后再统一恢复成用户指定的账户名。
            fetch_key = source_url or account
            posts = await fetch_account_posts(fetch_key, platform, limit=limit)  # 采集新发布。
            for post in posts:
                post.account = account  # 统一恢复成用户指定的账户名。
            # 采集端返回 mock 数据（post_id 以 mock- 开头或标题带【模拟】）时拒绝入库。
            if any(
                post.post_id.startswith("mock-") or post.title.startswith("【模拟】")
                for post in posts
            ):
                raise RuntimeError(
                    "采集端返回了模拟账户数据，监控任务已拒绝入库。"
                    "请重启 Collect MCP 以加载真实平台适配器。"
                )
            # 查库中已存在的 post_id，用于去重。
            known = self.store.existing_ids(platform, [post.post_id for post in posts])
            # 只保留有 post_id 且不在已知集合里的新发布。
            new_posts = [post for post in posts if post.post_id and post.post_id not in known]
            rows: List[Dict] = []  # 待入库的新发布记录列表。
            # 是否对 B 站视频做内容提取：开关开启，且非首次运行或配置了首次通知。
            enrich = cfg["process_video_content"] and (not first_run or cfg["notify_on_first_run"])
            for post in new_posts:
                row = asdict(post)  # AccountPost -> dict，作为入库基础字段。
                # 预置视频内容字段为空，来源默认 metadata。
                row.update({"transcript": "", "summary": "", "keywords": [], "content_source": "metadata"})
                if enrich and platform == "bilibili" and post.url:
                    try:
                        # 提取视频字幕/ASR 内容（经 mcp_access 网关，优先 Video MCP、失败进程内直调）。
                        content = await self._extract_video(post.url)
                        row.update({
                            "transcript": content.transcript,          # 转写全文。
                            "summary": content.summary,                # 摘要。
                            "keywords": content.keywords,              # 关键词列表。
                            "content_source": content.content_source,  # 内容来源标记。
                            "content": content.summary or post.content,  # 内容列优先用摘要。
                        })
                    except Exception as exc:
                        # 单条视频内容提取失败不中断整轮，记 warning 后仍入库基础信息。
                        logger.warning(f"[account-monitor] 视频 {post.post_id} 内容提取失败: {exc}")
                # 每条处理完立即入库（upsert 幂等），避免整批最后才写、中途中断全丢。
                self.store.save_posts([row], notified=False)
                rows.append(row)

            # 是否通知：有新增内容，且非首次运行或配置了首次通知。
            should_notify = bool(rows) and (not first_run or cfg["notify_on_first_run"])
            publish_result = None
            if should_notify:
                # 构造通知用的 AccountPost 列表，内容列优先用摘要。
                notify_posts = [AccountPost(
                    post_id=row["post_id"], account=row["account"], platform=row["platform"],
                    title=row["title"], content=row.get("summary") or row.get("content", ""),
                    published_at=row.get("published_at", ""), url=row.get("url", ""),
                ) for row in rows]
                # 生成简报并推送（走 publish MCP / 进程内降级）。
                publish_result = await publish_briefing(
                    PipelineResult(
                        task_type="account_follow", items=notify_posts,  # 流水线任务类型。
                        queried_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    ),
                    channels=cfg["notify_channels"],  # 通知通道列表。
                )
                self.store.save_posts(rows, notified=True)  # 通知成功后回写已通知标记。
            self.store.finish_check(account, platform)  # 记录本次检查完成时间。
            return {
                "account": account,           # 账户标识。
                "platform": platform,         # 平台。
                "fetched": len(posts),        # 本次采集条数。
                "new": len(rows),             # 新增（去重后）条数。
                "first_run": first_run,       # 是否首次运行。
                "notified": bool(publish_result),  # 是否完成通知。
                "publish": asdict(publish_result) if publish_result else None,  # 通知结果对象。
            }
        except Exception as exc:
            # 失败也要记录检查状态（写入 last_error），便于后台排查。
            self.store.finish_check(account, platform, str(exc))
            logger.error(f"[account-monitor] {account}@{platform} 检查失败: {exc}")
            return {
                "account": account,      # 账户标识。
                "platform": platform,    # 平台。
                "error": str(exc),       # 失败原因，供上层展示。
            }

    async def check_all(self) -> Dict[str, Dict]:
        """检查全部已启用账户，返回 {账户: 检查结果} 字典。

        说明:
            - ``initialize()`` 先初始化表并注册 config.ini 账户（幂等）。
            - ``store.enabled_accounts()`` 从 MySQL 读取全部启用账户（定时任务的
              持久化任务清单），逐个调用 check_account（当前为顺序执行）。
            - 每个账户的 source_url 取持久化的 space_url，保证动态注册的账户也能抓到。
        """
        self.initialize()  # 先初始化表并注册 config.ini 账户（幂等）。
        results = {}  # 账户 -> 检查结果 dict。
        # 从 MySQL 读取全部启用账户（定时任务的持久化任务清单）。
        for row in self.store.enabled_accounts():
            account, platform, url = row["account"], row["platform"], row["space_url"]
            # 逐个检查；source_url 取持久化地址，保证动态注册账户也能抓到。
            results[account] = await self.check_account(account, platform, source_url=url)
        return results

    def status(self) -> List[Dict]:
        """查询监控状态：各账户已发现发布数、最后检查时间与错误。

        说明:
            - ``store.status()`` 用 LEFT JOIN 统计每个账户的 post_count 等状态信息。
        """
        self.initialize()  # 确保表存在并注册配置账户。
        return self.store.status()  # LEFT JOIN 统计各账户发布数等状态。
