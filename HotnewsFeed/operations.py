"""CLI/Web 的确定性操作入口。

明确按钮和斜杠命令不需要 Agent 推理；它们直接调用流水线或 A2A 业务能力，避免把固定
工作流塞回 CoordinatorAgent。
"""

import json
from typing import List, Optional

from a2a.protocol import delegate
from mcp_servers.mcp_access import sync_call
from task_pipelines.account_follow_pipeline import run as account_follow
from task_pipelines.hotspot_query_pipeline import run as hotspot
from task_pipelines.latest_news_pipeline import run as latest
from task_pipelines.schemas import PipelineResult


def run_hotspot(module: str, top_n: int = 10, time_window_hours: int = 24,
                keywords: Optional[List[str]] = None) -> PipelineResult:
    return sync_call(hotspot(module, top_n, time_window_hours, keywords))


def run_latest(module: str, count: int = 20,
               keywords: Optional[List[str]] = None) -> PipelineResult:
    return sync_call(latest(module, count, keywords))


def run_account_follow(account: str, platform: str, since: Optional[str] = None,
                       limit: int = 20) -> PipelineResult:
    return sync_call(account_follow(account, platform, since, limit))


def run_monitor(action: str, account: str = "", platform: str = "bilibili",
                url: str = "") -> PipelineResult:
    task_map = {
        "add": "register_monitor",
        "run": "check_monitors",
        "status": "monitor_status",
        "stop": "stop_monitor",
    }
    if action not in task_map:
        raise ValueError(f"未知监控动作: {action}")
    if action == "add":
        if not url:
            raise ValueError("注册监控必须提供账户主页 URL")
        if not account and platform == "bilibili":
            from tools.bilibili import extract_space_uid
            account = f"bilibili_{extract_space_uid(url)}"
    if action == "stop" and not account:
        raise ValueError("停止监控必须提供账户名")
    params = {"account": account.lstrip("@"), "platform": platform, "url": url}
    response = json.loads(delegate("account_monitor", task_map[action], params, "operations"))
    if not response.get("ok"):
        return PipelineResult(task_type="account_monitor", items=[], error=response.get("error"))
    value = response.get("result")
    items = value if isinstance(value, list) else ([] if value is None else [value])
    return PipelineResult(task_type="account_monitor", items=items)
