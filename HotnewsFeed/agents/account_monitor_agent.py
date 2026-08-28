#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Account Monitor Agent：管理持续账户监控任务并编排首次检查。"""

from python_a2a import A2AServer, AgentCard, AgentSkill, run_server

from a2a.protocol import encode_result, parse_task, reply_text
from account_monitor import AccountMonitorService
from create_logger import logger
from mcp_servers.mcp_access import sync_call


agent_card = AgentCard(
    name="account_monitor",
    description=(
        "账户监控 Agent：注册或停止持续监控任务、立即检查新发布、查询监控状态；"
        "B站新视频可继续委派 VideoAgent 提取字幕/ASR并写入MySQL"
    ),
    url="http://localhost:8009",
    version="1.0.0",
    capabilities={"streaming": False, "memory": True},
    skills=[
        AgentSkill(
            id="register_monitor",
            name="register_monitor",
            description="注册账户持续监控，并可立即完成一次基线检查",
            tags=["account", "monitor", "bilibili", "register"],
            examples=[
                '{"account":"bilibili_312249633","platform":"bilibili",'
                '"registered":true,"checked":{"fetched":10,"new":10,"first_run":true}}'
            ],
            input_modes=["application/json"], output_modes=["application/json"],
        ),
        AgentSkill(
            id="check_monitors",
            name="check_monitors",
            description="立即检查全部已启用账户，发现新发布后进行去重、视频处理和通知",
            tags=["account", "monitor", "check"],
            examples=['{"bilibili_312249633":{"fetched":10,"new":1,"notified":true}}'],
            input_modes=["application/json"], output_modes=["application/json"],
        ),
        AgentSkill(
            id="monitor_status",
            name="monitor_status",
            description="返回当前监控账户、已发现发布数量、最后检查时间和错误",
            tags=["account", "monitor", "status"],
            examples=['[{"account":"bilibili_312249633","post_count":10,"last_error":null}]'],
            input_modes=["application/json"], output_modes=["application/json"],
        ),
    ],
)


class AccountMonitorAgent(A2AServer):
    """持续账户监控的下游业务 Agent；Coordinator 只负责把大任务交给它。"""

    name = "account_monitor"
    role = "账户监控注册 · 增量检查 · 视频处理 · 状态管理"

    def __init__(self):
        super().__init__(agent_card=agent_card)
        self.service = AccountMonitorService()

    def register_monitor(self, account: str, platform: str, url: str,
                         check_now: bool = True) -> dict:
        result = self.service.register_account(account, platform, url)
        if check_now:
            result["checked"] = sync_call(
                self.service.check_account(account, platform, source_url=url)
            )
        return result

    def handle_message(self, message):
        task_type, params, from_agent = parse_task(message)
        logger.info(f"[account-monitor-agent] 收到 A2A 消息: {task_type} from={from_agent}")
        try:
            if task_type == "register_monitor":
                result = self.register_monitor(
                    params.get("account", ""), params.get("platform", "bilibili"),
                    params.get("url", ""), params.get("check_now", True),
                )
            elif task_type == "check_monitors":
                result = sync_call(self.service.check_all())
            elif task_type == "monitor_status":
                result = self.service.status()
            elif task_type == "stop_monitor":
                result = self.service.stop_account(
                    params.get("account", ""), params.get("platform", "bilibili")
                )
            else:
                raise ValueError(f"未知任务类型: {task_type}")
            text = encode_result(True, result, None)
        except Exception as exc:
            logger.error(f"[account-monitor-agent] 处理失败: {exc}", exc_info=True)
            text = encode_result(False, None, str(exc))
        return reply_text(text, message.message_id, message.conversation_id)


def create_account_monitor_agent():
    logger.info("=== 账户监控 Agent 信息 ===")
    logger.info(f"名称: {AccountMonitorAgent.name}")
    logger.info(f"职责: {AccountMonitorAgent.role}")
    return AccountMonitorAgent()


if __name__ == "__main__":
    run_server(create_account_monitor_agent(), host="127.0.0.1", port=8009)
