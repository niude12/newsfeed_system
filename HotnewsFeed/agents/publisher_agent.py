#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名: publisher_agent.py
项目: HotnewsFeed

本文件干什么：
    输出 Agent —— 覆盖输出阶段能力（与 tools/mcp 的「输出」层对齐）：
      1) optimize_output     去重 · 排序 · 引用 · 个性化呈现（共享出口）
      2) publish_briefing    生成热点简报（摘要 · 重要性 · 来源 · 可信度）并推送（飞书·邮件·Webhook·Web UI）
    由原 briefing / output_optimizer 两个 Agent 合并而来。
    简报通常在任务完成后经 A2A 交接 + 用户确认调用（见 coordinator_agent.handoff_briefing）。

    真实实现经 MCP 输出服务器调用 tools/publish.py（见 mcp_servers/mcp_publish_server.py）：
    Agent 方法 → mcp_servers.mcp_access.sync_call() → 官方 mcp 客户端 streamable-http
    连接远端 MCP 服务器(:8006) → 调用注册工具；远端不可达时自动降级进程内直调 tools.*。

    A2A 协议：直接继承官方包 python_a2a 的 A2AServer，挂一张标准 AgentCard（含 skill）；
    协议入口是 handle_message(message) —— 收到 Message 后按 metadata 里的 task_type 路由。

    用法：
        python -m agents.publisher_agent    # 跑一段「优化→生成简报→推送」模拟演示
"""

from typing import List, Optional

from python_a2a import A2AServer, AgentCard, AgentSkill, run_server

from a2a.protocol import encode_result, parse_task, reply_text
from config import Config
from create_logger import logger
from task_pipelines.schemas import PipelineResult
from tools.publish import PublishResult

# Config() 创建配置对象（模拟版主要用于触发配置读取）
conf = Config()

# ===== Agent Card（python_a2a 标准卡片，供 A2A 发现 / 协调器路由）=====
agent_card = AgentCard(
    name="publisher",
    description="输出 Agent：对任务结果去重排序呈现；生成热点简报并推送（飞书·邮件·Webhook·Web UI）",
    url="http://localhost:8003",          # A2A 网络端点（真实部署时改实际地址）
    version="1.0.0",
    capabilities={"streaming": True, "memory": True},
    skills=[
        AgentSkill(
            id="optimize",
            name="optimize_output",
            description="整理任务结果，生成适合用户阅读的统一展示结构",
            tags=["output", "formatting", "presentation"],
            examples=[
                "整理这次热点查询结果用于终端展示",
                "将最新新闻结果转换为统一输出格式",
            ],
            input_modes=["text/plain"],
            output_modes=["text/plain"],
        ),
        AgentSkill(
            id="briefing",
            name="publish_briefing",
            description="根据任务结果生成简报，并发送到指定输出通道",
            tags=["briefing", "publishing", "notification"],
            examples=[
                "根据这次查询结果生成一份简报",
                "生成简报并保存到Web UI输出目录",
                "将简报推送到飞书和Webhook",
            ],
            input_modes=["text/plain"],
            output_modes=["text/plain"],
        ),
    ],
)


class PublisherAgent(A2AServer):
    """输出 Agent（原 briefing + output_optimizer 合并）"""

    name = "publisher"
    role = "结果优化呈现 · 简报生成与推送"

    def __init__(self):
        super().__init__(agent_card=agent_card)

    def optimize_output(self, result: PipelineResult) -> PipelineResult:
        """去重 · 排序 · 引用 · 个性化呈现，向用户展示最终结果"""
        logger.info(f"[publisher] 优化输出: task_type={result.task_type} items={len(result.items)}")
        # 【模拟】真实实现做去重/排序/引用增强；当前原样返回
        return result

    def publish_briefing(self, result: PipelineResult,
                         channels: Optional[List[str]] = None,
                         template: str = "default") -> PublishResult:
        """生成简报（摘要 · 重要性 · 来源 · 可信度）并推送 → 经 MCP 调用 tools.publish.publish_briefing"""
        from mcp_servers.mcp_access import publish_briefing as _mcp_gateway
        from mcp_servers.mcp_access import sync_call
        channels = channels or ["feishu", "web_ui"]
        logger.info(f"[publisher] 生成简报 template={template}，推送通道={channels}")
        pub = sync_call(_mcp_gateway(result, channels, template))
        logger.info(f"[publisher] 简报 {pub.briefing_id} 推送状态: {pub.channels}")
        return pub

    # ===== A2A 入口（python_a2a 协议钩子）=====
    def handle_message(self, message) -> object:
        """收到 A2A 消息：解析 task_type / params → 路由执行 → 回传"""
        task_type, params, from_agent = parse_task(message)
        logger.info(f"[publisher] 收到 A2A 消息: {task_type} from={from_agent}")
        try:
            if task_type == "optimize":
                result = self.optimize_output(params.get("result"))
                ok, error = True, None
            elif task_type == "briefing":
                result = self.publish_briefing(params.get("result"), params.get("channels"),
                                               params.get("template", "default"))
                ok, error = True, None
            else:
                ok, result, error = False, None, f"未知任务类型: {task_type}"
        except Exception as exc:
            logger.error(f"[publisher] 处理失败: {exc}")
            ok, result, error = False, None, str(exc)
        # 回传：统一 encode_result（dataclass 序列化成 dict，对端可还原成 DTO）
        text = encode_result(ok, result, error)
        return reply_text(text=text, parent_message_id=message.message_id,
                          conversation_id=message.conversation_id)


# ===== 创建输出 Agent =====
def create_publisher_agent():
    """创建输出 Agent（仿照 create_order_mcp_server 的工厂模式）"""
    logger.info("=== 输出Agent信息 ===")
    logger.info(f"名称: {PublisherAgent.name}")
    logger.info(f"职责: {PublisherAgent.role}")
    return PublisherAgent()


if __name__ == "__main__":
    run_server(create_publisher_agent(), host="127.0.0.1", port=8003)
    # # 业务方法直接调用：取一份热点结果 → 优化 → 生成简报并推送
    # from agents.coordinator_agent import CoordinatorAgent
    # result = CoordinatorAgent().run_hotspot("科技", top_n=3)
    # agent = create_publisher_agent()
    # optimized = agent.optimize_output(result)
    # pub = agent.publish_briefing(optimized, channels=["feishu", "web_ui"])
    # print(f"\n简报 {pub.briefing_id} 推送状态: {pub.channels}")
    #
    # # A2A 入口演示：构造 Message（task_type/params 放 metadata）→ handle_message → 回传
    # print("\n=== A2A 入口演示 ===")
    # from a2a.protocol import delegate
    # text = delegate(agent, task_type="briefing",
    #                 params={"result": optimized, "channels": ["feishu"]}, from_agent="coordinator")
    # print("回传:", text)
