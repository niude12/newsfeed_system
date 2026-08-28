#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名: processor_agent.py
项目: HotnewsFeed

本文件干什么：
    加工 Agent —— 覆盖加工阶段能力（与 tools/mcp 的「加工」层对齐），任务①完整链路：
      1) cluster_events   规范化 → Embedding → 相似度聚类 → 事件簇
      2) score_heat       来源数 × 时效衰减 × 讨论量 × 权重 → 热点排序
      3) verify_events    多源交叉核验 → 回填 可信 / 存疑 / 证据不足
    由原 cluster / heat_ranking / verification 三个 Agent 合并而来。

    真实实现经 MCP 加工服务器调用 tools/process.py（见 mcp_servers/mcp_process_server.py）：
    Agent 方法 → mcp_servers.mcp_access.sync_call() → 官方 mcp 客户端 streamable-http
    连接远端 MCP 服务器(:8005) → 调用注册工具；远端不可达时自动降级进程内直调 tools.*。

    A2A 协议：直接继承官方包 python_a2a 的 A2AServer，挂一张标准 AgentCard（含 skill）；
    协议入口是 handle_message(message) —— 收到 Message 后按 metadata 里的 task_type 路由。

    用法：
        python -m agents.processor_agent    # 跑一段「聚类→热度→核验」模拟演示
"""

from typing import List

from python_a2a import A2AServer, AgentCard, AgentSkill, run_server

from a2a.protocol import encode_result, parse_task, reply_text
from config import Config
from create_logger import logger
from task_pipelines.schemas import HotEvent, NewsItem
from tools.process import EventCluster

# Config() 创建配置对象（模拟版主要用于触发配置读取）
conf = Config()

# ===== Agent Card（python_a2a 标准卡片，供 A2A 发现 / 协调器路由）=====
agent_card = AgentCard(
    name="processor",
    description="加工 Agent：对原始资讯聚类成事件簇、计算热度排序、多源交叉核验可信度",
    url="http://localhost:8002",          # A2A 网络端点（真实部署时改实际地址）
    version="1.0.0",
    capabilities={"streaming": True, "memory": True},
    skills=[
        AgentSkill(
            id="cluster_events",
            name="cluster_events",
            description="将新闻列表按照内容相似度聚合为事件簇",
            tags=["news", "clustering", "embedding"],
            examples=[
                "将这50条科技新闻聚合为事件簇",
                "按照0.8相似度阈值对新闻列表进行聚类",
            ],
            input_modes=["text/plain"],
            output_modes=["text/plain"],
        ),
        AgentSkill(
            id="score_heat",
            name="score_heat",
            description="根据事件规模、来源数量和时效性计算事件热度并排序",
            tags=["news", "ranking", "heat-score"],
            examples=[
                "计算这些事件簇近24小时的热度得分",
                "按热度从高到低排列这些新闻事件",
            ],
            input_modes=["text/plain"],
            output_modes=["text/plain"],
        ),
        AgentSkill(
            id="verify_events",
            name="verify_events",
            description="结合多个新闻来源核验事件，并标记为可信、存疑或证据不足",
            tags=["news", "verification", "credibility"],
            examples=[
                "核验这些热点事件的可信度",
                "判断这些新闻事件是否有足够的多源证据",
            ],
            input_modes=["text/plain"],
            output_modes=["text/plain"],
        ),
    ],
)


class ProcessorAgent(A2AServer):
    """加工 Agent（原 cluster + heat_ranking + verification 合并）"""

    name = "processor"
    role = "事件聚类 · 热度评分 · 交叉核验"

    def __init__(self):
        super().__init__(agent_card=agent_card)

    def cluster_events(self, news_items: List[NewsItem], threshold: float = 0.8) -> List[EventCluster]:
        """规范化 → Embedding → 相似度聚类，产出事件簇 → 经 MCP 调用 tools.process.cluster_events"""
        from mcp_servers.mcp_access import cluster_events as _mcp_gateway
        from mcp_servers.mcp_access import sync_call
        logger.info(f"[processor] 对 {len(news_items)} 条资讯聚类（threshold={threshold}）")
        clusters = sync_call(_mcp_gateway(news_items, threshold))
        logger.info(f"[processor] 聚类完成，共 {len(clusters)} 个事件簇")
        return clusters

    def score_heat(self, clusters: List[EventCluster], time_window_hours: int = 24) -> List[HotEvent]:
        """按来源数 · 时效衰减 · 讨论量 · 权重计算热度 → 经 MCP 调用 tools.process.score_heat"""
        from mcp_servers.mcp_access import score_heat as _mcp_gateway
        from mcp_servers.mcp_access import sync_call
        logger.info(f"[processor] 计算 {len(clusters)} 个事件簇热度（窗口 {time_window_hours}h）")
        events = sync_call(_mcp_gateway(clusters, time_window_hours))
        logger.info(f"[processor] 热度计算完成，最高 {events[0].heat_score if events else 0}")
        return events

    def verify_events(self, events: List[HotEvent]) -> List[HotEvent]:
        """多源交叉核验，回填 可信 / 存疑 / 证据不足 → 经 MCP 调用 tools.process.verify_events"""
        from mcp_servers.mcp_access import verify_events as _mcp_gateway
        from mcp_servers.mcp_access import sync_call
        logger.info(f"[processor] 多源交叉核验 {len(events)} 个热点事件")
        events = sync_call(_mcp_gateway(events))
        logger.info(f"[processor] 核验完成")
        return events

    # ===== A2A 入口（python_a2a 协议钩子）=====
    def handle_message(self, message) -> object:
        """收到 A2A 消息：解析 task_type / params → 路由执行 → 回传"""
        task_type, params, from_agent = parse_task(message)
        logger.info(f"[processor] 收到 A2A 消息: {task_type} from={from_agent}")
        try:
            if task_type == "cluster_events":
                result = self.cluster_events(params.get("news_items", []), params.get("threshold", 0.8))
                ok, error = True, None
            elif task_type == "score_heat":
                result = self.score_heat(params.get("clusters", []), params.get("time_window_hours", 24))
                ok, error = True, None
            elif task_type == "verify_events":
                result = self.verify_events(params.get("events", []))
                ok, error = True, None
            else:
                ok, result, error = False, None, f"未知任务类型: {task_type}"
        except Exception as exc:
            logger.error(f"[processor] 处理失败: {exc}")
            ok, result, error = False, None, str(exc)
        # 回传：统一 encode_result（dataclass 序列化成 dict，对端可还原成 DTO）
        text = encode_result(ok, result, error)
        return reply_text(text=text, parent_message_id=message.message_id,
                          conversation_id=message.conversation_id)


# ===== 创建加工 Agent =====
def create_processor_agent():
    """创建加工 Agent（仿照 create_order_mcp_server 的工厂模式）"""
    logger.info("=== 加工Agent信息 ===")
    logger.info(f"名称: {ProcessorAgent.name}")
    logger.info(f"职责: {ProcessorAgent.role}")
    return ProcessorAgent()


if __name__ == "__main__":
    # import sys
    # if "--serve" in sys.argv:
    #     # 独立 A2A 服务器入口：python -m agents.processor_agent --serve（:8002）
    #     from python_a2a import run_server
    run_server(create_processor_agent(), host="127.0.0.1", port=8002)
    # # 业务方法直接调用：采集(借 CollectorAgent) → 聚类 → 热度 → 核验
    # from agents.collector_agent import CollectorAgent
    # news = CollectorAgent().collect_news("科技", limit=3)
    # agent = create_processor_agent()
    # clusters = agent.cluster_events(news)
    # events = agent.score_heat(clusters)
    # verified = agent.verify_events(events)
    # print(f"\n核验后热点事件 {len(verified)} 条：")
    # for e in verified:
    #     print(f"  - {e.title}（热度 {e.heat_score}，可信度 {e.credibility}）")
    #
    # # A2A 入口演示：构造 Message（task_type/params 放 metadata）→ handle_message → 回传
    # print("\n=== A2A 入口演示 ===")
    # from a2a.protocol import delegate
    # text = delegate(agent, task_type="cluster_events",
    #                 params={"news_items": news, "threshold": 0.8}, from_agent="coordinator")
    # print("回传:", text)
