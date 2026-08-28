#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名: collector_agent.py
项目: HotnewsFeed

本文件干什么：
    采集 Agent —— 覆盖采集阶段能力（与 tools/mcp 的「采集」层对齐）：
      1) collect_news          多源资讯采集（RSS · GNews · 热榜 API · HN），任务①②③ 复用
      2) fetch_account_posts   账户发布监控，任务③使用（原 account_monitor_agent 合并而来）

    真实实现经 MCP 采集服务器调用 tools/collect.py（见 mcp_servers/mcp_collect_server.py）：
    Agent 方法 → mcp_servers.mcp_access.sync_call() → 官方 mcp 客户端 streamable-http
    连接远端 MCP 服务器(:8004) → 调用注册工具；远端不可达时自动降级进程内直调 tools.*。

    A2A 协议：直接继承官方包 python_a2a 的 A2AServer，挂一张标准 AgentCard（含 skill）；
    协议入口是 handle_message(message) —— 收到 Message 后按 metadata 里的 task_type 路由。

    用法：
        python -m agents.collector_agent    # 跑一段模拟演示（业务方法 + A2A 入口）
"""

from typing import List, Optional

from python_a2a import A2AServer, AgentCard, AgentSkill, run_server

from a2a.protocol import encode_result, parse_task, reply_text
from create_logger import logger
from task_pipelines.schemas import AccountPost, NewsItem
from config import Config
from langchain_openai import ChatOpenAI

conf = Config()
# 可选依赖：LLM 客户端（langchain_openai 未安装时降级为 None，不影响模拟链路）
try:
    llm = ChatOpenAI(
        model=conf.llm["model_name"],
        base_url=conf.llm["base_url"],
        api_key=conf.llm["api_key"],
        temperature=conf.temperature,
    )
    HAS_LANGCHAIN = True
except ImportError:
    llm = None
    HAS_LANGCHAIN = False

# ===== Agent Card（python_a2a 标准卡片，供 A2A 发现 / 协调器路由）=====
agent_card = AgentCard(
    name="collector",
    description="采集 Agent：按模块/关键词多源采集最新资讯；拉取指定账户新发布内容",
    url="http://localhost:8001",          # A2A 网络端点（真实部署时改实际地址）
    version="1.0.0",
    capabilities={"streaming": True, "memory": True},
    skills=[
        AgentSkill(
            id="collect_news",
            name="collect_news",
            description="按照模块、关键词、来源和时间条件采集新闻原始数据",
            tags=["news", "collection", "rss", "hotlist"],
            examples=[
                "采集科技模块最新50条新闻",
                "采集包含俄乌关键词的国际新闻",
                "采集今天发布的足球相关新闻",
            ],
            input_modes=["text/plain"],
            output_modes=["text/plain"],
        ),
        AgentSkill(
            id="fetch_account_posts",
            name="fetch_account_posts",
            description="拉取指定平台账户在给定时间之后发布的新内容",
            tags=["account", "posts", "collection"],
            examples=[
                "拉取微博账户 @新京报 最新发布的10条内容",
                "获取微信公众号人民日报在指定时间之后的新文章",
                "检查某账户从2026-08-26开始发布的新作品",
            ],
            input_modes=["text/plain"],
            output_modes=["text/plain"],
        ),
    ],
)


class CollectorAgent(A2AServer):
    """采集 Agent（原 collector + account_monitor 合并）"""

    name = "collector"
    role = "多源资讯采集 · 账户发布监控"

    def __init__(self):
        super().__init__(agent_card=agent_card)
        self.llm = llm   # 供真实实现里的 LLM 增强使用（当前模拟版用不到）

    def collect_news(self, module: str, keywords: Optional[List[str]] = None,
                     sources: Optional[List[str]] = None,
                     since: Optional[str] = None, limit: int = 50) -> List[NewsItem]:
        """多源资讯采集（RSS · Hacker News · 国内热榜）→ 经 MCP 调用 tools.collect.collect_news"""
        from mcp_servers.mcp_access import collect_news as _mcp_gateway
        from mcp_servers.mcp_access import sync_call
        logger.info(f"[collector] 开始采集「{module}」资讯（limit={limit}）")
        items = sync_call(_mcp_gateway(module, keywords, sources, since, limit))
        logger.info(f"[collector] 采集完成，共 {len(items)} 条")
        return items

    def fetch_account_posts(self, account: str, platform: str,
                            since: Optional[str] = None, limit: int = 50) -> List[AccountPost]:
        """账户发布监控：拉取指定账户新发布（新闻/作品）→ 经 MCP 调用 tools.collect.fetch_account_posts"""
        from mcp_servers.mcp_access import fetch_account_posts as _mcp_gateway
        from mcp_servers.mcp_access import sync_call
        logger.info(f"[collector] 拉取 {account}@{platform} 新发布（limit={limit}）")
        posts = sync_call(_mcp_gateway(account, platform, since, limit))
        logger.info(f"[collector] 账户发布拉取完成，共 {len(posts)} 条")
        return posts

    # ===== A2A 入口（python_a2a 协议钩子）=====
    def handle_message(self, message) -> object:
        """收到 A2A 消息：解析 task_type / params → 路由执行 → 回传

        python_a2a 的协议入口是 handle_message（A2AServer.handle_message）。
        task_type / params 放在 message.metadata.custom_fields 里（见 a2a.protocol.send_task）。
        """
        task_type, params, from_agent = parse_task(message)
        logger.info(f"[collector] 收到 A2A 消息: {task_type} from={from_agent}")
        try:
            if task_type == "collect_news":
                result = self.collect_news(params.get("module", "科技"), params.get("keywords"),
                                           params.get("sources"), params.get("since"), params.get("limit", 50))
                ok, error = True, None
            elif task_type == "fetch_account_posts":
                result = self.fetch_account_posts(params.get("account", "未知账户"),
                                                  params.get("platform", "weibo"),
                                                  params.get("since"), params.get("limit", 50))
                ok, error = True, None
            else:
                ok, result, error = False, None, f"未知任务类型: {task_type}"
        except Exception as exc:
            logger.error(f"[collector] 处理失败: {exc}")
            ok, result, error = False, None, str(exc)
        # 回传：统一 encode_result（dataclass 序列化成 dict，对端可还原成 DTO）
        text = encode_result(ok, result, error)
        return reply_text(text=text, parent_message_id=message.message_id,
                          conversation_id=message.conversation_id)


# ===== 创建采集 Agent =====
def create_collector_agent():
    """创建采集 Agent（仿照 create_order_mcp_server 的工厂模式）"""
    logger.info("=== 采集Agent信息 ===")
    logger.info(f"名称: {CollectorAgent.name}")
    logger.info(f"职责: {CollectorAgent.role}")
    return CollectorAgent()


if __name__ == "__main__":
    # import sys
    # if "--serve" in sys.argv:
    #     # 独立 A2A 服务器入口：python -m agents.collector_agent --serve（:8001）
    #     from python_a2a import run_server
    #     run_server(create_collector_agent(), host="0.0.0.0", port=8001)
    #     sys.exit(0)
    # 业务方法直接调用（链路演示）
    agent = create_collector_agent()
    run_server(agent, host="127.0.0.1", port=8001)
    # news = agent.collect_news("科技", limit=3)
    # posts = agent.fetch_account_posts("@新京报", "weibo", limit=3)
    # print(f"\n采集资讯 {len(news)} 条：")
    # for n in news:
    #     print(f"  - {n.title}")
    # print(f"账户发布 {len(posts)} 条：")
    # for p in posts:
    #     print(f"  - {p.title}")
    #
    # # A2A 入口演示：构造 Message（task_type/params 放 metadata）→ handle_message → 回传
    # print("\n=== A2A 入口演示 ===")
    # from a2a.protocol import delegate
    # text = delegate(agent, task_type="collect_news",
    #                 params={"module": "财经", "limit": 2}, from_agent="coordinator")
    # print("回传:", text)

