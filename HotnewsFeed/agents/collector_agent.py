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

模块依赖:
- ``python_a2a``             : 官方 A2A 协议包。A2AServer 是 A2A 服务器基类（协议入口
                               handle_message），AgentCard / AgentSkill 声明能力供发现与路由。
- ``a2a.protocol``           : 项目 A2A 适配层。encode_result / parse_task / reply_text
                               处理协议消息（与协调器 delegate 的 {ok,result,error} 契约对齐）。
- ``mcp_servers.mcp_access`` : Agent → MCP 访问层网关。collect_news / fetch_account_posts
                               是采集网关函数，sync_call 把协程桥进事件循环执行。
- ``langchain_openai``       : ChatOpenAI LLM 客户端（可选依赖，未安装时 HAS_LANGCHAIN=False）。
- ``Config``                 : 全局配置单例（config.ini）。conf.llm 提供 LLM 参数。
- ``task_pipelines.schemas`` : 统一 DTO 数据模型。AccountPost / NewsItem 是本 Agent 的输出类型。

典型调用链::

    协调器(coordinator)  --A2A HTTP-->  CollectorAgent.handle_message(message)
      → parse_task(message) 取出 task_type / params
      → collect_news / fetch_account_posts（Agent 业务方法）
      → mcp_servers.mcp_access.sync_call(网关函数(...))
      → 官方 mcp 客户端 streamable-http 连远端 MCP 采集服务器(:8004)
      → 远端不可达 → 降级进程内直调 tools/collect.py
      → encode_result(ok, result, error) → reply_text 回传给协调器

对外暴露的接口：
- CollectorAgent          : 采集 Agent 类（继承 A2AServer）。对外：collect_news（多源资讯采集）、
                            fetch_account_posts（账户发布监控）、handle_message（A2A 协议入口）。
- create_collector_agent  : 工厂函数，返回 CollectorAgent 实例。

用法：
    python -m agents.collector_agent    # 跑一段模拟演示（业务方法 + A2A 入口）
"""

# 类型注解：List / Optional 用于函数签名。
from typing import List, Optional

# 官方 A2A 协议包：A2AServer 服务器基类、AgentCard / AgentSkill 能力声明、run_server 启动服务器。
from python_a2a import A2AServer, AgentCard, AgentSkill, run_server

# 项目 A2A 适配层：parse_task 解析路由信息、encode_result / reply_text 构造回传消息。
from a2a.protocol import encode_result, parse_task, reply_text
# 项目统一日志器（控制台 + 文件双通道）。
from create_logger import logger
# 统一 DTO 数据模型：AccountPost / NewsItem 是本 Agent 的输出类型。
from task_pipelines.schemas import AccountPost, NewsItem
# 全局配置单例：conf.llm（LLM 参数）。
from config import Config
# LLM 客户端（可选依赖）：langchain_openai 的 ChatOpenAI，供真实实现里的 LLM 增强使用。
from langchain_openai import ChatOpenAI

# 全局配置对象（读取 config.ini）；这里主要用 conf.llm 构造 LLM 客户端。
conf = Config()
# 可选依赖：LLM 客户端（langchain_openai 未安装时降级为 None，不影响模拟链路）。
try:
    # 用 config.ini [llm] 的 base_url / api_key / model_name + 采样温度构造 ChatOpenAI。
    llm = ChatOpenAI(
        model=conf.llm["model_name"],
        base_url=conf.llm["base_url"],
        api_key=conf.llm["api_key"],
        temperature=conf.temperature,
    )
    HAS_LANGCHAIN = True
except ImportError:
    # langchain_openai 未安装：降级为 None，业务链路不依赖 LLM 也能跑。
    llm = None
    HAS_LANGCHAIN = False

# ===== Agent Card（python_a2a 标准卡片，供 A2A 发现 / 协调器路由）=====
# AgentCard 声明本 Agent 的名称 / 描述 / 网络端点 / 能力 / 技能列表；
# 两个 AgentSkill（collect_news / fetch_account_posts）对应该 Agent 对外提供的采集能力。
agent_card = AgentCard(
    name="collector",
    description="采集 Agent：按模块/关键词多源采集最新资讯；拉取指定账户新发布内容",
    url="http://localhost:8001",          # A2A 网络端点（真实部署时改实际地址）
    version="1.0.0",
    capabilities={"streaming": True, "memory": True},
    skills=[
        AgentSkill(id="execute", name="execute",
                   description="接收 objective/context，由采集 Agent 的 LLM 自主选择采集工具",
                   tags=["agent-loop", "tool-selection"],
                   examples=["自主选择新闻采集或账户发布拉取工具"],
                   input_modes=["application/json"], output_modes=["application/json"]),
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
        # 技能②：账户发布监控（task_type=fetch_account_posts）。
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
    """采集 Agent（原 collector + account_monitor 合并）。

    直接继承官方包 python_a2a 的 A2AServer，通过模块级 agent_card（含两个 skill）对外声明能力。
    核心职责：
      - collect_news：按模块/关键词多源采集原始资讯（RSS · Hacker News · 关键词搜索源）；
      - fetch_account_posts：拉取指定账户在给定时间后的新发布（新闻/作品）；
      - handle_message：A2A 协议入口，按 metadata 里的 task_type 路由执行并回传。
    业务数据一律经 mcp_servers.mcp_access 网关走 MCP 采集服务器（:8004），
    远端不可达时网关内部自动降级进程内直调 tools/collect.py。
    """

    name = "collector"
    role = "多源资讯采集 · 账户发布监控"

    def __init__(self):
        """初始化采集 Agent：把模块级 agent_card 传给 A2AServer 基类，并挂上全局 LLM。"""
        super().__init__(agent_card=agent_card)
        self.llm = llm   # 供真实实现里的 LLM 增强使用（当前模拟版用不到）

    def collect_news(self, module: str, keywords: Optional[List[str]] = None,
                     sources: Optional[List[str]] = None,
                     since: Optional[str] = None, limit: int = 50) -> List[NewsItem]:
        """多源资讯采集（RSS · Hacker News · 关键词搜索源）→ 经 MCP 调用 tools.collect.collect_news。

        经 mcp_servers.mcp_access 网关调用采集工具：优先 streamable-http 连远端
        MCP 采集服务器(:8004)，失败自动降级进程内直调 tools/collect.collect_news。
        采集工具内部会做关键词过滤、模块相关性过滤、去重与时间倒序。

        参数:
            module:   新闻模块，如 "科技" / "财经" / "体育"。
            keywords: 附加关键词过滤（None 表示不限）。
            sources:  指定采集源（如 ["rss", "hn"]；None 用默认源列表）。
            since:    只采集该时间点之后的资讯（ISO 8601）。
            limit:    单次采集上限（默认 50）。

        返回:
            List[NewsItem]：原始资讯列表（已按时间倒序）。
        """
        # 延迟 import 网关：保持模块顶层轻量，且只有真正采集时才依赖 mcp_servers。
        from mcp_servers.mcp_access import collect_news as _mcp_gateway
        from mcp_servers.mcp_access import sync_call
        # 打印采集开始日志：模块与采样上限。
        logger.info(f"[collector] 开始采集「{module}」资讯（limit={limit}）")
        # sync_call 把协程桥进事件循环执行（Agent 业务方法是同步的）。
        # 网关函数内部：优先 streamable-http 连远端 MCP 采集服务器(:8004)，失败降级直调 tools。
        items = sync_call(_mcp_gateway(module, keywords, sources, since, limit))
        # 打印采集结果条数，便于核对是否取到数据。
        logger.info(f"[collector] 采集完成，共 {len(items)} 条")
        # 返回原始资讯列表（已按时间倒序）。
        return items

    def fetch_account_posts(self, account: str, platform: str,
                            since: Optional[str] = None, limit: int = 50) -> List[AccountPost]:
        """账户发布监控：拉取指定账户新发布（新闻/作品）→ 经 MCP 调用 tools.collect.fetch_account_posts。

        经 mcp_servers.mcp_access 网关调用采集工具：优先 streamable-http 连远端
        MCP 采集服务器(:8004)，失败自动降级进程内直调 tools/collect.fetch_account_posts。
        采集工具内部按 since 过滤、按时间倒序返回。

        参数:
            account:  账户标识（如 "@新京报" 或 "bilibili_312249633"）。
            platform: 平台（weibo / wechat / xiaohongshu / bilibili）。
            since:    只返回该时间点之后的新发布（ISO 8601）。
            limit:    单次拉取上限（默认 50）。

        返回:
            List[AccountPost]：账户发布内容列表。
        """
        # 延迟 import 网关：保持模块顶层轻量。
        from mcp_servers.mcp_access import fetch_account_posts as _mcp_gateway
        from mcp_servers.mcp_access import sync_call
        # 打印拉取开始日志：账户/平台/条数上限。
        logger.info(f"[collector] 拉取 {account}@{platform} 新发布（limit={limit}）")
        # sync_call 把协程桥进事件循环执行（Agent 业务方法是同步的）。
        # 网关函数内部按 since 过滤并按时间倒序返回。
        posts = sync_call(_mcp_gateway(account, platform, since, limit))
        # 打印拉取结果条数。
        logger.info(f"[collector] 账户发布拉取完成，共 {len(posts)} 条")
        # 返回账户发布列表。
        return posts

    # ===== A2A 入口（python_a2a 协议钩子）=====
    def handle_message(self, message) -> object:
        """A2A 协议入口：收到 Message → 解析 task_type / params → 路由执行 → 回传。

        python_a2a 的 A2AServer 默认 handle_task 会桥接本方法；
        task_type / params 放在 message.metadata.custom_fields 里（见 a2a.protocol.send_task）。
        按 task_type 分发：
          - collect_news          多源资讯采集；
          - fetch_account_posts   账户发布监控；
        未知任务类型或执行异常都记 ok=False + error。最后统一 encode_result 编码成
        {ok, result, error} JSON，经 reply_text 构造 AGENT 角色回传消息。

        参数:
            message: python_a2a 的 Message 对象（含 content / metadata / message_id / conversation_id）。

        返回:
            python_a2a 的 Message 对象（回传给请求方）。
        """
        # 解析路由信息：task_type / params 在 metadata.custom_fields 里。
        task_type, params, from_agent = parse_task(message)
        # 打印收到的 A2A 任务类型与来源，便于链路追踪。
        logger.info(f"[collector] 收到 A2A 消息: {task_type} from={from_agent}")
        try:
            # 按任务类型分发到对应的业务方法；成功时 ok=True、error=None。
            if task_type == "execute":
                from agents.runtime.specialist_loop import execute_specialist
                from tools.registry import tool_descriptions
                result = execute_specialist(
                    params.get("objective", ""), params.get("context") or {},
                    {"collect_news": self.collect_news, "fetch_account_posts": self.fetch_account_posts},
                    tool_descriptions(["collect_news", "fetch_account_posts"]),
                )
                ok, error = True, None
            elif task_type == "collect_news":
                # 资讯采集：module 默认科技，limit 默认 50，关键词/来源/时间从 params 取。
                result = self.collect_news(params.get("module", "科技"), params.get("keywords"),
                                           params.get("sources"), params.get("since"), params.get("limit", 50))
                ok, error = True, None
            elif task_type == "fetch_account_posts":
                # 账户发布监控：account 默认未知账户，platform 默认 weibo。
                result = self.fetch_account_posts(params.get("account", "未知账户"),
                                                  params.get("platform", "weibo"),
                                                  params.get("since"), params.get("limit", 50))
                ok, error = True, None
            else:
                # 未知任务类型：不抛异常，以 ok=False + error 回传。
                ok, result, error = False, None, f"未知任务类型: {task_type}"
        except Exception as exc:
            # 业务执行异常：记录日志并把异常信息透出到回传 error 字段。
            logger.error(f"[collector] 处理失败: {exc}")
            ok, result, error = False, None, str(exc)
        # 回传：统一 encode_result（dataclass 序列化成 dict，对端可还原成 DTO）。
        text = encode_result(ok, result, error)
        # 用 reply_text 构造 AGENT 角色回传消息，挂上父消息与会话 ID。
        return reply_text(text=text, parent_message_id=message.message_id,
                          conversation_id=message.conversation_id)


# ===== 创建采集 Agent =====
def create_collector_agent():
    """创建采集 Agent 实例（仿照 create_order_mcp_server 的工厂模式）。

    打印 Agent 名称与职责日志后返回 CollectorAgent()（内部已挂上模块级 agent_card）。

    返回:
        CollectorAgent 实例。
    """
    # 打印 Agent 启动信息：名称与职责。
    logger.info("=== 采集Agent信息 ===")
    logger.info(f"名称: {CollectorAgent.name}")
    logger.info(f"职责: {CollectorAgent.role}")
    # 返回实例（内部已挂上模块级 agent_card）。
    return CollectorAgent()


if __name__ == "__main__":
    # import sys
    # if "--serve" in sys.argv:
    #     # 独立 A2A 服务器入口：python -m agents.collector_agent --serve（:8001）
    #     from python_a2a import run_server
    #     run_server(create_collector_agent(), host="0.0.0.0", port=8001)
    #     sys.exit(0)
    # 启动采集 Agent 的 A2A 服务器（独立部署入口，默认绑定 127.0.0.1:8001，
    # 与 AgentCard.url 及 a2a.protocol._AGENT_ENDPOINTS["collector"] 一致）。
    agent = create_collector_agent()                       # 创建采集 Agent 实例。
    run_server(agent, host="127.0.0.1", port=8001)         # 启动 A2A 服务器并阻塞监听。
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
