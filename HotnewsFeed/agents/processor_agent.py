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

模块依赖:
- ``python_a2a``             : 官方 A2A 协议包。A2AServer 是 A2A 服务器基类（协议入口
                               handle_message），AgentCard / AgentSkill 声明能力供发现与路由。
- ``a2a.protocol``           : 项目 A2A 适配层。encode_result / parse_task / reply_text
                               处理协议消息（与协调器 delegate 的 {ok,result,error} 契约对齐）。
- ``mcp_servers.mcp_access`` : Agent → MCP 访问层网关。cluster_events / score_heat / verify_events
                               是加工网关函数，sync_call 把协程桥进事件循环执行。
- ``tools.process``          : 数据加工类工具。EventCluster 是事件簇 DTO（聚类输出类型）。
- ``Config``                 : 全局配置单例（config.ini）。
- ``task_pipelines.schemas`` : 统一 DTO 数据模型。HotEvent / NewsItem 是本 Agent 的输入输出类型。

典型调用链::

    协调器(coordinator)  --A2A HTTP-->  ProcessorAgent.handle_message(message)
      → parse_task(message) 取出 task_type / params
      → cluster_events / score_heat / verify_events（Agent 业务方法）
      → mcp_servers.mcp_access.sync_call(网关函数(...))
      → 官方 mcp 客户端 streamable-http 连远端 MCP 加工服务器(:8005)
      → 远端不可达 → 降级进程内直调 tools/process.py
      → encode_result(ok, result, error) → reply_text 回传给协调器

对外暴露的接口：
- ProcessorAgent          : 加工 Agent 类（继承 A2AServer）。对外：cluster_events（事件聚类）、
                            score_heat（热度评分）、verify_events（多源交叉核验）、
                            handle_message（A2A 协议入口）。
- create_processor_agent  : 工厂函数，返回 ProcessorAgent 实例。

用法：
    python -m agents.processor_agent    # 跑一段「聚类→热度→核验」模拟演示
"""

# 类型注解：List 用于函数签名。
from typing import List

# 官方 A2A 协议包：A2AServer 服务器基类、AgentCard / AgentSkill 能力声明、run_server 启动服务器。
from python_a2a import A2AServer, AgentCard, AgentSkill, run_server

# 项目 A2A 适配层：parse_task 解析路由信息、encode_result / reply_text 构造回传消息。
from a2a.protocol import encode_result, parse_task, reply_text
# 全局配置单例：conf.llm（LLM 参数）。
from config import Config
# 项目统一日志器（控制台 + 文件双通道）。
from create_logger import logger
# 统一 DTO 数据模型：HotEvent（热点事件输出）/ NewsItem（原始资讯输入）。
from task_pipelines.schemas import HotEvent, NewsItem
# 事件簇 DTO：tools.process 里的聚类输出类型（cluster_events 的返回元素）。
from tools.process import EventCluster

# Config() 创建配置对象（模拟版主要用于触发配置读取）
conf = Config()

# ===== Agent Card（python_a2a 标准卡片，供 A2A 发现 / 协调器路由）=====
# AgentCard 声明本 Agent 的名称 / 描述 / 网络端点 / 能力 / 技能列表；
# 三个 AgentSkill（cluster_events / score_heat / verify_events）对应加工阶段的三步能力。
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
        # 技能②：热度评分（task_type=score_heat）。
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
        # 技能③：多源交叉核验（task_type=verify_events）。
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
    """加工 Agent（原 cluster + heat_ranking + verification 合并）。

    直接继承官方包 python_a2a 的 A2AServer，通过模块级 agent_card（含三个 skill）对外声明能力。
    核心职责：
      - cluster_events：把原始资讯按内容相似度聚成事件簇（规范化 → Embedding → 余弦相似度贪婪聚类）；
      - score_heat：按来源数 × 时效衰减 × 讨论量 × 权重计算事件热度并排序；
      - verify_events：多源交叉核验，回填每条事件的可信度（可信 / 存疑 / 证据不足）；
      - handle_message：A2A 协议入口，按 metadata 里的 task_type 路由执行并回传。
    业务数据一律经 mcp_servers.mcp_access 网关走 MCP 加工服务器（:8005），
    远端不可达时网关内部自动降级进程内直调 tools/process.py。
    """

    name = "processor"
    role = "事件聚类 · 热度评分 · 交叉核验"

    def __init__(self):
        """初始化加工 Agent：把模块级 agent_card 传给 A2AServer 基类。"""
        super().__init__(agent_card=agent_card)

    def cluster_events(self, news_items: List[NewsItem], threshold: float = 0.8) -> List[EventCluster]:
        """规范化 → Embedding → 相似度聚类，产出事件簇 → 经 MCP 调用 tools.process.cluster_events。

        经 mcp_servers.mcp_access 网关调用加工工具：优先 streamable-http 连远端
        MCP 加工服务器(:8005)，失败自动降级进程内直调 tools/process.cluster_events。
        聚类算法：每条资讯与已有簇质心求余弦相似度，最高相似度 ≥ threshold 就并入该簇，
        否则新开一簇（embedding 失败时降级为 TF 词频向量）。

        参数:
            news_items: 原始资讯列表（List[NewsItem]）。
            threshold:  相似度阈值（默认 0.8），高于阈值合并为一个事件簇。

        返回:
            List[EventCluster]：事件簇列表。
        """
        # 延迟 import 网关：保持模块顶层轻量。
        from mcp_servers.mcp_access import cluster_events as _mcp_gateway
        from mcp_servers.mcp_access import sync_call
        # 打印聚类开始日志：输入条数与相似度阈值。
        logger.info(f"[processor] 对 {len(news_items)} 条资讯聚类（threshold={threshold}）")
        # sync_call 把协程桥进事件循环执行（Agent 业务方法是同步的）。
        # 网关内部：规范化 → Embedding → 相似度聚类，产出事件簇。
        clusters = sync_call(_mcp_gateway(news_items, threshold))
        # 打印聚类结果簇数。
        logger.info(f"[processor] 聚类完成，共 {len(clusters)} 个事件簇")
        # 返回事件簇列表（List[EventCluster]）。
        return clusters

    def score_heat(self, clusters: List[EventCluster], time_window_hours: int = 24) -> List[HotEvent]:
        """按来源数 · 时效衰减 · 讨论量 · 权重计算热度 → 经 MCP 调用 tools.process.score_heat。

        经 mcp_servers.mcp_access 网关调用加工工具：优先 streamable-http 连远端
        MCP 加工服务器(:8005)，失败自动降级进程内直调 tools/process.score_heat。
        热度算法：基础分 + 讨论量（关联资讯数）+ 来源多样性 + 时效奖励，封顶 100；
        超过 time_window_hours 窗口的簇被过滤（不再算热点）。

        参数:
            clusters:          事件簇列表（List[EventCluster]）。
            time_window_hours: 热度回溯窗口（小时，默认 24）。

        返回:
            List[HotEvent]：带 heat_score 的热点事件列表（按热度降序）。
        """
        # 延迟 import 网关：保持模块顶层轻量。
        from mcp_servers.mcp_access import score_heat as _mcp_gateway
        from mcp_servers.mcp_access import sync_call
        # 打印热度计算开始日志：簇数与回溯窗口。
        logger.info(f"[processor] 计算 {len(clusters)} 个事件簇热度（窗口 {time_window_hours}h）")
        # sync_call 把协程桥进事件循环执行（Agent 业务方法是同步的）。
        # 网关内部按来源数 × 时效衰减 × 讨论量计算热度并排序。
        events = sync_call(_mcp_gateway(clusters, time_window_hours))
        # 打印最高热度分（events 为空时打印 0）。
        logger.info(f"[processor] 热度计算完成，最高 {events[0].heat_score if events else 0}")
        # 返回按热度降序的热点事件列表（List[HotEvent]）。
        return events

    def verify_events(self, events: List[HotEvent]) -> List[HotEvent]:
        """多源交叉核验，回填 可信 / 存疑 / 证据不足 → 经 MCP 调用 tools.process.verify_events。

        经 mcp_servers.mcp_access 网关调用加工工具：优先 streamable-http 连远端
        MCP 加工服务器(:8005)，失败自动降级进程内直调 tools/process.verify_events。
        核验算法：LLM 按 verify_prompt 判断每条事件可信度，LLM 失败时降级为
        启发式规则（来源数 / 关联资讯数阈值）。

        参数:
            events: 待核验的热点事件列表（List[HotEvent]）。

        返回:
            List[HotEvent]：回填了 credibility 字段（可信 / 存疑 / 证据不足）的事件列表。
        """
        # 延迟 import 网关：保持模块顶层轻量。
        from mcp_servers.mcp_access import verify_events as _mcp_gateway
        from mcp_servers.mcp_access import sync_call
        # 打印核验开始日志：待核验事件数。
        logger.info(f"[processor] 多源交叉核验 {len(events)} 个热点事件")
        # sync_call 把协程桥进事件循环执行（Agent 业务方法是同步的）。
        # 网关内部用 LLM / 启发式规则回填每条事件的可信度。
        events = sync_call(_mcp_gateway(events))
        # 打印核验完成日志。
        logger.info(f"[processor] 核验完成")
        # 返回回填了 credibility 字段的事件列表。
        return events

    # ===== A2A 入口（python_a2a 协议钩子）=====
    def handle_message(self, message) -> object:
        """A2A 协议入口：收到 Message → 解析 task_type / params → 路由执行 → 回传。

        python_a2a 的 A2AServer 默认 handle_task 会桥接本方法；
        task_type / params 放在 message.metadata.custom_fields 里（见 a2a.protocol.send_task）。
        按 task_type 分发：
          - cluster_events   事件聚类；
          - score_heat       热度评分；
          - verify_events    多源交叉核验；
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
        logger.info(f"[processor] 收到 A2A 消息: {task_type} from={from_agent}")
        try:
            # 按任务类型分发到对应的业务方法；成功时 ok=True、error=None。
            if task_type == "cluster_events":
                # 事件聚类：news_items 默认空列表，threshold 默认 0.8。
                result = self.cluster_events(params.get("news_items", []), params.get("threshold", 0.8))
                ok, error = True, None
            elif task_type == "score_heat":
                # 热度评分：clusters 默认空列表，time_window_hours 默认 24。
                result = self.score_heat(params.get("clusters", []), params.get("time_window_hours", 24))
                ok, error = True, None
            elif task_type == "verify_events":
                # 多源交叉核验：events 默认空列表。
                result = self.verify_events(params.get("events", []))
                ok, error = True, None
            else:
                # 未知任务类型：不抛异常，以 ok=False + error 回传。
                ok, result, error = False, None, f"未知任务类型: {task_type}"
        except Exception as exc:
            # 业务执行异常：记录日志并把异常信息透出到回传 error 字段。
            logger.error(f"[processor] 处理失败: {exc}")
            ok, result, error = False, None, str(exc)
        # 回传：统一 encode_result（dataclass 序列化成 dict，对端可还原成 DTO）。
        text = encode_result(ok, result, error)
        # 用 reply_text 构造 AGENT 角色回传消息，挂上父消息与会话 ID。
        return reply_text(text=text, parent_message_id=message.message_id,
                          conversation_id=message.conversation_id)


# ===== 创建加工 Agent =====
def create_processor_agent():
    """创建加工 Agent 实例（仿照 create_order_mcp_server 的工厂模式）。

    打印 Agent 名称与职责日志后返回 ProcessorAgent()（内部已挂上模块级 agent_card）。

    返回:
        ProcessorAgent 实例。
    """
    # 打印 Agent 启动信息：名称与职责。
    logger.info("=== 加工Agent信息 ===")
    logger.info(f"名称: {ProcessorAgent.name}")
    logger.info(f"职责: {ProcessorAgent.role}")
    # 返回实例（内部已挂上模块级 agent_card）。
    return ProcessorAgent()


if __name__ == "__main__":
    # import sys
    # if "--serve" in sys.argv:
    #     # 独立 A2A 服务器入口：python -m agents.processor_agent --serve（:8002）
    #     from python_a2a import run_server
    # 启动加工 Agent 的 A2A 服务器（默认绑定 127.0.0.1:8002，与 AgentCard.url 及
    # a2a.protocol._AGENT_ENDPOINTS["processor"] 一致）。
    run_server(create_processor_agent(), host="127.0.0.1", port=8002)   # 启动加工 Agent 的 A2A 服务器并阻塞监听。
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
