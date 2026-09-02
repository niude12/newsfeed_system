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

模块依赖:
- ``python_a2a``             : 官方 A2A 协议包。A2AServer 是 A2A 服务器基类（协议入口
                               handle_message），AgentCard / AgentSkill 声明能力供发现与路由。
- ``a2a.protocol``           : 项目 A2A 适配层。encode_result / parse_task / reply_text
                               处理协议消息（与协调器 delegate 的 {ok,result,error} 契约对齐）。
- ``mcp_servers.mcp_access`` : Agent → MCP 访问层网关。publish_briefing 是输出网关函数，
                               sync_call 把协程桥进事件循环执行。
- ``tools.publish``          : 数据输出类工具。PublishResult 是简报推送结果 DTO。
- ``Config``                 : 全局配置单例（config.ini）。
- ``task_pipelines.schemas`` : 统一 DTO 数据模型。PipelineResult / HotEvent / NewsItem / AccountPost /
                               dto_from_dict（把 A2A 回传的 JSON 还原成 DTO）。

典型调用链::

    协调器(coordinator)  --A2A HTTP-->  PublisherAgent.handle_message(message)
      → parse_task(message) 取出 task_type / params
      → optimize_output / publish_briefing（Agent 业务方法）
      → _restore_pipeline_result(params["result"])   # 把 A2A 传来的 dict 还原成 PipelineResult
      → mcp_servers.mcp_access.sync_call(publish_briefing(...))
      → 官方 mcp 客户端 streamable-http 连远端 MCP 输出服务器(:8006)
      → 远端不可达 → 降级进程内直调 tools/publish.py
      → encode_result(ok, result, error) → reply_text 回传给协调器

对外暴露的接口：
- PublisherAgent          : 输出 Agent 类（继承 A2AServer）。对外：optimize_output（结果优化呈现）、
                            publish_briefing（简报生成与推送）、handle_message（A2A 协议入口）。
- _restore_pipeline_result: 模块级辅助函数，把 A2A/HTTP 传来的字典还原成 PipelineResult。
- create_publisher_agent  : 工厂函数，返回 PublisherAgent 实例。

用法：
    python -m agents.publisher_agent    # 跑一段「优化→生成简报→推送」模拟演示
"""

# 类型注解：List / Optional 用于函数签名。
from typing import List, Optional

# 官方 A2A 协议包：A2AServer 服务器基类、AgentCard / AgentSkill 能力声明、run_server 启动服务器。
from python_a2a import A2AServer, AgentCard, AgentSkill, run_server

# 项目 A2A 适配层：parse_task 解析路由信息、encode_result / reply_text 构造回传消息。
from a2a.protocol import encode_result, parse_task, reply_text
# 全局配置单例（config.ini）。
from config import Config
# 项目统一日志器（控制台 + 文件双通道）。
from create_logger import logger
# 统一 DTO 数据模型：PipelineResult（上游任务结果）/ 各类条目 DTO / dto_from_dict（JSON→DTO）。
from task_pipelines.schemas import (AccountPost, HotEvent, NewsItem,
                                    PipelineResult, dto_from_dict)
# 简报推送结果 DTO：tools.publish 里的 PublishResult（briefing_id + 各通道推送状态）。
from tools.publish import PublishResult

# Config() 创建配置对象（模拟版主要用于触发配置读取）
conf = Config()


def _restore_pipeline_result(value) -> PipelineResult:
    """把 A2A/HTTP 传来的字典还原成带具体 item DTO 的 PipelineResult。

    A2A 交接（coordinator → publisher）时，PipelineResult 会被 encode_result 序列化成
    dict（dataclass → asdict），本函数负责把它还原成真正的 PipelineResult 对象，
    并按 task_type 把 items 里的 dict 逐条还原成对应的 DTO（HotEvent / NewsItem / AccountPost）。
    - 传入的本来就是 PipelineResult 实例（同进程演示用）→ 原样返回；
    - 传入非 dict 非 PipelineResult → 抛 TypeError；
    - task_type 不在映射表里 → items 保持原样（dict 列表），不做 DTO 还原。

    参数:
        value: A2A/HTTP 传来的结果，PipelineResult 实例或 dict。

    返回:
        PipelineResult：还原后的结果对象。

    抛出:
        TypeError: value 既不是 PipelineResult 也不是 dict 时抛出。
    """
    # 同进程演示：本来就是 PipelineResult 实例，直接返回。
    if isinstance(value, PipelineResult):
        return value
    # A2A/HTTP 传过来应是 JSON dict；否则入参不合法。
    if not isinstance(value, dict):
        raise TypeError("简报任务 result 必须是 PipelineResult 或 JSON 对象")
    # 取出任务类型，并据此决定 items 里每个 dict 要还原成哪种 DTO。
    task_type = str(value.get("task_type") or "")
    # 任务类型 → DTO 类型 的映射表（决定 items 里每个 dict 还原成什么对象）。
    item_type = {
        "hotspot_query": HotEvent,     # 热点查询 → HotEvent
        "latest_news": NewsItem,       # 最新新闻 → NewsItem
        "account_follow": AccountPost, # 账户发布 → AccountPost
    }.get(task_type)
    # 原始 items 列表（缺省为空 list）。
    raw_items = value.get("items") or []
    # 能映射到 DTO 类型时逐条还原；否则（未知 task_type）保留原始 dict 列表。
    items = (
        # 逐条用 dto_from_dict 把 dict 还原成指定 DTO 对象。
        [dto_from_dict(item_type, item) for item in raw_items]
        if item_type else raw_items
    )
    # 按 PipelineResult 字段重组（缺省键用默认值补齐）。
    return PipelineResult(
        task_type=task_type,                       # 任务类型原样透传。
        items=items,                               # 还原后的 items（DTO 列表或原样 dict）。
        queried_at=str(value.get("queried_at") or ""),  # 查询时刻（缺省空串）。
        elapsed_ms=int(value.get("elapsed_ms") or 0),   # 耗时毫秒（缺省 0）。
        error=value.get("error"),                  # 错误信息（可能为 None）。
    )

# ===== Agent Card（python_a2a 标准卡片，供 A2A 发现 / 协调器路由）=====
# AgentCard 声明本 Agent 的名称 / 描述 / 网络端点 / 能力 / 技能列表；
# 两个 AgentSkill（optimize_output / publish_briefing）对应该 Agent 对外提供的输出能力。
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
        # 技能②：简报生成与推送（task_type=briefing）。
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
    """输出 Agent（原 briefing + output_optimizer 合并）。

    直接继承官方包 python_a2a 的 A2AServer，通过模块级 agent_card（含两个 skill）对外声明能力。
    核心职责：
      - optimize_output：对上游任务结果做去重 · 排序 · 引用 · 个性化呈现（共享出口）；
      - publish_briefing：根据任务结果生成热点简报并推送到指定通道（飞书/邮件/Webhook/Web UI）；
      - handle_message：A2A 协议入口，按 metadata 里的 task_type 路由执行并回传。
    入参（PipelineResult）经 _restore_pipeline_result 统一还原，兼容同进程对象与 A2A/HTTP dict；
    业务数据经 mcp_servers.mcp_access 网关走 MCP 输出服务器（:8006），
    远端不可达时网关内部自动降级进程内直调 tools/publish.py。
    """

    name = "publisher"
    role = "结果优化呈现 · 简报生成与推送"

    def __init__(self):
        """初始化输出 Agent：把模块级 agent_card 传给 A2AServer 基类。"""
        super().__init__(agent_card=agent_card)

    def optimize_output(self, result: PipelineResult) -> PipelineResult:
        """去重 · 排序 · 引用 · 个性化呈现，向用户展示最终结果。

        先把入参统一还原成 PipelineResult（兼容同进程对象与 A2A/HTTP dict），
        再做输出优化。【模拟】真实实现做去重/排序/引用增强，当前版本原样返回。

        参数:
            result: 上游任务结果（PipelineResult 实例或 JSON dict）。

        返回:
            PipelineResult：优化后的结果（当前模拟版为原样返回）。
        """
        # 统一还原入参：兼容 A2A/HTTP dict 与同进程 PipelineResult 对象。
        result = _restore_pipeline_result(result)
        # 打印优化信息：任务类型与条目数。
        logger.info(f"[publisher] 优化输出: task_type={result.task_type} items={len(result.items)}")
        # 【模拟】真实实现做去重/排序/引用增强；当前原样返回。
        return result

    def publish_briefing(self, result: PipelineResult,
                         channels: Optional[List[str]] = None,
                         template: str = "default") -> PublishResult:
        """生成简报（摘要 · 重要性 · 来源 · 可信度）并推送 → 经 MCP 调用 tools.publish.publish_briefing。

        先把入参统一还原成 PipelineResult，再经 mcp_servers.mcp_access 网关调用输出工具：
        优先 streamable-http 连远端 MCP 输出服务器(:8006)，失败自动降级进程内直调
        tools/publish.publish_briefing。推送工具内部用 LLM 生成简报正文（失败降级手写
        Markdown），并逐通道推送（飞书 webhook / 通用 webhook / 邮件 SMTP / Web UI 落盘）。

        参数:
            result:   上游任务结果（PipelineResult 实例或 JSON dict）。
            channels: 推送通道列表，None 时默认 ["feishu", "web_ui"]。
            template: 简报模板名，默认 "default"（其它模板名暂按 default 处理）。

        返回:
            PublishResult：简报推送结果（briefing_id + 各通道推送状态 channels）。
        """
        # 延迟 import 网关：保持模块顶层轻量。
        from mcp_servers.mcp_access import publish_briefing as _mcp_gateway
        from mcp_servers.mcp_access import sync_call
        # 统一还原入参：兼容 A2A/HTTP 传来的 dict 与同进程的 PipelineResult 对象。
        result = _restore_pipeline_result(result)
        # 默认推送通道：飞书 + Web UI（未显式指定时）。
        channels = channels or ["feishu", "web_ui"]
        # 打印生成简报的模板与推送通道，便于核对输出目标。
        logger.info(f"[publisher] 生成简报 template={template}，推送通道={channels}")
        # sync_call 把协程桥进事件循环执行（Agent 业务方法是同步的）。
        # 网关内部用 LLM 生成简报正文并逐通道推送（失败降级手写 Markdown）。
        pub = sync_call(_mcp_gateway(result, channels, template))
        # 打印简报 ID 与各通道推送状态。
        logger.info(f"[publisher] 简报 {pub.briefing_id} 推送状态: {pub.channels}")
        # 返回简报推送结果（briefing_id + 各通道状态）。
        return pub

    # ===== A2A 入口（python_a2a 协议钩子）=====
    def handle_message(self, message) -> object:
        """A2A 协议入口：收到 Message → 解析 task_type / params → 路由执行 → 回传。

        python_a2a 的 A2AServer 默认 handle_task 会桥接本方法；
        task_type / params 放在 message.metadata.custom_fields 里（见 a2a.protocol.send_task）。
        按 task_type 分发：
          - optimize   结果优化呈现（optimize_output）；
          - briefing   简报生成与推送（publish_briefing，协调器 handoff_briefing 即调用此任务）；
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
        logger.info(f"[publisher] 收到 A2A 消息: {task_type} from={from_agent}")
        try:
            # 按任务类型分发到对应的业务方法；成功时 ok=True、error=None。
            if task_type == "optimize":
                # 结果优化呈现：把上游 PipelineResult 还原后做去重/排序/引用。
                result = self.optimize_output(params.get("result"))
                ok, error = True, None
            elif task_type == "briefing":
                # 简报生成与推送：result 是 PipelineResult，channels/template 可选。
                result = self.publish_briefing(params.get("result"), params.get("channels"),
                                               params.get("template", "default"))
                ok, error = True, None
            else:
                # 未知任务类型：不抛异常，以 ok=False + error 回传。
                ok, result, error = False, None, f"未知任务类型: {task_type}"
        except Exception as exc:
            # 业务执行异常：记录日志并把异常信息透出到回传 error 字段。
            logger.error(f"[publisher] 处理失败: {exc}")
            ok, result, error = False, None, str(exc)
        # 回传：统一 encode_result（dataclass 序列化成 dict，对端可还原成 DTO）。
        text = encode_result(ok, result, error)
        # 用 reply_text 构造 AGENT 角色回传消息，挂上父消息与会话 ID。
        return reply_text(text=text, parent_message_id=message.message_id,
                          conversation_id=message.conversation_id)


# ===== 创建输出 Agent =====
def create_publisher_agent():
    """创建输出 Agent 实例（仿照 create_order_mcp_server 的工厂模式）。

    打印 Agent 名称与职责日志后返回 PublisherAgent()（内部已挂上模块级 agent_card）。

    返回:
        PublisherAgent 实例。
    """
    # 打印 Agent 启动信息：名称与职责。
    logger.info("=== 输出Agent信息 ===")
    logger.info(f"名称: {PublisherAgent.name}")
    logger.info(f"职责: {PublisherAgent.role}")
    # 返回实例（内部已挂上模块级 agent_card）。
    return PublisherAgent()


if __name__ == "__main__":
    # 启动输出 Agent 的 A2A 服务器（默认绑定 127.0.0.1:8003，与 AgentCard.url 及
    # a2a.protocol._AGENT_ENDPOINTS["publisher"] 一致；协调器 handoff_briefing 即委派到此）。
    run_server(create_publisher_agent(), host="127.0.0.1", port=8003)   # 启动输出 Agent 的 A2A 服务器并阻塞监听。
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
