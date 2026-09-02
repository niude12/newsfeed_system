# -*- coding: utf-8 -*-
"""工具注册中心（tools/registry.py）

该模块是 tools 包的「对外注册出口」：把各工具模块里 MCP 无关的纯函数，
包装成带元信息（name / description / handler）的 :class:`ToolDefinition`，
再按四个 MCP 服务器分组导出，供 mcp_servers/ 里的服务器文件挂载。

模块依赖:
- ``tools/collect.py``   采集类：collect_news（多源资讯采集）、fetch_account_posts（账户发布监控）。
- ``tools/process.py``   加工类：cluster_events（事件聚类）、score_heat（热度评分）、
                         verify_events（多源交叉验证）。
- ``tools/publish.py``   输出类：publish_briefing（简报生成与推送）。
- ``tools/video.py``     视频类：extract_video_content（B 站视频内容提取）。

典型调用链::

    mcp_servers/mcp_*_server.py
        ->  from tools.registry import COLLECT_TOOLS / PROCESS_TOOLS / PUBLISH_TOOLS / VIDEO_TOOLS
        ->  register_to(server, 对应分组)      # 把工具定义挂到 FastMCP 实例上
        ->  MCP 客户端按 name 调用工具，底层实际执行 handler

对外暴露的接口：
- ``ToolDefinition``  : MCP 工具定义的数据结构（dataclass）。
- ``COLLECT_TOOLS`` / ``PROCESS_TOOLS`` / ``PUBLISH_TOOLS`` / ``VIDEO_TOOLS``
                      : 四组工具定义列表，分别对应 :8004 / :8005 / :8006 / :8007 四个 MCP 服务器。
- ``register_to``     : 把一组工具定义注册到 FastMCP 服务器实例。
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List

from tools.collect import collect_news, fetch_account_posts
from tools.process import cluster_events, score_heat, verify_events
from tools.publish import publish_briefing
from tools.video import extract_video_content


@dataclass
class ToolDefinition:
    """MCP 工具定义（注册所需的元信息）。

    一个 ToolDefinition 描述「一个可被 MCP 客户端调用的工具」：
    name 是客户端调用名，description 是给 Agent 看的「何时该调用它」的说明，
    handler 是真正的底层 Python 函数（入参 / 返回值由该函数的类型注解与 docstring 决定）。

    参数:
        name:        MCP 工具名（客户端通过这个名字调用）。
        description: 工具描述（Agent 依据它判断何时调用）。
        handler:     底层工具函数（可被流水线 / 前端直接调用，保持 MCP 无关）。
    """
    name: str                        # MCP 工具名（客户端调用名）
    description: str                 # 工具描述（Agent 判断何时调用）
    handler: Callable[..., Any]      # 底层工具函数（入参/返回由函数注解决定）
    input_schema: Dict[str, Any] = field(default_factory=dict)
    output_schema: str = "Any"
    side_effect: str = "read"
    requires_confirmation: bool = False
    idempotent: bool = True
    timeout_seconds: int = 30
    max_retries: int = 0
    tags: List[str] = field(default_factory=list)


# ===== 采集类工具（mcp_collect_server :8004）=====
# 每个 ToolDefinition 的三要素：name=客户端调用名，description=给 Agent 的调用说明，
# handler=真正的实现函数。这里只是「登记」，不执行任何逻辑。
COLLECT_TOOLS: List[ToolDefinition] = [
    ToolDefinition(
        name="collect_news",
        description="按模块/关键词从多个资讯源采集最新资讯（RSS · Hacker News · 国内热榜）",
        handler=collect_news,
    ),
    ToolDefinition(
        name="fetch_account_posts",
        description="拉取指定账户的新发布内容（新闻/作品）",
        handler=fetch_account_posts,
    ),
]

# ===== 加工类工具（mcp_process_server :8005）=====
PROCESS_TOOLS: List[ToolDefinition] = [
    ToolDefinition(
        name="cluster_events",
        description="对原始资讯做去重与事件聚类，返回事件簇列表",
        handler=cluster_events,
    ),
    ToolDefinition(
        name="score_heat",
        description="计算事件簇热度并排序，返回热点事件列表（来源数×时效衰减×讨论量×权重）",
        handler=score_heat,
    ),
    ToolDefinition(
        name="verify_events",
        description="对热点事件做多源交叉验证，回填可信度结论（可信/存疑/证据不足）",
        handler=verify_events,
    ),
]

# ===== 输出类工具（mcp_publish_server :8006）=====
PUBLISH_TOOLS: List[ToolDefinition] = [
    ToolDefinition(
        name="publish_briefing",
        description="根据任务结果生成热点简报并推送到指定通道（飞书·邮件·Webhook·Web UI）",
        handler=publish_briefing,
        side_effect="external_write", requires_confirmation=True,
        idempotent=False, timeout_seconds=60, tags=["publish"],
    ),
]

# ===== 视频内容工具（mcp_video_server :8007）=====
VIDEO_TOOLS: List[ToolDefinition] = [
    ToolDefinition(
        name="extract_video_content",
        description="提取 B 站视频元数据和字幕；无字幕时提取音频转写，再生成摘要和关键词",
        handler=extract_video_content,
        side_effect="long_running", timeout_seconds=300,
        tags=["video", "subtitle", "asr"],
    ),
]


def register_to(server, tool_defs: List[ToolDefinition]) -> None:
    """把一组工具定义注册到 FastMCP 服务器对象上。

    参数:
        server:   FastMCP 实例（来自 mcp.server.fastmcp.FastMCP）。
        tool_defs: 要注册的工具定义列表（COLLECT_TOOLS / PROCESS_TOOLS / PUBLISH_TOOLS / VIDEO_TOOLS）。

    返回:
        None（原地修改 server，注册后 MCP 客户端即可发现并调用这些工具）。

    说明:
        ``server.tool(name=..., description=...)`` 是 FastMCP 的注册装饰器工厂：
        先调用它得到一个装饰器，再把 ``td.handler`` 传进去完成注册。
        工具函数本身保持 MCP 无关，因此同一函数也能被流水线 / 前端直接调用。
    """
    # 遍历工具定义列表，逐个注册到 FastMCP 服务器上。
    for td in tool_defs:
        # server.tool(...) 返回装饰器，立刻用 td.handler 调用它完成注册。
        server.tool(name=td.name, description=td.description)(td.handler)


def tool_descriptions(names: List[str]) -> Dict[str, str]:
    """从唯一工具注册中心读取给 Agent 的工具说明。"""
    wanted = set(names)
    all_tools = COLLECT_TOOLS + PROCESS_TOOLS + PUBLISH_TOOLS + VIDEO_TOOLS
    return {tool.name: tool.description for tool in all_tools if tool.name in wanted}
