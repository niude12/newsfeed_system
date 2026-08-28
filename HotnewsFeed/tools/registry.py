# -*- coding: utf-8 -*-
"""工具注册中心（tools/registry.py）

把 tools 包里的工具函数注册为「MCP 工具定义」，按三个 MCP 服务器分组：
  - COLLECT_TOOLS  → mcp_collect_server（:8004）采集类
  - PROCESS_TOOLS  → mcp_process_server（:8005）加工类
  - PUBLISH_TOOLS  → mcp_publish_server（:8006）输出类
  - VIDEO_TOOLS    → mcp_video_server（:8007）视频内容提取

MCP 服务器文件通过 register_to() 把对应分组挂到 FastMCP 上；
工具函数本身保持 mcp 无关，可被流水线 / 前端直接调用。
"""

from dataclasses import dataclass
from typing import Any, Callable, List

from tools.collect import collect_news, fetch_account_posts
from tools.process import cluster_events, score_heat, verify_events
from tools.publish import publish_briefing
from tools.video import extract_video_content


@dataclass
class ToolDefinition:
    """MCP 工具定义（注册所需的元信息）"""
    name: str                        # MCP 工具名（客户端调用名）
    description: str                 # 工具描述（Agent 判断何时调用）
    handler: Callable[..., Any]      # 底层工具函数（入参/返回由函数注解决定）


# ===== 采集类工具（mcp_collect_server :8004）=====
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
    ),
]

# ===== 视频内容工具（mcp_video_server :8007）=====
VIDEO_TOOLS: List[ToolDefinition] = [
    ToolDefinition(
        name="extract_video_content",
        description="提取 B 站视频元数据和字幕；无字幕时提取音频转写，再生成摘要和关键词",
        handler=extract_video_content,
    ),
]


def register_to(server, tool_defs: List[ToolDefinition]) -> None:
    """把一组工具定义注册到 FastMCP 服务器对象上。

    Args:
        server: FastMCP 实例（mcp.server.fastmcp.FastMCP）。
        tool_defs: 要注册的工具定义列表（COLLECT_TOOLS / PROCESS_TOOLS / PUBLISH_TOOLS）。
    """
    for td in tool_defs:
        server.tool(name=td.name, description=td.description)(td.handler)
