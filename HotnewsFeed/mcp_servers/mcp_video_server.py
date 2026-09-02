#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Video MCP：视频字幕、音频转写和内容摘要（:8007）。

一个「视频内容提取」MCP 服务器。与其它三个 MCP 服务器结构一致：
工具定义（名称/描述/实现）统一注册在 tools/registry.py（VIDEO_TOOLS），
本文件只负责：创建 FastMCP 服务器 → 挂载工具 → 启动监听。

模块依赖:
- ``FastMCP``     : mcp 官方库提供的「MCP 服务器」类（注意 mcp 必须 <2，2.x 改名
                     MCPServer 会破坏 FastMCP v1 代码）。负责创建服务器、注册工具、
                     按 transport 启动监听。这里用 streamable-http 传输。
- ``tools.registry.VIDEO_TOOLS`` : 视频内容类工具定义列表（名称/描述/实现），
                     由 register_to() 统一挂载到 FastMCP 上。
- ``tools.registry.register_to`` : 把一组 ToolDefinition 注册到 FastMCP 服务器对象的辅助函数。

典型调用链::

    mcp_access.extract_video_content()  [Agent 侧，streamable-http 客户端]
      → 连 http://127.0.0.1:8007/mcp
      → 本服务器 FastMCP 收到 call_tool → 执行 VIDEO_TOOLS 里注册的处理器
      → 序列化返回给客户端

对外暴露的接口（MCP 工具）：
- extract_video_content : 提取视频元数据和字幕；无字幕时提取音频转写，再生成摘要和关键词。

    启动：python -m mcp_servers.mcp_video_server
    启动后监听 http://127.0.0.1:8007/mcp
    依赖：pip install mcp
"""

# FastMCP：mcp 官方库提供的「MCP 服务器」类，用来创建服务器、注册工具。
from mcp.server.fastmcp import FastMCP

# 工具注册中心：视频内容类工具定义 + 注册辅助函数
from tools.registry import VIDEO_TOOLS, register_to

# 创建服务器对象 video_mcp：
#   name          服务名称（给 MCP 客户端看）
#   instructions  服务说明（告诉客户端这个服务能干什么）
#   host/port     监听地址：只在本机 8007 端口
#   log_level     只打印 ERROR 及以上日志，避免刷屏
video_mcp = FastMCP(
    name="VideoContent", instructions="视频内容提取：字幕优先，无字幕时音频转写。",  # 服务名 + 服务说明。
    log_level="ERROR", host="127.0.0.1", port=8007,  # 只打印 ERROR 及以上日志；只在本机 8007 端口监听。
)

# 挂载视频类工具（定义见 tools/registry.py）：register_to 遍历 VIDEO_TOOLS，
# 逐个调用 server.tool(...) 注册到 video_mcp 上。
register_to(video_mcp, VIDEO_TOOLS)


def create_video_mcp_server():
    """创建并启动视频内容 MCP 服务器（阻塞运行，直到 Ctrl+C）。

    打印服务器信息 → video_mcp.run(transport="streamable-http") 阻塞监听 :8007。

    返回:
        None。注意：run() 不会返回，会一直阻塞服务到被中断。

    抛出:
        Exception: 服务器启动失败时抛出（本函数未捕获，直接向上抛）。

    说明:
        FastMCP.run(transport=...) 是 mcp 官方库提供的服务器启动入口；
        transport="streamable-http" 让服务器以 HTTP 方式提供 MCP 端点（MCP 官方推荐）。
    """
    print("=== 视频内容 MCP 服务器 ===")   # 分隔线标题，标识这是视频内容服务器的信息。
    print("服务器已启动，请访问 http://127.0.0.1:8007/mcp")  # 启动成功提示。
    # 启动阻塞监听；streamable-http 是 MCP 官方推荐的 HTTP 传输方式。
    video_mcp.run(transport="streamable-http")  # 阻塞：一直服务到 Ctrl+C 才停止。


if __name__ == "__main__":
    # 直接运行本文件时，创建并启动视频内容 MCP 服务器
    # （加了 __main__ 保护，import 本模块不会误启动服务器）
    create_video_mcp_server()  # 启动视频内容 MCP 服务器（阻塞运行）。
