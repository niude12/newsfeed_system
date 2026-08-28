#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Video MCP：视频字幕、音频转写和内容摘要（:8007）。"""

from mcp.server.fastmcp import FastMCP

from tools.registry import VIDEO_TOOLS, register_to

video_mcp = FastMCP(
    name="VideoContent", instructions="视频内容提取：字幕优先，无字幕时音频转写。",
    log_level="ERROR", host="127.0.0.1", port=8007,
)
register_to(video_mcp, VIDEO_TOOLS)


def create_video_mcp_server():
    print("=== 视频内容 MCP 服务器 ===")
    print("服务器已启动，请访问 http://127.0.0.1:8007/mcp")
    video_mcp.run(transport="streamable-http")


if __name__ == "__main__":
    create_video_mcp_server()
