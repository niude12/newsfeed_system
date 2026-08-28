# -*- coding: utf-8 -*-
"""MCP 服务器包（按数据生命周期分 3 个 FastMCP 服务器，写法仿照 mcp_order_server.py）

每个服务器只负责「创建 FastMCP → 从 tools/registry.py 挂载对应工具组 → 启动监听」，
工具定义（名称/描述/实现）统一维护在 tools/registry.py 的 *_TOOLS 分组中。

mcp_collect_server.py  → 采集类：挂载 COLLECT_TOOLS（collect_news · fetch_account_posts，:8004）
mcp_process_server.py  → 加工类：挂载 PROCESS_TOOLS（cluster_events · score_heat · verify_events，:8005）
mcp_publish_server.py  → 输出类：挂载 PUBLISH_TOOLS（publish_briefing，:8006）
mcp_video_server.py    → 视频类：挂载 VIDEO_TOOLS（extract_video_content，:8007）
"""
