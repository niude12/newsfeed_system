#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名: mcp_process_server.py
项目: HotnewsFeed
创建日期: 2026/8/26

本文件干什么：
    一个「数据加工」MCP 服务器（写法仿照 mcp_order_server.py）。
    工具定义（名称/描述/实现）统一注册在 tools/registry.py（PROCESS_TOOLS），
    本文件只负责：创建 FastMCP 服务器 → 挂载工具 → 启动监听。
    加工能力：cluster_events（事件聚类）/ score_heat（热度排行）/ verify_events（交叉核验）。

模块依赖:
- ``FastMCP``     : mcp 官方库提供的「MCP 服务器」类（注意 mcp 必须 <2，2.x 改名
                     MCPServer 会破坏 FastMCP v1 代码）。负责创建服务器、注册工具、
                     按 transport 启动监听。这里用 streamable-http 传输。
- ``tools.registry.PROCESS_TOOLS`` : 加工类工具定义列表（名称/描述/实现），
                     由 register_to() 统一挂载到 FastMCP 上。
- ``tools.registry.register_to``   : 把一组 ToolDefinition 注册到 FastMCP 服务器对象的辅助函数。

典型调用链::

    mcp_access.cluster_events() / score_heat() / verify_events()  [Agent 侧客户端]
      → 连 http://127.0.0.1:8005/mcp
      → 本服务器 FastMCP 收到 call_tool → 执行 PROCESS_TOOLS 里注册的处理器
      → 序列化返回给客户端

对外暴露的接口（MCP 工具）：
- cluster_events : 对原始资讯做去重与事件聚类，返回事件簇列表。
- score_heat     : 计算事件簇热度并排序，返回热点事件列表。
- verify_events  : 对热点事件做多源交叉验证，回填可信度结论。

    启动：python -m mcp_servers.mcp_process_server
    启动后监听 http://127.0.0.1:8005/mcp
    依赖：pip install mcp
"""
# ======================= 引入需要用到的模块 =======================
# FastMCP：mcp 官方库提供的「MCP 服务器」类，用来创建服务器、注册工具。
from mcp.server.fastmcp import FastMCP

# 工具注册中心：加工类工具定义 + 注册辅助函数
from tools.registry import PROCESS_TOOLS, register_to

# TODO: 接入项目统一 logger 与 Config（参照 mcp_order_server.py）


# ===== 创建 MCP 服务器对象 =====
# FastMCP(...) 创建服务器对象 process_mcp：
#   name          服务名称（给 MCP 客户端看）
#   instructions  服务说明（告诉客户端这个服务能干什么）
#   host/port     监听地址：只在本机 8005 端口
#   log_level     只打印 ERROR 及以上日志，避免刷屏
process_mcp = FastMCP(name="NewsProcess",                 # 服务名：给 MCP 客户端 / Agent 识别。
                      instructions="数据加工：事件聚类、热度排行与多源交叉核验。",  # 服务说明（告诉客户端能干什么）。
                      log_level="ERROR",                  # 只打印 ERROR 及以上日志，避免刷屏。
                      host="127.0.0.1", port=8005)        # 只在本机 8005 端口监听。


# ===== 挂载加工类工具（定义见 tools/registry.py）=====
# register_to 遍历 PROCESS_TOOLS，逐个调用 server.tool(...) 注册到 process_mcp 上。
register_to(process_mcp, PROCESS_TOOLS)


# ===== 创建加工 MCP 服务器 =====
def create_process_mcp_server():
    """创建并启动加工 MCP 服务器（阻塞运行，直到 Ctrl+C）。

    打印服务器信息 → process_mcp.run(transport="streamable-http") 阻塞监听 :8005。

    返回:
        None。注意：run() 不会返回，会一直阻塞服务到被中断。

    抛出:
        Exception: 服务器启动失败时抛出，被本函数内部捕获并打印。

    说明:
        FastMCP.run(transport=...) 是 mcp 官方库提供的服务器启动入口；
        transport="streamable-http" 让服务器以 HTTP 方式提供 MCP 端点（MCP 官方推荐）。
    """
    # TODO: 接入项目 logger，打印服务器信息（参照 create_order_mcp_server）
    print("=== 加工MCP服务器信息 ===")    # 分隔线标题，标识这是加工服务器的信息。
    print(f"名称: {process_mcp.name}")    # 打印服务名（给 MCP 客户端看）。
    print(f"描述: {process_mcp.instructions}")  # 打印服务说明（这个服务能干什么）。

    # 启动阻塞监听；streamable-http 是 MCP 官方推荐的 HTTP 传输方式。
    try:
        print("服务器已启动，请访问 http://127.0.0.1:8005/mcp")  # 启动成功提示。
        process_mcp.run(transport="streamable-http")  # 阻塞监听：一直服务到 Ctrl+C 才停止。
    except Exception as e:  # 启动 / 运行期间异常统一捕获。
        print(f"服务器启动失败: {e}")  # 打印失败原因，便于排查。


if __name__ == "__main__":
    # 直接运行本文件时，创建并启动加工 MCP 服务器
    # （加了 __main__ 保护，import 本模块不会误启动服务器）
    create_process_mcp_server()  # 启动加工 MCP 服务器（阻塞运行）。
