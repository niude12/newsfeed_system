#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名: mcp_collect_server.py
项目: HotnewsFeed
创建日期: 2026/8/26

本文件干什么：
    一个「资讯采集」MCP 服务器（写法仿照 mcp_order_server.py）。
    工具定义（名称/描述/实现）统一注册在 tools/registry.py（COLLECT_TOOLS），
    本文件只负责：创建 FastMCP 服务器 → 挂载工具 → 启动监听。
    采集能力：collect_news（多源资讯采集）/ fetch_account_posts（账户发布监控）。

    启动：python -m mcp_servers.mcp_collect_server
    启动后监听 http://127.0.0.1:8004/mcp
    依赖：pip install mcp
"""
# ======================= 引入需要用到的模块 =======================
# FastMCP：mcp 官方库提供的「MCP 服务器」类，用来创建服务器、注册工具。
from mcp.server.fastmcp import FastMCP

# 工具注册中心：采集类工具定义 + 注册辅助函数
from tools.registry import COLLECT_TOOLS, register_to

# TODO: 接入项目统一 logger 与 Config（参照 mcp_order_server.py）


# ===== 创建 MCP 服务器对象 =====
# FastMCP(...) 创建服务器对象 collect_mcp：
#   name          服务名称（给 MCP 客户端看）
#   instructions  服务说明（告诉客户端这个服务能干什么）
#   host/port     监听地址：只在本机 8004 端口
#   log_level     只打印 ERROR 及以上日志，避免刷屏
collect_mcp = FastMCP(name="NewsCollect",
                      instructions="资讯采集：从多个资讯源/账户拉取最新内容。",
                      log_level="ERROR",
                      host="127.0.0.1", port=8004)


# ===== 挂载采集类工具（定义见 tools/registry.py）=====
register_to(collect_mcp, COLLECT_TOOLS)


# ===== 创建采集 MCP 服务器 =====
def create_collect_mcp_server():
    # TODO: 接入项目 logger，打印服务器信息（参照 create_order_mcp_server）
    print("=== 采集MCP服务器信息 ===")
    print(f"名称: {collect_mcp.name}")
    print(f"描述: {collect_mcp.instructions}")

    # ===== 运行服务器 =====
    # collect_mcp.run(transport=...)：让服务器开始监听 8004 端口，等待客户端调用。
    # 这个方法会「阻塞」住整个程序，一直服务到 Ctrl+C 才停止。
    # transport 指定传输方式，streamable-http 是 MCP 官方推荐的 HTTP 传输方式。
    try:
        print("服务器已启动，请访问 http://127.0.0.1:8004/mcp")
        collect_mcp.run(transport="streamable-http")
    except Exception as e:
        print(f"服务器启动失败: {e}")


if __name__ == "__main__":
    # 直接运行本文件时，创建并启动采集 MCP 服务器
    # （加了 __main__ 保护，import 本模块不会误启动服务器）
    create_collect_mcp_server()
