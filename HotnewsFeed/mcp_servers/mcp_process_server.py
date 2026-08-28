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
process_mcp = FastMCP(name="NewsProcess",
                      instructions="数据加工：事件聚类、热度排行与多源交叉核验。",
                      log_level="ERROR",
                      host="127.0.0.1", port=8005)


# ===== 挂载加工类工具（定义见 tools/registry.py）=====
register_to(process_mcp, PROCESS_TOOLS)


# ===== 创建加工 MCP 服务器 =====
def create_process_mcp_server():
    # TODO: 接入项目 logger，打印服务器信息（参照 create_order_mcp_server）
    print("=== 加工MCP服务器信息 ===")
    print(f"名称: {process_mcp.name}")
    print(f"描述: {process_mcp.instructions}")

    try:
        print("服务器已启动，请访问 http://127.0.0.1:8005/mcp")
        process_mcp.run(transport="streamable-http")
    except Exception as e:
        print(f"服务器启动失败: {e}")


if __name__ == "__main__":
    # 直接运行本文件时，创建并启动加工 MCP 服务器
    # （加了 __main__ 保护，import 本模块不会误启动服务器）
    create_process_mcp_server()
