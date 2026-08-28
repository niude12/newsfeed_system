# -*- coding: utf-8 -*-
"""Agent → MCP 访问层（mcp_servers/mcp_access.py）

把 tools/* 的工具函数包装成「先经 MCP 协议连远端 MCP 服务器、失败降级进程内直调」的网关，
供 agents / task_pipelines 统一调用 —— 这是 agent「连接 MCP 协议拿到工具」的真实落地。

链路：
    Agent 业务方法（同步）
      → sync_call() 桥进事件循环
      → 网关函数（collect_news / cluster_events / ...，async）
          → 优先：mcp_call_tool() —— 用官方 mcp 客户端的 streamable-http 传输，
            连接远端 FastMCP 服务器（:8004/:8005/:8006）→ initialize 握手 →
            call_tool 调用注册工具 → 把返回的 JSON 还原成 DTO 对象
          → 降级：远端不可达时（服务器没起 / 网络被拦），直接进程内调用 tools.*

三个 MCP 服务器端点与 mcp_servers/*.py 的 host/port 一一对应。
"""

import asyncio
import json
import threading
from dataclasses import asdict
from typing import Any, Dict, List, Optional

# 官方 MCP 客户端：streamable-http 传输 + 会话（真正的 HTTP 客户端连接）
import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from create_logger import logger
from task_pipelines.schemas import (AccountPost, HotEvent, NewsItem,
                                    PipelineResult, dto_from_dict)


def _no_proxy_http_client(headers=None, timeout=None, auth=None) -> httpx.AsyncClient:
    """MCP 客户端专用的 httpx 工厂：显式 trust_env=False，绝不读取系统代理。

    背景（Windows 常见坑）：httpx 默认 trust_env=True 时会通过 urllib 读取
    Windows 注册表的 IE/WinINet 系统代理设置（企业 / VPN 代理）。系统代理会把
    127.0.0.1 也转发出去，代理对 localhost 返回 502，导致本地 MCP 服务器连不上
    （现象：curl 直连 200、mcp 客户端 502）。这里关闭 trust_env，保证客户端
    永远直连本机 MCP 服务器。
    """
    return httpx.AsyncClient(
        follow_redirects=True,
        timeout=timeout or httpx.Timeout(30.0),
        headers=headers,
        auth=auth,
        trust_env=False,
        http1=True,
    )

# ===== MCP 服务器的 streamable-http 端点 =====
MCP_URLS: Dict[str, str] = {
    "collect": "http://127.0.0.1:8004/mcp",   # mcp_collect_server.py
    "process": "http://127.0.0.1:8005/mcp",   # mcp_process_server.py
    "publish": "http://127.0.0.1:8006/mcp",   # mcp_publish_server.py
    "video": "http://127.0.0.1:8007/mcp",     # mcp_video_server.py
}


# ===== 同步 ↔ 异步桥 =====
def sync_call(coro) -> Any:
    """在同步上下文里执行协程（Agent 业务方法 / A2A handle_message 都是同步的）

    - 没有运行中的事件循环 → asyncio.run 直接跑；
    - 已处于事件循环（如 FastMCP / uvicorn 线程里）→ 起子线程跑，
      避免 "Cannot run the event loop while another loop is running"。
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    box: Dict[str, Any] = {}

    def _runner():
        box["result"] = asyncio.run(coro)

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    t.join()
    return box["result"]


# ===== MCP 客户端（真正的 HTTP 协议调用）=====
async def mcp_call_tool(server: str, tool_name: str, arguments: Dict[str, Any],
                        timeout: float = 30.0) -> Any:
    """经 MCP 协议调用远端服务器上注册的工具，返回解析后的 JSON（dict / list）。

    真实 HTTP 客户端链路：streamablehttp_client 连到 {host}:{port}/mcp →
    ClientSession.initialize() 与服务器握手 → call_tool 执行远端工具 →
    FastMCP 把 DTO 返回序列化成 JSON 文本 → json.loads 还原成 dict/list。
    """
    url = MCP_URLS[server]
    logger.info(f"[mcp] -> {server}({url}) 调用工具 {tool_name}")
    async with streamablehttp_client(url, timeout=timeout,
                                     httpx_client_factory=_no_proxy_http_client) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, arguments)
            if result.isError:
                raise RuntimeError(f"MCP 工具 {tool_name} 返回错误: {result.content}")
            texts = [c.text for c in result.content if hasattr(c, "text")]
            return _parse_payload(texts)


async def mcp_list_tools(server: str, timeout: float = 30.0) -> List[str]:
    """连远端服务器列工具名（验证注册 / 调试用）"""
    url = MCP_URLS[server]
    async with streamablehttp_client(url, timeout=timeout,
                                     httpx_client_factory=_no_proxy_http_client) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            return [t.name for t in tools.tools]


async def mcp_agent_call(query: str, server: str = "collect",
                         system: Optional[str] = None, verbose: bool = False) -> str:
    """Agent 作为 MCP 客户端，由 LangChain「工具调用 Agent」驱动 MCP 工具（用户给订票模板的同构实现）。

    链路（照模板）：
        streamablehttp_client(远端 MCP 服务器) → ClientSession → initialize 握手
          → load_mcp_tools(session) 把服务器上注册的工具读成 LangChain 工具对象
          → ChatPromptTemplate(system/human/agent_scratchpad)
          → create_tool_calling_agent(llm, tools, prompt) → AgentExecutor → ainvoke(query)
    由 LLM 自己决定调哪个工具、填什么参数，最终返回其回答文本。

    Args:
        query: 用户自然语言请求（如 "采集科技模块的热点资讯"）。
        server: 连哪台 MCP 服务器：collect / process / publish。
        system: 覆盖默认系统提示词（教 LLM 怎么用这批工具）。
        verbose: 是否打印 Agent 每步调用过程（排障用）。

    Returns:
        str: LLM 最终回答文本。
    """
    # 延迟导入：保持 mcp_access 顶层轻量（pipelines 会 import 它），仅此函数依赖 langchain
    from langchain.agents import AgentExecutor, create_tool_calling_agent
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_mcp_adapters.tools import load_mcp_tools

    from config import Config
    from langchain_openai import ChatOpenAI

    conf = Config()
    llm = ChatOpenAI(
        model=conf.llm["model_name"], base_url=conf.llm["base_url"],
        api_key=conf.llm["api_key"], temperature=conf.temperature,
    )
    url = MCP_URLS[server]
    logger.info(f"[mcp] agent_call -> {server}({url}) query={query}")
    async with streamablehttp_client(url, timeout=30,
                                     httpx_client_factory=_no_proxy_http_client) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            # 从 MCP 会话里把注册的工具翻译成 LangChain 工具对象（模板核心一步）
            tools = await load_mcp_tools(session)
            prompt = ChatPromptTemplate.from_messages([
                ("system", system or (
                    "你是一个热点资讯助手，通过调用工具获取最新资讯。"
                    "仔细分析工具需要的参数，从用户信息中提取；"
                    "信息不足则追问，不要编造参数。")),
                ("human", "{input}"),
                ("placeholder", "{agent_scratchpad}"),
            ])
            agent = create_tool_calling_agent(llm, tools, prompt)
            agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=verbose)
            response = await agent_executor.ainvoke({"input": query})
            return response["output"]


def _parse_payload(texts: List[str]) -> Any:
    """解析 FastMCP 回传的文本块，兼容三种形态：

    - 单块且是完整 JSON（对象 / 数组）→ 整体解析返回；
    - 多块 = FastMCP 把 List[DTO] 逐条序列化成多条独立 JSON → 逐块解析合并成 list；
    - 非 JSON 文本 → 原样保留。
    """
    if not texts:
        return {}
    joined = "\n".join(texts).strip()
    try:
        return json.loads(joined)
    except json.JSONDecodeError:
        pass
    parsed: list = []
    for t in texts:
        s = t.strip()
        if not s:
            continue
        try:
            parsed.append(json.loads(s))
        except json.JSONDecodeError:
            parsed.append(s)
    return parsed


def _rows(raw: Any) -> List[dict]:
    """取出行列表（自动展平嵌套），兼容三种形态：
    1. 列表        [row, row, ...]  → 多条
    2. 单条裸行     {row}           → FastMCP 把 List[DTO] 长度为 1 时序列化成单个 dict
    3. 包装形态     {'result': [...]}
    """
    if isinstance(raw, dict):
        data = raw.get("result") if isinstance(raw.get("result"), list) else [raw]
    elif isinstance(raw, list):
        data = raw
    else:
        data = []
    rows: list = []
    for r in data:
        if isinstance(r, list):
            rows.extend(r)
        else:
            rows.append(r)
    return [r for r in rows if isinstance(r, dict)]


# ===== 采集网关 =====
async def collect_news(module: str, keywords: Optional[List[str]] = None,
                       sources: Optional[List[str]] = None,
                       since: Optional[str] = None, limit: int = 50) -> List[NewsItem]:
    """多源资讯采集：优先走 MCP 采集服务器，失败降级进程内直调"""
    try:
        raw = await mcp_call_tool("collect", "collect_news", {
            "module": module, "keywords": keywords, "sources": sources,
            "since": since, "limit": limit,
        })
        return [dto_from_dict(NewsItem, r) for r in _rows(raw)]
    except Exception as exc:
        logger.warning(f"[mcp] collect_news 走 MCP 失败，降级进程内直调: {exc}")
        from tools import collect as _t
        return await _t.collect_news(module, keywords, sources, since, limit)


async def fetch_account_posts(account: str, platform: str,
                              since: Optional[str] = None, limit: int = 50) -> List[AccountPost]:
    """账户发布监控：优先走 MCP 采集服务器，失败降级进程内直调"""
    try:
        raw = await mcp_call_tool("collect", "fetch_account_posts", {
            "account": account, "platform": platform, "since": since, "limit": limit,
        })
        return [dto_from_dict(AccountPost, r) for r in _rows(raw)]
    except Exception as exc:
        logger.warning(f"[mcp] fetch_account_posts 走 MCP 失败，降级进程内直调: {exc}")
        from tools import collect as _t
        return await _t.fetch_account_posts(account, platform, since, limit)


# ===== 加工网关 =====
async def cluster_events(news_items: List[NewsItem], threshold: float = 0.8):
    """事件聚类：优先走 MCP 加工服务器，失败降级进程内直调（返回 List[EventCluster]）"""
    from tools.process import EventCluster
    try:
        raw = await mcp_call_tool("process", "cluster_events", {
            "news_items": [asdict(i) if not isinstance(i, dict) else i for i in news_items],
            "threshold": threshold,
        })
        return [dto_from_dict(EventCluster, r) for r in _rows(raw)]
    except Exception as exc:
        logger.warning(f"[mcp] cluster_events 走 MCP 失败，降级进程内直调: {exc}")
        from tools import process as _t
        return await _t.cluster_events(news_items, threshold)


async def score_heat(clusters, time_window_hours: int = 24) -> List[HotEvent]:
    """热度评分：优先走 MCP 加工服务器，失败降级进程内直调"""
    try:
        raw = await mcp_call_tool("process", "score_heat", {
            "clusters": [asdict(c) if not isinstance(c, dict) else c for c in clusters],
            "time_window_hours": time_window_hours,
        })
        return [dto_from_dict(HotEvent, r) for r in _rows(raw)]
    except Exception as exc:
        logger.warning(f"[mcp] score_heat 走 MCP 失败，降级进程内直调: {exc}")
        from tools import process as _t
        return await _t.score_heat(clusters, time_window_hours)


async def verify_events(events: List[HotEvent]) -> List[HotEvent]:
    """多源交叉核验：优先走 MCP 加工服务器，失败降级进程内直调"""
    try:
        raw = await mcp_call_tool("process", "verify_events", {
            "events": [asdict(e) if not isinstance(e, dict) else e for e in events],
        })
        return [dto_from_dict(HotEvent, r) for r in _rows(raw)]
    except Exception as exc:
        logger.warning(f"[mcp] verify_events 走 MCP 失败，降级进程内直调: {exc}")
        from tools import process as _t
        return await _t.verify_events(events)


# ===== 输出网关 =====
async def publish_briefing(task_result: PipelineResult,
                           channels: Optional[List[str]] = None,
                           template: str = "default"):
    """生成简报并推送：优先走 MCP 输出服务器，失败降级进程内直调（返回 PublishResult）"""
    from tools.publish import PublishResult
    try:
        raw = await mcp_call_tool("publish", "publish_briefing", {
            "task_result": asdict(task_result) if not isinstance(task_result, dict) else task_result,
            "channels": channels, "template": template,
        })
        return dto_from_dict(PublishResult, raw if isinstance(raw, dict) else {})
    except Exception as exc:
        logger.warning(f"[mcp] publish_briefing 走 MCP 失败，降级进程内直调: {exc}")
        from tools import publish as _t
        return await _t.publish_briefing(task_result, channels, template)


# ===== 视频内容网关 =====
async def extract_video_content(video_url: str, platform: str = "bilibili",
                                prefer_subtitle: bool = True):
    """字幕/音频转写与摘要：优先走 Video MCP，失败降级进程内直调。"""
    from tools.video import VideoContent
    try:
        raw = await mcp_call_tool("video", "extract_video_content", {
            "video_url": video_url,
            "platform": platform,
            "prefer_subtitle": prefer_subtitle,
        }, timeout=300.0)
        return dto_from_dict(VideoContent, raw if isinstance(raw, dict) else {})
    except Exception as exc:
        logger.warning(f"[mcp] extract_video_content 走 MCP 失败，降级进程内直调: {exc}")
        from tools.video import extract_video_content as _extract
        return await _extract(video_url, platform, prefer_subtitle)
