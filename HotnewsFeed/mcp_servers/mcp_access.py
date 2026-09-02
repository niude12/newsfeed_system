# -*- coding: utf-8 -*-
"""Agent → MCP 访问层（mcp_servers/mcp_access.py）

把 tools/* 的工具函数包装成「先经 MCP 协议连远端 MCP 服务器、失败降级进程内直调」的网关，
供 agents / task_pipelines 统一调用 —— 这是 agent「连接 MCP 协议拿到工具」的真实落地。

模块依赖:
- ``httpx``        : 底层 HTTP 客户端。所有 MCP 连接都通过 _no_proxy_http_client() 创建，
                     显式 trust_env=False 屏蔽 Windows 系统代理，避免把 127.0.0.1 转发给
                     代理导致 localhost 502（Windows 常见坑，见该函数 docstring）。
- ``mcp``          : 官方 MCP 客户端（注意 mcp 版本必须 <2；2.x 改名 MCPServer 会破坏
                     FastMCP v1 代码）。streamablehttp_client 提供 streamable-http 传输
                     （MCP 官方推荐的 HTTP 传输），ClientSession 负责会话/握手/调用。
- ``create_logger``: 项目统一日志器，网关各处打印调用与降级日志。
- ``task_pipelines.schemas`` : 各 DTO 数据模型（NewsItem / AccountPost / HotEvent /
                     PipelineResult）与 dto_from_dict 还原工具，用于把 MCP 回传 JSON
                     还原成 dataclass 对象。
- ``tools.*``      : 进程内直调降级时的真实工具实现（collect / process / publish / video）。

典型调用链::

    Agent 业务方法（同步）
      → sync_call() 桥进事件循环
      → 网关函数（collect_news / cluster_events / ...，async）
          → 优先：mcp_call_tool() —— 用官方 mcp 客户端的 streamable-http 传输，
            连接远端 FastMCP 服务器（:8004/:8005/:8006/:8007）→ initialize 握手 →
            call_tool 调用注册工具 → 把返回的 JSON 还原成 DTO 对象
          → 降级：远端不可达时（服务器没起 / 网络被拦），直接进程内调用 tools.*

对外暴露的接口：
- 网关函数：collect_news / fetch_account_posts / cluster_events / score_heat /
  verify_events / publish_briefing / extract_video_content（均 async，MCP 优先、失败降级）。
- 同步桥：sync_call() —— 在同步上下文（Agent 业务方法 / A2A handle_message）里执行 async 网关。
- 协议层：mcp_call_tool() / mcp_list_tools() / mcp_agent_call()（LangChain Agent 驱动 MCP 工具）。

已知坑（Windows / FastMCP）：
1. httpx 默认 trust_env=True 会读系统代理，把 127.0.0.1 也转发出去，代理对 localhost 返回 502；
   现象是「curl 直连 200、mcp 客户端 502」。所有连接都注入 _no_proxy_http_client 规避。
2. FastMCP 序列化 List[DTO] 看长度：≥2 逐条拆成多条 TextContent（每条独立 JSON）；=1 时是
   单个 dict。_parse_payload / _rows 已兼容三种形态，避免「服务器返回 1 条、调用方拿到 0 条」。

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

    参数:
        headers: 附加 HTTP 请求头（dict，默认 None，由 httpx 自行处理）。
        timeout: 超时配置，可传 httpx.Timeout 实例或数字（默认 30 秒）。
        auth:    httpx 认证对象（默认 None）。

    返回:
        httpx.AsyncClient：配置好的异步 HTTP 客户端，
        可传给 streamablehttp_client 的 httpx_client_factory 参数使用。

    说明:
        - httpx.AsyncClient 是 httpx 的异步客户端，负责真正的 HTTP 传输。
        - trust_env=False 是关键：让 httpx 不读环境变量 / Windows 注册表代理，直连目标地址。
        - http1=True 强制走 HTTP/1.1，规避部分代理/服务器对 HTTP/2 的兼容问题。
    """
    # 组装 httpx.AsyncClient：follow_redirects 跟随重定向、timeout 默认 30 秒、
    # trust_env=False 屏蔽系统代理（关键，避免 localhost 被代理劫持）。
    return httpx.AsyncClient(
        follow_redirects=True,                    # 允许自动跟随 3xx 重定向（MCP 端点常带跳转）。
        timeout=timeout or httpx.Timeout(30.0),   # 调用方未给超时时默认 30 秒，防请求挂死。
        headers=headers,                          # 透传调用方附加请求头（如 Content-Type）。
        auth=auth,                                # 透传 httpx 认证对象（本模块一般不启用）。
        trust_env=False,                          # 关键：不读系统/注册表代理，直连 127.0.0.1。
        http1=True,                               # 强制 HTTP/1.1，规避部分代理/服务器对 HTTP/2 兼容问题。
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

    参数:
        coro: 待执行的协程对象（通常是某个 async 网关函数的调用结果）。

    返回:
        Any：协程执行后的返回值（具体类型取决于传入的协程）。

    抛出:
        透传协程内抛出的任何异常。

    说明:
        asyncio.get_running_loop() -> 探测当前线程是否已有运行中的事件循环，
                                      没有时抛 RuntimeError。
        threading.Thread(daemon=True) -> 起守护子线程执行 asyncio.run(coro)，
                                        结果通过 box dict 传回主线程；
                                        用子线程是为了避开「事件循环嵌套」限制。
    """
    # 先探测当前线程是否已有运行中的事件循环。
    try:
        asyncio.get_running_loop()  # 有运行中的循环则正常返回；没有则抛 RuntimeError。
    except RuntimeError:
        # 没有事件循环（普通同步线程）→ asyncio.run 直接跑完返回。
        return asyncio.run(coro)
    # 已处于事件循环（FastMCP / uvicorn 线程）→ 不能在同一个循环里再 asyncio.run，
    # 起一个守护子线程跑协程，用 box dict 把结果传回主线程。
    box: Dict[str, Any] = {}  # 用于子线程向主线程传回执行结果。

    def _runner():
        # 子线程里没有运行中的循环，asyncio.run 可以正常工作。
        box["result"] = asyncio.run(coro)  # 把协程返回值塞进 box，主线程 join 后读取。

    t = threading.Thread(target=_runner, daemon=True)  # daemon=True：主线程退出不等待它。
    t.start()  # 启动子线程执行 _runner。
    t.join()   # 阻塞主线程直到子线程跑完，确保拿到结果再返回。
    return box["result"]  # 取回子线程写入的协程执行结果。


# ===== MCP 客户端（真正的 HTTP 协议调用）=====
async def mcp_call_tool(server: str, tool_name: str, arguments: Dict[str, Any],
                        timeout: float = 30.0) -> Any:
    """经 MCP 协议调用远端服务器上注册的工具，返回解析后的 JSON（dict / list）。

    真实 HTTP 客户端链路：streamablehttp_client 连到 {host}:{port}/mcp →
    ClientSession.initialize() 与服务器握手 → call_tool 执行远端工具 →
    FastMCP 把 DTO 返回序列化成 JSON 文本 → json.loads 还原成 dict/list。

    参数:
        server:    目标服务器标识，必须是 MCP_URLS 的键
                   （collect / process / publish / video）。
        tool_name: 要调用的工具名（如 "collect_news"、"cluster_events"）。
        arguments: 传给远端工具的参数 dict（键名必须与工具函数的形参一致）。
        timeout:   HTTP 传输超时（秒），默认 30；视频转写等长任务可传更大值。

    返回:
        Any：解析后的 JSON 数据（dict / list / 标量），具体形态取决于远端工具返回值。

    抛出:
        KeyError: server 不在 MCP_URLS 中时抛出（URL 映射缺失）。
        RuntimeError: MCP 工具返回 isError 时抛出，附远端 content 便于排查。
        httpx / mcp 底层网络异常: 远端不可达时向上抛出，由网关函数捕获后降级。

    说明:
        - streamablehttp_client 是官方 mcp 包的 streamable-http 传输客户端上下文管理器，
          返回 (read, write, _) 三个流对象给 ClientSession 使用。
        - httpx_client_factory=_no_proxy_http_client 注入「不读系统代理」的 httpx 客户端，
          规避 Windows 下 localhost 502 的坑。
        - ClientSession.initialize() 完成 MCP 握手（版本协商 + capabilities 交换）。
        - result.content 是 TextContent 列表；result.isError 为 True 表示远端执行失败。
        - FastMCP 序列化 List[DTO]：≥2 条拆成多个 TextContent（各自独立 JSON），
          因此用 _parse_payload 逐块解析合并（详见该函数）。
    """
    url = MCP_URLS[server]  # 按服务器标识取 streamable-http 端点（127.0.0.1:800x/mcp）。
    logger.info(f"[mcp] -> {server}({url}) 调用工具 {tool_name}")  # 记录调用日志，便于排查链路。
    # streamablehttp_client 是官方 mcp 的 streamable-http 传输客户端上下文管理器，
    # 返回 (read, write, _) 三个流对象；注入不读系统代理的 httpx 客户端。
    async with streamablehttp_client(url, timeout=timeout,
                                     httpx_client_factory=_no_proxy_http_client) as (read, write, _):
        # ClientSession 用 (read, write) 流与远端服务器维持一个 MCP 会话。
        async with ClientSession(read, write) as session:
            # initialize() 完成 MCP 握手（版本协商 + 能力交换），之后才能调用工具。
            await session.initialize()
            # call_tool 真正在远端执行已注册工具，返回 CallToolResult。
            result = await session.call_tool(tool_name, arguments)
            if result.isError:  # isError 为 True 表示远端执行失败。
                # 把远端 content 塞进异常信息，方便上层排查具体错误原因。
                raise RuntimeError(f"MCP 工具 {tool_name} 返回错误: {result.content}")
            # 取出所有 TextContent 块的文本；FastMCP 可能把 List[DTO] 拆成多条文本块。
            texts = [c.text for c in result.content if hasattr(c, "text")]
            # 逐块 / 整体解析 JSON，兼容 FastMCP 的多种序列化形态。
            return _parse_payload(texts)


async def mcp_list_tools(server: str, timeout: float = 30.0) -> List[str]:
    """连远端服务器列工具名（验证注册 / 调试用）。

    参数:
        server:  目标服务器标识（MCP_URLS 的键）。
        timeout: HTTP 传输超时（秒），默认 30。

    返回:
        List[str]：远端服务器上已注册的工具名列表。

    抛出:
        同 mcp_call_tool：网络异常 / KeyError 等会向上抛。

    说明:
        session.list_tools() 返回 ToolsResult，.tools 是 Tool 对象列表，
        取各 Tool 的 name 字段即为工具名。
    """
    url = MCP_URLS[server]  # 按服务器标识取 streamable-http 端点。
    async with streamablehttp_client(url, timeout=timeout,
                                     httpx_client_factory=_no_proxy_http_client) as (read, write, _):
        async with ClientSession(read, write) as session:  # 建立 MCP 会话。
            await session.initialize()  # 握手完成后才能查询工具列表。
            # list_tools 返回 ToolsResult，.tools 是 Tool 对象列表，取 name 字段。
            tools = await session.list_tools()
            return [t.name for t in tools.tools]  # 只需工具名，供验证注册 / 调试使用。


async def mcp_agent_call(query: str, server: str = "collect",
                         system: Optional[str] = None, verbose: bool = False) -> str:
    """Agent 作为 MCP 客户端，由 LangChain「工具调用 Agent」驱动 MCP 工具（用户给订票模板的同构实现）。

    链路（照模板）：
        streamablehttp_client(远端 MCP 服务器) → ClientSession → initialize 握手
          → load_mcp_tools(session) 把服务器上注册的工具读成 LangChain 工具对象
          → ChatPromptTemplate(system/human/agent_scratchpad)
          → create_tool_calling_agent(llm, tools, prompt) → AgentExecutor → ainvoke(query)
    由 LLM 自己决定调哪个工具、填什么参数，最终返回其回答文本。

    参数:
        query:   用户自然语言请求（如 "采集科技模块的热点资讯"）。
        server:  连哪台 MCP 服务器：collect / process / publish（MCP_URLS 的键）。
        system:  覆盖默认系统提示词（教 LLM 怎么用这批工具）；None 时用内置默认提示词。
        verbose: 是否打印 Agent 每步调用过程（排障用）。

    返回:
        str: LLM 最终回答文本（AgentExecutor 输出的 output 字段）。

    抛出:
        网络异常 / LLM 调用异常：向上抛，由调用方处理。

    说明:
        - 延迟导入 langchain 相关模块：保持本模块顶层轻量（pipelines 会 import 本模块），
          只有真正用到 Agent 时才引入重量级依赖。
        - ChatOpenAI 读取 config.ini [llm] 段的 model_name / base_url / api_key 构造。
        - load_mcp_tools(session) 是 langchain-mcp-adapters 提供的桥接函数，
          把 MCP 注册工具翻译成 LangChain 可用的工具对象（模板核心一步）。
        - create_tool_calling_agent + AgentExecutor 是 LangChain 标准的「工具调用 Agent」，
          agent_scratchpad 占位符存放 LLM 中间推理 / 调用记录。
    """
    # 延迟导入：保持 mcp_access 顶层轻量（pipelines 会 import 它），仅此函数依赖 langchain。
    from langchain.agents import AgentExecutor, create_tool_calling_agent  # LangChain 工具调用 Agent。
    from langchain_core.prompts import ChatPromptTemplate  # 消息模板（system/human/scratchpad）。
    from langchain_mcp_adapters.tools import load_mcp_tools  # 把 MCP 工具桥接成 LangChain 工具。

    from config import Config  # 全局配置单例（读 config.ini）。
    from langchain_openai import ChatOpenAI  # OpenAI 兼容 LLM 客户端（连 dashscope 等网关）。

    conf = Config()  # 读取全局配置。
    llm = ChatOpenAI(
        model=conf.llm["model_name"],    # LLM 模型名（config.ini [llm] 段）。
        base_url=conf.llm["base_url"],   # OpenAI 兼容接口地址（走 dashscope 等网关）。
        api_key=conf.llm["api_key"],     # API 密钥。
        temperature=conf.temperature,    # 采样温度，控制输出随机性。
    )
    url = MCP_URLS[server]  # 按服务器标识取端点。
    logger.info(f"[mcp] agent_call -> {server}({url}) query={query}")  # 记录 Agent 调用日志。
    async with streamablehttp_client(url, timeout=30,
                                     httpx_client_factory=_no_proxy_http_client) as (read, write, _):
        async with ClientSession(read, write) as session:  # 建立 MCP 会话。
            await session.initialize()  # 握手。
            # 从 MCP 会话里把注册的工具翻译成 LangChain 工具对象（模板核心一步）。
            tools = await load_mcp_tools(session)
            # system 提示词教 LLM 用工具；agent_scratchpad 是 LangChain 的「中间步骤占位符」。
            prompt = ChatPromptTemplate.from_messages([
                ("system", system or (   # 用调用方 system 覆盖默认；None 时用内置助手提示词。
                    "你是一个热点资讯助手，通过调用工具获取最新资讯。"
                    "仔细分析工具需要的参数，从用户信息中提取；"
                    "信息不足则追问，不要编造参数。")),
                ("human", "{input}"),           # 用户输入占位。
                ("placeholder", "{agent_scratchpad}"),  # LLM 中间推理/工具调用记录占位。
            ])
            agent = create_tool_calling_agent(llm, tools, prompt)  # 组装「工具调用 Agent」。
            agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=verbose)  # 执行器封装。
            response = await agent_executor.ainvoke({"input": query})  # 运行 Agent，传用户 query。
            return response["output"]  # Agent 最终回答文本。


def _parse_payload(texts: List[str]) -> Any:
    """解析 FastMCP 回传的文本块，兼容三种形态：

    - 单块且是完整 JSON（对象 / 数组）→ 整体解析返回；
    - 多块 = FastMCP 把 List[DTO] 逐条序列化成多条独立 JSON → 逐块解析合并成 list；
    - 非 JSON 文本 → 原样保留。

    参数:
        texts: FastMCP 返回的 TextContent 文本块列表。

    返回:
        Any：解析后的数据。空列表 → {}；单块完整 JSON → 解析后的 dict/list；
              多块 → 每块解析后的元素组成的 list；非 JSON 块 → 原样字符串。

    说明:
        json.loads -> 把 JSON 字符串还原成 Python 对象；解析失败抛 JSONDecodeError。
        FastMCP 序列化 List[DTO] 时看长度：长度 ≥2 拆成多条 TextContent（每条独立 JSON），
        长度 =1 直接序列化成单个 dict。这里先尝试把整块拼接整体解析，
        失败再逐块解析，兼容上述各种形态。
    """
    if not texts:  # 没有任何文本块（空返回）→ 按空 dict 处理。
        return {}
    # 先尝试把所有文本块拼接成一个整体 JSON 解析（对应单块完整 JSON 的常见情况）。
    joined = "\n".join(texts).strip()  # 多块拼接 + 去首尾空白，便于整体 json.loads。
    try:
        return json.loads(joined)  # 整体解析成功（单块完整 JSON）直接返回。
    except json.JSONDecodeError:
        pass  # 整体不是合法 JSON，落到下面逐块解析。
    # 整体解析失败 → 逐块解析，兼容 FastMCP 把 List[DTO] 拆成多条独立 JSON 的情况。
    parsed: list = []  # 逐块解析结果的收集列表。
    for t in texts:  # 遍历每个文本块。
        s = t.strip()  # 去掉块首尾空白，避免空白块干扰解析。
        if not s:  # 空白块跳过。
            continue
        try:
            parsed.append(json.loads(s))  # 该块是独立 JSON → 解析后追加。
        except json.JSONDecodeError:
            parsed.append(s)  # 非 JSON 文本块原样保留为字符串。
    return parsed


def _rows(raw: Any) -> List[dict]:
    """取出行列表（自动展平嵌套），兼容三种形态：
    1. 列表        [row, row, ...]  → 多条
    2. 单条裸行     {row}           → FastMCP 把 List[DTO] 长度为 1 时序列化成单个 dict
    3. 包装形态     {'result': [...]}

    参数:
        raw: mcp_call_tool 返回的原始数据（dict / list / 其它）。

    返回:
        List[dict]：行数据列表（过滤掉非 dict 元素；嵌套 list 被展平）。

    说明:
        - 坑 2（见模块 docstring）：FastMCP 对 List[DTO] 长度 =1 时序列化成单个 dict，
          若一律当 {"result": [...]} 包装解包会取到空 → 出现「服务器返回 1 条、协调器拿到 0 条」。
          这里只在 value 是 list 时才当作包装形态解包，单条裸 dict 则直接 [raw]。
        - 行内若仍是 list（极少数嵌套返回），递归展平一层。
    """
    # 兼容三种形态：list 直接取；单条裸 dict 只有当其含 {"result": [...]} 时才解包，
    # 否则视为单行数据；其它类型视为无数据。
    if isinstance(raw, dict):  # dict 形态：可能是单条行，也可能是 {"result": [...]} 包装。
        # 只有 value 是 list 才当包装解包（否则会把单条行误当包装取空 → 拿到 0 条）。
        data = raw.get("result") if isinstance(raw.get("result"), list) else [raw]
    elif isinstance(raw, list):  # list 形态：本身就是行列表。
        data = raw
    else:  # 其它类型（None/标量）→ 视为无数据。
        data = []
    rows: list = []  # 展平后的行收集列表。
    for r in data:  # 遍历每个元素。
        if isinstance(r, list):  # 行内仍嵌套 list → 展平一层。
            rows.extend(r)
        else:  # 普通行直接追加。
            rows.append(r)
    # 只保留 dict 行，过滤掉非 dict 元素（避免把异常值带进 DTO 还原）。
    return [r for r in rows if isinstance(r, dict)]


# ===== 采集网关 =====
async def collect_news(module: str, keywords: Optional[List[str]] = None,
                       sources: Optional[List[str]] = None,
                       since: Optional[str] = None, limit: int = 50) -> List[NewsItem]:
    """多源资讯采集：优先走 MCP 采集服务器，失败降级进程内直调。

    参数:
        module:   新闻模块名（如 "科技" / "财经" / "体育"），透传给远端工具。
        keywords: 附加关键词过滤列表，None 表示不限。
        sources:  指定采集源（rss / hn / 关键词搜索源），None 用默认源列表。
        since:    只采集该时间点之后的资讯（ISO 8601）。
        limit:    单次采集上限，默认 50。

    返回:
        List[NewsItem]：原始资讯列表（DTO 对象，已用 dto_from_dict 还原）。

    抛出:
        正常情况下不抛出：MCP 路径失败会被捕获并降级到进程内直调 tools.collect。

    说明:
        - MCP 优先：mcp_call_tool 连 :8004 采集服务器执行远端 collect_news 工具。
        - 降级：远端不可达（服务器没起 / 网络被拦）时，捕获异常并进程内直调
          tools.collect.collect_news（同一份实现，MCP 服务器注册的也是它）。
        - _rows(raw) 负责把 FastMCP 三种返回形态统一成行列表；
          dto_from_dict(NewsItem, r) 把每行 JSON dict 还原成 NewsItem dataclass。
    """
    try:
        # 优先走 MCP：连 :8004 采集服务器执行远端 collect_news 工具。
        raw = await mcp_call_tool("collect", "collect_news", {
            "module": module,     # 新闻模块名（如 科技/财经/体育），透传远端。
            "keywords": keywords, # 关键词过滤列表（None 表示不限）。
            "sources": sources,   # 指定采集源（None 用默认 rss/hn）。
            "since": since,       # 只采集该时刻之后（ISO 8601）。
            "limit": limit,       # 单次采集上限。
        })
        # _rows 统一行形态；dto_from_dict 把每行 JSON 还原成 NewsItem。
        return [dto_from_dict(NewsItem, r) for r in _rows(raw)]
    except Exception as exc:
        # 远端不可达 / 调用失败 → 降级：进程内直调 tools.collect.collect_news（同一实现）。
        logger.warning(f"[mcp] collect_news 走 MCP 失败，降级进程内直调: {exc}")
        from tools import collect as _t  # 延迟导入降级实现，避免顶层依赖。
        return await _t.collect_news(module, keywords, sources, since, limit)


async def fetch_account_posts(account: str, platform: str,
                              since: Optional[str] = None, limit: int = 50) -> List[AccountPost]:
    """账户发布监控：优先走 MCP 采集服务器，失败降级进程内直调。

    参数:
        account:  账户标识（weibo uid / 公众号名 / B 站 UID 等）。
        platform: 平台（weibo / wechat / xiaohongshu / bilibili）。
        since:    只返回该时间点之后的新发布（ISO 8601）。
        limit:    单次拉取上限，默认 50。

    返回:
        List[AccountPost]：账户发布内容列表（DTO 对象）。

    抛出:
        正常情况下不抛出：MCP 失败自动降级进程内直调。

    说明:
        链路与 collect_news 相同：优先 :8004 远端 fetch_account_posts，失败降级
        tools.collect.fetch_account_posts；返回的每行 JSON 用 dto_from_dict 还原成 AccountPost。
    """
    try:
        # 优先走 MCP：连 :8004 采集服务器执行远端 fetch_account_posts 工具。
        raw = await mcp_call_tool("collect", "fetch_account_posts", {
            "account": account,    # 账户标识（weibo uid / 公众号名 / B 站 UID 等）。
            "platform": platform,  # 平台（weibo / wechat / xiaohongshu / bilibili）。
            "since": since,        # 只返回该时刻之后的新发布（ISO 8601）。
            "limit": limit,        # 单次拉取上限。
        })
        # _rows 统一行形态；dto_from_dict 把每行 JSON 还原成 AccountPost。
        return [dto_from_dict(AccountPost, r) for r in _rows(raw)]
    except Exception as exc:
        # 远端不可达 / 调用失败 → 降级：进程内直调 tools.collect.fetch_account_posts。
        logger.warning(f"[mcp] fetch_account_posts 走 MCP 失败，降级进程内直调: {exc}")
        from tools import collect as _t  # 延迟导入降级实现。
        return await _t.fetch_account_posts(account, platform, since, limit)


# ===== 加工网关 =====
async def cluster_events(news_items: List[NewsItem], threshold: float = 0.8):
    """事件聚类：优先走 MCP 加工服务器，失败降级进程内直调（返回 List[EventCluster]）。

    参数:
        news_items: 原始资讯列表（NewsItem 或 dict，dict 会原样传给远端）。
        threshold:  相似度阈值，默认 0.8；高于阈值合并为一个事件簇。

    返回:
        List[EventCluster]：事件簇列表（DTO 对象）。

    抛出:
        正常情况下不抛出：MCP 失败自动降级进程内直调。

    说明:
        - EventCluster 定义在 tools/process.py（dataclass），本模块只 import 用于还原结果。
        - asdict(i) 把 dataclass 转成 dict 以便 JSON 序列化传远端；本身已是 dict 则原样。
        - 降级路径 tools.process.cluster_events 与 MCP 服务器注册的是同一个实现。
    """
    # EventCluster 只在本函数内用到，延迟 import 避免顶层引入 tools.process 的重量级依赖。
    from tools.process import EventCluster
    try:
        # asdict 把 dataclass 序列化为 dict（FastMCP 需要 JSON 可序列化入参）。
        raw = await mcp_call_tool("process", "cluster_events", {
            # 事件簇/资讯列表：dataclass 转 dict，本身是 dict 则原样。
            "news_items": [asdict(i) if not isinstance(i, dict) else i for i in news_items],
            "threshold": threshold,  # 相似度阈值，默认 0.8。
        })
        # _rows 统一行形态；dto_from_dict 把每行 JSON 还原成 EventCluster。
        return [dto_from_dict(EventCluster, r) for r in _rows(raw)]
    except Exception as exc:
        # 远端不可达 / 调用失败 → 降级：进程内直调 tools.process.cluster_events。
        logger.warning(f"[mcp] cluster_events 走 MCP 失败，降级进程内直调: {exc}")
        from tools import process as _t  # 延迟导入降级实现。
        # 降级直调要求 DTO：A2A 侧进来的 news_items 可能是 List[dict]（经 A2A 序列化后
        # 未还原成 NewsItem），dto_from_dict 对已还原的 DTO 原样返回、对 dict 重建实例，
        # 两种输入都安全，避免 tools/process 属性访问 `it.title` 崩 AttributeError。
        return await _t.cluster_events(
            [dto_from_dict(NewsItem, i) for i in news_items], threshold
        )


async def score_heat(clusters, time_window_hours: int = 24) -> List[HotEvent]:
    """热度评分：优先走 MCP 加工服务器，失败降级进程内直调。

    参数:
        clusters:          事件簇列表（EventCluster 或 dict）。
        time_window_hours: 热度回溯窗口（小时），默认 24；超出窗口的簇不再算热点。

    返回:
        List[HotEvent]：带 heat_score 的热点事件列表（DTO 对象，按热度降序）。

    抛出:
        正常情况下不抛出：MCP 失败自动降级进程内直调。

    说明:
        链路与 cluster_events 相同：优先 :8005 远端 score_heat，失败降级
        tools.process.score_heat；返回行用 dto_from_dict 还原成 HotEvent。
    """
    try:
        # asdict 把 dataclass 序列化为 dict；本身已是 dict 则原样传远端。
        raw = await mcp_call_tool("process", "score_heat", {
            # 事件簇列表：dataclass 转 dict，本身是 dict 则原样。
            "clusters": [asdict(c) if not isinstance(c, dict) else c for c in clusters],
            "time_window_hours": time_window_hours,  # 热度回溯窗口（小时），默认 24。
        })
        # _rows 统一行形态；dto_from_dict 把每行 JSON 还原成 HotEvent。
        return [dto_from_dict(HotEvent, r) for r in _rows(raw)]
    except Exception as exc:
        # 远端不可达 / 调用失败 → 降级：进程内直调 tools.process.score_heat。
        logger.warning(f"[mcp] score_heat 走 MCP 失败，降级进程内直调: {exc}")
        from tools import process as _t  # 延迟导入降级实现。
        # A2A 侧进来的 clusters 可能是 List[dict]，先归一化成 EventCluster 再直调。
        from tools.process import EventCluster
        return await _t.score_heat(
            [dto_from_dict(EventCluster, c) for c in clusters], time_window_hours
        )


async def verify_events(events: List[HotEvent]) -> List[HotEvent]:
    """多源交叉核验：优先走 MCP 加工服务器，失败降级进程内直调。

    参数:
        events: 待核验的热点事件列表（HotEvent 或 dict）。

    返回:
        List[HotEvent]：回填 credibility 字段（可信 / 存疑 / 证据不足）后的事件列表。

    抛出:
        正常情况下不抛出：MCP 失败自动降级进程内直调。

    说明:
        链路与 cluster_events 相同：优先 :8005 远端 verify_events，失败降级
        tools.process.verify_events；返回行用 dto_from_dict 还原成 HotEvent。
    """
    try:
        # asdict 把 dataclass 序列化为 dict；本身已是 dict 则原样传远端。
        raw = await mcp_call_tool("process", "verify_events", {
            # 待核验事件列表：dataclass 转 dict，本身是 dict 则原样。
            "events": [asdict(e) if not isinstance(e, dict) else e for e in events],
        })
        # _rows 统一行形态；dto_from_dict 把每行 JSON 还原成 HotEvent。
        return [dto_from_dict(HotEvent, r) for r in _rows(raw)]
    except Exception as exc:
        # 远端不可达 / 调用失败 → 降级：进程内直调 tools.process.verify_events。
        logger.warning(f"[mcp] verify_events 走 MCP 失败，降级进程内直调: {exc}")
        from tools import process as _t  # 延迟导入降级实现。
        # A2A 侧进来的 events 可能是 List[dict]，先归一化成 HotEvent 再直调。
        return await _t.verify_events([dto_from_dict(HotEvent, e) for e in events])


# ===== 输出网关 =====
async def publish_briefing(task_result: PipelineResult,
                           channels: Optional[List[str]] = None,
                           template: str = "default"):
    """生成简报并推送：优先走 MCP 输出服务器，失败降级进程内直调（返回 PublishResult）。

    参数:
        task_result: 上游任务流水线结果（PipelineResult，转 dict 传给远端）。
        channels:    推送通道列表（feishu / email / webhook / web_ui），None 用默认。
        template:    简报模板名，默认 "default"。

    返回:
        PublishResult：各通道推送状态（DTO 对象，channels 字段为 {通道: 是否成功}）。

    抛出:
        正常情况下不抛出：MCP 失败自动降级进程内直调。

    说明:
        - PublishResult 定义在 tools/publish.py（dataclass）。
        - 与其它网关不同，publish_briefing 返回的是单个 PublishResult 而非列表，
          因此远端返回单个 dict 时直接 dto_from_dict(PublishResult, raw)；raw 不是 dict
          则给空 dict（构造一个默认 PublishResult）。
    """
    # PublishResult 只在本函数内用到，延迟 import 避免顶层引入 tools.publish 的重量级依赖。
    from tools.publish import PublishResult
    try:
        # asdict 把 PipelineResult 序列化为 dict；本身已是 dict 则原样传远端。
        raw = await mcp_call_tool("publish", "publish_briefing", {
            # 任务流水线结果：dataclass 转 dict，本身是 dict 则原样。
            "task_result": asdict(task_result) if not isinstance(task_result, dict) else task_result,
            "channels": channels,  # 推送通道列表（None 用默认四通道）。
            "template": template,  # 简报模板名（默认 "default"）。
        })
        # 单对象返回：raw 是 dict 就直接还原，否则给空 dict 兜底。
        return dto_from_dict(PublishResult, raw if isinstance(raw, dict) else {})
    except Exception as exc:
        # 远端不可达 / 调用失败 → 降级：进程内直调 tools.publish.publish_briefing。
        logger.warning(f"[mcp] publish_briefing 走 MCP 失败，降级进程内直调: {exc}")
        from tools import publish as _t  # 延迟导入降级实现。
        return await _t.publish_briefing(task_result, channels, template)


# ===== 视频内容网关 =====
async def extract_video_content(video_url: str, platform: str = "bilibili",
                                prefer_subtitle: bool = True):
    """字幕/音频转写与摘要：优先走 Video MCP，失败降级进程内直调。

    参数:
        video_url:       视频链接（B 站等）。
        platform:        视频平台，默认 "bilibili"。
        prefer_subtitle: 是否优先字幕（True 时先取字幕，无字幕再走音频转写）。

    返回:
        VideoContent：视频内容 DTO（含标题 / 转写文本 / 摘要 / 关键词）。

    抛出:
        正常情况下不抛出：MCP 失败自动降级进程内直调；但平台不支持时 tools.video 会抛 ValueError。

    说明:
        - VideoContent 定义在 tools/video.py（dataclass）。
        - timeout=300.0：视频下载 + 转写是长任务，给足 HTTP 传输超时（默认 30 秒不够）。
        - 降级路径 tools.video.extract_video_content 与 MCP 服务器注册的是同一个实现。
    """
    # VideoContent 只在本函数内用到，延迟 import 避免顶层引入 tools.video 的重量级依赖。
    from tools.video import VideoContent
    try:
        # 长任务：下载 + 转写可能超过默认 30 秒，这里显式放宽到 300 秒。
        raw = await mcp_call_tool("video", "extract_video_content", {
            "video_url": video_url,              # 视频链接（B 站等）。
            "platform": platform,                # 视频平台（默认 bilibili）。
            "prefer_subtitle": prefer_subtitle,  # 是否优先字幕（True 先字幕，无字幕再转写）。
        }, timeout=300.0)
        # 单对象返回：raw 是 dict 就直接还原，否则给空 dict 兜底。
        return dto_from_dict(VideoContent, raw if isinstance(raw, dict) else {})
    except Exception as exc:
        # 远端不可达 / 调用失败 → 降级：进程内直调 tools.video.extract_video_content。
        logger.warning(f"[mcp] extract_video_content 走 MCP 失败，降级进程内直调: {exc}")
        from tools.video import extract_video_content as _extract  # 延迟导入降级实现。
        return await _extract(video_url, platform, prefer_subtitle)
