#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""HotnewsFeed 本地 Web 测试入口。

本文件只提供页面和 HTTP API，不启动任何 MCP/A2A/数据库服务。运行前由用户
按项目流程手动启动所需服务；页面右上角可以检查当前连接状态。

模块职责:
    基于 Flask 提供一套本地 Web 控制台（http://127.0.0.1:8080）：
    - 页面：智能对话、功能直选、离线查询、账户监控、服务状态 五个面板；
    - HTTP API：/api/status、/api/chat、/api/direct、/api/offline、
      /api/monitor、/api/briefing；
    - 底层复用 main.py 的 format_result / query_offline_news 做展示与离线查询，
      业务调用则经 CoordinatorAgent（首个业务请求才懒加载创建）。

模块依赖:
- ``Flask``            : Web 框架。app 是全局 Flask 实例；render_template_string
                         渲染内嵌的 PAGE 单页模板；jsonify 返回 JSON 响应。
- ``CoordinatorAgent`` : agents/coordinator_agent.py。create_coordinator_agent()
                         创建实例；route() 做自然语言意图识别并路由；
                         run_hotspot / run_latest / run_account_follow /
                         run_account_monitor_from_text 是其四大业务方法。
- ``a2a.protocol.delegate`` : a2a/protocol.py。把简报任务经真实 HTTP A2A 委派
                         给 PublisherAgent（:8003）。
- ``main.format_result / query_offline_news`` : 结果格式化与离线查询复用。
- ``task_pipelines.schemas`` : PipelineResult / HotEvent / NewsItem / AccountPost
                         等 DTO 与 dto_from_dict（JSON dict → DTO 还原）。
- ``mcp_servers.mcp_access`` : MCP_URLS / mcp_list_tools / sync_call，用于
                         服务状态面板探测四台 MCP 服务器。

典型调用链::

    浏览器前端 fetch('/api/chat')
      → api_chat() → _get_coordinator().route(message)   # CoordinatorAgent
          → intent_agent 意图识别 → A2A 子 Agent → MCP 网关（降级进程内直调）
      → _pipeline_response(result) → jsonify（结构化 result + display 文本）
      → 前端展示

    浏览器前端 fetch('/api/briefing')
      → api_briefing() → _pipeline_from_dict() 还原 PipelineResult
      → a2a.protocol.delegate("publisher", "briefing", ...)   # 真实 HTTP A2A
      → jsonify({ok, result, error})

对外暴露的接口（HTTP API）:
- GET  /             单页前端（render_template_string(PAGE)）。
- GET  /api/status   并行探测 MCP + A2A 服务状态。
- POST /api/chat     自然语言入口（Coordinator 意图识别 → A2A → MCP）。
- POST /api/direct   功能直选入口（跳过 LLM 意图识别）。
- POST /api/offline  离线新闻查询（Redis → Milvus → MySQL）。
- POST /api/monitor  账户持续监控（注册 / 立即检查 / 状态 / 停止）。
- POST /api/briefing 显式生成简报（A2A 委派 PublisherAgent）。

启动：
    python app.py
访问：
    http://127.0.0.1:8080
"""

import json
import socket
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, render_template_string, request

from create_logger import logger
from task_pipelines.schemas import (AccountPost, HotEvent, NewsItem,
                                    PipelineResult, dto_from_dict)


# ===== Flask 应用与全局状态 =====
app = Flask(__name__)
# 让 Flask 返回的 JSON 里的中文保持可读（不转成 \uXXXX），方便前端直接展示。
app.config["JSON_AS_ASCII"] = False

# CoordinatorAgent 实例（懒加载：首个业务请求才创建）；加锁避免并发重复创建。
_coordinator = None
_coordinator_lock = threading.RLock()

# 四台 A2A 子 Agent 服务器端点（host, port），仅用于 TCP 连通性探测；
# 实际业务委派走 a2a.protocol._AGENT_ENDPOINTS（真实 HTTP A2A）。
A2A_ENDPOINTS = {
    "collector": ("127.0.0.1", 8001),         # 采集子 Agent。
    "processor": ("127.0.0.1", 8002),         # 加工子 Agent。
    "publisher": ("127.0.0.1", 8003),         # 输出（简报）子 Agent。
    "account_monitor": ("127.0.0.1", 8009),   # 账户持续监控子 Agent。
}


def _jsonable(value: Any) -> Any:
    """把 dataclass/datetime/集合递归转换为 Flask 可序列化数据。

    参数:
        value: 任意 Python 对象（DTO dataclass / dict / 集合 / datetime / 普通值）。

    返回:
        Any：全是 Python 内置类型（dict / list / str / int / float / bool / None）
        的结构，可直接交给 flask.jsonify 或 json.dumps 序列化。

    说明:
        - is_dataclass / asdict 来自标准库 dataclasses：把 PipelineResult、
          HotEvent 等 DTO 递归展开成 dict。
        - datetime / date 转 ISO 8601 字符串（value.isoformat()），否则 Flask
          默认无法序列化。
        - dict 的键统一转成 str，避免整型键等非字符串键在 JSON 里出问题。
    """
    # dataclass → asdict 展开成 dict 后再递归转换。
    if is_dataclass(value):
        return _jsonable(asdict(value))
    # dict：键统一转成 str，值递归转换，避免整型键等非字符串键出问题。
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    # list/tuple/set：逐元素递归转换后返回列表。
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        # 时间对象统一序列化成 ISO 8601 字符串。
        return value.isoformat()
    # 其他普通类型（int/str/bool/None 等）原样返回。
    return value


def _get_coordinator():
    """首次收到业务请求时才创建 CoordinatorAgent（懒加载 + 加锁单例）。

    返回:
        CoordinatorAgent 实例（见 agents/coordinator_agent.py）。

    说明:
        - 全局变量 _coordinator 用 RLock 保护，避免多线程并发创建多个实例。
        - 延迟 import create_coordinator_agent：只有真正要处理业务时才引入
          coordinator 及其依赖（LLM 等），加快 app.py 启动速度。
    """
    # 声明操作的是模块级全局 _coordinator 变量。
    global _coordinator
    # 加锁：避免多个线程同时进入创建分支产生多个实例。
    with _coordinator_lock:
        if _coordinator is None:
            # 延迟 import：只有首次需要时才引入 coordinator 及其 LLM 依赖。
            from agents.coordinator_agent import create_coordinator_agent
            # 用工厂方法创建协调调度 Agent。
            _coordinator = create_coordinator_agent()
        return _coordinator


def _pipeline_response(result: PipelineResult):
    """统一封装前端需要的结构化结果和可读文本。

    参数:
        result: PipelineResult 实例（task_type / items / queried_at / elapsed_ms /
                error，见 task_pipelines/schemas.py）。

    返回:
        dict：{"ok": 是否成功, "result": 结构化结果, "display": 可读文本}，
        供 jsonify 直接序列化返回给前端。

    说明:
        - result.error 为 None 视为成功（ok=True），否则 ok=False。
        - _jsonable(result) 把 DTO 递归转成纯 JSON 结构，供前端按字段渲染。
        - main.format_result 复用控制台的格式化逻辑，把结果转成多行可读文本，
          前端简单场景直接展示 display 即可。
    """
    # 延迟 import：复用 main.py 的结果格式化函数。
    from main import format_result
    return {
        "ok": not bool(result.error),      # 无 error 视为成功。
        "result": _jsonable(result),       # DTO 递归转纯 JSON 结构。
        "display": format_result(result),  # 多行可读文本供前端直接展示。
        # 透传 error：follow_up 追问 / 业务错误时前端 callApi 用 body.error 展示真实原因，
        # 而不是笼统的「请求失败」（此前该字段缺失，追问被吞成通用报错）。
        "error": result.error,
        "trace": _jsonable(result.trace) if getattr(result, "trace", None) else [],
    }


def _keywords(value: Any) -> Optional[List[str]]:
    """把前端字符串或数组规范成关键词列表。

    参数:
        value: 前端传的关键词字段，可能是数组（["足球","英超"]）、字符串
               （"足球，英超"）或 None / 空串。

    返回:
        Optional[List[str]]：去空白、去空项后的关键词列表；没有任何有效关键词
        时返回 None（等价于“不加关键词过滤”）。

    说明:
        - 字符串形态兼容中文逗号（“，”）与英文逗号（","），统一 replace 后 split。
        - 空项（全空白 / 空串）会被过滤掉；空列表转成 None，避免下游把空列表
          当成“过滤出零条”处理。
    """
    if isinstance(value, list):
        # 数组形态：逐项转 str 并去空白。
        values = [str(item).strip() for item in value]
    else:
        # 字符串形态：中英文逗号都切分（先把中文逗号替换成英文逗号）。
        values = [part.strip() for part in str(value or "").replace("，", ",").split(",")]
    # 过滤掉空项（全空白 / 空串）。
    values = [item for item in values if item]
    # 没有有效关键词时返回 None（等价于“不加关键词过滤”）。
    return values or None


def _pipeline_from_dict(data: Dict[str, Any]) -> PipelineResult:
    """把浏览器保存的查询结果还原为 PublisherAgent 可接受的 DTO。

    前端把一次查询的完整结果（_pipeline_response 里的 result）回传用于生成简报，
    这里把其中的 dict 还原回 PipelineResult 及对应条目的 DTO 类型。

    参数:
        data: 浏览器回传的查询结果字典（含 task_type / items / queried_at /
              elapsed_ms / error 等键）。

    返回:
        PipelineResult：task_type 与条目 DTO 均已还原的完整结果对象。

    抛出:
        ValueError: task_type 不是 hotspot_query / latest_news / account_follow
        （account_monitor 与 follow_up 不支持生成简报）时抛出。

    说明:
        - item_type 映射：task_type → 条目 DTO 类（HotEvent / NewsItem /
          AccountPost）。
        - task_pipelines.schemas.dto_from_dict 负责把单条 dict 还原成 DTO 实例，
          缺的键用 dataclass 默认值补齐、不存在的键自动忽略。
    """
    # 取任务类型；空值统一成空串，避免下面映射查不到键。
    task_type = str(data.get("task_type") or "")
    # task_type → 条目 DTO 类型映射；映射不到说明该任务类型不支持生成简报。
    item_type = {
        "hotspot_query": HotEvent,      # 热点查询条目 → HotEvent。
        "latest_news": NewsItem,        # 最新新闻条目 → NewsItem。
        "account_follow": AccountPost,  # 账户发布条目 → AccountPost。
    }.get(task_type)
    if item_type is None:
        # account_monitor / follow_up 等任务类型不允许生成简报。
        raise ValueError(f"任务类型 {task_type!r} 暂不支持生成简报")
    # 逐条 dict → DTO：dto_from_dict 兼容“缺键用默认值、多余键忽略”的宽松还原。
    items = [dto_from_dict(item_type, item) for item in data.get("items") or []]
    return PipelineResult(
        task_type=task_type,                          # 原样回填任务类型。
        items=items,                                  # 还原后的条目 DTO 列表。
        queried_at=str(data.get("queried_at") or ""), # 查询时间，缺省空串。
        elapsed_ms=int(data.get("elapsed_ms") or 0),  # 耗时（毫秒），缺省 0。
        error=data.get("error"),                      # 错误信息原样透传。
    )


def _tcp_alive(host: str, port: int, timeout: float = 0.6) -> bool:
    """只做 TCP 端口探测，不触发 A2A 业务调用。

    参数:
        host:    目标主机（如 "127.0.0.1"）。
        port:    目标端口（如 8009）。
        timeout: 连接超时秒数（默认 0.6，服务状态面板要并发探测多台，宜短）。

    返回:
        bool：端口能建立 TCP 连接返回 True；连接失败 / 超时返回 False。

    说明:
        socket.create_connection 只做三次握手，不发送任何 A2A 协议数据，
        因此不会对子 Agent 产生业务副作用，适合“页面右上角检查连接状态”场景。
    """
    try:
        # 建立 TCP 连接（只做三次握手，不发任何业务数据）；成功则端口在线。
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        # OSError 覆盖连接被拒（WinError 10061）、超时、DNS 失败等所有情况。
        return False


def _mcp_probe(name: str, url: str) -> Dict[str, Any]:
    """连接一台 MCP 服务并读取其工具清单。

    参数:
        name: MCP 服务器名（collect / process / publish / video，见 MCP_URLS）。
        url:  MCP 服务器的 streamable-http 端点地址。

    返回:
        dict：{"name", "url", "ok", "tools"}，ok=True 时 tools 是该服务器注册的
        工具名列表；ok=False 时带 error 字符串说明原因。

    说明:
        - mcp_servers.mcp_access.mcp_list_tools：经官方 mcp 客户端的
          streamable-http 传输连远端服务器、initialize 握手后列工具名（异步）。
        - sync_call：把异步协程桥进同步上下文（见 mcp_access.sync_call），
          这里给 3 秒超时；任何异常都按“不可达”封装，不向上抛。
    """
    # 延迟 import：探测 MCP 才引入访问层。
    from mcp_servers.mcp_access import mcp_list_tools, sync_call
    try:
        # sync_call 跑 mcp_list_tools：连远端服务器并读工具清单（3 秒超时）。
        tools = sync_call(mcp_list_tools(name, timeout=3.0))
        # 成功：返回在线状态与工具名列表。
        return {"name": name, "url": url, "ok": True, "tools": tools}
    except Exception as exc:
        # 失败：返回不可达状态与错误原因（tools 置空）。
        return {"name": name, "url": url, "ok": False, "tools": [], "error": str(exc)}


def service_status() -> Dict[str, Any]:
    """并行探测四台 MCP 和五台 A2A 服务；绝不自动启动进程。

    返回:
        dict：{"mcp": {服务器名: 探测结果}, "a2a": {子 Agent 名: 探测结果}}，
        其中 mcp 结果含工具清单，a2a 结果只含 TCP 连通性与端口。

    说明:
        - MCP 探测用 ThreadPoolExecutor 并发执行 _mcp_probe（每台要连 HTTP +
          initialize 握手，串行会很慢），as_completed 逐条取结果。
        - A2A 探测只做 socket 端口探测（_tcp_alive），速度快且不会向子 Agent
          发送任何业务数据。
        - 本函数只读不写，绝不自动创建 / 启动进程（页面提示手动启动所需服务）。
    """
    # 延迟 import：拿到四台 MCP 服务器的端点字典。
    from mcp_servers.mcp_access import MCP_URLS

    # 并发探测四台 MCP：先提交任务，再按完成顺序收集结果。
    mcp: Dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=len(MCP_URLS)) as pool:
        # 每台服务器提交一个 _mcp_probe 任务，字典记录 任务→服务器名。
        jobs = {pool.submit(_mcp_probe, name, url): name for name, url in MCP_URLS.items()}
        for job in as_completed(jobs):
            result = job.result()               # 取该任务的探测结果。
            mcp[result["name"]] = result        # 按服务器名存回结果字典。

    # 五台 A2A 子 Agent：仅做 TCP 端口探测，不触发 A2A 业务调用。
    a2a = {
        name: {
            "name": name,                        # 子 Agent 名。
            "url": f"http://{host}:{port}",      # HTTP 端点展示地址。
            "ok": _tcp_alive(host, port),        # 只探测端口是否在线。
            "port": port,                        # 端口号。
        }
        for name, (host, port) in A2A_ENDPOINTS.items()
    }
    return {"mcp": mcp, "a2a": a2a}


@app.get("/")
def index():
    """首页：返回内嵌的单页 Web 控制台。

    页面用途（PAGE 字符串）：包含 智能对话 / 功能直选 / 离线查询 / 账户监控 /
    服务状态 五个面板，全部由原生 JS 调用下方 /api/* 接口完成数据交互。
    """
    # render_template_string 把 PAGE（Jinja2 模板字符串）渲染成 HTML 返回给浏览器；
    # PAGE 里没有模板变量，实际相当于原样输出单页前端。
    return render_template_string(PAGE)


@app.get("/api/status")
def api_status():
    """服务状态接口：返回四台 MCP + 五台 A2A 的连通性探测结果。

    页面“服务状态”面板和右上角“检查服务状态”按钮都会调用它。
    """
    # 探测结果与 ok 标志一起封装成 JSON 返回给前端。
    return jsonify({"ok": True, "services": service_status()})


@app.post("/api/chat")
def api_chat():
    """自然语言入口：Coordinator 意图识别 → A2A 子 Agent → MCP。

    请求体（JSON）: {"message": "自然语言话术"}。

    返回:
        200 + _pipeline_response(result)（含 ok / result / display）；
        400：message 为空时返回错误提示。

    说明:
        - request.get_json(silent=True) 解析请求体 JSON，解析失败时返回 None，
          or {} 兜底成空字典。
        - _get_coordinator().route(message) 是 CoordinatorAgent 的自然语言路由：
          intent_agent 做 LLM 意图识别，再按意图走 run_hotspot / run_latest /
          run_account_follow / run_account_monitor_from_text，最终经 A2A/MCP
          拿到 PipelineResult。
        - 用 _coordinator_lock 保护 route 调用，避免多个 Web 请求并发进同一个
          Agent 实例产生共享状态竞争。
    """
    # 解析请求体 JSON；解析失败时 get_json(silent=True) 返回 None，用空字典兜底。
    payload = request.get_json(silent=True) or {}
    # 取 message 字段并去掉首尾空白。
    message = str(payload.get("message") or "").strip()
    if not message:
        # message 为空：返回 400 错误提示。
        return jsonify({"ok": False, "error": "请输入要查询或执行的内容"}), 400
    logger.info(f"[app] 自然语言请求: {message}")
    # 持锁执行路由，保证同一时刻只有一个请求在跑 Coordinator 的业务逻辑。
    with _coordinator_lock:
        # CoordinatorAgent.route() 做 LLM 意图识别并路由到对应业务方法。
        result = _get_coordinator().route(message)
    # 封装成统一结构（ok/result/display）返回给前端。
    return jsonify(_pipeline_response(result))


@app.post("/api/direct")
def api_direct():
    """功能直选入口：跳过 LLM 意图识别，但仍由 Coordinator A2A 调子 Agent。

    请求体（JSON）:
        {"action": "hotspot"|"latest"|"account_follow",
         "module": 模块名, "limit": 数量, "hours": 时间窗口(小时),
         "keywords": 关键词, "account": 账户名, "platform": 平台, "since": 起始时间}

    返回:
        200 + _pipeline_response(result)；400：action 非法时返回错误提示。

    说明:
        - 这是“功能直选”面板使用的接口：用户在前端下拉框里选功能并填参数，
          不经过 intent_agent，直接调用 Coordinator 的 run_hotspot / run_latest /
          run_account_follow（这些方法内部仍是真实 HTTP A2A 委派子 Agent）。
        - max(1, min(50, int(...))) 把 limit 夹在 [1, 50]，hours 夹到 >= 1，
          防止前端传越界值；int(payload.get("limit") or 10) 缺省用 10。
        - _keywords 把字符串 / 数组关键词规范成列表，None 表示不加过滤。
    """
    # 解析请求体 JSON，空值用空字典兜底。
    payload = request.get_json(silent=True) or {}
    # 取 action 字段（指定功能名）并去空白。
    action = str(payload.get("action") or "").strip()
    # 取（或懒加载创建）Coordinator 实例。
    agent = _get_coordinator()
    with _coordinator_lock:
        if action == "hotspot":
            # 热点查询：模块 + topN + 时间窗口 + 可选关键词。
            result = agent.run_hotspot(
                module=str(payload.get("module") or "科技"),           # 模块名，缺省“科技”。
                top_n=max(1, min(50, int(payload.get("limit") or 10))),  # topN 夹到 [1,50]，缺省 10。
                time_window_hours=max(1, int(payload.get("hours") or 24)),  # 时间窗口小时，缺省 24。
                keywords=_keywords(payload.get("keywords")),           # 关键词列表，None=不过滤。
            )
        elif action == "latest":
            # 最新新闻：模块 + 数量 + 可选关键词。
            result = agent.run_latest(
                module=str(payload.get("module") or "科技"),           # 模块名，缺省“科技”。
                count=max(1, min(50, int(payload.get("limit") or 10))),  # 数量夹到 [1,50]，缺省 10。
                keywords=_keywords(payload.get("keywords")),           # 关键词列表，None=不过滤。
            )
        elif action == "account_follow":
            # 一次性账户发布：账户名 + 平台 + 起始时间 + 数量。
            result = agent.run_account_follow(
                account=str(payload.get("account") or "").strip(),      # 账户名。
                platform=str(payload.get("platform") or "bilibili").strip(),  # 平台，缺省 bilibili。
                since=str(payload.get("since") or "").strip() or None,  # 起始时间，空串转 None。
                limit=max(1, min(50, int(payload.get("limit") or 10))),  # 数量夹到 [1,50]，缺省 10。
            )
        else:
            # 未知功能名：返回 400 错误。
            return jsonify({"ok": False, "error": f"未知直选功能: {action}"}), 400
    return jsonify(_pipeline_response(result))


@app.post("/api/offline")
def api_offline():
    """离线查询：Redis 精确缓存 → Milvus 向量 → MySQL 原文。

    请求体（JSON）: {"query": "查询内容", "limit": 可选数量}。

    返回:
        200 + {"ok": True, "result": {"source": ..., "items": [...]}}；
        400：query 为空时返回错误提示。

    说明:
        - main.query_offline_news 复用命令行入口的离线查询逻辑，内部创建
          OfflineNewsService().query()，编排 Redis 精确缓存 → Milvus 向量召回
          ID → MySQL 取原文（最多 3 条）。
        - limit 用 max(1, min(3, ...)) 夹到 [1, 3]，离线库设计最多返回 3 条。
        - _jsonable(result) 把查询结果 dict 递归转成纯 JSON 结构返回前端。
    """
    # 解析请求体 JSON，空值用空字典兜底。
    payload = request.get_json(silent=True) or {}
    # 取 query 字段并去空白。
    query = str(payload.get("query") or "").strip()
    if not query:
        # 查询内容为空：返回 400 错误提示。
        return jsonify({"ok": False, "error": "离线查询内容不能为空"}), 400
    # 延迟 import：复用 main.py 的离线查询入口。
    from main import query_offline_news
    # 执行查询；limit 夹到 [1,3]（离线库最多返回 3 条），缺省 3。
    result = query_offline_news(query, limit=max(1, min(3, int(payload.get("limit") or 3))))
    return jsonify({"ok": True, "result": _jsonable(result)})


@app.post("/api/monitor")
def api_monitor():
    """账户持续监控入口；实际工作由 :8009 AccountMonitorAgent 完成。

    请求体（JSON）:
        {"action": "add"|"run"|"status"|"stop",
         "account": 账户名, "platform": 平台, "url": 账户主页 URL}

    返回:
        200 + _pipeline_response(result)；400：参数缺失或动作非法时返回错误提示。

    说明:
        - 本接口不实现监控业务，只是把前端的动作 + 参数拼成一句自然语言话术，
          交给 _get_coordinator().run_account_monitor_from_text(text)：Coordinator
          内部做槽位抽取（动作 / 账户 / 平台 / URL），再经真实 HTTP A2A 分发给
          AccountMonitorAgent（:8009）执行。
        - action="add" 必须有 URL；action="stop" 必须有账户名。
        - 平台默认 "bilibili"（前端下拉框也主要是 B 站 / RSS）。
    """
    # 解析请求体 JSON，空值用空字典兜底。
    payload = request.get_json(silent=True) or {}
    # 取监控动作（缺省 status），并统一转小写。
    action = str(payload.get("action") or "status").strip().lower()
    # 账户名、平台、URL 分别取并去空白。
    account = str(payload.get("account") or "").strip()
    platform = str(payload.get("platform") or "bilibili").strip()
    url = str(payload.get("url") or "").strip()
    if action == "add":
        if not url:
            # 注册监控必须有主页 URL。
            return jsonify({"ok": False, "error": "注册监控必须提供账户主页 URL"}), 400
        # 账户名可选；lstrip('@') 兼容用户可能手动多打了一个 @ 的情况。
        account_text = f"@{account.lstrip('@')}" if account else ""
        # 拼成“持续监控<平台>账户 <账户名> <URL>”的话术。
        text = f"持续监控{platform}账户 {account_text} {url}".strip()
    elif action == "run":
        # 立即检查全部已注册账户。
        text = "立即执行账户监控"
    elif action == "status":
        # 查看监控状态和已发现数量。
        text = "查看账户监控状态"
    elif action == "stop":
        if not account:
            # 停止监控必须指定账户名。
            return jsonify({"ok": False, "error": "停止监控必须提供账户名"}), 400
        text = f"停止监控 @{account.lstrip('@')} {platform}"
    else:
        # 未知动作：返回 400 错误。
        return jsonify({"ok": False, "error": f"未知监控动作: {action}"}), 400
    # 把拼好的话术交给 Coordinator 的账户监控入口（内部 A2A 分发给 :8009）。
    with _coordinator_lock:
        result = _get_coordinator().run_account_monitor_from_text(text)
    return jsonify(_pipeline_response(result))


@app.post("/api/briefing")
def api_briefing():
    """显式生成简报；Web 页面使用按钮确认，不调用终端 input()。

    请求体（JSON）:
        {"result": 浏览器保存的查询结果 dict, "channels": ["web_ui"]}。

    返回:
        JSON：{"ok": bool, "result": 结构化结果, "error": 错误信息}。

    说明:
        - 与命令行不同，Web 场景没有终端 input() 可反问；页面用“生成简报”按钮
          显式触发，用户确认动作已经由点击按钮表达。
        - _pipeline_from_dict 把前端回传的查询结果 dict 还原成 PipelineResult 及
          对应 DTO 类型（PublisherAgent 才能接受）。
        - a2a.protocol.delegate 是 A2A 委派入口：target 传字符串 "publisher" 时
          走真实 HTTP A2A（AgentNetwork → :8003），返回回传文本（JSON 字符串）。
        - 通道 channels 默认 ["web_ui"]，即简报推送到本 Web 界面。
    """
    # 解析请求体 JSON，空值用空字典兜底。
    payload = request.get_json(silent=True) or {}
    # 把前端回传的查询结果 dict 还原成 PipelineResult（含 DTO 条目）。
    result = _pipeline_from_dict(payload.get("result") or {})
    # 推送通道列表，缺省只推送到本 Web 界面。
    channels = payload.get("channels") or ["web_ui"]
    if not isinstance(channels, list):
        # 兼容单字符串通道（如 "web_ui"）→ 包成列表。
        channels = [str(channels)]
    # 延迟 import：A2A 委派入口。
    from a2a.protocol import delegate
    # 经真实 HTTP A2A 委派 PublisherAgent 生成简报并推送。
    text = delegate(
        "publisher", "briefing",
        {"result": result, "channels": channels, "template": "default"},
        from_agent="web-app",
    )
    # 委派回传的是 JSON 字符串，解析成 dict。
    response = json.loads(text)
    return jsonify({"ok": bool(response.get("ok")), "result": _jsonable(response.get("result")),
                    "error": response.get("error")})


@app.errorhandler(Exception)
def handle_unexpected_error(exc):
    """把业务异常转换成 JSON，避免前端只看到 Flask HTML 错误页。

    参数:
        exc: 未被捕获的异常实例（任何抛到路由层之外的 Exception）。

    返回:
        JSON 响应：{"ok": False, "error": 异常描述}，HTTP 状态码 500。

    说明:
        - Flask 默认的全局异常处理会返回 HTML 错误页，前端 fetch 时解析 JSON
          会失败；这里统一转成 JSON，让前端能展示可读的错误信息。
        - 异常记 logger.error 并带 traceback（exc_info=True），便于服务端排查。
    """
    # 记录完整 traceback 便于服务端排查。
    logger.error(f"[app] 请求处理失败: {exc}", exc_info=True)
    # 统一转成 JSON + 500 状态码，前端能解析并展示错误信息。
    return jsonify({"ok": False, "error": str(exc)}), 500


# ===== 单页前端模板（内嵌 HTML/CSS/JS，原样输出，勿改）=====
# PAGE 是一个 raw 字符串（r"\"\"\""），存放整份前端页面：
# 五个面板（智能对话 / 功能直选 / 离线查询 / 账户监控 / 服务状态）的 DOM、
# 样式与原生 JS。JS 通过 fetch 调用上方 /api/* 接口，并在页面右上角汇总
# MCP/A2A 服务在线数。模板内没有 Jinja2 变量，render_template_string 基本是
# 原样输出。index() 路由把它渲染成 HTML 返回。
PAGE = r"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>HotnewsFeed 控制台</title>
  <style>
    :root{--bg:#f3f5f2;--panel:#fff;--ink:#17201d;--muted:#68736e;--line:#dce2de;--green:#12664f;--green2:#e6f3ed;--orange:#c26122;--red:#b23b3b;--shadow:0 16px 45px rgba(31,49,41,.08)}
    *{box-sizing:border-box} body{margin:0;background:radial-gradient(circle at 10% 0,#dcefe6 0,transparent 30%),var(--bg);color:var(--ink);font-family:"Microsoft YaHei",system-ui,sans-serif}
    header{background:#102b24;color:#fff;padding:28px max(24px,calc((100% - 1220px)/2));display:flex;gap:24px;align-items:center;justify-content:space-between}
    h1{font-size:27px;margin:0 0 6px}.subtitle{color:#bed4ca;font-size:14px}.status-summary{font-size:13px;padding:9px 13px;border:1px solid #41675c;border-radius:999px;cursor:pointer;background:#183d33;color:#eaf6f1}
    main{max-width:1220px;margin:24px auto;padding:0 20px 48px;display:grid;grid-template-columns:245px 1fr;gap:20px}.panel{background:var(--panel);border:1px solid var(--line);border-radius:18px;box-shadow:var(--shadow)}
    aside{padding:15px;height:max-content;position:sticky;top:18px}.tab{display:block;width:100%;border:0;background:transparent;text-align:left;padding:13px 14px;border-radius:11px;color:var(--muted);font-size:15px;cursor:pointer;margin:3px 0}.tab.active{background:var(--green2);color:var(--green);font-weight:700}.notice{font-size:12px;line-height:1.65;background:#fff7e8;color:#765020;padding:12px;border-radius:10px;margin-top:18px}
    section.view{display:none;padding:25px}.view.active{display:block}.view h2{margin:0 0 7px}.desc{color:var(--muted);font-size:14px;margin-bottom:20px;line-height:1.7}
    .chips{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:15px}.chip{border:1px solid #b9d6ca;background:#f4fbf8;color:#185e4b;border-radius:999px;padding:7px 11px;cursor:pointer;font-size:13px}
    textarea,input,select{width:100%;border:1px solid #cfd8d3;border-radius:10px;padding:11px 12px;font:inherit;background:#fff;color:var(--ink);outline:none}textarea:focus,input:focus,select:focus{border-color:#4a947b;box-shadow:0 0 0 3px #e5f3ee}textarea{min-height:105px;resize:vertical}
    .row{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:13px}.row.three{grid-template-columns:repeat(3,minmax(0,1fr))}.field{margin-bottom:13px}.field label{display:block;font-size:13px;color:#4f5d57;margin:0 0 6px}
    button.primary,button.secondary{border:0;border-radius:10px;padding:11px 17px;font:inherit;font-weight:700;cursor:pointer}.primary{background:var(--green);color:white}.secondary{background:#edf2ef;color:#2b5145}.actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
    pre.output{white-space:pre-wrap;word-break:break-word;background:#13231f;color:#e6f4ef;padding:18px;border-radius:13px;min-height:115px;line-height:1.72;font-family:"Microsoft YaHei",sans-serif;margin-top:18px}.loading{opacity:.65;pointer-events:none}.error{color:#ffb3a7}.offline-card{border:1px solid var(--line);padding:14px;border-radius:12px;margin:10px 0}.offline-card a{color:var(--green)}
    .service-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:9px;margin-top:14px}.service{border:1px solid var(--line);border-radius:11px;padding:11px;font-size:12px}.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px}.ok .dot{background:#32a16f}.bad .dot{background:#ce5b51}.service small{display:block;color:var(--muted);margin-top:5px;word-break:break-all}
    @media(max-width:800px){main{grid-template-columns:1fr}aside{position:static;display:flex;overflow:auto}.tab{white-space:nowrap;width:auto}.notice{display:none}.row,.row.three,.service-grid{grid-template-columns:1fr}header{align-items:flex-start;flex-direction:column}}
  </style>
</head>
<body>
<header><div><h1>HotnewsFeed</h1><div class="subtitle">实时热点资讯多智能体系统 · 本地功能测试台</div></div><button class="status-summary" onclick="loadStatus()" id="statusSummary">检查服务状态</button></header>
<main>
  <aside class="panel">
    <button class="tab active" data-view="chat">智能对话</button>
    <button class="tab" data-view="direct">功能直选</button>
    <button class="tab" data-view="offline">离线查询</button>
    <button class="tab" data-view="monitor">账户监控</button>
    <button class="tab" data-view="services">服务状态</button>
    <div class="notice">本页面不会启动后台服务。请手动启动所需 A2A、MCP、MySQL、Redis 和 Milvus。</div>
  </aside>
  <div>
    <section class="panel view active" id="view-chat">
      <h2>智能对话</h2><div class="desc">输入自然语言，由 Coordinator 识别意图并经 A2A 调用功能 Agent。可参考下面示例。</div>
      <div class="chips">
        <button class="chip">帮我查找今天的俄乌战争新闻</button><button class="chip">查询体育模块的足球热点新闻</button>
        <button class="chip">持续监控B站账户 https://space.bilibili.com/312249633/video</button><button class="chip">查看账户监控状态</button>
      </div>
      <textarea id="chatInput" placeholder="例如：帮我查询科技模块最近的热点新闻"></textarea>
      <div class="actions"><button class="primary" onclick="chat()">发送请求</button><button class="secondary briefing" onclick="briefing()" disabled>将本次结果生成简报</button></div>
      <pre class="output" id="chatOutput">查询结果将在这里显示。</pre>
    </section>

    <section class="panel view" id="view-direct">
      <h2>功能直选</h2><div class="desc">跳过 LLM 意图识别，直接选择业务功能；底层仍通过 Coordinator → A2A → MCP。</div>
      <div class="row three"><div class="field"><label>功能</label><select id="directAction"><option value="latest">最新新闻</option><option value="hotspot">热点新闻</option><option value="account_follow">一次性账户发布</option></select></div><div class="field"><label>模块</label><select id="directModule"><option>科技</option><option>财经</option><option>体育</option><option>娱乐</option><option>国际</option></select></div><div class="field"><label>返回数量</label><input id="directLimit" type="number" value="5" min="1" max="50"></div></div>
      <div class="row"><div class="field"><label>关键词（多个用逗号）</label><input id="directKeywords" placeholder="例如：足球，英超"></div><div class="field"><label>热点时间窗口（小时）</label><input id="directHours" type="number" value="24" min="1"></div></div>
      <div class="row"><div class="field"><label>账户名（一次性账户查询使用）</label><input id="directAccount" placeholder="例如：bilibili_312249633 或 @新京报"></div><div class="field"><label>平台</label><select id="directPlatform"><option value="bilibili">bilibili</option><option value="weibo">weibo</option><option value="wechat">wechat</option><option value="rss">rss</option></select></div></div>
      <div class="actions"><button class="primary" onclick="directRun()">执行</button><button class="secondary briefing" onclick="briefing()" disabled>生成简报</button></div><pre class="output" id="directOutput">请选择功能并填写参数。</pre>
    </section>

    <section class="panel view" id="view-offline">
      <h2>离线新闻库</h2><div class="desc">Redis 精确缓存未命中后，通过 Milvus 向量召回 ID，再去 MySQL 获取原文；最多返回三条。</div>
      <div class="field"><label>查询内容</label><input id="offlineInput" placeholder="例如：今日俄乌战争新闻"></div><button class="primary" onclick="offlineRun()">查询离线库</button><div id="offlineOutput"><pre class="output">离线查询结果将在这里显示。</pre></div>
    </section>

    <section class="panel view" id="view-monitor">
      <h2>账户持续监控</h2><div class="desc">需要手动启动 AccountMonitorAgent :8009。注册后任务保存在 MySQL，定时轮询仍由 scheduler 单独启动。</div>
      <div class="row"><div class="field"><label>账户主页 URL</label><input id="monitorUrl" placeholder="https://space.bilibili.com/312249633/video"></div><div class="field"><label>账户名（可选）</label><input id="monitorAccount" placeholder="不填时根据B站UID生成"></div></div>
      <div class="field"><label>平台</label><select id="monitorPlatform"><option value="bilibili">bilibili</option><option value="rss">rss</option></select></div>
      <div class="actions"><button class="primary" onclick="monitorRun('add')">注册</button><button class="secondary" onclick="monitorRun('run')">立即检查全部</button><button class="secondary" onclick="monitorRun('status')">查看状态</button><button class="secondary" onclick="monitorRun('stop')">停止这个账户</button></div>
      <pre class="output" id="monitorOutput">操作结果将在这里显示。</pre>
    </section>

    <section class="panel view" id="view-services">
      <h2>后台服务状态</h2><div class="desc">这里只检测端口和 MCP 工具注册，不会自动创建或停止进程。</div><button class="primary" onclick="loadStatus()">重新检查</button><div class="service-grid" id="serviceGrid"></div>
    </section>
  </div>
</main>
<script>
let lastResult=null;
const $=id=>document.getElementById(id);
const tabs=document.querySelectorAll('.tab');
tabs.forEach(t=>t.onclick=()=>{tabs.forEach(x=>x.classList.remove('active'));document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));t.classList.add('active');$('view-'+t.dataset.view).classList.add('active')});
document.querySelectorAll('.chip').forEach(c=>c.onclick=()=>{$('chatInput').value=c.textContent;$('chatInput').focus()});

async function callApi(url,data=null){const options=data===null?{}:{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)};const response=await fetch(url,options);let body;try{body=await response.json()}catch(e){throw new Error('服务器返回了无法解析的响应')};if(!response.ok||body.ok===false)throw new Error(body.error||'请求失败');return body}
function busy(el,on,text='处理中，请稍候……'){el.classList.toggle('loading',on);if(on)el.textContent=text}
function saveResult(body){lastResult=body.result||null;document.querySelectorAll('.briefing').forEach(b=>b.disabled=!(lastResult&&['hotspot_query','latest_news','account_follow'].includes(lastResult.task_type)))}
function fail(el,e){el.classList.remove('loading');el.innerHTML='<span class="error">[请求失败] '+escapeHtml(e.message)+'</span>'}
function escapeHtml(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}

async function chat(){const out=$('chatOutput');busy(out,true);try{const body=await callApi('/api/chat',{message:$('chatInput').value});out.classList.remove('loading');let text=body.display||JSON.stringify(body.result,null,2);if(body.trace&&body.trace.length){text+='\n\n== Agent Trace ==\n'+body.trace.map(x=>`${x.step_id}. ${x.agent}.${x.skill} → ${x.status} (${x.duration_ms}ms)${x.error?' · '+x.error:''}`).join('\n')}out.textContent=text;saveResult(body)}catch(e){fail(out,e)}}
async function directRun(){const out=$('directOutput');busy(out,true);try{const body=await callApi('/api/direct',{action:$('directAction').value,module:$('directModule').value,limit:Number($('directLimit').value),hours:Number($('directHours').value),keywords:$('directKeywords').value,account:$('directAccount').value,platform:$('directPlatform').value});out.classList.remove('loading');out.textContent=body.display;saveResult(body)}catch(e){fail(out,e)}}
async function offlineRun(){const holder=$('offlineOutput');holder.innerHTML='<pre class="output loading">查询中，请稍候……</pre>';try{const body=await callApi('/api/offline',{query:$('offlineInput').value,limit:3});const rows=body.result.items||[];holder.innerHTML=rows.length?rows.map((r,i)=>`<div class="offline-card"><b>${i+1}. [${escapeHtml(r.module)}] ${escapeHtml(r.title)}</b><div>${escapeHtml(r.source||'')} · ${escapeHtml(r.published_at||'时间未知')}</div>${r.url?`<a href="${escapeHtml(r.url)}" target="_blank">查看原文</a>`:''}<p>${escapeHtml((r.summary||r.content||'').slice(0,350))}</p></div>`).join(''):'<pre class="output">未检索到结果。</pre>'}catch(e){holder.innerHTML='<pre class="output"><span class="error">[查询失败] '+escapeHtml(e.message)+'</span></pre>'}}
async function monitorRun(action){const out=$('monitorOutput');busy(out,true);try{const body=await callApi('/api/monitor',{action,url:$('monitorUrl').value,account:$('monitorAccount').value,platform:$('monitorPlatform').value});out.classList.remove('loading');out.textContent=body.display||JSON.stringify(body.result,null,2)}catch(e){fail(out,e)}}
async function briefing(){if(!lastResult)return;const buttons=document.querySelectorAll('.briefing');buttons.forEach(b=>b.disabled=true);try{const body=await callApi('/api/briefing',{result:lastResult,channels:['web_ui']});alert('简报生成完成：'+JSON.stringify(body.result));}catch(e){alert('简报失败：'+e.message)}finally{buttons.forEach(b=>b.disabled=false)}}
async function loadStatus(){const grid=$('serviceGrid'),summary=$('statusSummary');grid.innerHTML='<div class="service">正在检查……</div>';summary.textContent='检查中……';try{const body=await callApi('/api/status');const all=[...Object.values(body.services.mcp),...Object.values(body.services.a2a)];const ok=all.filter(x=>x.ok).length;summary.textContent=`服务 ${ok}/${all.length} 在线`;grid.innerHTML=all.map(s=>`<div class="service ${s.ok?'ok':'bad'}"><b><span class="dot"></span>${escapeHtml(s.name)}</b><small>${escapeHtml(s.url)}${s.tools&&s.tools.length?'<br>工具：'+escapeHtml(s.tools.join(', ')):''}</small></div>`).join('')}catch(e){summary.textContent='状态检查失败';grid.innerHTML='<div class="service bad">'+escapeHtml(e.message)+'</div>'}}
loadStatus();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    # 启动 Flask 开发服务器：仅监听本机 127.0.0.1:8080。
    # debug=False 关闭自动重载（避免多线程 / 懒加载 Agent 被重复初始化）；
    # threaded=True 允许并发处理多个 HTTP 请求（前端各面板会同时轮询状态）。
    logger.info("[app] Web 测试入口启动：http://127.0.0.1:8080")
    app.run(host="127.0.0.1", port=8080, debug=False, threaded=True)
