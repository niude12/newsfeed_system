#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A2A 适配层：基于官方包 python_a2a 的便捷封装

协议类型（Message / MessageRole / TextContent / AgentCard / A2AServer ...）
全部来自官方包 python_a2a，本项目不再手写协议类型。
这里只保留几个项目常用的小工具：

  send_task         构造一条 A2A 委派消息（task_type / params 放进 metadata）
  parse_task        从收到的消息里解析 (task_type, params, from_agent)
  delegate          向目标 Agent 委派任务（agent 名 → 真实 HTTP A2A；A2AServer 实例 → 同进程演示）
  reply_text        构造回传消息
  ask_user_confirm  反问用户（简报交接的确认）

注意：业务路由信息（task_type / params）放在 Message.metadata.custom_fields 里，
它是 python_a2a 的通用扩展字段，随消息一起序列化，走 HTTP 也不会丢。

模块依赖:
- ``python_a2a``           : 官方 A2A 协议包。本模块只 import 官方类型，不手写协议。
  - ``Message``            : 一条 A2A 消息，含 content / role / message_id / metadata 等。
  - ``MessageRole``        : 消息角色枚举（USER / AGENT）。
  - ``TextContent``        : 文本内容载体，放在 Message.content 里。
  - ``Metadata``           : 消息元数据，custom_fields 是通用扩展字段（业务路由放这里）。
  - ``Task``               : A2A 任务对象，含 id / message / status / artifacts 等。
  - ``TaskState``          : 任务状态枚举（COMPLETED / FAILED / INPUT_REQUIRED 等）。
  - ``A2AServer``          : A2A 服务器基类，子 agent 继承它实现 handle_message。
  - ``AgentNetwork``       : A2A 客户端网络，按 agent 名管理 HTTP 连接并投递任务。
- ``create_logger.logger`` : 全局日志器（delegate 委派日志 [handoff] 用）。

典型调用链（真实 HTTP A2A 委派）::

    coordinator 调用 delegate("collector", "collect_news", {...})
      -> _http_delegate("collector", ...)
      -> _get_client("collector")          # 取/建 A2A 客户端（AgentNetwork）
      -> Task(id=uuid, message=send_task(...).to_dict())
      -> client.send_task_async(task)      # HTTP POST {url}/tasks/send
      -> 子 agent 服务端 handle_task 桥接 handle_message
      -> 回传文本进 raw.artifacts[0].parts[0].text
      -> 返回 {ok, result, error} JSON 字符串

对外暴露的接口（同时由 a2a/__init__.py 再导出）：
- send_task / parse_task / reply_text : 消息构造与解析。
- delegate                            : 委派任务（名字符串走 HTTP，A2AServer 实例走同进程）。
- ask_user_confirm                    : 终端反问用户（简报交接确认，模拟实现）。
- encode_result / _json_default       : 回传结果编码（dataclass → dict → JSON）。
"""

import asyncio
import json
import uuid
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Optional, Tuple

from python_a2a import (A2AServer, AgentNetwork, Message, MessageRole,
                        Metadata, Task, TaskState, TextContent)

from create_logger import logger


# ===== 委派消息 / 解析 / 回传 =====
def send_task(task_type: str, params: Optional[Dict[str, Any]] = None,
              from_agent: str = "?") -> Message:
    """构造一条 A2A 委派消息：task_type / params 放进 metadata.custom_fields

    python_a2a 的 Message 本体只带 content / role，业务路由信息放 metadata，
    无论同进程调用还是 HTTP 投递都能完整携带。

    参数:
        task_type:  业务任务类型（如 "collect_news" / "monitor_status"），
                    会同时写入 content 文本与 metadata.custom_fields。
        params:     任务参数字典，写入 metadata.custom_fields["params"]；
                    传 None 时用空字典 {} 兜底。
        from_agent: 发送方 agent 标识（默认 "?"），用于接收方做来源跟踪。

    返回:
        Message：一条 role=USER 的 A2A 委派消息，可直接投递或经 .to_dict() 序列化。

    抛出:
        不抛异常（纯数据构造）。

    说明:
        - ``Message``    : python_a2a 的消息模型，content 放文本、role 标角色、
          metadata 放业务元数据。
        - ``TextContent``: 文本内容载体，构造参数 text 即消息正文。
        - ``MessageRole``: 角色枚举，这里用 USER 表示「用户向 Agent 发起任务」。
        - ``Metadata``   : 元数据模型，custom_fields 是任意 dict，随消息序列化，
          走 HTTP 也不会丢——这就是业务路由信息放这里的根本原因。
    """
    # 构造一条 python_a2a 的 Message：content 放文本、role 标角色、metadata 带业务路由。
    return Message(
        content=TextContent(text=f"task:{task_type}"),  # 正文以 task: 前缀标记任务类型。
        role=MessageRole.USER,                          # 委派消息一律以 USER 角色发出。
        metadata=Metadata(custom_fields={
            "task_type": task_type,   # 路由键：接收方 handle_message 据此分发。
            "params": params or {},   # 业务参数：无参数时兜底为空字典。
            "from_agent": from_agent, # 来源标记：便于日志 / 排障。
        }),
    )


def parse_task(message: Message) -> Tuple[str, Dict[str, Any], str]:
    """从收到的 A2A 消息里解析 (task_type, params, from_agent)

    与 send_task 对应：把 metadata.custom_fields 里的路由信息取回来。

    参数:
        message: 收到的 A2A 消息（Message 实例）。

    返回:
        Tuple[str, Dict[str, Any], str]：依次为
        - task_type : 任务类型；缺省回退 "text"。
        - params    : 任务参数字典；缺省或为 None 时回退空字典 {}。
        - from_agent: 来源 agent 标识；缺省回退 "?"。

    抛出:
        不抛异常（纯字段读取，取不到用默认值兜底）。

    说明:
        message.metadata 可能为 None（上游消息没带 metadata 时），用 ``if message.metadata
        else {}`` 防御；custom_fields 也可能不存在，再套一层 ``or {}`` 双保险。
        返回值与 send_task 写入的键一一对应，保证「构造 → 解析」可逆往返。
    """
    # 先防御 metadata 为 None，再取 custom_fields（可能不存在，用 or {} 兜底）。
    meta = (message.metadata.custom_fields if message.metadata else {}) or {}
    # 按 send_task 写入的顺序取出三个路由字段；缺省用默认值兜底，保证解析永不报错。
    return (meta.get("task_type", "text"),
            meta.get("params", {}) or {},
            meta.get("from_agent", "?"))


def reply_text(text: str, parent_message_id: Optional[str] = None,
               conversation_id: Optional[str] = None) -> Message:
    """构造回传消息（AGENT 角色，挂上父消息 / 会话 ID）

    各子 agent 的 handle_message 收到委派后，用它在同一会话里回传给调用方。

    参数:
        text:             回传正文（一般放 encode_result 生成的 JSON 字符串）。
        parent_message_id: 被回复的父消息 ID（原委派消息的 message_id），
                           用于串起「请求 → 应答」的会话上下文。
        conversation_id:   会话 ID（同一轮对话的多个消息共享），可为 None。

    返回:
        Message：一条 role=AGENT 的 A2A 回传消息。

    抛出:
        不抛异常（纯数据构造）。

    说明:
        ``MessageRole.AGENT`` 表示消息来自 Agent（服务端），与委派时的 USER 角色对应；
        挂上 parent_message_id / conversation_id 是为了让对端把回复关联回原任务。
    """
    # 构造一条 role=AGENT 的回传消息，挂上父消息 / 会话 ID 以便对端关联回原任务。
    return Message(
        content=TextContent(text=text),
        role=MessageRole.AGENT,                    # 回传消息一律以 AGENT 角色发出。
        parent_message_id=parent_message_id,       # 关联父消息，保持会话上下文。
        conversation_id=conversation_id,           # 关联会话 ID。
    )


# ===== 回传结果编码（dataclass → dict，保证 DTO 经 A2A 可逆往返）=====
def _json_default(obj):
    """JSON 编码兜底：dataclass 实例 → asdict 递归成 dict；其余 → str

    参数:
        obj: json.dumps 遇到无法直接序列化的对象时回调进来的任意 Python 对象。

    返回:
        dict 或 str：dataclass 实例递归转成 dict，其它对象一律转成字符串。

    抛出:
        不抛异常（str 兜底保证任何对象都能被序列化）。

    说明:
        is_dataclass(obj) 判断是否为 dataclass 实例；asdict(obj) 递归把 dataclass
        及其嵌套子 dataclass 全部转成 dict，是 DTO 走 JSON 往返的关键。
    """
    # dataclass 实例要递归转成 dict，否则 json.dumps 无法直接序列化。
    if is_dataclass(obj):
        return asdict(obj)  # dataclass → dict（递归处理嵌套字段）。
    return str(obj)         # 其余类型（datetime / 自定义对象等）退化为字符串。


def encode_result(ok: bool, result, error=None) -> str:
    """把 A2A 执行结果编码成回传 JSON。

    各 Agent 的 handle_message 统一用它回传 {ok, result, error}；
    result 里的 dataclass（NewsItem / EventCluster / HotEvent ...）会被 asdict
    序列化成 dict，对端可经 task_pipelines.schemas.dto_from_dict 还原成 DTO。

    参数:
        ok:     是否成功（True / False），作为回传契约的第一键。
        result: 成功时的业务结果（DTO / dict / list 等）；失败时通常传 None。
        error:  失败时的错误信息字符串；成功时为 None。

    返回:
        str：JSON 字符串，形如 {"ok": true, "result": ..., "error": null}，
        保证 ensure_ascii=False（中文不转义）便于对端直接阅读。

    抛出:
        不抛异常（json.dumps 内置 default=_json_default，兜底任意类型）。

    说明:
        该 JSON 就是 A2A 回传消息的正文，各 Agent 的 handle_message 统一用它封装
        结果，对端再用 json.loads 解析并还原 DTO，形成统一的 {ok,result,error} 契约。
    """
    # 统一编码成 {ok, result, error} 契约 JSON：中文不转义（ensure_ascii=False），
    # 遇 dataclass 等不可序列化对象走 _json_default 兜底转 dict/str。
    return json.dumps({"ok": ok, "result": result, "error": error},
                      ensure_ascii=False, default=_json_default)


# ===== A2A 网络（真实 HTTP 委派，对齐 SmartVoyage）=====
# 子 agent 独立部署为 A2A 服务器；coordinator 经 AgentNetwork 投递任务（HTTP）。
# 端口与各子 agent 的 AgentCard.url 一致。
_AGENT_ENDPOINTS = {
    "collector": "http://127.0.0.1:8001",        # 采集子 agent 的 HTTP A2A 端点。
    "processor": "http://127.0.0.1:8002",        # 加工子 agent 的 HTTP A2A 端点。
    "publisher": "http://127.0.0.1:8003",        # 发布子 agent 的 HTTP A2A 端点。
    "account_monitor": "http://127.0.0.1:8009",  # 账户监控子 agent 的 HTTP A2A 端点。
}
_network = None  # 懒加载：首次委派才建网络，服务器没起不影响 import
# 连接失败特征串（A2AClient 把连不上也封装成 status=FAILED，据此识别「服务器未启动」）
_CONN_ERROR_HINTS = (
    "Max retries exceeded",                       # urllib3 重试耗尽。
    "Failed to establish a new connection",       # 连接建立失败。
    "NewConnectionError",                         # 新连接错误。
    "ConnectionError",                            # 通用连接错误。
    "WinError 10061",  # 目标计算机积极拒绝（Windows 连接被拒）
)
# 超时特征串：A2A 客户端 30s 总超时，超时经 _send_task 包成 FAILED、error 形如
# "Read timed out"。与服务端未启动不同——服务端很可能仍在继续处理，重试会重复干活，
# 因此单独识别并回传可操作提示（不按「服务器未启动」抛 RuntimeError）。
_TIMEOUT_HINTS = (
    "Read timed out",        # urllib3 读超时：客户端等不及服务端返回。
    "Connection timed out",  # 连接超时。
    "timed out",             # 兜底：任何带 "timed out" 的错误文本。
)


def _get_client(name: str):
    """取 A2A 客户端；首次访问某 agent 才 add（连不上时不抛异常，get_agent 返回 None）

    参数:
        name: 子 agent 名字符串（须在 _AGENT_ENDPOINTS 里有对应端点）。

    返回:
        A2A 客户端对象；若该 agent 尚未注册 / 服务器未启动，可能返回 None。

    抛出:
        KeyError: name 不在 _AGENT_ENDPOINTS 里且网络首次建立时会抛（调用方传入非法 agent 名）。

    说明:
        AgentNetwork 是 python_a2a 的 HTTP 客户端网络：add(name, url) 注册端点，
        get_agent(name) 取回对应 A2A 客户端（内部持有到 {url}/tasks/send 的连接）。
        这里用 _network.agent_urls 兼作「尝试过」标记，避免对同一 agent 重复 add。
    """
    global _network
    # 懒加载：全局只建一个 AgentNetwork，首次委派时才初始化。
    if _network is None:
        _network = AgentNetwork()
    # 先按名字取已有客户端（AgentNetwork 内部按名字缓存连接）。
    client = _network.get_agent(name)
    # 取不到且没尝试过 → 注册端点后再取一次（agent_urls 兼作「尝试过」标记）。
    if client is None and name not in _network.agent_urls:
        _network.add(name, _AGENT_ENDPOINTS[name])  # 用预置端点表注册该 agent 的 URL。
        client = _network.get_agent(name)           # 注册后再取一次，拿到可用的客户端。
    return client  # 可能为 None：服务器未启动时 get_agent 返回 None 而非抛异常。


def _http_delegate(agent_name: str, task_type: str,
                   params: Optional[Dict[str, Any]] = None,
                   from_agent: str = "?") -> str:
    """经 AgentNetwork HTTP 投递任务给子 agent 服务器，等待回传（真实 A2A，对齐 SmartVoyage）

    链路：Task(id, message=send_task(...)) → A2AClient.send_task_async → {url}/tasks/send
    → 服务端默认 handle_task 桥接子 agent.handle_message → 回传文本进 artifacts → 这里取回。

    参数:
        agent_name: 子 agent 名字符串（须在 _AGENT_ENDPOINTS 里，如 "collector"）。
        task_type:  业务任务类型（如 "collect_news"），透传给 send_task。
        params:     任务参数字典，透传给 send_task。
        from_agent: 发送方 agent 标识，透传给 send_task。

    返回:
        str：子 agent 回传的文本（即 {ok, result, error} JSON 字符串）。
        完成态取 artifacts 里的文本；非完成态也按 {ok,result,error} 契约拼一个 JSON 返回。

    抛出:
        RuntimeError: 子 agent 服务器未启动（连接被拒等特征错误），提示先运行
                      ``python -m agents.{agent_name}_agent --serve``。

    说明:
        - ``Task``            : python_a2a 的任务模型，id 用 uuid4 生成唯一任务号，
          message 传 send_task 序列化后的 dict（.to_dict()）。
        - ``send_task_async`` : A2A 客户端发起 HTTP 投递的异步方法，这里用 asyncio.run
          从同步上下文驱动它（本模块没有事件循环常驻）。
        - ``TaskState``       : 任务状态枚举；COMPLETED 表示子 agent 已完成并回传文本。
        - 回传文本约定放在 ``raw.artifacts[0].parts[0].text``（A2A artifact 结构）。
        - A2AClient 把「服务器连不上」也封装成 FAILED，因此要用 _CONN_ERROR_HINTS
          从错误信息里识别真正的「未启动」，转成可操作的 RuntimeError 提示。
    """
    client = _get_client(agent_name)  # 先取（必要时注册）目标 agent 的 A2A 客户端。
    if client is None:
        # 客户端都没取到 → 说明该 agent 未注册或网络初始化失败。
        raise RuntimeError(
            f"子 agent「{agent_name}」服务器未启动？请先运行 "
            f"python -m agents.{agent_name}_agent --serve"
        )
    logger.info(f"[handoff] delegate -> {agent_name} task={task_type}")  # 记录一次委派动作，便于排障。
    # 组装 A2A 任务：唯一 ID + 委派消息（send_task 构造后转 dict）。
    task = Task(id=str(uuid.uuid4()),
                message=send_task(task_type, params, from_agent).to_dict())
    # send_task_async 是异步方法，用 asyncio.run 在同步上下文里驱动（会阻塞到回传）。
    raw = asyncio.run(client.send_task_async(task))
    # 完成态：子 agent 回传的 {ok,result,error} JSON 文本在 artifacts[0].parts[0].text
    if raw.status.state == TaskState.COMPLETED:
        # 遍历第一条 artifact 的 parts，取 text 类型部分作为回传文本。
        for part in (raw.artifacts or [{}])[0].get("parts", []):
            if part.get("type") == "text" and "text" in part:
                return part["text"]
        # 完成但没有文本 → 按契约回一个失败 JSON。
        return json.dumps({"ok": False, "error": f"{agent_name} 未返回文本"},
                          ensure_ascii=False)
    # 非完成态（FAILED / INPUT_REQUIRED）：把 status.message 里的错误透出，保持 {ok,result,error} 契约
    msg = raw.status.message  # status.message 可能是 dict 也可能是字符串，需兼容解析。
    error = msg.get("error") if isinstance(msg, dict) else (str(msg) if msg else "")
    # 命中超时特征串 → 子 agent 可能仍在继续处理；提示不要盲目重试，避免重复干活。
    if error and any(hint in error for hint in _TIMEOUT_HINTS):
        return json.dumps(
            {"ok": False,
             "error": f"子 agent「{agent_name}」处理超过 A2A 30s 超时（{error}）。"
                      "服务端可能仍在继续处理，请稍后查看结果；直接重试可能造成重复采集/处理"},
            ensure_ascii=False,
        )
    # 命中连接失败特征串（服务器连不上/连接被拒）→ 说明是「未启动」而非业务失败。
    if error and any(hint in error for hint in _CONN_ERROR_HINTS):
        # A2AClient 把「服务器连不上」也封装成 FAILED；转成可操作的提示
        raise RuntimeError(
            f"子 agent「{agent_name}」服务器未启动？请先运行 "
            f"python -m agents.{agent_name}_agent --serve"
        )
    logger.error(f"[handoff] delegate -> {agent_name} 未完成: {raw.status.state} {error}")
    # 普通失败（非连接问题）：把错误原文塞进契约 JSON 回传，让上层看到具体原因。
    return json.dumps({"ok": False, "error": error or f"{agent_name} 未完成任务（{raw.status.state}）"},
                      ensure_ascii=False)


# ===== 委派（A2A 交接入口）=====
def delegate(target, task_type: str, params: Optional[Dict[str, Any]] = None,
             from_agent: str = "?") -> str:
    """向目标 Agent 委派任务并等待回传，返回回传文本（JSON 字符串）

    target 传 agent 名字符串 → 经 AgentNetwork HTTP 投递（真实 A2A，coordinator 使用）；
    target 传 A2AServer 实例 → 同进程直接调用 handle_message（各子 agent __main__ 演示用）。

    参数:
        target:     目标 Agent。可以是名字符串（走 HTTP）或 A2AServer 实例（走同进程），
                    传 None 时返回「目标未注册」的失败 JSON。
        task_type:  业务任务类型（如 "collect_news" / "monitor_status"）。
        params:     任务参数字典，透传给 send_task。
        from_agent: 发送方 agent 标识，透传给 send_task。

    返回:
        str：目标 Agent 回传的文本（JSON 字符串，遵循 {ok, result, error} 契约）。
        目标是 A2AServer 实例时，取 reply.content.text 作为回传文本。

    抛出:
        RuntimeError: 目标是名字符串且服务器未启动时由 _http_delegate 抛出。

    说明:
        这是 coordinator 委派任务给子 agent 的统一入口：生产环境用名字符串走真实
        HTTP A2A（见 _http_delegate）；本地 / 单测用 A2AServer 实例直接调其
        handle_message 方法（不经过网络，同进程同步完成）。
    """
    # 按 target 类型分流：名字符串走真实 HTTP A2A 委派，实例走同进程直调。
    if isinstance(target, str):
        return _http_delegate(target, task_type, params, from_agent)
    # 以下是 A2AServer 实例（同进程委派）分支。
    logger.info(f"[handoff] delegate -> {getattr(target, 'name', '?')} task={task_type}")
    if target is None:
        # 目标为 None 时无法委派，按契约返回失败 JSON。
        return json.dumps({"ok": False, "error": "目标 Agent 未注册（target is None）"}, ensure_ascii=False)
    # 直接同步调用目标 handle_message（A2AServer 的方法），传入 send_task 构造的消息。
    reply = target.handle_message(send_task(task_type, params, from_agent))
    # 回传正文：优先取 content.text（TextContent），否则退化为 str(reply.content)。
    return reply.content.text if hasattr(reply.content, "text") else str(reply.content)


# ===== 反问用户 =====
def ask_user_confirm(prompt: str) -> bool:
    """反问用户（如“是否生成简报？”），返回用户确认结果（True 生成 / False 跳过）

    参数:
        prompt: 向用户展示的提问文本（会附加 "[y/N]: " 提示后缀）。

    返回:
        bool：True 表示用户确认（输入 y/yes/是）；False 表示放弃（其它输入）。

    抛出:
        不抛异常——输入流关闭（EOFError）时按「放弃」处理。

    说明:
        【模拟】真实实现：走前端 / IM 通道收集用户回复。
        当前版本：终端 input() 交互，输入 y/yes/是 视为确认，其余视为放弃。
    """
    try:
        # 读取终端输入：去掉首尾空白并转小写，统一大小写比较。
        answer = input(f"{prompt} [y/N]: ").strip().lower()
    except EOFError:
        answer = ""  # 输入流被关闭（如管道结束）时视为放弃。
    # 只有 y/yes/是 视为确认，其余（含回车、n、其它）一律当作放弃。
    return answer in ("y", "yes", "是")
