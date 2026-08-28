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
    """
    return Message(
        content=TextContent(text=f"task:{task_type}"),
        role=MessageRole.USER,
        metadata=Metadata(custom_fields={
            "task_type": task_type,
            "params": params or {},
            "from_agent": from_agent,
        }),
    )


def parse_task(message: Message) -> Tuple[str, Dict[str, Any], str]:
    """从收到的 A2A 消息里解析 (task_type, params, from_agent)

    与 send_task 对应：把 metadata.custom_fields 里的路由信息取回来。
    """
    meta = (message.metadata.custom_fields if message.metadata else {}) or {}
    return (meta.get("task_type", "text"),
            meta.get("params", {}) or {},
            meta.get("from_agent", "?"))


def reply_text(text: str, parent_message_id: Optional[str] = None,
               conversation_id: Optional[str] = None) -> Message:
    """构造回传消息（AGENT 角色，挂上父消息 / 会话 ID）"""
    return Message(
        content=TextContent(text=text),
        role=MessageRole.AGENT,
        parent_message_id=parent_message_id,
        conversation_id=conversation_id,
    )


# ===== 回传结果编码（dataclass → dict，保证 DTO 经 A2A 可逆往返）=====
def _json_default(obj):
    """JSON 编码兜底：dataclass 实例 → asdict 递归成 dict；其余 → str"""
    if is_dataclass(obj):
        return asdict(obj)
    return str(obj)


def encode_result(ok: bool, result, error=None) -> str:
    """把 A2A 执行结果编码成回传 JSON。

    各 Agent 的 handle_message 统一用它回传 {ok, result, error}；
    result 里的 dataclass（NewsItem / EventCluster / HotEvent ...）会被 asdict
    序列化成 dict，对端可经 task_pipelines.schemas.dto_from_dict 还原成 DTO。
    """
    return json.dumps({"ok": ok, "result": result, "error": error},
                      ensure_ascii=False, default=_json_default)


# ===== A2A 网络（真实 HTTP 委派，对齐 SmartVoyage）=====
# 子 agent 独立部署为 A2A 服务器；coordinator 经 AgentNetwork 投递任务（HTTP）。
# 端口与各子 agent 的 AgentCard.url 一致。
_AGENT_ENDPOINTS = {
    "collector": "http://127.0.0.1:8001",
    "processor": "http://127.0.0.1:8002",
    "publisher": "http://127.0.0.1:8003",
    "video": "http://127.0.0.1:8008",
    "account_monitor": "http://127.0.0.1:8009",
}
_network = None  # 懒加载：首次委派才建网络，服务器没起不影响 import
# 连接失败特征串（A2AClient 把连不上也封装成 status=FAILED，据此识别「服务器未启动」）
_CONN_ERROR_HINTS = (
    "Max retries exceeded",
    "Failed to establish a new connection",
    "NewConnectionError",
    "ConnectionError",
    "WinError 10061",  # 目标计算机积极拒绝（Windows 连接被拒）
)


def _get_client(name: str):
    """取 A2A 客户端；首次访问某 agent 才 add（连不上时不抛异常，get_agent 返回 None）"""
    global _network
    if _network is None:
        _network = AgentNetwork()
    client = _network.get_agent(name)
    if client is None and name not in _network.agent_urls:  # agent_urls 兼作「尝试过」标记
        _network.add(name, _AGENT_ENDPOINTS[name])
        client = _network.get_agent(name)
    return client


def _http_delegate(agent_name: str, task_type: str,
                   params: Optional[Dict[str, Any]] = None,
                   from_agent: str = "?") -> str:
    """经 AgentNetwork HTTP 投递任务给子 agent 服务器，等待回传（真实 A2A，对齐 SmartVoyage）

    链路：Task(id, message=send_task(...)) → A2AClient.send_task_async → {url}/tasks/send
    → 服务端默认 handle_task 桥接子 agent.handle_message → 回传文本进 artifacts → 这里取回。
    """
    client = _get_client(agent_name)
    if client is None:
        raise RuntimeError(
            f"子 agent「{agent_name}」服务器未启动？请先运行 "
            f"python -m agents.{agent_name}_agent --serve"
        )
    logger.info(f"[handoff] delegate -> {agent_name} task={task_type}")
    task = Task(id=str(uuid.uuid4()),
                message=send_task(task_type, params, from_agent).to_dict())
    raw = asyncio.run(client.send_task_async(task))
    # 完成态：子 agent 回传的 {ok,result,error} JSON 文本在 artifacts[0].parts[0].text
    if raw.status.state == TaskState.COMPLETED:
        for part in (raw.artifacts or [{}])[0].get("parts", []):
            if part.get("type") == "text" and "text" in part:
                return part["text"]
        return json.dumps({"ok": False, "error": f"{agent_name} 未返回文本"},
                          ensure_ascii=False)
    # 非完成态（FAILED / INPUT_REQUIRED）：把 status.message 里的错误透出，保持 {ok,result,error} 契约
    msg = raw.status.message
    error = msg.get("error") if isinstance(msg, dict) else (str(msg) if msg else "")
    if error and any(hint in error for hint in _CONN_ERROR_HINTS):
        # A2AClient 把「服务器连不上」也封装成 FAILED；转成可操作的提示
        raise RuntimeError(
            f"子 agent「{agent_name}」服务器未启动？请先运行 "
            f"python -m agents.{agent_name}_agent --serve"
        )
    logger.error(f"[handoff] delegate -> {agent_name} 未完成: {raw.status.state} {error}")
    return json.dumps({"ok": False, "error": error or f"{agent_name} 未完成任务（{raw.status.state}）"},
                      ensure_ascii=False)


# ===== 委派（A2A 交接入口）=====
def delegate(target, task_type: str, params: Optional[Dict[str, Any]] = None,
             from_agent: str = "?") -> str:
    """向目标 Agent 委派任务并等待回传，返回回传文本（JSON 字符串）

    target 传 agent 名字符串 → 经 AgentNetwork HTTP 投递（真实 A2A，coordinator 使用）；
    target 传 A2AServer 实例 → 同进程直接调用 handle_message（各子 agent __main__ 演示用）。
    """
    if isinstance(target, str):
        return _http_delegate(target, task_type, params, from_agent)
    logger.info(f"[handoff] delegate -> {getattr(target, 'name', '?')} task={task_type}")
    if target is None:
        return json.dumps({"ok": False, "error": "目标 Agent 未注册（target is None）"}, ensure_ascii=False)
    reply = target.handle_message(send_task(task_type, params, from_agent))
    return reply.content.text if hasattr(reply.content, "text") else str(reply.content)


# ===== 反问用户 =====
def ask_user_confirm(prompt: str) -> bool:
    """反问用户（如“是否生成简报？”），返回用户确认结果（True 生成 / False 跳过）

    【模拟】真实实现：走前端 / IM 通道收集用户回复。
    当前版本：终端 input() 交互，输入 y/yes/是 视为确认，其余视为放弃。
    """
    try:
        answer = input(f"{prompt} [y/N]: ").strip().lower()
    except EOFError:
        answer = ""
    return answer in ("y", "yes", "是")
