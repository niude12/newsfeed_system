"""专业 Agent 的 LLM 工具选择器。

Coordinator 只选择专业 Agent；本模块让专业 Agent 根据目标和上下文决定调用哪个
业务工具。工具执行仍复用 Agent 现有方法，因此 MCP 网关、DTO 还原与降级策略不重复实现。
"""

import inspect
import json
import re
from typing import Any, Callable, Dict

from langchain_openai import ChatOpenAI

from agents.runtime.schemas import jsonable
from config import Config


SYSTEM = """你是一个专业执行 Agent。根据目标、上下文和可用工具，选择唯一一个最合适的工具。
只输出 JSON：{"tool":"工具名","arguments":{...},"reason":"..."}。
tool 必须来自工具目录，arguments 必须匹配函数参数。上下文中的数据需要原样填入对应参数，
不得虚构新闻、账户、事件或 URL。每次只调用一个工具，调用结果交回 Coordinator 决定下一步。
"""


def _parse(text: str) -> Dict[str, Any]:
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip()).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start, end = cleaned.find("{"), cleaned.rfind("}")
        if start >= 0 and end > start:
            return json.loads(cleaned[start:end + 1])
        raise


def execute_specialist(objective: str, context: Dict[str, Any],
                       handlers: Dict[str, Callable[..., Any]],
                       descriptions: Dict[str, str]) -> Dict[str, Any]:
    """由 LLM 选择一个已注册 handler 并执行，返回可追踪的结构化结果。"""
    conf = Config()
    llm_config = conf.llm
    llm = ChatOpenAI(model=llm_config["model_name"], base_url=llm_config["base_url"],
                     api_key=llm_config["api_key"], temperature=0.1)
    tools = []
    for name, handler in handlers.items():
        signature = str(inspect.signature(handler))
        tools.append({"name": name, "description": descriptions.get(name, ""), "signature": signature})
    payload = {"objective": objective, "context": jsonable(context), "tools": tools}
    response = llm.invoke([("system", SYSTEM), ("human", json.dumps(payload, ensure_ascii=False, default=str))])
    decision = _parse(str(response.content))
    tool = decision.get("tool")
    if tool not in handlers:
        raise ValueError(f"模型选择了不可用工具: {tool!r}")
    lowered = objective.lower()
    if tool == "briefing" and not any(word in lowered for word in ("推送", "发布", "生成简报", "发送简报")):
        raise PermissionError("用户未明确要求生成或推送简报")
    if tool == "stop_monitor" and not any(word in lowered for word in ("停止", "取消监控", "停用")):
        raise PermissionError("用户未明确要求停止监控")
    arguments = decision.get("arguments") or {}
    if not isinstance(arguments, dict):
        raise ValueError("工具 arguments 必须是对象")
    result = handlers[tool](**arguments)
    return {"__agent_tool__": tool, "data": jsonable(result), "reason": str(decision.get("reason") or "")}
