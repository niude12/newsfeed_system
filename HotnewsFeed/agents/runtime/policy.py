"""Agent 动作的确定性安全边界。"""

from typing import Dict, Optional

from agents.runtime.schemas import AgentAction


CONFIRMATION_SKILLS = {"briefing", "stop_monitor"}


def validate_action(action: AgentAction, allowed: Dict[str, set], user_text: str) -> Optional[str]:
    if action.action != "delegate":
        return None
    if not action.agent or action.agent not in allowed:
        return f"不可用的 Agent: {action.agent!r}"
    if not action.skill or action.skill not in allowed[action.agent]:
        return f"Agent {action.agent!r} 不提供 skill {action.skill!r}"
    if action.agent == "publisher":
        explicit_publish = any(word in user_text.lower() for word in ("推送", "发布", "生成简报", "发送简报"))
        if not explicit_publish:
            return "调用 PublisherAgent 前需要用户明确要求生成或推送简报"
    if action.skill in CONFIRMATION_SKILLS:
        explicit = any(word in user_text.lower() for word in ("确认", "同意", "推送", "发布", "停止", "取消监控"))
        if not explicit:
            return f"调用 {action.skill} 前需要用户明确确认"
    return None
