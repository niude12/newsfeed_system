"""自主 Agent 运行时的稳定数据协议。"""

from dataclasses import asdict, dataclass, field, is_dataclass
from typing import Any, Dict, List, Literal, Optional


def jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return {k: jsonable(v) for k, v in asdict(value).items()}
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    return value


@dataclass
class AgentAction:
    action: Literal["delegate", "ask_user", "finish"]
    reason: str = ""
    agent: Optional[str] = None
    skill: Optional[str] = None
    arguments: Dict[str, Any] = field(default_factory=dict)
    message: str = ""
    output_ref: Optional[int] = None


@dataclass
class AgentObservation:
    step_id: int
    agent: str
    skill: str
    status: Literal["success", "error", "blocked"]
    result: Any = None
    error: Optional[str] = None
    duration_ms: int = 0

    def prompt_summary(self) -> Dict[str, Any]:
        value = jsonable(self.result)
        if isinstance(value, list):
            summary: Any = {"type": "list", "count": len(value), "sample": value[:2]}
        elif isinstance(value, dict):
            summary = {"type": "object", "keys": list(value)[:20], "value": value}
        else:
            summary = value
        return {
            "step_id": self.step_id,
            "agent": self.agent,
            "skill": self.skill,
            "status": self.status,
            "result": summary,
            "error": self.error,
        }


@dataclass
class AgentRunResult:
    status: Literal["completed", "input_required", "failed"]
    task_type: str
    output: Any = None
    message: str = ""
    error: Optional[str] = None
    trace: List[AgentObservation] = field(default_factory=list)
    iterations: int = 0
    elapsed_ms: int = 0
