"""Plan → Execute → Observe → Replan 的 Coordinator 循环。"""

import json
import re
import time
from dataclasses import asdict
from typing import Any, Callable, Dict, List, Optional

from langchain_openai import ChatOpenAI

from a2a.protocol import delegate
from agents.runtime.agent_catalog import allowed_skill_map, build_agent_catalog
from agents.runtime.policy import validate_action
from agents.runtime.schemas import AgentAction, AgentObservation, AgentRunResult, jsonable
from config import Config
from create_logger import logger
from prompt.coordinator_prompt import SYSTEM_PROMPT


def _parse_json(text: str) -> Dict[str, Any]:
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip()).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start, end = cleaned.find("{"), cleaned.rfind("}")
        if start >= 0 and end > start:
            return json.loads(cleaned[start:end + 1])
        raise


def _resolve_refs(value: Any, observations: List[AgentObservation]) -> Any:
    if isinstance(value, dict):
        if set(value) == {"$observation"}:
            index = int(value["$observation"])
            if index < 0 or index >= len(observations):
                raise ValueError(f"观察引用不存在: {index}")
            observation = observations[index]
            if observation.status != "success":
                raise ValueError(f"观察 {index} 不是成功结果")
            if isinstance(observation.result, dict) and "__agent_tool__" in observation.result:
                return observation.result.get("data")
            return observation.result
        return {key: _resolve_refs(item, observations) for key, item in value.items()}
    if isinstance(value, list):
        return [_resolve_refs(item, observations) for item in value]
    return value


class CoordinatorLoop:
    """能力目录驱动的单步决策循环；执行器不包含业务流程分支。"""

    def __init__(self, config: Optional[Config] = None,
                 delegate_fn: Callable[..., str] = delegate,
                 planner: Optional[Callable[[str], Dict[str, Any]]] = None):
        self.config = config or Config()
        runtime = self.config.agent_runtime
        self.max_iterations = runtime["max_iterations"]
        self.max_agent_calls = runtime["max_agent_calls"]
        self.max_total_seconds = runtime["max_total_seconds"]
        self.delegate_fn = delegate_fn
        self._planner_override = planner
        self.catalog = build_agent_catalog()
        self.allowed = allowed_skill_map(self.catalog)
        llm_config = self.config.llm
        self.llm = None if planner else ChatOpenAI(
            model=llm_config["model_name"],
            base_url=llm_config["base_url"],
            api_key=llm_config["api_key"],
            temperature=0.1,
        )

    def _plan(self, user_text: str, observations: List[AgentObservation]) -> AgentAction:
        payload = {
            "user_request": user_text,
            "capability_catalog": self.catalog,
            "observations": [item.prompt_summary() for item in observations],
            "iteration": len(observations) + 1,
        }
        prompt = json.dumps(payload, ensure_ascii=False, default=str)
        if self._planner_override:
            raw = self._planner_override(prompt)
        else:
            response = self.llm.invoke([("system", SYSTEM_PROMPT), ("human", prompt)])
            raw = _parse_json(str(response.content))
        return AgentAction(**{key: value for key, value in raw.items()
                              if key in AgentAction.__dataclass_fields__})

    def run(self, user_text: str) -> AgentRunResult:
        started = time.perf_counter()
        observations: List[AgentObservation] = []
        calls = 0
        for iteration in range(1, self.max_iterations + 1):
            elapsed = time.perf_counter() - started
            if elapsed > self.max_total_seconds:
                return self._result("failed", "agent_run", observations, iteration - 1, started,
                                    error="Agent 运行超过总时限")
            try:
                action = self._plan(user_text, observations)
            except Exception as exc:
                return self._result("failed", "agent_run", observations, iteration, started,
                                    error=f"规划模型返回无效动作: {exc}")

            if action.action == "ask_user":
                return self._result("input_required", "follow_up", observations, iteration, started,
                                    message=action.message or action.reason)
            if action.action == "finish":
                output = None
                if action.output_ref is not None:
                    if not (0 <= action.output_ref < len(observations)):
                        return self._result("failed", "agent_run", observations, iteration, started,
                                            error=f"结束动作引用了不存在的观察 {action.output_ref}")
                    output = observations[action.output_ref].result
                elif observations:
                    output = observations[-1].result
                task_type = self._task_type(observations)
                return self._result("completed", task_type, observations, iteration, started,
                                    output=output, message=action.message)

            error = validate_action(action, self.allowed, user_text)
            if error:
                observations.append(AgentObservation(len(observations), action.agent or "", action.skill or "",
                                                     "blocked", error=error))
                continue
            if calls >= self.max_agent_calls:
                return self._result("failed", "agent_run", observations, iteration, started,
                                    error="超过 Agent 调用次数限制")
            calls += 1
            step_started = time.perf_counter()
            try:
                arguments = _resolve_refs(action.arguments, observations)
                text = self.delegate_fn(action.agent, action.skill, arguments, "coordinator")
                envelope = json.loads(text)
                if not envelope.get("ok"):
                    raise RuntimeError(envelope.get("error") or "子 Agent 执行失败")
                observation = AgentObservation(len(observations), action.agent or "", action.skill or "",
                                               "success", result=envelope.get("result"),
                                               duration_ms=int((time.perf_counter() - step_started) * 1000))
            except Exception as exc:
                observation = AgentObservation(len(observations), action.agent or "", action.skill or "",
                                               "error", error=str(exc),
                                               duration_ms=int((time.perf_counter() - step_started) * 1000))
            observations.append(observation)
            logger.info("[agent-loop] step=%s %s.%s status=%s", observation.step_id,
                        observation.agent, observation.skill, observation.status)
        return self._result("failed", "agent_run", observations, self.max_iterations, started,
                            error="超过 Agent 最大迭代次数")

    @staticmethod
    def _task_type(observations: List[AgentObservation]) -> str:
        skills = set()
        for item in observations:
            if item.status != "success":
                continue
            if isinstance(item.result, dict) and item.result.get("__agent_tool__"):
                skills.add(item.result["__agent_tool__"])
            else:
                skills.add(item.skill)
        if "check_monitors" in skills or "monitor_status" in skills or "register_monitor" in skills or "stop_monitor" in skills:
            return "account_monitor"
        if "fetch_account_posts" in skills:
            return "account_follow"
        if skills & {"cluster_events", "score_heat", "verify_events"}:
            return "hotspot_query"
        if "collect_news" in skills:
            return "latest_news"
        return "agent_run"

    @staticmethod
    def _result(status: str, task_type: str, observations: List[AgentObservation], iterations: int,
                started: float, output: Any = None, message: str = "", error: Optional[str] = None) -> AgentRunResult:
        return AgentRunResult(status=status, task_type=task_type, output=jsonable(output), message=message,
                              error=error, trace=observations, iterations=iterations,
                              elapsed_ms=int((time.perf_counter() - started) * 1000))
