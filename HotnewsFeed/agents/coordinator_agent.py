"""HotnewsFeed 自主协调 Agent。

本模块只负责 Agent Loop 与 A2A dispatch，不包含意图枚举、槽位正则或固定业务流水线。
明确的按钮/斜杠命令位于 ``operations.py``。
"""

from dataclasses import asdict

from python_a2a import A2AServer, AgentCard, AgentSkill, run_server

from a2a.protocol import encode_result, parse_task, reply_text
from agents.runtime import CoordinatorLoop
from config import Config
from create_logger import logger
from task_pipelines.schemas import AccountPost, HotEvent, NewsItem, PipelineResult, dto_from_dict


agent_card = AgentCard(
    name="coordinator",
    description="基于能力目录逐步规划、委派、观察并重新规划的协调 Agent",
    url="http://localhost:8000",
    version="2.0.0",
    capabilities={"streaming": False, "memory": True},
    skills=[AgentSkill(
        id="dispatch", name="dispatch",
        description="处理自然语言请求并自主选择专业 Agent",
        tags=["orchestration", "agent-loop", "a2a"],
        examples=["查询科技热点", "持续监控某个 B 站账户"],
        input_modes=["application/json"], output_modes=["application/json"],
    )],
)


class CoordinatorAgent(A2AServer):
    name = "coordinator"
    role = "Plan · Execute · Observe · Replan"

    def __init__(self):
        super().__init__(agent_card=agent_card)
        self.config = Config()
        self.loop = CoordinatorLoop(config=self.config)

    def route(self, query: str) -> PipelineResult:
        """执行唯一的自主调度路径，并适配现有 UI 使用的 PipelineResult。"""
        run = self.loop.run(query)
        trace = [asdict(item) for item in run.trace] if self.config.agent_runtime["show_trace"] else None
        if run.status == "input_required":
            return PipelineResult("follow_up", [], elapsed_ms=run.elapsed_ms,
                                  error=run.message or "需要补充信息", trace=trace)
        if run.status == "failed":
            return PipelineResult("agent_run", [], elapsed_ms=run.elapsed_ms,
                                  error=run.error or "Agent 执行失败", trace=trace)

        output = run.output
        if isinstance(output, dict) and "__agent_tool__" in output:
            output = output.get("data")
        raw_items = output if isinstance(output, list) else ([] if output is None else [output])
        item_type = {
            "hotspot_query": HotEvent,
            "latest_news": NewsItem,
            "account_follow": AccountPost,
        }.get(run.task_type)
        items = [dto_from_dict(item_type, item) for item in raw_items] if item_type else raw_items
        return PipelineResult(run.task_type, items, elapsed_ms=run.elapsed_ms, trace=trace)

    def handle_message(self, message):
        task_type, params, _from_agent = parse_task(message)
        try:
            if task_type != "dispatch":
                raise ValueError(f"未知任务类型: {task_type}")
            result = self.route(str(params.get("query") or ""))
            text = encode_result(not bool(result.error), result, result.error)
        except Exception as exc:
            logger.exception("[coordinator] dispatch 失败: %s", exc)
            text = encode_result(False, None, str(exc))
        return reply_text(text, message.message_id, message.conversation_id)


def create_coordinator_agent():
    logger.info("=== Coordinator Agent：%s ===", CoordinatorAgent.role)
    return CoordinatorAgent()


if __name__ == "__main__":
    run_server(create_coordinator_agent(), host="127.0.0.1", port=8000)
