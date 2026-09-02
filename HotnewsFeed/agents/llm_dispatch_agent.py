"""自主调度器的兼容 A2A 服务入口。

旧版本在这里一次性生成完整 steps，执行期间不能根据工具结果调整。现在唯一调度内核是
``agents.runtime.CoordinatorLoop``；本文件仅保留 :8010 A2A 服务兼容性。
"""

from dataclasses import asdict

from python_a2a import A2AServer, AgentCard, AgentSkill, run_server

from a2a.protocol import encode_result, parse_task, reply_text
from agents.runtime import CoordinatorLoop
from create_logger import logger


agent_card = AgentCard(
    name="llm_dispatcher",
    description="Plan→Execute→Observe→Replan 自主多 Agent 调度器",
    url="http://localhost:8010",
    version="2.0.0",
    capabilities={"streaming": False, "memory": True},
    skills=[AgentSkill(
        id="dispatch", name="dispatch",
        description="根据 AgentCard 能力目录逐步规划、委派、观察并重新规划",
        tags=["orchestration", "agent-loop", "a2a"],
        examples=["查询科技热点", "查看某 B 站账户的监控状态"],
        input_modes=["application/json"], output_modes=["application/json"],
    )],
)


class LLMDispatchAgent(A2AServer):
    name = "llm_dispatcher"

    def __init__(self):
        super().__init__(agent_card=agent_card)
        self.loop = CoordinatorLoop()

    def route(self, query: str, **_ignored):
        return asdict(self.loop.run(query))

    def handle_message(self, message):
        task_type, params, _from_agent = parse_task(message)
        try:
            if task_type != "dispatch":
                raise ValueError(f"未知任务类型: {task_type}")
            result = self.route(str(params.get("query") or ""))
            text = encode_result(True, result, None)
        except Exception as exc:
            logger.exception("[llm-dispatch] 执行失败: %s", exc)
            text = encode_result(False, None, str(exc))
        return reply_text(text, message.message_id, message.conversation_id)


def create_llm_dispatch_agent():
    return LLMDispatchAgent()


if __name__ == "__main__":
    run_server(create_llm_dispatch_agent(), host="127.0.0.1", port=8010)
