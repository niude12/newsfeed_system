"""自主 Agent 循环的离线契约测试（不访问 LLM、网络或数据库）。"""

import json
import unittest
from unittest.mock import patch

from agents.runtime.coordinator_loop import CoordinatorLoop
from agents.runtime.specialist_loop import execute_specialist


class _Config:
    agent_runtime = {
        "max_iterations": 6,
        "max_agent_calls": 4,
        "max_total_seconds": 30,
    }
    llm = {"model_name": "unused", "base_url": "http://unused", "api_key": "unused"}


class AgentRuntimeTests(unittest.TestCase):
    def test_observation_drives_next_step_and_finish(self):
        actions = iter([
            {"action": "delegate", "agent": "collector", "skill": "execute",
             "arguments": {"objective": "采集科技新闻", "context": {"module": "科技", "limit": 2}}},
            {"action": "delegate", "agent": "processor", "skill": "execute",
             "arguments": {"objective": "聚类新闻", "context": {"news_items": {"$observation": 0}}}},
            {"action": "finish", "output_ref": 1, "message": "完成"},
        ])
        delegated = []

        def planner(_prompt):
            return next(actions)

        def fake_delegate(agent, skill, params, _from_agent):
            delegated.append((agent, skill, params))
            if agent == "collector":
                result = {"__agent_tool__": "collect_news", "data": [{"title": "A"}]}
            else:
                self.assertEqual([{"title": "A"}], params["context"]["news_items"])
                result = {"__agent_tool__": "cluster_events", "data": [{"title": "事件A"}]}
            return json.dumps({"ok": True, "result": result}, ensure_ascii=False)

        run = CoordinatorLoop(config=_Config(), delegate_fn=fake_delegate, planner=planner).run("科技热点")
        self.assertEqual("completed", run.status)
        self.assertEqual("hotspot_query", run.task_type)
        self.assertEqual(2, len(delegated))
        self.assertEqual("事件A", run.output["data"][0]["title"])

    def test_invalid_skill_is_blocked_without_delegate(self):
        actions = iter([
            {"action": "delegate", "agent": "collector", "skill": "delete_everything", "arguments": {}},
            {"action": "finish", "message": "已阻止"},
        ])
        called = []
        run = CoordinatorLoop(config=_Config(), delegate_fn=lambda *args: called.append(args),
                              planner=lambda _prompt: next(actions)).run("删除全部")
        self.assertEqual("completed", run.status)
        self.assertFalse(called)
        self.assertEqual("blocked", run.trace[0].status)

    def test_failed_call_can_be_replanned(self):
        actions = iter([
            {"action": "delegate", "agent": "collector", "skill": "execute",
             "arguments": {"objective": "采集", "context": {}}},
            {"action": "delegate", "agent": "collector", "skill": "execute",
             "arguments": {"objective": "换参数重试", "context": {"module": "科技"}}},
            {"action": "finish", "output_ref": 1},
        ])
        count = 0

        def fake_delegate(*_args):
            nonlocal count
            count += 1
            if count == 1:
                return json.dumps({"ok": False, "error": "source unavailable"})
            return json.dumps({"ok": True, "result": {"__agent_tool__": "collect_news", "data": []}})

        run = CoordinatorLoop(config=_Config(), delegate_fn=fake_delegate,
                              planner=lambda _prompt: next(actions)).run("查询新闻")
        self.assertEqual(["error", "success"], [item.status for item in run.trace])
        self.assertEqual("completed", run.status)

    def test_specialist_llm_selects_registered_tool(self):
        class _Response:
            content = '{"tool":"collect_news","arguments":{"module":"科技","limit":2},"reason":"匹配采集任务"}'

        class _LLM:
            def __init__(self, **_kwargs):
                pass

            def invoke(self, _messages):
                return _Response()

        with patch("agents.runtime.specialist_loop.ChatOpenAI", _LLM):
            result = execute_specialist(
                "采集科技新闻", {},
                {"collect_news": lambda module, limit: [module, limit]},
                {"collect_news": "采集新闻"},
            )
        self.assertEqual("collect_news", result["__agent_tool__"])
        self.assertEqual(["科技", 2], result["data"])


if __name__ == "__main__":
    unittest.main()
