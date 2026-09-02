"""确定性操作层测试。"""

import json
import unittest
from unittest.mock import patch

from operations import run_monitor


class OperationsTests(unittest.TestCase):
    def test_bilibili_registration_derives_account(self):
        calls = []

        def fake_delegate(agent, task, params, from_agent):
            calls.append((agent, task, params, from_agent))
            return json.dumps({"ok": True, "result": {"registered": True}})

        with patch("operations.delegate", fake_delegate):
            result = run_monitor("add", url="https://space.bilibili.com/312249633/video")
        self.assertFalse(result.error)
        self.assertEqual("register_monitor", calls[0][1])
        self.assertEqual("bilibili_312249633", calls[0][2]["account"])

    def test_monitor_run_maps_to_background_check(self):
        calls = []

        def fake_delegate(_agent, task, _params, _from_agent):
            calls.append(task)
            return json.dumps({"ok": True, "result": {"accepted": True}})

        with patch("operations.delegate", fake_delegate):
            result = run_monitor("run")
        self.assertEqual(["check_monitors"], calls)
        self.assertTrue(result.items[0]["accepted"])


if __name__ == "__main__":
    unittest.main()
