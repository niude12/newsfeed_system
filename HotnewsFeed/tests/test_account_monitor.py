# -*- coding: utf-8 -*-

import json
import unittest
from unittest.mock import patch

from a2a.protocol import send_task
from agents.account_monitor_agent import AccountMonitorAgent
from agents.video_agent import VideoAgent
from tools.bilibili import extract_space_uid, fetch_bilibili_space_videos
from tools.video import VideoContent, _pick_subtitle


class _FakeYDL:
    def __init__(self, options):
        self.options = options

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def extract_info(self, url, download=False):
        return {"entries": [{
            "id": "BV1TEST12345", "title": "新视频", "timestamp": 1787820000,
            "webpage_url": "https://www.bilibili.com/video/BV1TEST12345",
            "description": "视频简介",
        }]}


class AccountMonitorTests(unittest.IsolatedAsyncioTestCase):
    def test_extract_uid(self):
        self.assertEqual(extract_space_uid("https://space.bilibili.com/312249633/video"), "312249633")

    async def test_space_entries_to_posts(self):
        with patch("yt_dlp.YoutubeDL", _FakeYDL):
            posts = await fetch_bilibili_space_videos(
                "bilibili_312249633", "https://space.bilibili.com/312249633/video", limit=3,
            )
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0].post_id, "BV1TEST12345")
        self.assertEqual(posts[0].platform, "bilibili")

    def test_prefer_chinese_subtitle(self):
        track = _pick_subtitle({
            "subtitles": {"en": [{"url": "en"}], "zh-CN": [{"url": "zh"}]}
        })
        self.assertEqual(track["url"], "zh")

    def test_video_agent_a2a_result(self):
        agent = VideoAgent()
        agent.extract = lambda *_args, **_kwargs: VideoContent(
            video_id="BV1", title="title", author="up", published_at="",
            transcript="text", summary="summary", keywords=["key"],
            source_url="https://example.com", content_source="subtitle",
        )
        reply = agent.handle_message(send_task(
            "extract_video_content", {"video_url": "https://example.com"}, "test",
        ))
        payload = json.loads(reply.content.text)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"]["content_source"], "subtitle")

    def test_account_monitor_agent_status_a2a_result(self):
        """Coordinator 的监控任务能经标准 A2A 消息落到下游监控 Agent。"""
        agent = AccountMonitorAgent()
        agent.service.status = lambda: [{"account": "bilibili_1", "post_count": 2}]
        reply = agent.handle_message(send_task("monitor_status", {}, "coordinator"))
        payload = json.loads(reply.content.text)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"][0]["account"], "bilibili_1")


if __name__ == "__main__":
    unittest.main()
