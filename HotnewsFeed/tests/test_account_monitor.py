# -*- coding: utf-8 -*-
"""账户监控（Account Monitor）相关功能的单元测试。

覆盖两类依赖：
1. B 站空间采集（tools/bilibili.py）—— UID 解析、yt-dlp 条目到 AccountPost 的转换、
   中文字幕优先选择；
2. 子 Agent 的 A2A 消息处理（agents/account_monitor_agent.py）——
   验证「标准 A2A 委派消息 → handle_message → 回传 {ok, result, error} JSON」这条链路。

测试方法（全部不触网）：
- 通过 patch("yt_dlp.YoutubeDL", _FakeYDL) 用本地假对象替换 yt-dlp，避免真实请求 B 站；
- 通过直接构造 Agent 实例并 stub 其 extract / service.status 方法，避免真实外部依赖。

对外暴露：
- ``AccountMonitorTests`` : unittest 测试用例类，可用 ``python -m tests.test_account_monitor``
  或 ``unittest.main()`` 直接运行。
"""

import json
import threading
import unittest
from unittest.mock import patch

# 待测目标：B 站采集工具与 A2A 消息构造 / Agent 处理。
from a2a.protocol import send_task
from agents.account_monitor_agent import AccountMonitorAgent
from tools.bilibili import extract_space_uid, fetch_bilibili_space_videos
from tools.video import _pick_subtitle


class _FakeYDL:
    """yt_dlp.YoutubeDL 的测试替身（test double）。

    只实现被测代码用到的上下文管理器接口（__enter__ / __exit__）与 extract_info，
    返回一份固定的 B 站空间「视频条目」结构，让 fetch_bilibili_space_videos
    不需要真正联网就能验证字段映射逻辑。

    说明:
        真实 yt_dlp.YoutubeDL 是上下文管理器，with 语句里调用 extract_info(url,
        download=False) 返回含 "entries" 键的播放列表 dict；这里用相同接口模拟。
    """

    def __init__(self, options):
        """保存构造参数（options 由 _ydl_options 生成，这里不校验）。

        参数:
            options: 传给 yt-dlp 的选项字典（本替身不使用，仅保持接口兼容）。
        """
        self.options = options  # 保留 options 供断言/排障查看，本替身逻辑上不消费它。

    def __enter__(self):
        """进入 with 语句时返回 self（供 with ... as ydl 使用）。"""
        return self  # with ... as ydl 拿到的就是本实例。

    def __exit__(self, *_):
        """退出 with 语句时返回 False（不吞异常）。"""
        return False  # 返回 False：让 with 块内的异常正常向上抛，不在这里拦截。

    def extract_info(self, url, download=False):
        """返回一份固定单条视频的播放列表结构。

        参数:
            url:      视频 / 空间地址（本替身不使用）。
            download: 是否下载（本替身不使用）。

        返回:
            dict：含 "entries" 键，值是单元素列表，元素字段与真实 yt-dlp 元数据对齐
            （id / title / timestamp / webpage_url / description）。
        """
        # 返回与真实 yt-dlp 同构的播放列表结构：外层 entries 列表，元素为单条视频元数据。
        return {"entries": [{
            "id": "BV1TEST12345", "title": "新视频", "timestamp": 1787820000,
            "webpage_url": "https://www.bilibili.com/video/BV1TEST12345",
            "description": "视频简介",
        }]}


class AccountMonitorTests(unittest.IsolatedAsyncioTestCase):
    """账户监控相关功能的单元测试用例。

    基类是 ``unittest.IsolatedAsyncioTestCase``——unittest 对 async 测试方法的扩展，
    每个 async 测试方法都会被放进独立事件循环执行。

    说明:
        - 测试方法命名以 test_ 开头，unittest 自动发现并逐个运行。
        - B 站相关用例用 _FakeYDL 替身避免触网；Agent 相关用例 stub 掉内部依赖。
    """

    def test_extract_uid(self):
        """验证 extract_space_uid 能从 B 站空间主页 URL 中提取纯数字 UID。

        该用例验证：输入 "https://space.bilibili.com/312249633/video" 时，
        extract_space_uid 用正则捕获 URL 中的数字段，返回 "312249633"。
        """
        # 若返回的不是纯数字 UID，则说明正则提取逻辑被破坏。
        self.assertEqual(extract_space_uid("https://space.bilibili.com/312249633/video"), "312249633")

    async def test_space_entries_to_posts(self):
        """验证 fetch_bilibili_space_videos 能把 yt-dlp 条目转换成 AccountPost 列表。

        该用例验证（用 _FakeYDL 替换真实 yt-dlp，不触网）：
        - 采集函数能解析空间 URL 并展开 entries；
        - 返回的 AccountPost 数量、post_id（BV 号）与 platform（bilibili）映射正确。
        """
        # patch 上下文里，yt_dlp.YoutubeDL 被替换为 _FakeYDL，extract_info 返回固定条目。
        with patch("yt_dlp.YoutubeDL", _FakeYDL):
            posts = await fetch_bilibili_space_videos(
                "bilibili_312249633", "https://space.bilibili.com/312249633/video", limit=3,
            )
        # 固定条目只有 1 条，因此 posts 应恰好 1 条。
        self.assertEqual(len(posts), 1)
        # BV 号应透传到 AccountPost.post_id。
        self.assertEqual(posts[0].post_id, "BV1TEST12345")
        # 平台应固定为 "bilibili"。
        self.assertEqual(posts[0].platform, "bilibili")

    def test_prefer_chinese_subtitle(self):
        """验证 _pick_subtitle 在英文与中文字幕并存时优先选择中文字幕。

        该用例验证：给定 subtitles 里同时有 "en" 与 "zh-CN" 两个音轨时，
        _pick_subtitle 会按 (zh-CN, zh-Hans, zh, ...) 的优先级取回中文字轨（"zh"）。
        """
        # 构造同时含 en 与 zh-CN 两个音轨的字幕表，验证选择器会优先命中 zh-CN。
        track = _pick_subtitle({
            "subtitles": {"en": [{"url": "en"}], "zh-CN": [{"url": "zh"}]}
        })
        # 若选成了 "en" 说明中文字幕优先逻辑失效。
        self.assertEqual(track["url"], "zh")

    def test_account_monitor_agent_status_a2a_result(self):
        """Coordinator 的监控任务能经标准 A2A 消息落到下游监控 Agent。

        该用例验证：AccountMonitorAgent.handle_message 处理 monitor_status 任务时，
        - 能正确分发到 service.status() 并把结果封装成 {ok, result, error} JSON；
        - result 数组中的账户字段与 stub 的返回值一致。
        """
        agent = AccountMonitorAgent()
        # stub 掉 service.status：真实实现会读取监控状态，这里直接返回固定数据。
        agent.service.status = lambda: [{"account": "bilibili_1", "post_count": 2}]
        # 经 handle_message 处理 monitor_status 委派消息（send_task 构造）。
        reply = agent.handle_message(send_task("monitor_status", {}, "coordinator"))
        payload = json.loads(reply.content.text)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"][0]["account"], "bilibili_1")

    def test_account_monitor_check_all_starts_once_in_background(self):
        """验证立即检查全部会立刻返回，并阻止运行期间重复启动后台任务。

        该用例用两个线程事件控制假的慢检查：第一次 A2A 请求应返回 started=True，
        慢检查结束前的第二次请求应返回 started=False，且实际 check_all 只执行一次。
        """
        agent = AccountMonitorAgent()
        started = threading.Event()  # 后台假检查已经进入的信号。
        release = threading.Event()  # 允许后台假检查结束的信号。
        calls = []  # 记录 check_all 实际执行次数，验证单实例保护。

        async def fake_check_all():
            # 记录一次实际执行，并通知测试主线程后台任务已经启动。
            calls.append("check_all")
            started.set()
            # 模拟耗时采集；由测试主线程释放，最长等待 2 秒防止测试永久挂起。
            release.wait(timeout=2)
            return {"bilibili_1": {"fetched": 1, "new": 0}}

        agent.service.check_all = fake_check_all
        try:
            # 第一次请求只负责启动后台任务，应立即得到成功受理结果。
            first_reply = agent.handle_message(send_task("check_monitors", {}, "coordinator"))
            first = json.loads(first_reply.content.text)
            self.assertTrue(first["ok"])
            self.assertTrue(first["result"]["accepted"])
            self.assertTrue(first["result"]["started"])
            self.assertTrue(started.wait(timeout=1))

            # 后台任务尚未结束时再次请求，不应创建第二条检查线程。
            second_reply = agent.handle_message(send_task("check_monitors", {}, "coordinator"))
            second = json.loads(second_reply.content.text)
            self.assertTrue(second["ok"])
            self.assertFalse(second["result"]["started"])
            self.assertEqual(calls, ["check_all"])
        finally:
            # 无论断言是否通过都释放并回收后台线程，避免影响其余测试。
            release.set()
            if agent._check_all_thread is not None:
                agent._check_all_thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
